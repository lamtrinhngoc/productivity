import os
import csv
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from datetime import datetime

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# =========================
# CONFIG
# =========================
# Đổi sang DEBUG để xem timing chi tiết, INFO để chạy production
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG")
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format="%(asctime)s - %(levelname)s - %(message)s")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

LINK_SPREADSHEET_URL  = "https://docs.google.com/spreadsheets/d/10eMZVnmtyyr5JAzDvpE5Brgh-8fw3lEKmGvL5m6eCUY"
MASTER_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1VlXicEr1FGrpdDcRpuv1aE2TAG-7QHEfWKNtFJF4nc8"

REQUIRED_COLS = ["Link", "Sheet 1", "Sheet 2", "Sheet 3", "Sheet 4", "Sheet 5"]

SCHEMA = [
    "date_update", "date_cdd_applied", "fullname", "source", "dob", "phone", "area",
    "address", "registration_area", "previous_work", "id_code", "note", "email", "rehire",
    "current_salary", "expected_ob_date", "position", "station_name", "storage",
    "reason_for_storage", "notes_for_recruitment", "recruiter_call", "recruiter_call_date",
    "recruiter_call_feedback", "recruiter_call_result", "hm_interview_date", "hm_interview",
    "hm_interview_feedback", "hm_interview_result", "offering", "offering_date", "accept",
    "accept_date", "onboard_date", "onboard", "reason_reject_ob", "finish_process",
    "fullname_ob", "phone_ob", "id_code_ob", "pic", "ticket_id", "rider_id"
]

DATE_COLS = [
    "date_update", "date_cdd_applied", "recruiter_call_date",
    "hm_interview_date", "offering_date", "accept_date", "onboard_date"
]

# =========================
# RATE LIMIT CONFIG
# =========================
# KEY INSIGHT: dùng batchGet lấy nhiều sheets trong 1 API call
#   Trước: 1000 sheets × 1 call = 1000 calls → ~18 phút
#   Sau:    200 files × 1 call  =  200 calls → ~4 phút
#
# Quota vẫn là 60 read/min nên giữ SAFE_READ_RPM = 55
# MAX_WORKERS = 4 là đủ — bottleneck là quota, không phải CPU

MAX_WORKERS    = int(os.getenv("GSHEETS_WORKERS",  "4"))
SAFE_READ_RPM  = int(os.getenv("GSHEETS_READ_RPM", "55"))
SAFE_WRITE_RPM = int(os.getenv("GSHEETS_WRITE_RPM","55"))
WINDOW         = 60.0
MAX_RETRY_ROUNDS = 4
DATE_FILTER_FROM = pd.Timestamp("2025-01-01")

# =========================
# RATE LIMITER
# =========================
class RateLimiter:
    """
    Sliding-window + min interval để tránh burst.
    Khi gặp 429: caller tự sleep 65s rồi gọi acquire() lại.
    """
    def __init__(self, limit: int, window: float = 60.0):
        self._limit        = limit
        self._window       = window
        self._min_interval = window / limit
        self._timestamps: deque = deque()
        self._last_issued  = 0.0
        self._lock         = threading.Lock()

    @property
    def min_interval(self) -> float:
        return self._min_interval

    def acquire(self):
        t_start = time.monotonic()
        while True:
            with self._lock:
                now = time.monotonic()
                while self._timestamps and (now - self._timestamps[0]) > self._window:
                    self._timestamps.popleft()
                window_ok   = len(self._timestamps) < self._limit
                interval_ok = (now - self._last_issued) >= self._min_interval
                if window_ok and interval_ok:
                    self._timestamps.append(now)
                    self._last_issued = now
                    waited = time.monotonic() - t_start
                    if waited > 2.0:   # chỉ log khi chờ lâu bất thường
                        logging.debug(f"[rate_limiter] chờ {waited:.1f}s trước khi được phép gọi API")
                    return
                waits = []
                if not window_ok:
                    waits.append(self._window - (now - self._timestamps[0]) + 0.02)
                if not interval_ok:
                    waits.append(self._min_interval - (now - self._last_issued) + 0.02)
            time.sleep(max(max(waits) if waits else 0.05, 0.02))

    def __repr__(self):
        return f"RateLimiter({self._limit}/min, gap={self._min_interval:.2f}s)"


_read_limiter  = RateLimiter(SAFE_READ_RPM)
_write_limiter = RateLimiter(SAFE_WRITE_RPM)

# =========================
# AUTH
# =========================
def authenticate():
    """Trả về (gspread_client, sheets_v4_service) dùng chung credentials."""
    creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    gs_client     = gspread.authorize(creds)
    sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return gs_client, sheets_service

# =========================
# UTILS
# =========================
_DATE_FORMATS = (
    '%y/%m/%d', '%Y/%m/%d', '%m/%d/%Y', '%m/%d/%y',
    '%d-%b-%y', '%d-%b-%Y', '%Y-%m-%d',
)

def try_parsing_date(text):
    if pd.isna(text) or not str(text).strip():
        return pd.NaT
    for fmt in _DATE_FORMATS:
        try:
            return pd.to_datetime(text, format=fmt, errors="raise")
        except ValueError:
            continue
    return pd.to_datetime(text, errors="coerce")

def _spreadsheet_id_from_url(url: str) -> str:
    """Trích spreadsheet ID từ Google Sheets URL."""
    import re
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", url)
    if not m:
        raise ValueError(f"Không thể trích ID từ URL: {url}")
    return m.group(1)

# =========================
# CORE: batchGet — 1 API call lấy nhiều sheets cùng lúc
# =========================
def _batch_get_sheets(
    service,
    spreadsheet_id: str,
    sheet_names: list[str],
    data_range: str = "B8:AR",
) -> dict[str, list]:
    """
    Dùng spreadsheets.values.batchGet để lấy nhiều sheet ranges trong 1 HTTP call.

    Trước: N sheets = N API calls
    Sau:   N sheets = 1 API call  ← đây là chìa khoá tối ưu

    Returns: {sheet_name: [[row], [row], ...]}
    """
    ranges = [f"'{name}'!{data_range}" for name in sheet_names]
    _read_limiter.acquire()
    t_net = time.monotonic()
    result = (
        service.spreadsheets()
               .values()
               .batchGet(
                   spreadsheetId=spreadsheet_id,
                   ranges=ranges,
                   valueRenderOption="FORMATTED_VALUE",
                   dateTimeRenderOption="FORMATTED_STRING",
               )
               .execute()
    )
    logging.debug(f"[network] batchGet {len(sheet_names)} sheets = {time.monotonic()-t_net:.2f}s | {spreadsheet_id}")
    out = {}
    for vr in result.get("valueRanges", []):
        # range trả về dạng "'Sheet Name'!B8:AR" → trích tên sheet
        raw_range = vr.get("range", "")
        sheet_name = raw_range.split("!")[0].strip("'")
        out[sheet_name] = vr.get("values", [])
    return out


def _parse_sheet_data(rows: list[list], schema: list[str]) -> list[dict]:
    """Chuyển raw rows → list of dicts, filter theo date_update."""
    if not rows:
        return []
    df = pd.DataFrame(rows)
    df.columns = schema[: len(df.columns)]
    df = df.reindex(columns=schema).fillna("")
    df["date_update"] = df["date_update"].apply(try_parsing_date)
    df = df[df["date_update"] >= DATE_FILTER_FROM]
    return df.to_dict("records")


def _is_quota_error(exc: Exception) -> bool:
    if hasattr(exc, "resp"):                          # googleapiclient error
        return int(getattr(exc.resp, "status", 0)) == 429
    if isinstance(exc, gspread.exceptions.APIError):  # gspread error
        resp = getattr(exc, "response", None)
        return getattr(resp, "status_code", 0) == 429
    return False


# =========================
# FETCH TASK — 1 file = 1 task = 1 API call (batchGet)
# =========================
def _fetch_spreadsheet(
    service,
    url: str,
    sheet_names: list[str],
    schema: list[str],
) -> tuple[list[dict], bool, bool]:
    """
    Đọc tất cả sheet_names từ 1 spreadsheet bằng 1 batchGet call.

    Returns: (rows, success, retryable)
    """
    ss_id = _spreadsheet_id_from_url(url)
    t_file = time.monotonic()
    for attempt in range(1, 5):
        try:
            raw = _batch_get_sheets(service, ss_id, sheet_names)
            rows = []
            for name in sheet_names:
                rows.extend(_parse_sheet_data(raw.get(name, []), schema))
            logging.debug(f"[file_ok] {len(rows)} rows, {len(sheet_names)} sheets, total={time.monotonic()-t_file:.2f}s | {ss_id}")
            return rows, True, False

        except Exception as exc:
            if attempt == 4:
                return [], False, not isinstance(exc, (ValueError, KeyError))
            if _is_quota_error(exc):
                wait = 65 + (attempt - 1) * 15
                logging.warning(f"  429 quota — chờ {wait}s (attempt {attempt}/4) | {url}")
                time.sleep(wait)
            else:
                wait = 3 * (2 ** (attempt - 1))
                logging.warning(f"  Lỗi tạm thời, retry {attempt}/4 sau {wait}s | {url}")
                time.sleep(wait)

    return [], False, True


# =========================
# SHEET TRACKER
# =========================
class SheetTracker:
    SUCCESS = "✅ success"
    FAILED  = "❌ failed"
    SKIPPED = "⏭️  skipped"

    def __init__(self):
        self._status:      dict[str, str] = {}   # key = url
        self._retry_count: dict[str, int] = {}
        self._lock = threading.Lock()

    def mark(self, url: str, status: str):
        with self._lock:
            self._status[url] = status

    def increment_retry(self, url: str):
        with self._lock:
            self._retry_count[url] = self._retry_count.get(url, 0) + 1

    def retries(self, url: str) -> int:
        return self._retry_count.get(url, 0)

    def print_report(self, total: int):
        s = {self.SUCCESS: 0, self.FAILED: 0, self.SKIPPED: 0}
        for v in self._status.values():
            s[v] = s.get(v, 0) + 1
        logging.info(
            f"━━ KẾT QUẢ: {s[self.SUCCESS]}/{total} files OK | "
            f"{s[self.FAILED]} lỗi | {s[self.SKIPPED]} skipped ━━"
        )
        for url, st in self._status.items():
            if st != self.SUCCESS:
                n = self._retry_count.get(url, 0)
                retry_str = f" (retried {n}x)" if n else ""
                logging.warning(f"  {st}{retry_str} | {url}")

    def export_csv(self):
        filename = f"sheet_status_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(filename, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["url", "status", "retried"])
            for url, st in sorted(self._status.items(), key=lambda x: x[1]):
                n = self._retry_count.get(url, 0)
                w.writerow([url, st, f"{n}x" if n else "-"])
        logging.info(f"📋 Status report: {filename}")


# =========================
# PARALLEL FETCH
# =========================
def fetch_all_spreadsheets(
    service,
    file_tasks: list[tuple[str, list[str]]],   # [(url, [sheet_names]), ...]
    schema: list[str],
    max_workers: int     = MAX_WORKERS,
    max_rounds: int      = MAX_RETRY_ROUNDS,
) -> tuple[pd.DataFrame, SheetTracker]:
    """
    file_tasks: mỗi phần tử = (url, [sheet1, sheet2, ...])
    1 task = 1 file = 1 batchGet API call (thay vì N calls như trước)
    """
    tracker  = SheetTracker()
    all_rows: list[dict] = []
    pending  = list(file_tasks)

    for round_no in range(1, max_rounds + 1):
        if not pending:
            break

        logging.info(f"━━ Round {round_no}/{max_rounds} — {len(pending)} files ━━")
        t_round = time.monotonic()
        failed_next = []

        iterator = tqdm(pending, desc=f"Round {round_no}", unit="file") if HAS_TQDM else pending

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_fetch_spreadsheet, service, url, names, schema): url
                for url, names in iterator
            }
            for future in as_completed(futures):
                url = futures[future]
                rows, success, retryable = future.result()

                if success:
                    all_rows.extend(rows)
                    tracker.mark(url, SheetTracker.SUCCESS)
                elif not retryable:
                    tracker.mark(url, SheetTracker.SKIPPED)
                else:
                    tracker.increment_retry(url)
                    # tìm lại names từ pending để retry
                    names = next(n for u, n in pending if u == url)
                    failed_next.append((url, names))

        round_elapsed = time.monotonic() - t_round
        ok_this_round = len([u for u, _ in (file_tasks if round_no == 1 else []) ]) - len(failed_next)
        logging.info(
            f"  Round {round_no} xong: {round_elapsed:.1f}s | "
            f"{len(failed_next)} files lỗi còn lại"
        )
        pending = failed_next
        if pending:
            backoff = min(10 * round_no, 90)
            logging.warning(f"  ↻ {len(pending)} files lỗi — chờ {backoff}s rồi retry…")
            time.sleep(backoff)

    for url, _ in pending:
        tracker.mark(url, SheetTracker.FAILED)

    tracker.print_report(len(file_tasks))
    tracker.export_csv()

    df = (
        pd.DataFrame.from_records(all_rows, columns=schema)
        if all_rows else pd.DataFrame(columns=schema)
    )
    return df, tracker


# =========================
# CLEAN / NORMALISE
# =========================
def normalize_dates(df: pd.DataFrame, date_cols: list[str]) -> pd.DataFrame:
    for col in date_cols:
        df[col] = df[col].apply(try_parsing_date).dt.strftime('%Y-%m-%d').fillna("")

    action_cols = ["recruiter_call", "hm_interview", "offering", "accept", "onboard"]
    for col in action_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["ticket_id"] = pd.to_numeric(df.get("ticket_id", 0), errors="coerce").fillna(0)
    mask = df["ticket_id"] < 20
    df.loc[mask, "ticket_id"] = df.loc[mask, action_cols].sum(axis=1)

    if "phone" in df.columns:
        df["phone"] = (
            df["phone"].astype(str)
                       .str.replace(r"\D", "", regex=True)
                       .str[-9:]
                       .replace({"nan": "", "NaN": "", "None": ""})
                       .fillna("")
        )

    required_keys = {"phone", "pic", "position", "ticket_id"}
    if required_keys.issubset(df.columns):
        df["id_code"] = df.get("id_code", "").fillna("").astype(str).str.strip()
        df["_has_id"] = (df["id_code"].str.len() > 0).astype(int)
        df = (
            df.sort_values(["_has_id", "ticket_id"], ascending=[False, False])
              .drop_duplicates(subset=["phone", "pic", "position"], keep="first")
              .drop(columns=["_has_id"])
              .reset_index(drop=True)
        )
    return df


# =========================
# WRITE TO MASTER
# =========================
def write_master(gs_client: gspread.Client, df: pd.DataFrame):
    master_ss = gs_client.open_by_url(MASTER_SPREADSHEET_URL)
    ws = master_ss.worksheet("Productivity")
    values = [df.columns.tolist()] + df.astype(str).values.tolist()

    _write_limiter.acquire()
    ws.clear()
    _write_limiter.acquire()
    ws.batch_update(
        [
            {"range": "A1",      "values": values},
            {"range": "AR1:AS1", "values": [["channel_by_prod", "team"]]},
            {"range": "AR2",     "values": [["=ARRAYFORMULA(IFNA(XLOOKUP(D2:D,Source!$A:$A,Source!$C:$C)))"]]},
            {"range": "AS2",     "values": [["=ARRAYFORMULA(IFNA(XLOOKUP(AO2:AO,Info!$C:$C,Info!$N:$N)))"]]},
        ],
        value_input_option="USER_ENTERED",
    )
    logging.info(f"✅ Written {len(df)} rows to Master")


# =========================
# MAIN
# =========================
def main():
    t0 = time.time()
    gs_client, sheets_service = authenticate()

    logging.info(
        f"Config — read: {_read_limiter} | write: {_write_limiter} | workers: {MAX_WORKERS}"
    )

    # Đọc danh sách link
    link_ss  = gs_client.open_by_url(LINK_SPREADSHEET_URL)
    ws_links = link_ss.worksheet("Productivity File")
    df_links = pd.DataFrame(ws_links.get_all_records())

    missing = [c for c in REQUIRED_COLS if c not in df_links.columns]
    if missing:
        raise ValueError(f"Thiếu cột: {missing}")

    # Gom sheet names theo từng URL → 1 file = 1 task
    from collections import defaultdict
    file_map: dict[str, list[str]] = defaultdict(list)
    for _, row in df_links.iterrows():
        url = str(row.get("Link", "")).strip()
        if not url:
            continue
        for col in REQUIRED_COLS[1:]:
            name = str(row.get(col, "")).strip()
            if name:
                file_map[url].append(name)

    file_tasks = [(url, names) for url, names in file_map.items()]
    total_sheets = sum(len(n) for _, n in file_tasks)
    logging.info(
        f"Tổng: {len(file_tasks)} files | {total_sheets} sheets | "
        f"API calls: {len(file_tasks)} (batchGet) thay vì {total_sheets} (trước đây)"
    )

    # Fetch
    all_data, tracker = fetch_all_spreadsheets(
        sheets_service, file_tasks, SCHEMA, max_workers=MAX_WORKERS
    )
    logging.info(f"Fetched {len(all_data)} raw rows")

    # Clean
    all_data = normalize_dates(all_data, DATE_COLS)
    all_data.replace([float("inf"), float("-inf")], "", inplace=True)
    all_data.fillna("", inplace=True)

    # Write
    write_master(gs_client, all_data)

    elapsed = time.time() - t0
    logging.info(f"✅ DONE trong {elapsed:.1f}s — {len(all_data)} rows")


if __name__ == "__main__":
    main()
