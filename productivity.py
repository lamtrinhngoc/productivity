import os
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from requests.exceptions import JSONDecodeError
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# =========================
# CONFIG
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

LINK_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/10eMZVnmtyyr5JAzDvpE5Brgh-8fw3lEKmGvL5m6eCUY"
MASTER_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/17rB2UiQ_tUdl4eX4nbOllq2_XDe3bBFA4RIoiv7v3lc/edit?gid=0#gid=0"

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
# CONFIG — rate limits
# =========================
# Google Sheets API quota: 60 read req/min & 60 write req/min per user.
#
# Each sheet task costs ~3 read calls (open_by_url + worksheet + ws.get).
# Spreadsheet objects are cached so repeated URLs only pay the ws.get cost.
#
# Constraint maths:
#   SAFE_READ_RPM = 55  →  min_interval = 60/55 ≈ 1.09s between any two reads
#   MAX_WORKERS   =  4  →  at most 4 threads compete; global limiter keeps total ≤ 55/min
#
# Do NOT raise MAX_WORKERS beyond 6 — it cannot speed things up because the
# bottleneck is the API quota, not CPU or network parallelism.

MAX_WORKERS      = int(os.getenv("GSHEETS_WORKERS",   "4"))
SAFE_READ_RPM    = int(os.getenv("GSHEETS_READ_RPM",  "55"))  # 5 under quota for safety margin
SAFE_WRITE_RPM   = int(os.getenv("GSHEETS_WRITE_RPM", "55"))
WINDOW           = 60.0
MAX_RETRY_ROUNDS = 5
DATE_FILTER_FROM = pd.Timestamp("2025-01-01")

# =========================
# RATE LIMITER
# Sliding-window counter + mandatory minimum inter-request interval.
# Both constraints must pass before a request proceeds.
# This prevents burst exhaustion (all 55 tokens consumed in first second).
# =========================
class RateLimiter:
    """
    Enforces:
      1. At most `limit` requests in any rolling `window`-second period.
      2. At least (window / limit) seconds between consecutive requests —
         spreads traffic evenly so no burst can exhaust the quota.
    """
    def __init__(self, limit: int, window: float = 60.0):
        self._limit        = limit
        self._window       = window
        self._min_interval = window / limit          # 60/55 ≈ 1.09 s
        self._timestamps: deque = deque()
        self._last_issued  = 0.0
        self._lock         = threading.Lock()

    @property
    def min_interval(self) -> float:
        return self._min_interval

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()

                # evict timestamps outside the rolling window
                while self._timestamps and (now - self._timestamps[0]) > self._window:
                    self._timestamps.popleft()

                window_ok   = len(self._timestamps) < self._limit
                interval_ok = (now - self._last_issued) >= self._min_interval

                if window_ok and interval_ok:
                    self._timestamps.append(now)
                    self._last_issued = now
                    return

                # calculate minimum required sleep
                waits = []
                if not window_ok:
                    waits.append(self._window - (now - self._timestamps[0]) + 0.02)
                if not interval_ok:
                    waits.append(self._min_interval - (now - self._last_issued) + 0.02)
                sleep_for = max(waits) if waits else 0.05

            time.sleep(max(sleep_for, 0.02))

    def __repr__(self):
        return f"RateLimiter({self._limit}/min, min_gap={self._min_interval:.2f}s)"


_read_limiter  = RateLimiter(SAFE_READ_RPM)
_write_limiter = RateLimiter(SAFE_WRITE_RPM)

logging.info(
    f"Rate limiters ready — read: {_read_limiter} | write: {_write_limiter} | workers: {MAX_WORKERS} | "
    f"min gap between reads: {_read_limiter.min_interval:.2f}s"
)

# =========================
# AUTH + CLIENT CACHE
# =========================
def authenticate_gspread() -> gspread.Client:
    creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    return gspread.authorize(creds)


class CachedGSpreadClient:
    """
    Wraps a gspread.Client with thread-safe spreadsheet & worksheet caching.
    Avoids redundant open_by_url / worksheet calls across threads.
    """
    def __init__(self, client: gspread.Client):
        self._client = client
        self._ss_cache: dict = {}
        self._ws_cache: dict = {}
        self._lock = threading.Lock()

    def open_by_url(self, url: str):
        with self._lock:
            if url not in self._ss_cache:
                _read_limiter.acquire()
                self._ss_cache[url] = self._client.open_by_url(url)
            return self._ss_cache[url]

    def worksheet(self, spreadsheet, name: str):
        key = (spreadsheet.id, name)
        with self._lock:
            if key not in self._ws_cache:
                _read_limiter.acquire()
                self._ws_cache[key] = spreadsheet.worksheet(name)
            return self._ws_cache[key]

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

# =========================
# READ SHEETS  (retry with exponential back-off)
# =========================
@retry(
    wait=wait_exponential(multiplier=1, min=3, max=30),   # 3 → 6 → 12 → 30s
    stop=stop_after_attempt(4),
    retry=retry_if_exception_type((gspread.exceptions.APIError, JSONDecodeError)),
    reraise=True,
)
def _fetch_range(ws):
    _read_limiter.acquire()
    return ws.get("B8:AR")


def read_worksheet(ws, schema: list[str]) -> list[dict]:
    data = _fetch_range(ws)
    if not data:
        return []
    df = pd.DataFrame(data)
    df.columns = schema[: len(df.columns)]
    df = df.reindex(columns=schema).fillna("")

    # filter by date_update ≥ DATE_FILTER_FROM  (vectorised)
    df["date_update"] = df["date_update"].apply(try_parsing_date)
    df = df[df["date_update"] >= DATE_FILTER_FROM]
    return df.to_dict("records")


def _fetch_task(client: CachedGSpreadClient, url: str, sheet_name: str, schema: list[str]):
    """Returns (rows, error_tuple_or_None)."""
    try:
        ss = client.open_by_url(url)
        ws = client.worksheet(ss, sheet_name)
        rows = read_worksheet(ws, schema)
        return rows, None
    except gspread.exceptions.WorksheetNotFound:
        logging.warning(f"Sheet not found: '{sheet_name}' in {url}")
        return [], None          # not worth retrying
    except Exception as exc:
        logging.error(f"Error fetching '{sheet_name}' from {url}: {exc}")
        return [], (url, sheet_name)

# =========================
# PARALLEL FETCH WITH AUTO-RETRY
# =========================
def fetch_all_sheets(
    client: CachedGSpreadClient,
    sheet_tasks: list[tuple],
    schema: list[str],
    max_workers: int = MAX_WORKERS,
    max_rounds: int = MAX_RETRY_ROUNDS,
) -> tuple[pd.DataFrame, list]:

    all_rows: list[dict] = []
    pending = list(sheet_tasks)

    for round_no in range(1, max_rounds + 1):
        if not pending:
            break

        logging.info(f"Round {round_no}: processing {len(pending)} sheets with {max_workers} workers")
        failed = []

        iterator = pending
        if HAS_TQDM and round_no == 1:
            iterator = tqdm(pending, desc="Fetching sheets", unit="sheet")

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_fetch_task, client, url, name, schema): (url, name)
                for url, name in iterator
            }
            for future in as_completed(futures):
                rows, err = future.result()
                all_rows.extend(rows)
                if err:
                    failed.append(err)

        pending = failed
        if pending:
            backoff = min(5 * round_no, 60)
            logging.warning(f"{len(pending)} sheets still failing — waiting {backoff}s before retry…")
            time.sleep(backoff)

    if pending:
        logging.error(f"Gave up on {len(pending)} sheets after {max_rounds} rounds:")
        for url, name in pending:
            logging.error(f"  ✗ {name} :: {url}")

    df = pd.DataFrame.from_records(all_rows, columns=schema) if all_rows else pd.DataFrame(columns=schema)
    return df, pending

# =========================
# CLEAN / NORMALISE
# =========================
def normalize_dates(df: pd.DataFrame, date_cols: list[str]) -> pd.DataFrame:
    # --- date columns → 'YYYY-MM-DD' strings ---
    for col in date_cols:
        parsed = df[col].apply(try_parsing_date)
        df[col] = parsed.dt.strftime('%Y-%m-%d').fillna("")

    # --- numeric action columns ---
    action_cols = ["recruiter_call", "hm_interview", "offering", "accept", "onboard"]
    for col in action_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- ticket_id: derive from action sum when < 20 ---
    df["ticket_id"] = pd.to_numeric(df.get("ticket_id", 0), errors="coerce").fillna(0)
    mask = df["ticket_id"] < 20
    df.loc[mask, "ticket_id"] = df.loc[mask, action_cols].sum(axis=1)

    # --- phone: keep last 9 digits ---
    if "phone" in df.columns:
        df["phone"] = (
            df["phone"].astype(str)
                       .str.replace(r"\D", "", regex=True)
                       .str[-9:]
                       .replace({"nan": "", "NaN": "", "None": ""})
                       .fillna("")
        )

    # --- deduplication: phone + pic + position
    #     prefer row with id_code; within that group, keep highest ticket_id
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
# WRITE TO MASTER (single batch)
# =========================
def write_master(client: CachedGSpreadClient, df: pd.DataFrame):
    master_ss = client.open_by_url(MASTER_SPREADSHEET_URL)
    ws = client.worksheet(master_ss, "Productivity")

    values = [df.columns.tolist()] + df.astype(str).values.tolist()

    _write_limiter.acquire()
    ws.clear()

    # Single batch_update → 1 API write call
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
    client = CachedGSpreadClient(authenticate_gspread())

    # --- read link list ---
    link_ss = client.open_by_url(LINK_SPREADSHEET_URL)
    ws_links = client.worksheet(link_ss, "Productivity File")
    df_links = pd.DataFrame(ws_links.get_all_records())

    missing = [c for c in REQUIRED_COLS if c not in df_links.columns]
    if missing:
        raise ValueError(f"Missing columns in Productivity File: {missing}")

    # --- build task list ---
    sheet_tasks = [
        (url, name)
        for url, *names in zip(df_links["Link"], *[df_links[c] for c in REQUIRED_COLS[1:]])
        if url and str(url).strip()
        for name in filter(None, names)
    ]
    logging.info(f"Total sheets to process: {len(sheet_tasks)}")

    # --- fetch ---
    all_data, error_log = fetch_all_sheets(client, sheet_tasks, SCHEMA, max_workers=MAX_WORKERS)
    logging.info(f"Fetched {len(all_data)} raw rows | {len(error_log)} sheets failed")

    # --- clean ---
    all_data = normalize_dates(all_data, DATE_COLS)
    all_data.replace([float("inf"), float("-inf")], "", inplace=True)
    all_data.fillna("", inplace=True)

    # --- write ---
    write_master(client, all_data)

    elapsed = time.time() - t0
    logging.info(f"✅ DONE in {elapsed:.1f}s — {len(all_data)} rows written")


if __name__ == "__main__":
    main()
