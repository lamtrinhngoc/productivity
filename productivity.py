import math
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
from tenacity import (
    retry, wait_exponential, wait_random, stop_after_attempt,
    retry_if_exception, before_sleep_log
)

# =========================
# CONFIG
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

LINK_SPREADSHEET_URL   = "https://docs.google.com/spreadsheets/d/10eMZVnmtyyr5JAzDvpE5Brgh-8fw3lEKmGvL5m6eCUY"
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

# Configurable via env var — no more silent hardcoded date
FILTER_DATE_FROM = pd.Timestamp(os.getenv("FILTER_DATE_FROM", "2025-07-01"))

# =========================
# TOKEN BUCKET RATE LIMITER
# =========================
READ_RATE_LIMIT  = int(os.getenv("GSHEETS_READ_RPM",  "40"))
WRITE_RATE_LIMIT = int(os.getenv("GSHEETS_WRITE_RPM", "40"))
WINDOW = 60.0

_read_tokens  = deque()
_write_tokens = deque()
_rate_lock    = threading.Lock()


def _acquire_token(tokens: deque, limit: int):
    while True:
        with _rate_lock:
            now = time.monotonic()
            while tokens and (now - tokens[0]) > WINDOW:
                tokens.popleft()
            if len(tokens) < limit:
                tokens.append(now)
                return
            wait_seconds = WINDOW - (now - tokens[0]) + 0.1
        time.sleep(max(wait_seconds, 0.1))


def rate_limit_read():
    _acquire_token(_read_tokens, READ_RATE_LIMIT)


def rate_limit_write():
    _acquire_token(_write_tokens, WRITE_RATE_LIMIT)


# =========================
# RETRY HELPERS
# =========================
def is_retryable_api_error(exc):
    """Retry on 429 (quota) and transient 5xx errors."""
    if isinstance(exc, gspread.exceptions.APIError):
        return exc.response.status_code in (429, 500, 502, 503)
    if isinstance(exc, JSONDecodeError):
        return True
    return False


RETRY_POLICY = dict(
    retry=retry_if_exception(is_retryable_api_error),
    wait=wait_exponential(multiplier=1, min=3, max=60) + wait_random(0, 2),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)


# =========================
# AUTH + CLIENT CACHE
# =========================
def authenticate_gspread():
    creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    return gspread.authorize(creds)


class GSpreadClientWithCache:
    """Thread-safe gspread client with spreadsheet/worksheet caching.
    Network I/O is performed outside the lock so threads only serialize
    on cache lookups, not on API round-trips.
    """

    def __init__(self, client):
        self.client = client
        self._ss_cache: dict = {}
        self._ws_cache: dict = {}
        self._lock = threading.Lock()

    @retry(**RETRY_POLICY)
    def open_by_url(self, url: str):
        with self._lock:
            if url in self._ss_cache:
                return self._ss_cache[url]

        rate_limit_read()
        result = self.client.open_by_url(url)

        with self._lock:
            self._ss_cache.setdefault(url, result)
            return self._ss_cache[url]

    @retry(**RETRY_POLICY)
    def worksheet(self, spreadsheet, name: str):
        key = (spreadsheet.id, name)
        with self._lock:
            if key in self._ws_cache:
                return self._ws_cache[key]

        rate_limit_read()
        result = spreadsheet.worksheet(name)

        with self._lock:
            self._ws_cache.setdefault(key, result)
            return self._ws_cache[key]


# =========================
# UTILS
# =========================
def try_parsing_date(text):
    if pd.isna(text) or not str(text).strip():
        return pd.NaT
    for fmt in ('%y/%m/%d', '%Y/%m/%d', '%m/%d/%Y', '%m/%d/%y',
                '%d-%b-%y', '%d-%b-%Y', '%Y-%m-%d'):
        try:
            return pd.to_datetime(text, format=fmt, errors="raise")
        except ValueError:
            continue
    try:
        return pd.to_datetime(text, errors="coerce")
    except Exception:
        return pd.NaT


# =========================
# READ SHEETS
# =========================
@retry(**RETRY_POLICY)
def read_worksheet_with_retry(ws, schema):
    rate_limit_read()
    data = ws.get("B8:AR")
    if not data:
        return []
    df = pd.DataFrame(data)
    df.columns = schema[:len(df.columns)]
    df = df.reindex(columns=schema).fillna("")
    df["date_update"] = df["date_update"].apply(try_parsing_date)
    df = df[df["date_update"] >= FILTER_DATE_FROM]
    return df.to_dict("records")


def get_sheet_data(client: GSpreadClientWithCache, url: str, sheet_name: str, schema: list):
    """Fetch rows from one worksheet. Raises on failure — caller handles errors."""
    sheet = client.open_by_url(url)
    ws    = client.worksheet(sheet, sheet_name)
    return read_worksheet_with_retry(ws, schema)


# =========================
# FETCH WITH RETRY ROUNDS
# =========================
def fetch_all_sheets(
    client: GSpreadClientWithCache,
    sheet_tasks: list,
    schema: list,
    max_workers: int = 4,
    max_rounds: int = 5,
) -> tuple:
    """Fetch all sheets in parallel with up to max_rounds retry rounds.

    - Successful sheets are logged immediately with row count.
    - WorksheetNotFound is logged and skipped (not retried).
    - Other failures are collected and retried next round with a back-off wait.
    - A final summary lists any sheets that never succeeded.

    Returns (combined_dataframe, permanently_failed_tasks).
    """
    all_rows: list = []
    pending   = list(sheet_tasks)

    for round_no in range(1, max_rounds + 1):
        if not pending:
            break

        logger.info(f"🔄 Round {round_no}/{max_rounds} — {len(pending)} sheet(s) to fetch")
        failed: list = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(get_sheet_data, client, url, name, schema): (url, name)
                for url, name in pending
            }

            for future in as_completed(future_to_task):
                url, name = future_to_task[future]
                try:
                    rows = future.result()
                    all_rows.extend(rows)
                    logger.info(f"  ✅ OK  — sheet '{name}' from {url}  ({len(rows)} rows)")
                except gspread.exceptions.WorksheetNotFound:
                    # Sheet genuinely doesn't exist — no point retrying
                    logger.error(f"  ❌ SKIP — sheet '{name}' not found in {url} (will not retry)")
                except Exception as exc:
                    logger.error(f"  ❌ FAIL — sheet '{name}' from {url}: {exc}")
                    failed.append((url, name))

        if failed and round_no < max_rounds:
            wait_sec = 5 * round_no
            logger.warning(
                f"  ⚠️  {len(failed)} sheet(s) failed — waiting {wait_sec}s before retry..."
            )
            time.sleep(wait_sec)

        pending = failed

    # Final summary
    if pending:
        logger.error(f"❌ {len(pending)} sheet(s) permanently failed after {max_rounds} rounds:")
        for url, name in pending:
            logger.error(f"   - '{name}' :: {url}")
    else:
        logger.info("✅ All sheets fetched successfully.")

    df_all = (
        pd.DataFrame.from_records(all_rows, columns=schema)
        if all_rows
        else pd.DataFrame(columns=schema)
    )
    return df_all, pending


# =========================
# CLEAN DATA
# =========================
def normalize_dates(df: pd.DataFrame, date_cols: list) -> pd.DataFrame:
    for col in date_cols:
        df[col] = df[col].apply(try_parsing_date).dt.strftime("%Y-%m-%d")
        df[col] = df[col].fillna("")

    action = ["recruiter_call", "hm_interview", "offering", "accept", "onboard"]
    for c in action:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ticket_id"] = pd.to_numeric(df.get("ticket_id", 0), errors="coerce").fillna(0)
    df.loc[df["ticket_id"] < 20, "ticket_id"] = df.loc[df["ticket_id"] < 20, action].sum(axis=1)

    if "phone" in df.columns:
        df["phone"] = df["phone"].astype(str).str.replace(r"\D", "", regex=True)
        df["phone"] = df["phone"].str[-9:]
        df["phone"] = df["phone"].replace(["nan", "NaN", "None"], "").fillna("")

    required_cols = {"phone", "pic", "position", "ticket_id"}
    if required_cols.issubset(df.columns):
        if "id_code" not in df.columns:
            df["id_code"] = ""
        df["id_code"] = df["id_code"].fillna("").astype(str).str.strip()

        selected_idx = []
        for _, group in df.groupby(["phone", "pic", "position"], sort=False):
            group_with_id = group[group["id_code"].str.len() > 0]
            keep_idx = (
                group_with_id["ticket_id"].idxmax()
                if not group_with_id.empty
                else group["ticket_id"].idxmax()
            )
            selected_idx.append(keep_idx)

        df = df.loc[selected_idx].reset_index(drop=True)

    return df


# =========================
# WRITE MASTER (optimized)
# =========================
@retry(**RETRY_POLICY)
def write_master(ws_master, values: list):
    """Write cleaned data to the master sheet.

    vs. original:
      - Header merged into first data block        → -1 API call
      - Block size adaptive by bytes, not row count → safer under 10 MB limit
      - 3 formula updates → 1 batch_update call     → -2 API calls & rate-limit waits
      - Each API call acquires its own rate-limit token
    """
    logger.info(f"🚀 Writing {len(values) - 1} rows to master sheet")

    # 1. Clear data range only
    rate_limit_write()
    ws_master.batch_clear(["A:AQ"])

    # 2. Write data in byte-aware blocks
    #    values[0] = header row, included as first row naturally
    BLOCK_BYTES_LIMIT = 8 * 1024 * 1024    # 8 MB — safely under Google's 10 MB limit
    row_pointer   = 1
    current_block: list = []
    current_size  = 0

    for row in values:
        row_str = [("" if (c is None or c != c) else c) for c in row]
        row_bytes = sum(len(str(s).encode("utf-8")) for s in row_str)

        if current_block and current_size + row_bytes > BLOCK_BYTES_LIMIT:
            end_row = row_pointer + len(current_block) - 1
            logger.info(
                f"  📦 Writing rows {row_pointer}–{end_row} "
                f"({len(current_block)} rows, {current_size / 1024:.1f} KB)"
            )
            rate_limit_write()
            ws_master.update(f"A{row_pointer}", current_block, value_input_option="RAW")
            row_pointer  += len(current_block)
            current_block = []
            current_size  = 0

        current_block.append(row_str)
        current_size += row_bytes

    if current_block:
        end_row = row_pointer + len(current_block) - 1
        logger.info(
            f"  📦 Writing rows {row_pointer}–{end_row} "
            f"({len(current_block)} rows, {current_size / 1024:.1f} KB) [final]"
        )
        rate_limit_write()
        ws_master.update(f"A{row_pointer}", current_block, value_input_option="USER_ENTERED")

    # 3. Formulas — 3 original calls collapsed into 1 batch_update
    logger.info("  ⚡ Writing formula columns (1 batch call)")
    rate_limit_write()
    ws_master.batch_update(
        [
            {
                "range": "AR1:AS1",
                "values": [["channel_by_prod", "team"]],
            },
            {
                "range": "AR2",
                "values": [["=ARRAYFORMULA(IFNA(XLOOKUP(D2:D,Source!$A:$A,Source!$C:$C)))"]],
            },
            {
                "range": "AS2",
                "values": [["=ARRAYFORMULA(IFNA(XLOOKUP(AO2:AO,Info!$C:$C,Info!$N:$N)))"]],
            },
        ],
        value_input_option="USER_ENTERED",
    )

    logger.info("✅ Master sheet write complete.")


# =========================
# MAIN
# =========================
def main():
    client = GSpreadClientWithCache(authenticate_gspread())

    # --- Load sheet task list ---
    link_spreadsheet = client.open_by_url(LINK_SPREADSHEET_URL)
    ws_links = client.worksheet(link_spreadsheet, "Productivity File")

    rate_limit_read()
    data_links = ws_links.get_all_records()
    df_links   = pd.DataFrame(data_links)

    if not all(c in df_links.columns for c in REQUIRED_COLS):
        raise ValueError(f"Productivity File is missing required columns: {REQUIRED_COLS}")

    sheet_tasks: list = []
    for _, row in df_links.iterrows():
        url = row["Link"]
        if url and isinstance(url, str) and url.strip():
            for col in REQUIRED_COLS[1:]:
                name = row[col]
                if name:
                    sheet_tasks.append((url, name))

    logger.info(f"📋 Total sheets to fetch: {len(sheet_tasks)}")

    # --- Fetch all sheets (built-in retry rounds, full per-sheet logging) ---
    all_data, permanently_failed = fetch_all_sheets(
        client, sheet_tasks, SCHEMA, max_workers=4, max_rounds=5
    )

    if permanently_failed:
        logger.warning(
            f"⚠️  {len(permanently_failed)} sheet(s) excluded from master — see errors above."
        )

    # --- Clean ---
    all_data = normalize_dates(all_data, DATE_COLS)
    all_data.replace([float("inf"), float("-inf")], "", inplace=True)
    all_data.fillna("", inplace=True)

    # --- Write ---
    master_spreadsheet = client.open_by_url(MASTER_SPREADSHEET_URL)
    ws_master = client.worksheet(master_spreadsheet, "Productivity")

    values = [all_data.columns.tolist()] + all_data.values.tolist()
    write_master(ws_master, values)

    logger.info("✅ DONE")


if __name__ == "__main__":
    main()
