import logging
import os
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from requests.exceptions import JSONDecodeError
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)


# =========================
# CONFIG
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

LINK_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/10eMZVnmtyyr5JAzDvpE5Brgh-8fw3lEKmGvL5m6eCUY"
MASTER_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1O2DOwLEKZvVthDOJBWQIuA5Vh0we9mOyqruuFBMpDSk"

REQUIRED_COLS = ["Link", "Sheet 1", "Sheet 2", "Sheet 3", "Sheet 4", "Sheet 5"]

SCHEMA = [
    "date_update",
    "date_cdd_applied",
    "fullname",
    "source",
    "phone",
    "dob",
    "gender",
    "area",
    "address",
    "registration_area",
    "previous_work",
    "id_code",
    "note",
    "email",
    "rehire",
    "current_salary",
    "expected_ob_date",
    "position",
    "station_name",
    "storage",
    "reason_for_storage",
    "notes_for_recruitment",
    "recruiter_call",
    "recruiter_call_date",
    "recruiter_call_feedback",
    "recruiter_call_result",
    "hm_interview_date",
    "hm_interview",
    "hm_interview_feedback",
    "hm_interview_result",
    "offering",
    "offering_date",
    "accept",
    "accept_date",
    "onboard_date",
    "onboard",
    "reason_reject_ob",
    "finish_process",
    "fullname_ob",
    "phone_ob",
    "id_code_ob",
    "pic",
    "ticket_id",
    "rider_id",
]

DATE_COLS = [
    "date_update",
    "date_cdd_applied",
    "recruiter_call_date",
    "hm_interview_date",
    "offering_date",
    "accept_date",
    "onboard_date",
]

GLOBAL_CUTOFF_DATE = "2025-07-01"
FILTER_DATE_FROM = pd.Timestamp(GLOBAL_CUTOFF_DATE)

VOLATILE_SOURCE_ROW = int(os.getenv("VOLATILE_SOURCE_ROW", "2"))
VOLATILE_STABLE_WAIT_SECONDS = int(os.getenv("VOLATILE_STABLE_WAIT_SECONDS", "15"))
VOLATILE_MAX_ROUNDS = int(os.getenv("VOLATILE_MAX_ROUNDS", "5"))

SOURCE_MAX_ROUNDS = int(os.getenv("SOURCE_MAX_ROUNDS", "3"))
SOURCE_RETRY_WAIT_SECONDS = int(os.getenv("SOURCE_RETRY_WAIT_SECONDS", "10"))

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
FORMULA_COLS = 2


# =========================
# TOKEN BUCKET RATE LIMITER
# =========================
READ_RATE_LIMIT = int(os.getenv("GSHEETS_READ_RPM", "55"))
WRITE_RATE_LIMIT = int(os.getenv("GSHEETS_WRITE_RPM", "45"))
WINDOW = 60.0

_read_tokens = deque()
_write_tokens = deque()
_rate_lock = threading.Lock()


@dataclass
class SourceTask:
    source_id: str
    row_number: int
    url: str
    sheet_names: list
    is_volatile: bool


@dataclass
class FetchResult:
    source_id: str
    success: bool
    rows: list
    row_count: int
    error: str


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
    if isinstance(exc, gspread.exceptions.APIError):
        status = getattr(exc.response, "status_code", None)
        return status in (429, 500, 502, 503, 504)
    if isinstance(exc, JSONDecodeError):
        return True
    return False


RETRY_POLICY = dict(
    retry=retry_if_exception(is_retryable_api_error),
    wait=wait_exponential(multiplier=1, min=2, max=60) + wait_random(0, 2),
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
    def __init__(self, client):
        self.client = client
        self._ss_cache = {}
        self._ws_cache = {}
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
    for fmt in (
        "%y/%m/%d",
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%d-%b-%y",
        "%d-%b-%Y",
        "%Y-%m-%d",
    ):
        try:
            return pd.to_datetime(text, format=fmt, errors="raise")
        except ValueError:
            continue
    try:
        return pd.to_datetime(text, errors="coerce")
    except Exception:
        return pd.NaT


def col_index_to_a1(col_index: int) -> str:
    if col_index <= 0:
        raise ValueError("Column index must be >= 1")
    chars = []
    while col_index > 0:
        col_index, rem = divmod(col_index - 1, 26)
        chars.append(chr(65 + rem))
    return "".join(reversed(chars))


# =========================
# SOURCE CONFIG
# =========================
def build_source_tasks(ws_links) -> list:
    rate_limit_read()
    data_links = ws_links.get_all_records()
    df_links = pd.DataFrame(data_links)

    if not all(col in df_links.columns for col in REQUIRED_COLS):
        raise ValueError(f"Productivity File is missing required columns: {REQUIRED_COLS}")

    tasks = []
    for idx, row in df_links.iterrows():
        row_number = idx + 2
        url = row.get("Link", "")
        if not url or not isinstance(url, str) or not url.strip():
            continue

        selected_names = [
            row[col].strip()
            for col in REQUIRED_COLS[1:]
            if row.get(col) and isinstance(row[col], str) and row[col].strip()
        ]
        if not selected_names:
            continue

        tasks.append(
            SourceTask(
                source_id=f"row_{row_number}",
                row_number=row_number,
                url=url.strip(),
                sheet_names=selected_names,
                is_volatile=(row_number == VOLATILE_SOURCE_ROW),
            )
        )
    return tasks


# =========================
# EXTRACT
# =========================
@retry(**RETRY_POLICY)
def count_source_rows(client: GSpreadClientWithCache, task: SourceTask):
    spreadsheet = client.open_by_url(task.url)

    rate_limit_read()
    available_sheet_names = {ws.title for ws in spreadsheet.worksheets()}
    selected_names = [name for name in task.sheet_names if name in available_sheet_names]
    missing_names = [name for name in task.sheet_names if name not in available_sheet_names]

    for missing in missing_names:
        logger.warning("[VOLATILE %s] Missing worksheet '%s' while counting", task.source_id, missing)

    if not selected_names:
        return 0, {}

    ranges = ["'{}'!B8:AR".format(name.replace("'", "''")) for name in selected_names]
    rate_limit_read()
    response = spreadsheet.values_batch_get(ranges)
    value_ranges = response.get("valueRanges", [])

    total_rows = 0
    per_sheet_counts = {}
    for idx, sheet_name in enumerate(selected_names):
        block = value_ranges[idx] if idx < len(value_ranges) else {}
        values = block.get("values", [])
        count = sum(1 for row in values if any(str(cell).strip() for cell in row))
        per_sheet_counts[sheet_name] = count
        total_rows += count
        logger.info("[VOLATILE %s] Count sheet '%s' = %s", task.source_id, sheet_name, count)

    logger.info("[VOLATILE %s] Total counted rows = %s", task.source_id, total_rows)
    return total_rows, per_sheet_counts


@retry(**RETRY_POLICY)
def fetch_source_records(client: GSpreadClientWithCache, task: SourceTask) -> FetchResult:
    spreadsheet = client.open_by_url(task.url)
    cutoff = FILTER_DATE_FROM

    rate_limit_read()
    available_sheet_names = {ws.title for ws in spreadsheet.worksheets()}

    selected_names = [name for name in task.sheet_names if name in available_sheet_names]
    missing_names = [name for name in task.sheet_names if name not in available_sheet_names]
    for missing in missing_names:
        logger.warning("Source %s: missing worksheet '%s', skipped.", task.source_id, missing)

    if not selected_names:
        return FetchResult(task.source_id, True, [], 0, "")

    logger.info(
        "[SOURCE %s] Start fetch with cutoff=%s. Sheets=%s",
        task.source_id,
        cutoff.strftime("%Y-%m-%d"),
        ", ".join(selected_names),
    )

    ranges = ["'{}'!B8:AR".format(name.replace("'", "''")) for name in selected_names]
    rate_limit_read()
    response = spreadsheet.values_batch_get(ranges)
    value_ranges = response.get("valueRanges", [])

    all_rows = []
    for idx, sheet_name in enumerate(selected_names):
        block = value_ranges[idx] if idx < len(value_ranges) else {}
        values = block.get("values", [])
        raw_rows = len(values)
        if not values:
            logger.info("[SOURCE %s] Sheet '%s': raw_rows=0, after_cutoff=0", task.source_id, sheet_name)
            continue

        df = pd.DataFrame(values)
        if df.empty:
            logger.info("[SOURCE %s] Sheet '%s': raw_rows=%s, after_cutoff=0", task.source_id, sheet_name, raw_rows)
            continue

        df.columns = SCHEMA[: len(df.columns)]
        df = df.reindex(columns=SCHEMA).fillna("")
        df["date_update"] = df["date_update"].apply(try_parsing_date)
        df = df[df["date_update"] >= cutoff]
        after_cutoff_rows = len(df)
        logger.info(
            "[SOURCE %s] Sheet '%s': raw_rows=%s, after_cutoff=%s",
            task.source_id,
            sheet_name,
            raw_rows,
            after_cutoff_rows,
        )
        if df.empty:
            continue

        df["__source_id"] = task.source_id
        all_rows.extend(df.to_dict("records"))

    return FetchResult(task.source_id, True, all_rows, len(all_rows), "")


def fetch_regular_source_with_rounds(client: GSpreadClientWithCache, task: SourceTask) -> FetchResult:
    for round_no in range(1, SOURCE_MAX_ROUNDS + 1):
        logger.info("[SOURCE %s] Round %s/%s start", task.source_id, round_no, SOURCE_MAX_ROUNDS)
        try:
            result = fetch_source_records(client, task)
            logger.info("[SOURCE %s] Round %s/%s success. rows=%s", task.source_id, round_no, SOURCE_MAX_ROUNDS, result.row_count)
            return result
        except Exception as exc:
            logger.error("[SOURCE %s] Round %s/%s failed: %s", task.source_id, round_no, SOURCE_MAX_ROUNDS, exc)
            if round_no < SOURCE_MAX_ROUNDS:
                logger.info("[SOURCE %s] Retry after %ss", task.source_id, SOURCE_RETRY_WAIT_SECONDS)
                time.sleep(SOURCE_RETRY_WAIT_SECONDS)

    return FetchResult(
        source_id=task.source_id,
        success=False,
        rows=[],
        row_count=0,
        error=f"Failed after {SOURCE_MAX_ROUNDS} rounds",
    )


def fetch_volatile_source_when_stable(client: GSpreadClientWithCache, task: SourceTask) -> FetchResult:
    for round_no in range(1, VOLATILE_MAX_ROUNDS + 1):
        logger.info("[VOLATILE %s] Stability round %s/%s start", task.source_id, round_no, VOLATILE_MAX_ROUNDS)

        try:
            count_1, per_sheet_1 = count_source_rows(client, task)
        except Exception as exc:
            logger.error("[VOLATILE %s] Round %s count #1 failed: %s", task.source_id, round_no, exc)
            continue

        logger.info("[VOLATILE %s] Waiting %ss before count #2", task.source_id, VOLATILE_STABLE_WAIT_SECONDS)
        time.sleep(VOLATILE_STABLE_WAIT_SECONDS)

        try:
            count_2, per_sheet_2 = count_source_rows(client, task)
        except Exception as exc:
            logger.error("[VOLATILE %s] Round %s count #2 failed: %s", task.source_id, round_no, exc)
            continue

        all_sheet_names = sorted(set(per_sheet_1) | set(per_sheet_2))
        changed_sheets = [name for name in all_sheet_names if per_sheet_1.get(name, 0) != per_sheet_2.get(name, 0)]
        is_stable = count_1 == count_2 and not changed_sheets

        logger.info(
            "[VOLATILE %s] Round %s counts: first=%s second=%s stable=%s",
            task.source_id, round_no, count_1, count_2, is_stable,
        )

        if changed_sheets:
            logger.warning("[VOLATILE %s] Round %s changed sheets -> %s", task.source_id, round_no, ", ".join(changed_sheets))

        if is_stable and count_2 > 0:
            logger.info("[VOLATILE %s] Round %s stable with rows=%s. Start processing.", task.source_id, round_no, count_2)
            try:
                result = fetch_source_records(client, task)
                logger.info("[VOLATILE %s] Round %s process success. rows=%s", task.source_id, round_no, result.row_count)
                return result
            except Exception as exc:
                logger.error("[VOLATILE %s] Round %s process failed: %s", task.source_id, round_no, exc)
        elif count_2 == 0:
            logger.warning("[VOLATILE %s] Round %s stable but 0 rows. Retry.", task.source_id, round_no)

        if round_no < VOLATILE_MAX_ROUNDS:
            logger.info("[VOLATILE %s] Move to next stability round", task.source_id)

    return FetchResult(
        source_id=task.source_id,
        success=False,
        rows=[],
        row_count=0,
        error=f"Volatile source not stable after {VOLATILE_MAX_ROUNDS} rounds",
    )


def run_parallel_fetch(client: GSpreadClientWithCache, tasks: list) -> dict:
    results = {}
    total_sources = len(tasks)
    completed_sources = 0
    logger.info("[EXTRACT] Start parallel fetch: sources=%s max_workers=%s", total_sources, max(1, MAX_WORKERS))

    with ThreadPoolExecutor(max_workers=max(1, MAX_WORKERS)) as executor:
        future_to_task = {}
        for task in tasks:
            logger.info(
                "[QUEUE] source=%s row=%s volatile=%s sheets=%s",
                task.source_id, task.row_number, task.is_volatile, ", ".join(task.sheet_names),
            )
            worker = fetch_volatile_source_when_stable if task.is_volatile else fetch_regular_source_with_rounds
            future_to_task[executor.submit(worker, client, task)] = task

        for future in as_completed(future_to_task):
            task = future_to_task[future]
            result = future.result()
            results[task.source_id] = result
            completed_sources += 1
            if result.success:
                logger.info("[DONE] %s success rows=%s progress=%s/%s", task.source_id, result.row_count, completed_sources, total_sources)
            else:
                logger.error("[DONE] %s failed: %s progress=%s/%s", task.source_id, result.error, completed_sources, total_sources)

    return results


# =========================
# TRANSFORM
# =========================
def normalize_dates(df: pd.DataFrame, date_cols: list) -> pd.DataFrame:
    for col in date_cols:
        df[col] = df[col].apply(try_parsing_date).dt.strftime("%Y-%m-%d")
        df[col] = df[col].fillna("")

    # action = ["recruiter_call", "hm_interview", "offering", "accept", "onboard"]
    # for c in action:
    #     df[c] = pd.to_numeric(df[c], errors="coerce")
    # df["ticket_id"] = pd.to_numeric(df.get("ticket_id", 0), errors="coerce").fillna(0)
    # df.loc[df["ticket_id"] < 20, "ticket_id"] = df.loc[df["ticket_id"] < 20, action].sum(axis=1)

    if "phone" in df.columns:
        df["phone"] = df["phone"].astype(str).str.replace(r"\D", "", regex=True)
        df["phone"] = df["phone"].str[-9:]
        df["phone"] = df["phone"].replace(["nan", "NaN", "None"], "").fillna("")

    required_cols = {"phone", "pic", "position", "ticket_id"}
    # if required_cols.issubset(df.columns):
    #     if "id_code" not in df.columns:
    #         df["id_code"] = ""
    #     df["id_code"] = df["id_code"].fillna("").astype(str).str.strip()

    #     selected_idx = []
    #     for _, group in df.groupby(["phone", "pic", "position"], sort=False):
    #         group_with_id = group[group["id_code"].str.len() > 0]
    #         keep_idx = (
    #             group_with_id["ticket_id"].idxmax()
    #             if not group_with_id.empty
    #             else group["ticket_id"].idxmax()
    #         )
    #         selected_idx.append(keep_idx)

    #     df = df.loc[selected_idx].reset_index(drop=True)

    return df


# =========================
# WRITE MASTER
# =========================
@retry(**RETRY_POLICY)
def write_master(ws_master, values: list):
    data_rows = max(len(values) - 1, 0)
    data_cols = len(values[0]) if values else len(SCHEMA)
    required_rows = max(len(values), 2)
    required_cols = data_cols + FORMULA_COLS

    logger.info("[LOAD] Write %s rows, %s columns to master", data_rows, data_cols)
    logger.info(
        "[LOAD] Current grid rows=%s cols=%s | required rows=%s cols=%s",
        ws_master.row_count, ws_master.col_count, required_rows, required_cols,
    )

    if ws_master.row_count < required_rows:
        logger.info("[LOAD] Expand rows only: %s -> %s", ws_master.row_count, required_rows)
        rate_limit_write()
        ws_master.resize(rows=required_rows)

    if ws_master.col_count < required_cols:
        raise ValueError(
            f"Worksheet has {ws_master.col_count} columns but requires at least {required_cols}. "
            "Please add columns manually as requested."
        )

    data_end_col = col_index_to_a1(data_cols)
    formula_start_col = col_index_to_a1(data_cols + 1)
    formula_end_col = col_index_to_a1(data_cols + FORMULA_COLS)

    logger.info("[LOAD] Clear ranges A:%s and %s:%s", data_end_col, formula_start_col, formula_end_col)
    rate_limit_write()
    ws_master.batch_clear([f"A:{data_end_col}", f"{formula_start_col}:{formula_end_col}"])

    block_bytes_limit = 8 * 1024 * 1024
    row_pointer = 1
    current_block = []
    current_size = 0

    for row in values:
        row_str = [("" if (cell is None or cell != cell) else cell) for cell in row]
        row_bytes = sum(len(str(cell).encode("utf-8")) for cell in row_str)
        if current_block and current_size + row_bytes > block_bytes_limit:
            end_row = row_pointer + len(current_block) - 1
            logger.info("[LOAD] Update block A%s:%s%s (%s rows)", row_pointer, data_end_col, end_row, len(current_block))
            rate_limit_write()
            ws_master.update(f"A{row_pointer}", current_block, value_input_option="USER_ENTERED")
            row_pointer += len(current_block)
            current_block = []
            current_size = 0

        current_block.append(row_str)
        current_size += row_bytes

    if current_block:
        end_row = row_pointer + len(current_block) - 1
        logger.info("[LOAD] Update final block A%s:%s%s (%s rows)", row_pointer, data_end_col, end_row, len(current_block))
        rate_limit_write()
        ws_master.update(f"A{row_pointer}", current_block, value_input_option="USER_ENTERED")

    rate_limit_write()
    ws_master.batch_update(
        [
            {"range": f"{formula_start_col}1:{formula_end_col}1", "values": [["channel_by_prod", "team"]]},
            {"range": f"{formula_start_col}2", "values": [["=ARRAYFORMULA(IFNA(XLOOKUP(D2:D,Source!$A:$A,Source!$C:$C)))"]]},
            {"range": f"{formula_end_col}2", "values": [["=ARRAYFORMULA(IFNA(XLOOKUP(AP2:AP,Info!$C:$C,Info!$N:$N)))"]]},
        ],
        value_input_option="USER_ENTERED",
    )
    logger.info("[LOAD] Master write complete")


# =========================
# MAIN
# =========================
def main():
    logger.info("=== START new_productivity ingestion ===")
    logger.info("[CONFIG] GLOBAL_CUTOFF_DATE=%s", GLOBAL_CUTOFF_DATE)
    logger.info("[CONFIG] VOLATILE_SOURCE_ROW=%s", VOLATILE_SOURCE_ROW)
    logger.info("[CONFIG] VOLATILE_STABLE_WAIT_SECONDS=%s", VOLATILE_STABLE_WAIT_SECONDS)
    logger.info("[CONFIG] VOLATILE_MAX_ROUNDS=%s", VOLATILE_MAX_ROUNDS)
    logger.info("[CONFIG] SOURCE_MAX_ROUNDS=%s", SOURCE_MAX_ROUNDS)

    client = GSpreadClientWithCache(authenticate_gspread())

    logger.info("[STEP 1/3] Load source configuration")
    link_spreadsheet = client.open_by_url(LINK_SPREADSHEET_URL)
    ws_links = client.worksheet(link_spreadsheet, "Productivity File")
    source_tasks = build_source_tasks(ws_links)
    logger.info("[STEP 1/3] Configured sources: %s", len(source_tasks))

    logger.info("[STEP 2/3] Fetch sources in parallel")
    master_spreadsheet = client.open_by_url(MASTER_SPREADSHEET_URL)
    ws_master = client.worksheet(master_spreadsheet, "Productivity")
    results = run_parallel_fetch(client, source_tasks)

    failed = [(sid, r.error) for sid, r in results.items() if not r.success]
    incoming_rows = []
    for source_id, result in results.items():
        if result.success and result.rows:
            incoming_rows.extend(result.rows)
        logger.info("[EXTRACT] %s success=%s rows=%s", source_id, result.success, result.row_count)

    logger.info("[STEP 3/3] Normalize and write master")
    incoming_df = pd.DataFrame.from_records(incoming_rows) if incoming_rows else pd.DataFrame(columns=SCHEMA)
    for col in SCHEMA:
        if col not in incoming_df.columns:
            incoming_df[col] = ""
    incoming_df = incoming_df.reindex(columns=SCHEMA).fillna("")

    all_data = normalize_dates(incoming_df, DATE_COLS)
    all_data.replace([float("inf"), float("-inf")], "", inplace=True)
    all_data.fillna("", inplace=True)
    all_data = all_data.reindex(columns=SCHEMA).fillna("")
    logger.info("[CLEAN] incoming=%s output=%s", len(incoming_df), len(all_data))

    if DRY_RUN:
        logger.info("[WRITE] DRY_RUN=true -> skip write master")
    else:
        write_master(ws_master, [all_data.columns.tolist()] + all_data.values.tolist())

    if failed:
        logger.warning("[END] Run completed with %s failed sources", len(failed))
        for source_id, err in failed:
            logger.warning("[END] %s -> %s", source_id, err)
    else:
        logger.info("[END] Run completed with no failed sources")

    logger.info("=== DONE ===")


if __name__ == "__main__":
    main()
