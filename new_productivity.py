import hashlib
import json
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
MASTER_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1DW-w44FLEU236XpeDBdXhIb6_KlwuGRnjzzMEAntNvA/edit?gid=0#gid=0"

REQUIRED_COLS = ["Link", "Sheet 1", "Sheet 2", "Sheet 3", "Sheet 4", "Sheet 5"]

SCHEMA = [
    "date_update",
    "date_cdd_applied",
    "fullname",
    "source",
    "dob",
    "phone",
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

FILTER_DATE_FROM = pd.Timestamp(os.getenv("FILTER_DATE_FROM", "2025-07-01"))
OVERLAP_HOURS = int(os.getenv("OVERLAP_HOURS", "24"))
VOLATILE_SOURCE_ROW = int(os.getenv("VOLATILE_SOURCE_ROW", "2"))
VOLATILE_LOOKBACK_HOURS = int(os.getenv("VOLATILE_LOOKBACK_HOURS", "72"))
STATE_WORKSHEET_NAME = os.getenv("STATE_WORKSHEET_NAME", "_IngestionState")

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"


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
    payload_hash: str
    row_count: int
    error: str
    is_volatile: bool


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
def utc_now_naive() -> pd.Timestamp:
    now = pd.Timestamp.utcnow()
    if getattr(now, "tzinfo", None) is not None:
        return now.tz_localize(None)
    return now


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


def compute_payload_hash(rows: list) -> str:
    if not rows:
        return ""

    normalized = []
    for row in rows:
        item = {}
        for k, v in row.items():
            if k.startswith("__"):
                continue
            item[k] = "" if pd.isna(v) else str(v)
        normalized.append(item)

    normalized.sort(key=lambda x: (x.get("id_code", ""), x.get("phone", ""), x.get("date_update", "")))
    payload = json.dumps(normalized, ensure_ascii=True, sort_keys=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def build_record_key(df: pd.DataFrame) -> pd.Series:
    size = len(df)

    id_code = df["id_code"].fillna("").astype(str).str.strip() if "id_code" in df.columns else pd.Series([""] * size)
    phone = df["phone"].fillna("").astype(str).str.strip() if "phone" in df.columns else pd.Series([""] * size)
    pic = df["pic"].fillna("").astype(str).str.strip() if "pic" in df.columns else pd.Series([""] * size)
    position = (
        df["position"].fillna("").astype(str).str.strip() if "position" in df.columns else pd.Series([""] * size)
    )

    key = id_code.where(id_code != "", phone + "|" + pic + "|" + position)
    empty_mask = key.str.strip() == ""
    if empty_mask.any():
        fallback = (
            df.fillna("")
            .astype(str)
            .agg("|".join, axis=1)
            .apply(lambda x: hashlib.sha1(x.encode("utf-8")).hexdigest())
        )
        key = key.mask(empty_mask, "row:" + fallback)
    return key


# =========================
# SOURCE CONFIG + STATE
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

        selected_names = []
        for col in REQUIRED_COLS[1:]:
            value = row.get(col, "")
            if value and isinstance(value, str) and value.strip():
                selected_names.append(value.strip())

        if not selected_names:
            continue

        source_id = f"row_{row_number}"
        tasks.append(
            SourceTask(
                source_id=source_id,
                row_number=row_number,
                url=url.strip(),
                sheet_names=selected_names,
                is_volatile=(row_number == VOLATILE_SOURCE_ROW),
            )
        )
    return tasks


def get_or_create_state_ws(client: GSpreadClientWithCache, master_spreadsheet):
    try:
        return client.worksheet(master_spreadsheet, STATE_WORKSHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        logger.info("State worksheet not found. Creating _IngestionState.")
        rate_limit_write()
        ws = master_spreadsheet.add_worksheet(title=STATE_WORKSHEET_NAME, rows=1000, cols=6)
        rate_limit_write()
        ws.update("A1", [["source_id", "last_success_at", "is_volatile", "updated_at"]])
        return ws


def load_state(ws_state) -> dict:
    rate_limit_read()
    records = ws_state.get_all_records()
    state = {}
    for row in records:
        source_id = str(row.get("source_id", "")).strip()
        if not source_id:
            continue
        state[source_id] = str(row.get("last_success_at", "")).strip()
    return state


def compute_source_cutoff(task: SourceTask, state_map: dict) -> pd.Timestamp:
    global_floor = FILTER_DATE_FROM
    now = utc_now_naive()

    if task.is_volatile:
        volatile_floor = now - pd.Timedelta(hours=VOLATILE_LOOKBACK_HOURS)
        return max(global_floor, volatile_floor)

    last_success_raw = state_map.get(task.source_id, "")
    last_success = try_parsing_date(last_success_raw)
    if pd.isna(last_success):
        return global_floor

    overlap_floor = last_success - pd.Timedelta(hours=OVERLAP_HOURS)
    return max(global_floor, overlap_floor)


def save_state(ws_state, tasks: list, state_map: dict, success_ids: set):
    now_str = utc_now_naive().strftime("%Y-%m-%d %H:%M:%S")
    for source_id in success_ids:
        state_map[source_id] = now_str

    rows = [["source_id", "last_success_at", "is_volatile", "updated_at"]]
    for task in tasks:
        rows.append(
            [task.source_id, state_map.get(task.source_id, ""), "1" if task.is_volatile else "0", now_str]
        )

    rate_limit_write()
    ws_state.batch_clear(["A:D"])
    rate_limit_write()
    ws_state.update("A1", rows, value_input_option="RAW")


# =========================
# EXTRACT
# =========================
@retry(**RETRY_POLICY)
def fetch_source_records(client: GSpreadClientWithCache, task: SourceTask, cutoff: pd.Timestamp) -> FetchResult:
    spreadsheet = client.open_by_url(task.url)

    rate_limit_read()
    available_sheet_names = {ws.title for ws in spreadsheet.worksheets()}

    selected_names = [name for name in task.sheet_names if name in available_sheet_names]
    missing_names = [name for name in task.sheet_names if name not in available_sheet_names]
    for missing in missing_names:
        logger.warning(f"Source {task.source_id}: missing worksheet '{missing}', skipped.")

    if not selected_names:
        return FetchResult(task.source_id, True, [], "", 0, "", task.is_volatile)

    ranges = [f"'{name.replace(\"'\", \"''\")}'!B8:AR" for name in selected_names]
    rate_limit_read()
    response = spreadsheet.values_batch_get(ranges)
    value_ranges = response.get("valueRanges", [])

    all_rows = []
    for block in value_ranges:
        values = block.get("values", [])
        if not values:
            continue

        df = pd.DataFrame(values)
        if df.empty:
            continue

        df.columns = SCHEMA[: len(df.columns)]
        df = df.reindex(columns=SCHEMA).fillna("")
        df["date_update"] = df["date_update"].apply(try_parsing_date)
        df = df[df["date_update"] >= cutoff]
        if df.empty:
            continue

        df["__source_id"] = task.source_id
        all_rows.extend(df.to_dict("records"))

    payload_hash = compute_payload_hash(all_rows)
    return FetchResult(task.source_id, True, all_rows, payload_hash, len(all_rows), "", task.is_volatile)


def run_parallel_fetch(client: GSpreadClientWithCache, tasks: list, state_map: dict) -> dict:
    results = {}
    with ThreadPoolExecutor(max_workers=max(1, MAX_WORKERS)) as executor:
        future_to_task = {}
        for task in tasks:
            cutoff = compute_source_cutoff(task, state_map)
            logger.info(
                f"Queue source {task.source_id} (volatile={task.is_volatile}) cutoff {cutoff.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            future = executor.submit(fetch_source_records, client, task, cutoff)
            future_to_task[future] = task

        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                result = future.result()
                results[task.source_id] = result
                logger.info(f"OK   {task.source_id}: {result.row_count} rows")
            except Exception as exc:
                logger.error(f"FAIL {task.source_id}: {exc}")
                results[task.source_id] = FetchResult(
                    task.source_id, False, [], "", 0, str(exc), task.is_volatile
                )
    return results


def stabilize_volatile_sources(client: GSpreadClientWithCache, tasks: list, state_map: dict, results: dict):
    for task in tasks:
        if not task.is_volatile:
            continue

        first = results.get(task.source_id)
        if not first or not first.success:
            continue

        cutoff = compute_source_cutoff(task, state_map)
        try:
            second = fetch_source_records(client, task, cutoff)
        except Exception as exc:
            logger.warning(f"Volatile re-read failed for {task.source_id}: {exc}")
            continue

        if second.payload_hash != first.payload_hash:
            logger.warning(
                f"Volatile source changed during run ({task.source_id}). "
                f"Use second read: {first.row_count} -> {second.row_count} rows."
            )
            results[task.source_id] = second


# =========================
# TRANSFORM + MERGE
# =========================
def read_existing_master(ws_master) -> pd.DataFrame:
    rate_limit_read()
    values = ws_master.get("A1:AQ")
    if not values:
        return pd.DataFrame(columns=SCHEMA)

    header = values[0]
    rows = values[1:]
    if not rows:
        return pd.DataFrame(columns=SCHEMA)

    df = pd.DataFrame(rows)
    df.columns = header[: len(df.columns)]
    for col in SCHEMA:
        if col not in df.columns:
            df[col] = ""
    return df.reindex(columns=SCHEMA).fillna("")


def merge_incremental(existing_df: pd.DataFrame, incoming_df: pd.DataFrame) -> pd.DataFrame:
    if existing_df.empty and incoming_df.empty:
        return pd.DataFrame(columns=SCHEMA)
    if existing_df.empty:
        return incoming_df.reindex(columns=SCHEMA).fillna("")
    if incoming_df.empty:
        return existing_df.reindex(columns=SCHEMA).fillna("")

    left = existing_df.reindex(columns=SCHEMA).fillna("").copy()
    right = incoming_df.reindex(columns=SCHEMA).fillna("").copy()

    left["__key"] = build_record_key(left)
    right["__key"] = build_record_key(right)
    left["__date"] = left["date_update"].apply(try_parsing_date).fillna(pd.Timestamp("1900-01-01"))
    right["__date"] = right["date_update"].apply(try_parsing_date).fillna(pd.Timestamp("1900-01-01"))
    left["__ticket"] = pd.to_numeric(left["ticket_id"], errors="coerce").fillna(0)
    right["__ticket"] = pd.to_numeric(right["ticket_id"], errors="coerce").fillna(0)

    merged = pd.concat([left, right], ignore_index=True)
    merged.sort_values(by=["__key", "__date", "__ticket"], inplace=True)
    merged = merged.drop_duplicates(subset=["__key"], keep="last")
    merged.drop(columns=["__key", "__date", "__ticket"], inplace=True, errors="ignore")
    return merged.reindex(columns=SCHEMA).fillna("")


def normalize_dates(df: pd.DataFrame, date_cols: list) -> pd.DataFrame:
    df = df.copy()

    for col in date_cols:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].apply(try_parsing_date).dt.strftime("%Y-%m-%d")
        df[col] = df[col].fillna("")

    action = ["recruiter_call", "hm_interview", "offering", "accept", "onboard"]
    for col in action:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if "ticket_id" not in df.columns:
        df["ticket_id"] = 0
    df["ticket_id"] = pd.to_numeric(df["ticket_id"], errors="coerce").fillna(0)
    mask = df["ticket_id"] < 20
    if mask.any():
        df.loc[mask, "ticket_id"] = df.loc[mask, action].sum(axis=1)

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

    return df.reindex(columns=SCHEMA).fillna("")


# =========================
# WRITE MASTER
# =========================
@retry(**RETRY_POLICY)
def write_master(ws_master, values: list):
    logger.info(f"Write {len(values) - 1} rows to master sheet")

    rate_limit_write()
    ws_master.batch_clear(["A:AQ", "AR:AS"])

    block_bytes_limit = 8 * 1024 * 1024
    row_pointer = 1
    current_block = []
    current_size = 0

    for row in values:
        row_str = [("" if (cell is None or cell != cell) else cell) for cell in row]
        row_bytes = sum(len(str(cell).encode("utf-8")) for cell in row_str)
        if current_block and current_size + row_bytes > block_bytes_limit:
            rate_limit_write()
            ws_master.update(f"A{row_pointer}", current_block, value_input_option="RAW")
            row_pointer += len(current_block)
            current_block = []
            current_size = 0

        current_block.append(row_str)
        current_size += row_bytes

    if current_block:
        rate_limit_write()
        ws_master.update(f"A{row_pointer}", current_block, value_input_option="USER_ENTERED")

    rate_limit_write()
    ws_master.batch_update(
        [
            {"range": "AR1:AS1", "values": [["channel_by_prod", "team"]]},
            {"range": "AR2", "values": [["=ARRAYFORMULA(IFNA(XLOOKUP(D2:D,Source!$A:$A,Source!$C:$C)))"]]},
            {"range": "AS2", "values": [["=ARRAYFORMULA(IFNA(XLOOKUP(AO2:AO,Info!$C:$C,Info!$N:$N)))"]]},
        ],
        value_input_option="USER_ENTERED",
    )


# =========================
# MAIN
# =========================
def main():
    logger.info("Start new_productivity ingestion")
    client = GSpreadClientWithCache(authenticate_gspread())

    link_spreadsheet = client.open_by_url(LINK_SPREADSHEET_URL)
    ws_links = client.worksheet(link_spreadsheet, "Productivity File")
    source_tasks = build_source_tasks(ws_links)
    logger.info(f"Configured sources: {len(source_tasks)}")

    master_spreadsheet = client.open_by_url(MASTER_SPREADSHEET_URL)
    ws_master = client.worksheet(master_spreadsheet, "Productivity")
    ws_state = get_or_create_state_ws(client, master_spreadsheet)
    state_map = load_state(ws_state)

    results = run_parallel_fetch(client, source_tasks, state_map)
    stabilize_volatile_sources(client, source_tasks, state_map, results)

    success_ids = {source_id for source_id, result in results.items() if result.success}
    failed = [(source_id, result.error) for source_id, result in results.items() if not result.success]

    incoming_rows = []
    for source_id, result in results.items():
        if result.success and result.rows:
            incoming_rows.extend(result.rows)
        logger.info(f"Source {source_id}: success={result.success} rows={result.row_count}")

    incoming_df = pd.DataFrame.from_records(incoming_rows)
    if incoming_df.empty:
        incoming_df = pd.DataFrame(columns=SCHEMA)
    else:
        for col in SCHEMA:
            if col not in incoming_df.columns:
                incoming_df[col] = ""
        incoming_df = incoming_df.reindex(columns=SCHEMA).fillna("")

    existing_df = read_existing_master(ws_master)
    merged_df = merge_incremental(existing_df, incoming_df)
    merged_df = normalize_dates(merged_df, DATE_COLS)
    merged_df.replace([float("inf"), float("-inf")], "", inplace=True)
    merged_df.fillna("", inplace=True)
    logger.info(
        f"Rows summary: incoming={len(incoming_df)}, existing={len(existing_df)}, merged={len(merged_df)}"
    )

    if DRY_RUN:
        logger.info("DRY_RUN=true, skip writing master and state.")
    else:
        values = [merged_df.columns.tolist()] + merged_df.values.tolist()
        write_master(ws_master, values)
        save_state(ws_state, source_tasks, state_map, success_ids)

    if failed:
        logger.warning(f"Run completed with {len(failed)} failed sources.")
        for source_id, err in failed:
            logger.warning(f" - {source_id}: {err}")
    else:
        logger.info("Run completed with no failed sources.")

    logger.info("DONE")


if __name__ == "__main__":
    main()
