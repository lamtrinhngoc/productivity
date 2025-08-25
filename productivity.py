import os
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from requests.exceptions import JSONDecodeError, ConnectionError, Timeout
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# =========================
# CONFIG
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

LINK_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/10eMZVnmtyyr5JAzDvpE5Brgh-8fw3lEKmGvL5m6eCUY"
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

DATE_FORMATS = [
    "%y/%m/%d", "%Y/%m/%d", "%m/%d/%Y", "%m/%d/%y",
    "%d-%b-%y", "%d-%b-%Y", "%Y-%m-%d"
]

# =========================
# RATE LIMITER
# =========================
READ_RATE_LIMIT = int(os.getenv("GSHEETS_READ_RPM", "55"))
WRITE_RATE_LIMIT = int(os.getenv("GSHEETS_WRITE_RPM", "55"))
WINDOW = 60.0

_read_tokens = deque()
_write_tokens = deque()
_lock = threading.Lock()

def _acquire_token(tokens: deque, limit: int, kind: str):
    while True:
        with _lock:
            now = time.monotonic()
            while tokens and (now - tokens[0]) > WINDOW:
                tokens.popleft()
            if len(tokens) < limit:
                tokens.append(now)
                return
            wait_seconds = max(WINDOW - (now - tokens[0]) + 0.01, 0.1)
        threading.Event().wait(wait_seconds)

def rate_limit_read():
    _acquire_token(_read_tokens, READ_RATE_LIMIT, "READ")

def rate_limit_write():
    _acquire_token(_write_tokens, WRITE_RATE_LIMIT, "WRITE")

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
        self._lock = threading.Lock()

    def open_by_url(self, url):
        with self._lock:
            if url not in self._ss_cache:
                rate_limit_read()
                self._ss_cache[url] = self.client.open_by_url(url)
            return self._ss_cache[url]

# =========================
# SAFE WRAPPERS
# =========================
def safe_worksheet(spreadsheet, name: str):
    rate_limit_read()
    return spreadsheet.worksheet(name)

def safe_get_range(worksheet, rng: str):
    rate_limit_read()
    return worksheet.get(rng)

def safe_get_all_records(worksheet):
    rate_limit_read()
    return worksheet.get_all_records()

# =========================
# DATE PARSING
# =========================
def try_parsing_date(text):
    for fmt in DATE_FORMATS:
        try:
            return pd.to_datetime(text, format=fmt)
        except (ValueError, TypeError):
            continue
    return pd.NaT

def normalize_dates(df, date_cols):
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        mask = df[col].isna()
        if mask.any():
            df.loc[mask, col] = df.loc[mask, col].apply(try_parsing_date)
        df[col] = df[col].dt.strftime("%Y-%m-%d")
    return df

# =========================
# READ SHEETS WITH RETRY
# =========================
@retry(wait=wait_exponential(multiplier=2, min=2, max=30),
       stop=stop_after_attempt(5),
       retry=retry_if_exception_type((gspread.exceptions.APIError, JSONDecodeError,
                                      ConnectionError, Timeout)))
def read_worksheet(sheet, sheet_name, schema):
    ws = safe_worksheet(sheet, sheet_name)
    data = safe_get_range(ws, "B8:AR")
    if not data:
        return pd.DataFrame(columns=schema)
    df = pd.DataFrame(data)
    df.columns = schema[:len(df.columns)]
    df = df.reindex(columns=schema).fillna("")
    df["date_update"] = pd.to_datetime(df["date_update"], errors="coerce")
    return df[df["date_update"] >= pd.Timestamp("2025-01-01")]

@retry(wait=wait_exponential(multiplier=2, min=2, max=30),
       stop=stop_after_attempt(5),
       retry=retry_if_exception_type((gspread.exceptions.APIError, JSONDecodeError,
                                      ConnectionError, Timeout)))
def get_sheet_data(client, url, sheet_name, schema):
    sheet = client.open_by_url(url)
    df = read_worksheet(sheet, sheet_name, schema)
    return df

# =========================
# FETCH SONG SONG
# =========================
def fetch_all_sheets(client, sheet_tasks, schema, max_workers=5):
    all_data = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_sheet_data, client, url, name, schema): (url, name)
                   for url, name in sheet_tasks}
        for future in as_completed(futures):
            try:
                all_data.append(future.result())
            except Exception as e:
                url, name = futures[future]
                logging.error(f"❌ Failed sheet {name} in {url}: {e}")
                # append empty df để không bỏ sót index
                all_data.append(pd.DataFrame(columns=schema))
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame(columns=schema)

# =========================
# REMOVE DUPLICATES
# =========================
def remove_duplicates(df):
    df["ticket_id_numeric"] = pd.to_numeric(df["ticket_id"], errors="coerce").fillna(0)
    df = df.sort_values("ticket_id_numeric", ascending=False)
    df = df.drop_duplicates(subset=["source", "phone", "pic"], keep="first")
    df = df.drop(columns=["ticket_id_numeric"])
    return df

# =========================
# WRITE MASTER
# =========================
@retry(wait=wait_exponential(multiplier=2, min=2, max=30),
       stop=stop_after_attempt(5),
       retry=retry_if_exception_type((gspread.exceptions.APIError, JSONDecodeError,
                                      ConnectionError, Timeout)))
def write_master(ws, df):
    values = [df.columns.tolist()] + df.values.tolist()
    rate_limit_write()
    ws.clear()
    rate_limit_write()
    ws.batch_update([
        {"range": "A1:AO1", "values": [df.columns.tolist()]},
        {"range": "A2:AO{}".format(len(df)+1), "values": df.values.tolist()},
        {"range": "AR1:AS1", "values": [["channel_by_prod", "team"]]},
        {"range": "AR2", "values": [["=ARRAYFORMULA(IFNA(XLOOKUP(D2:D,Source!$A:$A,Source!$C:$C)))"]]},
        {"range": "AS2", "values": [["=ARRAYFORMULA(IFNA(XLOOKUP(AO2:AO,Info!$C:$C,Info!$N:$N)))"]]}
    ])

# =========================
# MAIN
# =========================
def main():
    client = GSpreadClientWithCache(authenticate_gspread())

    # đọc danh sách link
    link_sheet = client.open_by_url(LINK_SPREADSHEET_URL)
    ws_links = safe_worksheet(link_sheet, "Productivity File")
    df_links = pd.DataFrame(safe_get_all_records(ws_links))

    if not all(c in df_links.columns for c in REQUIRED_COLS):
        raise Exception("Thiếu cột trong Productivity File")

    # tạo tasks
    sheet_tasks = [(url, name) for url, names in zip(df_links["Link"], df_links[REQUIRED_COLS[1:]].values.tolist())
                   if isinstance(url, str) and url.strip()
                   for name in filter(None, names)]
    logging.info(f"Total sheets to fetch: {len(sheet_tasks)}")

    # fetch song song với retry từng sheet
    all_data = fetch_all_sheets(client, sheet_tasks, SCHEMA, max_workers=5)
    all_data = normalize_dates(all_data, DATE_COLS)
    all_data.fillna("", inplace=True)

    # remove duplicates
    all_data = remove_duplicates(all_data)

    # ghi vào Master
    master_sheet = client.open_by_url(MASTER_SPREADSHEET_URL)
    ws_master = safe_worksheet(master_sheet, "Productivity")
    write_master(ws_master, all_data)

    logging.info("✅ DONE")

if __name__ == "__main__":
    main()
