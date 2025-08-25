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

# =========================
# TOKEN BUCKET RATE LIMITER (separate READ / WRITE)
# =========================
READ_RATE_LIMIT = int(os.getenv("GSHEETS_READ_RPM", "59"))   # 59 để có buffer nhỏ
WRITE_RATE_LIMIT = int(os.getenv("GSHEETS_WRITE_RPM", "60"))
WINDOW = 60.0

_read_tokens = deque()
_write_tokens = deque()
_lock = threading.Lock()

def _acquire_token(tokens: deque, limit: int, kind: str):
    """Token bucket with monotonic clock, non-recursive."""
    while True:
        with _lock:
            now = time.monotonic()
            # Remove expired tokens
            while tokens and (now - tokens[0]) > WINDOW:
                tokens.popleft()

            if len(tokens) < limit:
                tokens.append(now)
                return  # allowed

            # Need to wait until the oldest token expires
            wait_seconds = WINDOW - (now - tokens[0]) + 0.01
        if wait_seconds > 0:
            logging.info(f"⏳ {kind} quota full ({len(tokens)}/{limit}). Sleeping {wait_seconds:.2f}s...")
            time.sleep(wait_seconds)
        else:
            # edge case guard
            time.sleep(0.01)

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
    """Cache Spreadsheet objects, và bọc tất cả lệnh đọc bằng limiter."""
    def __init__(self, client):
        self.client = client
        self._ss_cache = {}
        self._lock = threading.Lock()

    def open_by_url(self, url):
        with self._lock:
            if url not in self._ss_cache:
                rate_limit_read()  # READ
                self._ss_cache[url] = self.client.open_by_url(url)
            return self._ss_cache[url]

# =========================
# SAFE WRAPPERS CHO CÁC LỆNH GSPREAD (đếm đúng từng READ)
# =========================
def safe_worksheet(spreadsheet, name: str):
    rate_limit_read()  # READ
    return spreadsheet.worksheet(name)

def safe_get_range(worksheet, rng: str):
    rate_limit_read()  # READ
    return worksheet.get(rng)

def safe_get_all_records(worksheet):
    rate_limit_read()  # READ
    return worksheet.get_all_records()

# =========================
# READ SHEETS (với retry)
# =========================
@retry(
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((gspread.exceptions.APIError, JSONDecodeError)),
    reraise=False  # sau 5 lần vẫn lỗi, return rỗng để job không chết
)
def read_worksheet_with_retry(sheet, sheet_name, schema):
    ws = safe_worksheet(sheet, sheet_name)        # READ
    data = safe_get_range(ws, "B8:AR")            # READ
    if not data:
        return pd.DataFrame(columns=schema)
    df = pd.DataFrame(data)
    df.columns = schema[:len(df.columns)]
    df = df.reindex(columns=schema).fillna("")
    df["date_update"] = pd.to_datetime(df["date_update"], errors="coerce")
    return df[df["date_update"] >= pd.Timestamp("2025-01-01")]

def get_sheet_data(client, url, sheet_name, schema):
    try:
        sheet = client.open_by_url(url)                       # READ (cached lần 1)
        df = read_worksheet_with_retry(sheet, sheet_name, schema)
        logging.info(f"✅ {sheet_name} từ {url}")
        return df
    except gspread.exceptions.WorksheetNotFound:
        logging.error(f"❌ Không tìm thấy sheet {sheet_name} trong {url}")
    except Exception as e:
        logging.error(f"❌ Lỗi sheet {sheet_name} từ {url}: {e}")
    return pd.DataFrame(columns=schema)

# =========================
# FETCH SONG SONG
# =========================
def fetch_all_sheets(client, sheet_tasks, schema, max_workers=5):
    all_data = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_sheet_data, client, url, name, schema): (url, name) for url, name in sheet_tasks}
        for future in as_completed(futures):
            try:
                all_data.append(future.result())
            except Exception as e:
                url, name = futures[future]
                logging.error(f"❌ Task fail {name} trong {url}: {e}")
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame(columns=schema)

# =========================
# CLEAN DATA
# =========================
def normalize_dates(df, date_cols):
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
    return df

# =========================
# MAIN
# =========================
def main():
    client = GSpreadClientWithCache(authenticate_gspread())

    # Đọc file danh sách link
    link_spreadsheet = client.open_by_url(LINK_SPREADSHEET_URL)   # READ (cached)
    ws_links = safe_worksheet(link_spreadsheet, "Productivity File")  # READ
    data_links = safe_get_all_records(ws_links)                       # READ
    df_links = pd.DataFrame(data_links)

    if not all(c in df_links.columns for c in REQUIRED_COLS):
        raise Exception("Thiếu cột trong Productivity File")

    # Tạo task theo từng sheet
    sheet_tasks = []
    for url, names in zip(df_links["Link"], df_links[REQUIRED_COLS[1:]].values.tolist()):
        if url and isinstance(url, str) and url.strip():
            for name in filter(None, names):
                sheet_tasks.append((url, name))

    logging.info(f"🔄 Tổng cộng {len(sheet_tasks)} sheet")

    # Lấy data song song (limiter sẽ điều tiết để không vượt READ RPM)
    all_data = fetch_all_sheets(client, sheet_tasks, SCHEMA, max_workers=5)
    all_data = normalize_dates(all_data, DATE_COLS)
    all_data.replace([float("inf"), float("-inf")], "", inplace=True)
    all_data.fillna("", inplace=True)

    # Ghi vào Master (WRITE). Lưu ý: lấy worksheet là READ, còn clear/update là WRITE.
    master_spreadsheet = client.open_by_url(MASTER_SPREADSHEET_URL)     # READ (cached)
    ws_master = safe_worksheet(master_spreadsheet, "Productivity")              # READ

    values = [all_data.columns.tolist()] + all_data.values.tolist()
    rate_limit_write(); ws_master.clear()    # WRITE
    rate_limit_write(); ws_master.update(values)  # WRITE

    # Thêm công thức (WRITE – gom vào batch_update 1 lần nếu muốn)
    rate_limit_write()
    ws_master.batch_update([
        {"range": "AR1:AS1", "values": [["channel_by_prod", "team"]]},
        {"range": "AR2", "values": [["=ARRAYFORMULA(ifna(XLOOKUP(D2:D,Source!$A:$A,Source!$C:$C)))"]]},
        {"range": "AS2", "values": [["=ARRAYFORMULA(ifna(XLOOKUP(AO2:AO,Info!$C:$C,Info!$N:$N)))"]]},
    ])

    logging.info("✅ DONE")

if __name__ == "__main__":
    main()

