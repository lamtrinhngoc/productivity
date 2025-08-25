import os
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from requests.exceptions import JSONDecodeError
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from collections import deque

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
# TOKEN BUCKET RATE LIMITER (60 requests / phút)
# =========================
RATE_LIMIT = 60
WINDOW = 60
tokens = deque()
lock = threading.Lock()

def rate_limiter():
    global tokens
    while True:
        with lock:
            now = time.time()
            # loại token cũ ngoài cửa sổ 60s
            while tokens and now - tokens[0] > WINDOW:
                tokens.popleft()

            if len(tokens) < RATE_LIMIT:
                tokens.append(now)
                return  # đủ quota thì cho qua

            sleep_time = WINDOW - (now - tokens[0]) + 0.1
        logging.info(f"⏳ Hết quota, chờ {sleep_time:.1f}s...")
        time.sleep(sleep_time)

# =========================
# AUTH
# =========================
def authenticate_gspread():
    creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    return gspread.authorize(creds)

class GSpreadClientWithCache:
    """Cache open_by_url để không tốn request"""
    def __init__(self, client):
        self.client = client
        self.cache = {}
        self.lock = threading.Lock()

    def open_by_url(self, url):
        with self.lock:
            if url not in self.cache:
                rate_limiter()
                self.cache[url] = self.client.open_by_url(url)
            return self.cache[url]

# =========================
# READ SHEETS
# =========================
@retry(
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((gspread.exceptions.APIError, JSONDecodeError)),
    reraise=False   # ❌ đổi từ True -> False để không crash khi retry hết số lần
)
def read_worksheet_with_retry(sheet, sheet_name, schema):
    rate_limiter()
    ws = sheet.worksheet(sheet_name)
    data = ws.get("B8:AR")
    if not data:
        return pd.DataFrame(columns=schema)
    df = pd.DataFrame(data)
    df.columns = schema[:len(df.columns)]
    df = df.reindex(columns=schema).fillna("")
    df["date_update"] = pd.to_datetime(df["date_update"], errors="coerce")
    return df[df["date_update"] >= pd.Timestamp("2025-01-01")]

def get_sheet_data(client, url, sheet_name, schema):
    try:
        sheet = client.open_by_url(url)
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

    # đọc link sheet
    link_spreadsheet = client.open_by_url(LINK_SPREADSHEET_URL)
    df_links = pd.DataFrame(link_spreadsheet.worksheet("Productivity File").get_all_records())

    if not all(c in df_links.columns for c in REQUIRED_COLS):
        raise Exception("Thiếu cột trong Productivity File")

    sheet_tasks = []
    for url, names in zip(df_links["Link"], df_links[REQUIRED_COLS[1:]].values.tolist()):
        if url and isinstance(url, str) and url.strip():
            for name in filter(None, names):
                sheet_tasks.append((url, name))

    logging.info(f"🔄 Tổng cộng {len(sheet_tasks)} sheet")

    # lấy data
    all_data = fetch_all_sheets(client, sheet_tasks, SCHEMA, max_workers=5)
    all_data = normalize_dates(all_data, DATE_COLS)
    all_data.replace([float("inf"), float("-inf")], "", inplace=True)
    all_data.fillna("", inplace=True)

    # update master
    master_spreadsheet = client.open_by_url(MASTER_SPREADSHEET_URL)
    ws = master_spreadsheet.worksheet("Test")

    values = [all_data.columns.tolist()] + all_data.values.tolist()
    ws.clear()
    ws.update(values)

    ws.batch_update([
        {"range": "AR1:AS1", "values": [["channel_by_prod", "team"]]},
        {"range": "AR2", "values": [["=ARRAYFORMULA(ifna(XLOOKUP(D2:D,Source!$A:$A,Source!$C:$C)))"]]},
        {"range": "AS2", "values": [["=ARRAYFORMULA(ifna(XLOOKUP(AO2:AO,Info!$C:$C,Info!$N:$N)))"]]},
    ])

    logging.info("✅ DONE")

if __name__ == "__main__":
    main()
