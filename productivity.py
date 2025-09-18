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
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type

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
# TOKEN BUCKET RATE LIMITER
# =========================
READ_RATE_LIMIT = int(os.getenv("GSHEETS_READ_RPM", "56"))   # 55 để có buffer
WRITE_RATE_LIMIT = int(os.getenv("GSHEETS_WRITE_RPM", "56"))
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
            wait_seconds = WINDOW - (now - tokens[0]) + 0.1
        time.sleep(wait_seconds if wait_seconds > 0 else 0.1)

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
        self._ws_cache = {}
        self._lock = threading.Lock()

    def open_by_url(self, url):
        with self._lock:
            if url not in self._ss_cache:
                rate_limit_read()
                self._ss_cache[url] = self.client.open_by_url(url)
            return self._ss_cache[url]

    def worksheet(self, spreadsheet, name: str):
        key = (spreadsheet.id, name)
        with self._lock:
            if key not in self._ws_cache:
                rate_limit_read()
                self._ws_cache[key] = spreadsheet.worksheet(name)
            return self._ws_cache[key]

# =========================
# SAFE WRAPPERS
# =========================
def safe_get_range(worksheet, rng: str):
    rate_limit_read()
    return worksheet.get(rng)

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
# READ SHEETS (retry)
# =========================
@retry(
    wait=wait_fixed(3),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type((gspread.exceptions.APIError, JSONDecodeError)),
    reraise=True
)
def read_worksheet_with_retry(ws, schema):
    data = safe_get_range(ws, "B8:AR")
    if not data:
        return []
    df = pd.DataFrame(data)
    df.columns = schema[:len(df.columns)]
    df = df.reindex(columns=schema).fillna("")
    df["date_update"] = df['date_update'].apply(try_parsing_date)
    df = df[df["date_update"] >= pd.Timestamp("2025-01-01")]
    return df.to_dict("records")

def get_sheet_data(client, url, sheet_name, schema, error_log):
    try:
        sheet = client.open_by_url(url)
        ws = client.worksheet(sheet, sheet_name)
        rows = read_worksheet_with_retry(ws, schema)
        logging.info(f"✅ {sheet_name} từ {url}")
        return rows
    except gspread.exceptions.WorksheetNotFound:
        logging.error(f"❌ Không tìm thấy sheet {sheet_name} trong {url}")
    except Exception as e:
        logging.error(f"❌ Lỗi sheet {sheet_name} từ {url}: {e}")
        error_log.append((url, sheet_name))
    return []

# =========================
# FETCH SONG SONG
# =========================
def fetch_all_sheets(client, sheet_tasks, schema, max_workers=4, max_rounds=5):
    all_rows = []
    error_log = sheet_tasks[:]

    for round_no in range(1, max_rounds + 1):
        if not error_log:
            break

        logging.info(f"🔄 Bắt đầu vòng {round_no}, còn {len(error_log)} sheet lỗi cần retry")
        current_errors = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(get_sheet_data, client, url, name, schema, current_errors): (url, name)
                for url, name in error_log
            }
            for future in as_completed(futures):
                try:
                    rows = future.result()
                    all_rows.extend(rows)
                except Exception as e:
                    url, name = futures[future]
                    logging.error(f"❌ Task fail {name} trong {url}: {e}")
                    current_errors.append((url, name))

        error_log = current_errors
        if error_log:
            sleep_time = 5 * round_no
            logging.warning(f"⚠️ Vẫn còn {len(error_log)} sheet lỗi, chờ {sleep_time}s rồi retry...")
            time.sleep(sleep_time)

    if error_log:
        logging.error(f"❌ Sau {max_rounds} vòng vẫn còn {len(error_log)} sheet lỗi:")
        for url, name in error_log:
            logging.error(f"   - {url} :: {name}")

    df_all = pd.DataFrame.from_records(all_rows, columns=schema)
    return df_all, error_log

# =========================
# CLEAN DATA
# =========================
def normalize_dates(df, date_cols):
    # Chuẩn hoá các cột ngày
    for col in date_cols:
        df[col] = df[col].apply(try_parsing_date).dt.strftime('%Y-%m-%d')
        df[col] = df[col].fillna("")

    # Chuẩn hoá ticket_id
    if "ticket_id" not in df.columns:
        df["ticket_id"] = -1
    else:
        df["ticket_id"] = pd.to_numeric(df["ticket_id"], errors="coerce").fillna(-1)

    # Chuẩn hoá phone: chỉ giữ 9 số cuối
    if "phone" in df.columns:
        df["phone"] = df["phone"].astype(str).str.replace(r"\D", "", regex=True)  # chỉ giữ số
        df["phone"] = df["phone"].str[-9:]                                        # lấy 9 số cuối
        df["phone"] = df["phone"].replace(["nan", "NaN", "None"], "").fillna("")

    # Lọc trùng theo phone + source + pic (lấy ticket_id lớn nhất)
    if {"phone", "pic"}.issubset(df.columns):
        idx = df.groupby(["phone", "pic"])["ticket_id"].idxmax()
        df = df.loc[idx].reset_index(drop=True)

    return df

# =========================
# MAIN
# =========================
def main():
    client = GSpreadClientWithCache(authenticate_gspread())

    # đọc danh sách link
    link_spreadsheet = client.open_by_url(LINK_SPREADSHEET_URL)
    ws_links = client.worksheet(link_spreadsheet, "Productivity File")
    data_links = ws_links.get_all_records()
    df_links = pd.DataFrame(data_links)

    if not all(c in df_links.columns for c in REQUIRED_COLS):
        raise Exception("Thiếu cột trong Productivity File")

    # tạo tasks
    sheet_tasks = []
    for url, names in zip(df_links["Link"], df_links[REQUIRED_COLS[1:]].values.tolist()):
        if url and isinstance(url, str) and url.strip():
            for name in filter(None, names):
                sheet_tasks.append((url, name))

    logging.info(f"🔄 Tổng cộng {len(sheet_tasks)} sheet")

    # lấy data song song
    all_data, error_log = fetch_all_sheets(client, sheet_tasks, SCHEMA, max_workers=6)

    if error_log:
        logging.warning(f"🔄 Thử chạy lại {len(error_log)} sheet lỗi")
        retry_data, retry_error = fetch_all_sheets(client, error_log, SCHEMA, max_workers=4)
        all_data = pd.concat([all_data, retry_data], ignore_index=True)
        if retry_error:
            logging.error(f"⚠️ Vẫn còn {len(retry_error)} sheet lỗi sau khi retry: {retry_error}")

    all_data = normalize_dates(all_data, DATE_COLS)
    all_data.replace([float("inf"), float("-inf")], "", inplace=True)
    all_data.fillna("", inplace=True)

    # ghi vào Master
    master_spreadsheet = client.open_by_url(MASTER_SPREADSHEET_URL)
    ws_master = client.worksheet(master_spreadsheet, "Productivity")

    values = [all_data.columns.tolist()] + all_data.values.tolist()

    rate_limit_write()
    ws_master.clear()
    ws_master.batch_update([
        {"range": "A1", "values": values},
        {"range": "AR1:AS1", "values": [["channel_by_prod", "team"]]},
        {"range": "AR2", "values": [["=ARRAYFORMULA(IFNA(XLOOKUP(D2:D,Source!$A:$A,Source!$C:$C)))"]]},
        {"range": "AS2", "values": [["=ARRAYFORMULA(IFNA(XLOOKUP(AO2:AO,Info!$C:$C,Info!$N:$N)))"]]}
    ], value_input_option="USER_ENTERED")

    logging.info("✅ DONE")

if __name__ == "__main__":
    main()





