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

# =========================
# CẤU HÌNH
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
# RATE LIMITER (tối đa 50 requests/phút để an toàn)
# =========================
RATE_LIMIT = 50
WINDOW = 60
api_call_times = []
api_lock = threading.Lock()

def rate_limiter():
    """Đảm bảo không vượt quá quota Google Sheets"""
    global api_call_times
    with api_lock:
        now = time.time()
        api_call_times = [t for t in api_call_times if now - t < WINDOW]

        if len(api_call_times) >= RATE_LIMIT:
            sleep_time = WINDOW - (now - api_call_times[0]) + 1
            logging.warning(f"Quota gần đầy ({len(api_call_times)}/60). Đang chờ {sleep_time:.1f}s...")
            time.sleep(sleep_time)
            now = time.time()
            api_call_times = [t for t in api_call_times if now - t < WINDOW]

        api_call_times.append(now)

# =========================
# GSPREAD CLIENT
# =========================
def authenticate_gspread():
    creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    return gspread.authorize(creds)

class GSpreadClientWithCache:
    """Cache để tránh gọi open_by_url nhiều lần"""
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
# ĐỌC DỮ LIỆU SHEET
# =========================
@retry(
    wait=wait_exponential(multiplier=2, min=4, max=60),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((gspread.exceptions.APIError, JSONDecodeError)),
    reraise=True
)
def read_worksheet_with_retry(sheet, sheet_name, schema):
    rate_limiter()
    worksheet = sheet.worksheet(sheet_name)
    data = worksheet.get("B8:AR")
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
        logging.info(f"✅ Hoàn thành sheet '{sheet_name}' trong {url}")
        return df
    except gspread.exceptions.WorksheetNotFound:
        logging.error(f"❌ Không tìm thấy sheet '{sheet_name}' trong {url}")
    except Exception as e:
        logging.error(f"❌ Lỗi khi đọc sheet '{sheet_name}' từ {url}: {e}")
    return pd.DataFrame(columns=schema)

# =========================
# CHẠY SONG SONG THEO NHÓM (batch)
# =========================
def fetch_all_sheets(client, sheet_tasks, schema, max_workers=3, batch_size=10):
    all_data = []
    for i in range(0, len(sheet_tasks), batch_size):
        batch = sheet_tasks[i:i+batch_size]
        logging.info(f"🔄 Đang xử lý batch {i//batch_size+1}/{(len(sheet_tasks)-1)//batch_size+1} ({len(batch)} sheets)")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(get_sheet_data, client, url, name, schema): (url, name) for url, name in batch}
            for future in as_completed(futures):
                try:
                    all_data.append(future.result())
                except Exception as e:
                    url, name = futures[future]
                    logging.error(f"❌ Task thất bại {name} trong {url}: {e}")

        # nghỉ giữa các batch để tránh quota
        if i + batch_size < len(sheet_tasks):
            logging.info("⏳ Nghỉ 70s để tránh quota...")
            time.sleep(70)

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame(columns=schema)

# =========================
# CHUẨN HÓA DỮ LIỆU
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

    # --- Mở bảng links
    link_spreadsheet = client.open_by_url(LINK_SPREADSHEET_URL)
    df_links = pd.DataFrame(link_spreadsheet.worksheet("Productivity File").get_all_records())

    if not all(col in df_links.columns for col in REQUIRED_COLS):
        raise Exception("Thiếu cột trong Productivity File.")

    sheet_urls = df_links["Link"].tolist()
    sheet_names = df_links[REQUIRED_COLS[1:]].values.tolist()

    # Chuẩn bị danh sách task
    sheet_tasks = []
    for url, names in zip(sheet_urls, sheet_names):
        if url and isinstance(url, str) and url.strip():
            for name in filter(None, names):
                sheet_tasks.append((url, name))

    # --- Chạy song song để lấy dữ liệu
    logging.info(f"🔄 Tổng cộng {len(sheet_tasks)} sheet cần xử lý...")
    all_data = fetch_all_sheets(client, sheet_tasks, SCHEMA, max_workers=3, batch_size=10)

    # Chuẩn hóa ngày tháng
    all_data = normalize_dates(all_data, DATE_COLS)
    all_data.replace([float("inf"), float("-inf")], "", inplace=True)
    all_data.fillna("", inplace=True)

    # --- Mở bảng Master
    master_spreadsheet = client.open_by_url(MASTER_SPREADSHEET_URL)
    master_sheet = master_spreadsheet.worksheet("Test")

    # --- Ghi dữ liệu vào master
    values = [all_data.columns.tolist()] + all_data.values.tolist()
    master_sheet.clear()
    master_sheet.update(values)

    # Thêm công thức
    master_sheet.batch_update([
        {"range": "AR1:AS1", "values": [["channel_by_prod", "team"]]},
        {"range": "AR2", "values": [["=ARRAYFORMULA(ifna(XLOOKUP(D2:D,Source!$A:$A,Source!$C:$C)))"]]},
        {"range": "AS2", "values": [["=ARRAYFORMULA(ifna(XLOOKUP(AO2:AO,Info!$C:$C,Info!$N:$N)))"]]},
    ])

    logging.info("✅ Dữ liệu đã được tổng hợp thành công vào Master Spreadsheet!")

if __name__ == "__main__":
    main()
