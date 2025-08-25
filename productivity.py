import os
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import JSONDecodeError
from tenacity import retry, wait_exponential, wait_random, wait_chain, stop_after_attempt, retry_if_exception_type

# ========================== CONFIG ==========================
MAX_WORKERS = 5  # số luồng đọc song song
API_SLEEP_THRESHOLD = 60  # nghỉ khi gần quota
LOG_FILE = "log_process.log"

# ========================== LOGGING ==========================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ========================== AUTH ==========================
def authenticate_gspread():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    return gspread.authorize(creds)

# ========================== CACHED CLIENT ==========================
class GSpreadClientWithCache:
    def __init__(self, client):
        self.client = client
        self.cache = {}

    def open_by_url(self, url):
        if url not in self.cache:
            try:
                self.cache[url] = self.client.open_by_url(url)
                logging.info(f"Mở thành công spreadsheet: {url}")
            except gspread.exceptions.APIError as e:
                logging.error(f"Không thể mở bảng: {url}. Lỗi: {e}")
                self.cache[url] = None
        return self.cache[url]

# ========================== DATE PARSE ==========================
def try_parse_date(text):
    if not text:
        return pd.NaT
    for fmt in ('%Y-%m-%d', '%y/%m/%d', '%Y/%m/%d', '%m/%d/%Y', '%d-%b-%y', '%d-%b-%Y'):
        try:
            return pd.to_datetime(text, format=fmt)
        except ValueError:
            continue
    return pd.NaT

# ========================== RETRY WRAPPER ==========================
@retry(
    wait=wait_chain(
        wait_exponential(multiplier=1, min=1, max=60) + wait_random(0, 1)
    ),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((gspread.exceptions.APIError, JSONDecodeError)),
    reraise=True
)
def read_sheet_with_retry(sheet, sheet_name, schema):
    logging.info(f"Đang đọc sheet '{sheet_name}' từ B8:AR ...")
    ws = sheet.worksheet(sheet_name)
    data = ws.get("B8:AR")  # Chỉ lấy từ B8 đến AR

    if not data:
        logging.warning(f"Sheet {sheet_name} không có dữ liệu từ B8:AR")
        return pd.DataFrame(columns=schema)

    df = pd.DataFrame(data)
    logging.info(f"{sheet_name}: Đọc được {df.shape[0]} dòng, {df.shape[1]} cột")

    # Fix cứng số cột theo schema
    df = df.iloc[:, :len(schema)]
    df.columns = schema
    df = df.reindex(columns=schema).fillna('')

    # Chuẩn hóa cột date_update
    df['date_update'] = df['date_update'].apply(try_parse_date)
    df = df[df['date_update'] >= pd.Timestamp("2025-01-01")]
    logging.info(f"{sheet_name}: Sau khi lọc còn {len(df)} dòng hợp lệ")
    return df

# ========================== GET DATA ==========================
def get_sheet_data(client_cache, url, sheet_name, schema):
    sheet = client_cache.open_by_url(url)
    if sheet is None:
        return pd.DataFrame(columns=schema)

    try:
        return read_sheet_with_retry(sheet, sheet_name, schema)
    except gspread.exceptions.WorksheetNotFound:
        logging.error(f"Không tìm thấy sheet '{sheet_name}' trong {url}")
    except Exception as e:
        logging.error(f"Lỗi đọc sheet '{sheet_name}' trong {url}: {e}")
    return pd.DataFrame(columns=schema)

# ========================== CLEAN DATA ==========================
def clean_and_deduplicate(df):
    if df.empty:
        return df
    if all(col in df.columns for col in ['source', 'phone', 'pic', 'ticket_id']):
        df['ticket_id'] = pd.to_numeric(df['ticket_id'], errors='coerce').fillna(0)
        df.sort_values(by=['source', 'phone', 'pic', 'ticket_id'],
                       ascending=[True, True, True, False], inplace=True)
        df = df.drop_duplicates(subset=['source', 'phone', 'pic'], keep='first')

    date_cols = ["date_update", "date_cdd_applied", "recruiter_call_date",
                 "hm_interview_date", "offering_date", "accept_date", "onboard_date"]
    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].apply(try_parse_date).dt.strftime('%Y-%m-%d')

    df.replace([float('inf'), float('-inf')], '', inplace=True)
    df.fillna('', inplace=True)
    return df

# ========================== MAIN ==========================
def main():
    start_time = time.time()
    logging.info("=== Bắt đầu quá trình tổng hợp dữ liệu ===")
    client_cache = GSpreadClientWithCache(authenticate_gspread())

    # Lấy danh sách link
    link_spreadsheet = client_cache.open_by_url('https://docs.google.com/spreadsheets/d/10eMZVnmtyyr5JAzDvpE5Brgh-8fw3lEKmGvL5m6eCUY')
    link_sheet = link_spreadsheet.worksheet("Productivity File")
    df_links = pd.DataFrame(link_sheet.get_all_records())
    sheet_urls = df_links['Link'].tolist()
    sheet_names = df_links[['Sheet 1', 'Sheet 2', 'Sheet 3', 'Sheet 4', 'Sheet 5']].values.tolist()

    schema = [
        "date_update", "date_cdd_applied", "fullname", "source", "dob", "phone", "area",
        "address", "registration_area", "previous_work", "id_code", "note", "email", "rehire",
        "current_salary", "expected_ob_date", "position", "station_name", "storage",
        "reason_for_storage", "notes_for_recruitment", "recruiter_call", "recruiter_call_date",
        "recruiter_call_feedback", "recruiter_call_result", "hm_interview_date", "hm_interview",
        "hm_interview_feedback", "hm_interview_result", "offering", "offering_date", "accept",
        "accept_date", "onboard_date", "onboard", "reason_reject_ob", "finish_process",
        "fullname_ob", "phone_ob", "id_code_ob", "pic", "ticket_id", "rider_id"
    ]

    all_data = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for url, names in zip(sheet_urls, sheet_names):
            if not url.strip():
                continue
            for name in filter(None, names):
                logging.info(f"Đưa vào hàng đợi: {url} - {name}")
                futures.append(executor.submit(get_sheet_data, client_cache, url, name, schema))

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            all_data.append(result)
            logging.info(f"Đã xử lý xong {i}/{len(futures)} sheet")
            if i % API_SLEEP_THRESHOLD == 0:
                logging.info("Tạm nghỉ 70s để tránh quota limit...")
                time.sleep(70)

    all_data = pd.concat(all_data, ignore_index=True)
    all_data = clean_and_deduplicate(all_data)

    # Ghi về Master
    logging.info(f"Đang ghi dữ liệu tổng hợp ({len(all_data)} dòng) về Master...")
    master_spreadsheet = client_cache.open_by_url('https://docs.google.com/spreadsheets/d/1VlXicEr1FGrpdDcRpuv1aE2TAG-7QHEfWKNtFJF4nc8')
    master_sheet = master_spreadsheet.worksheet("Test")
    master_sheet.clear()
    master_sheet.update([all_data.columns.values.tolist()] + all_data.values.tolist())

    elapsed = time.time() - start_time
    logging.info(f"=== Hoàn thành! Tổng thời gian: {elapsed:.2f} giây, Tổng dòng: {len(all_data)} ===")

if __name__ == "__main__":
    main()
