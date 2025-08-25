import os
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import logging
import time
from requests.exceptions import JSONDecodeError
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# ===== CONFIG =====
MAX_WORKERS_PER_BATCH = 10    # Số luồng tối đa mỗi batch
BATCH_QUOTA_LIMIT = 60        # Số request tối đa mỗi batch (Google Sheets quota per minute)
LOG_FILE = "log_process.log"

# ===== LOGGING =====
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ===== AUTH =====
def authenticate_gspread():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    return gspread.authorize(creds)

# ===== DATE PARSER =====
def try_parsing_date(text):
    for fmt in ('%y/%m/%d', '%Y/%m/%d', '%m/%d/%Y', '%m/%d/%y', '%d-%b-%y', '%d-%b-%Y', '%Y-%m-%d'):
        try:
            return pd.to_datetime(text, format=fmt)
        except ValueError:
            pass
    return pd.NaT

# ===== RETRY DECORATOR =====
@retry(
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((gspread.exceptions.APIError, JSONDecodeError)),
    reraise=True
)
def read_worksheet_with_retry(sheet, sheet_name, schema):
    worksheet = sheet.worksheet(sheet_name)
    data = worksheet.get('B8:AR')
    if not data:
        return pd.DataFrame(columns=schema)
    df = pd.DataFrame(data)
    df.columns = schema[:len(df.columns)]
    df = df.reindex(columns=schema)
    df = df.fillna('')
    df['date_update'] = df['date_update'].apply(try_parsing_date)
    df = df[df['date_update'] >= pd.Timestamp("2025-01-01")]
    return df

# ===== WORKER TASK =====
def process_one_sheet(client, url, sheet_name, schema):
    try:
        sheet = client.open_by_url(url)
        df = read_worksheet_with_retry(sheet, sheet_name, schema)
        logging.info(f"[OK] Sheet '{sheet_name}' trong file '{url}' ({len(df)} dòng).")
        return df
    except gspread.exceptions.WorksheetNotFound:
        logging.error(f"[MISS] Không tìm thấy sheet '{sheet_name}' trong file {url}")
    except Exception as e:
        logging.error(f"[ERR] Sheet '{sheet_name}' của {url}: {e}")
    return pd.DataFrame(columns=schema)

# ===== MAIN =====
def main():
    start_time = time.time()
    client = authenticate_gspread()

    # --- Đọc danh sách link ---
    link_spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/10eMZVnmtyyr5JAzDvpE5Brgh-8fw3lEKmGvL5m6eCUY')
    link_sheet = link_spreadsheet.worksheet("Productivity File")
    data = link_sheet.get_all_records()
    df_links = pd.DataFrame(data)

    required_cols = ['Link', 'Sheet 1', 'Sheet 2', 'Sheet 3', 'Sheet 4', 'Sheet 5']
    if not all(col in df_links.columns for col in required_cols):
        raise Exception("Thiếu cột trong Productivity File.")

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

    # --- Tạo task list ---
    tasks = []
    for _, row in df_links.iterrows():
        url = row['Link']
        if not url or not isinstance(url, str) or not url.strip():
            continue
        for name in [row['Sheet 1'], row['Sheet 2'], row['Sheet 3'], row['Sheet 4'], row['Sheet 5']]:
            if name:
                tasks.append((url, name))

    all_data = pd.DataFrame(columns=schema)
    total_tasks = len(tasks)
    completed = 0

    # --- Chia batch ---
    for i in range(0, total_tasks, BATCH_QUOTA_LIMIT):
        batch = tasks[i:i + BATCH_QUOTA_LIMIT]
        batch_start_time = time.time()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_PER_BATCH) as executor:
            futures = [executor.submit(process_one_sheet, client, url, name, schema) for url, name in batch]
            for future in as_completed(futures):
                df = future.result()
                all_data = pd.concat([all_data, df], ignore_index=True)
                completed += 1
                elapsed = time.time() - start_time
                eta = (elapsed / completed) * (total_tasks - completed) if completed else 0
                logging.info(f"Tiến độ: {completed}/{total_tasks} ({completed/total_tasks:.0%}) - ETA: {timedelta(seconds=int(eta))}")

        # Nếu batch chạy xong sớm thì nghỉ cho đủ 1 phút
        batch_elapsed = time.time() - batch_start_time
        if batch_elapsed < 60:
            sleep_time = 60 - batch_elapsed
            logging.info(f"Batch {i//BATCH_QUOTA_LIMIT+1} xong ({len(batch)} tasks). Nghỉ {sleep_time:.1f}s để tránh vượt quota...")
            time.sleep(sleep_time)

    # --- Ghi Master ---
    master_spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1VlXicEr1FGrpdDcRpuv1aE2TAG-7QHEfWKNtFJF4nc8')
    master_sheet = master_spreadsheet.worksheet("Test")
    master_sheet.clear()
    master_sheet.update([all_data.columns.values.tolist()] + all_data.values.tolist())

    logging.info(f"[DONE] {len(all_data)} dòng. Thời gian chạy: {timedelta(seconds=int(time.time()-start_time))}")

if __name__ == "__main__":
    main()
