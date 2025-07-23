import os
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import logging
import time
from requests.exceptions import JSONDecodeError
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# Cấu hình logging
logging.basicConfig(level=logging.INFO)

def authenticate_gspread():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    return gspread.authorize(creds)

class GSpreadClientWithCache:
    def __init__(self, client):
        self.client = client
        self.cache = {}

    def open_by_url(self, url):
        if url not in self.cache:
            try:
                self.cache[url] = self.client.open_by_url(url)
            except gspread.exceptions.APIError as e:
                logging.error(f"Không thể mở bảng. Lỗi: {e}")
                self.cache[url] = None
        return self.cache[url]

@retry(
    wait=wait_exponential(multiplier=2, min=4, max=60),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((gspread.exceptions.APIError, JSONDecodeError)),
    reraise=True
)
def read_worksheet_with_retry(sheet, sheet_name, schema):
    worksheet = sheet.worksheet(sheet_name)
    data = worksheet.get('B8:AP') 
    if not data:
        return pd.DataFrame(columns=schema)
    df = pd.DataFrame(data)
    df.columns = schema[:len(df.columns)]
    df = df.reindex(columns=schema)
    df['date_update'] = df['date_update'].apply(try_parsing_date)
    df = df[df['date_update'] >= pd.Timestamp("2025-01-01")]
    return df

def get_sheet_data(client, url, sheet_name, schema):
    sheet = client.open_by_url(url)
    if sheet is None:
        return pd.DataFrame(columns=schema)

    try:
        df = read_worksheet_with_retry(sheet, sheet_name, schema)
        return df
    except gspread.exceptions.WorksheetNotFound:
        logging.error(f"Không tìm thấy sheet với tên {sheet_name}")
    except Exception as e:
        logging.error(f"Lỗi khi đọc sheet '{sheet_name}': {e}")

    return pd.DataFrame(columns=schema)

def try_parsing_date(text):
    for fmt in ('%y/%m/%d', '%Y/%m/%d', '%m/%d/%Y', '%m/%d/%y', '%d-%b-%y', '%d-%b-%Y'):
        try:
            return pd.to_datetime(text, format=fmt)
        except ValueError:
            pass
    return pd.NaT

def main():
    client = GSpreadClientWithCache(authenticate_gspread())

    # Mở bảng danh sách links
    link_spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/10eMZVnmtyyr5JAzDvpE5Brgh-8fw3lEKmGvL5m6eCUY')
    if link_spreadsheet is None:
        raise Exception("Không thể mở bảng chứa danh sách các link. Kiểm tra quyền truy cập và URL.")

    link_sheet = link_spreadsheet.worksheet("Productivity File")
    data = link_sheet.get_all_records()
    df_links = pd.DataFrame(data)

    required_cols = ['Link', 'Sheet 1', 'Sheet 2', 'Sheet 3', 'Sheet 4', 'Sheet 5']
    if not all(col in df_links.columns for col in required_cols):
        raise Exception("Thiếu cột trong Productivity File. Kiểm tra lại.")

    sheet_urls = df_links['Link'].tolist()
    sheet_names = df_links[['Sheet 1', 'Sheet 2', 'Sheet 3', 'Sheet 4', 'Sheet 5']].values.tolist()

    # Mở bảng tổng
    master_spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1VlXicEr1FGrpdDcRpuv1aE2TAG-7QHEfWKNtFJF4nc8')
    if master_spreadsheet is None:
        raise Exception("Không thể mở bảng tổng. Kiểm tra quyền truy cập và URL.")

    master_sheet = master_spreadsheet.worksheet("Productivity")
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

    all_data = pd.DataFrame(columns=schema)
    api_call_count = 0

    for url, names in zip(sheet_urls, sheet_names):
        if not url or not isinstance(url, str) or not url.strip():
            logging.warning("Bỏ qua một dòng vì không có URL hợp lệ.")
            continue  # Bỏ qua nếu URL trống
            
        for name in names:
            if name:
                logging.info(f"Đang xử lý sheet '{name}'")
                sheet_data = get_sheet_data(client, url, name, schema)
                all_data = pd.concat([all_data, sheet_data], ignore_index=True)
                api_call_count += 1
                
                if api_call_count % 15 == 0:
                    logging.info("Đã gọi API 15 lần, chờ 1 phút trước khi tiếp tục...")
                    time.sleep(70)

    # Xử lý ngày tháng
    for col in ["date_update", "date_cdd_applied", "recruiter_call_date", "hm_interview_date", "offering_date", "accept_date", "onboard_date"]:
        all_data[col] = all_data[col].apply(try_parsing_date).dt.strftime('%Y-%m-%d')

    all_data.replace([float('inf'), float('-inf')], '', inplace=True)
    all_data.fillna('', inplace=True)

    # Ghi dữ liệu vào master
    master_sheet.clear()
    master_sheet.update([all_data.columns.values.tolist()] + all_data.values.tolist())
    master_sheet.batch_update([{
        'range': 'AR1:AS1',
        'values': [['channel_by_prod', 'team']]
    }])
    master_sheet.values_update(
        'AR2',
        params={'valueInputOption': 'USER_ENTERED'},
        body={'values': [['=ARRAYFORMULA(IFNA(XLOOKUP(D2:D, Source!A:A, Source!C:C)))']]}
    )
    
    master_sheet.values_update(
        'AS2',
        params={'valueInputOption': 'USER_ENTERED'},
        body={'values': [['=ARRAYFORMULA(IFNA(XLOOKUP(AO2:AO, Info!C:C, Info!N:N)))']]}
    )

    logging.info("Dữ liệu đã được tổng hợp thành công vào Master Spreadsheet!")

if __name__ == "__main__":
    main()
