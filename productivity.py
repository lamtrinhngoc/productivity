import os
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import logging
import time
from datetime import datetime
from requests.exceptions import JSONDecodeError

# Cấu hình logging
logging.basicConfig(level=logging.INFO)

def authenticate_gspread():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    return gspread.authorize(creds)

def open_spreadsheet_by_url(client, url):
    try:
        return client.open_by_url(url)
    except gspread.exceptions.APIError as e:
        logging.error(f"Không thể mở bảng. Lỗi: {e}")
        return None

def get_sheet_data(client, url, sheet_name, schema, retries=5):
    sheet = open_spreadsheet_by_url(client, url)
    if sheet is None:
        return pd.DataFrame(columns=schema)
    
    attempt = 0
    while attempt < retries:
        try:
            worksheet = sheet.worksheet(sheet_name)
            data = worksheet.get('B8:AP')
            df = pd.DataFrame(data)
            df.dropna(subset=df.columns[1:8], how='all', inplace=True)
            df.columns = schema[:len(df.columns)]
            df = df.reindex(columns=schema)
            return df
        except gspread.exceptions.WorksheetNotFound:
            logging.error(f"Không tìm thấy sheet với tên {sheet_name}")
            return pd.DataFrame(columns=schema)
        except JSONDecodeError as e:
            logging.error(f"Lỗi JSONDecodeError khi đọc dữ liệu từ {sheet_name}: {e}")
        except gspread.exceptions.APIError as e:
            logging.error(f"Lỗi API khi đọc dữ liệu từ {sheet_name}: {e}")
        except Exception as e:
            logging.error(f"Lỗi không mong muốn khi đọc dữ liệu từ {sheet_name}: {e}")
        
        attempt += 1
        logging.info(f"Thử lại lần {attempt} cho sheet '{sheet_name}'")
        time.sleep(5)
    
    logging.error(f"Không thể đọc dữ liệu từ sheet '{sheet_name}' sau {retries} lần thử")
    return pd.DataFrame(columns=schema)

def try_parsing_date(text):
    for fmt in ('%y/%m/%d', '%Y/%m/%d', '%m/%d/%Y', '%m/%d/%y', '%d-%b-%y', '%d-%b-%Y'):
        try:
            return pd.to_datetime(text, format=fmt)
        except ValueError:
            pass
    return pd.NaT

def main():
    client = authenticate_gspread()

    link_spreadsheet = open_spreadsheet_by_url(client, 'https://docs.google.com/spreadsheets/d/10eMZVnmtyyr5JAzDvpE5Brgh-8fw3lEKmGvL5m6eCUY/edit?gid=0#gid=0')
    if link_spreadsheet is None:
        raise Exception("Không thể mở bảng chứa danh sách các link. Kiểm tra quyền truy cập và URL.")

    link_sheet = link_spreadsheet.worksheet("Overview")
    data = link_sheet.get_all_records()
    df_links = pd.DataFrame(data)
    sheet_urls = df_links['Link'].tolist()
    sheet_names = df_links[['Sheet 1', 'Sheet 2', 'Sheet 3', 'Sheet 4', 'Sheet 5']].values.tolist()

    master_spreadsheet = open_spreadsheet_by_url(client, 'https://docs.google.com/spreadsheets/d/1VlXicEr1FGrpdDcRpuv1aE2TAG-7QHEfWKNtFJF4nc8/edit?gid=0#gid=0')
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
        "fullname_ob", "phone_ob", "id_code_ob", "pic"
    ]

    all_data = pd.DataFrame(columns=schema)
    api_call_count = 0

    for url, names in zip(sheet_urls, sheet_names):
        for name in names:
            if name:
                logging.info(f"Đang xử lý sheet '{name}'")
                sheet_data = get_sheet_data(client, url, name, schema)
                all_data = pd.concat([all_data, sheet_data], ignore_index=True)
                api_call_count += 1
                
                if api_call_count % 15 == 0:
                    logging.info("Đã gọi API 15 lần, chờ 1 phút trước khi tiếp tục...")
                    time.sleep(70)

    for col in ["date_update", "date_cdd_applied", "recruiter_call_date", "hm_interview_date", "offering_date", "accept_date", "onboard_date"]:
        all_data[col] = all_data[col].apply(try_parsing_date).dt.strftime('%Y-%m-%d')
    
    all_data.replace([float('inf'), float('-inf')], '', inplace=True)
    all_data.fillna('', inplace=True)
    master_sheet.clear()
    master_sheet.update([all_data.columns.values.tolist()] + all_data.values.tolist())
    master_sheet.update_cell(2, 42, '=ARRAYFORMULA(ifna(XLOOKUP(D2:D,Source!$A:$A,Source!$C:$C)))')
    master_sheet.update_cell(1, 42, 'channel_by_prod')
    
    logging.info("Dữ liệu đã được tổng hợp thành công vào Master Spreadsheet!")

if __name__ == "__main__":
    main()
