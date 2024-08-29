import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
import pandas as pd
import logging
import time
from datetime import datetime


# Cấu hình logging
logging.basicConfig(level=logging.INFO)

def main():
    # Xác thực và tạo client cho gspread
    
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file('credentials.json', scopes = scopes)
    client = gspread.authorize(creds)

    def open_spreadsheet_by_url(url):
        try:
            return client.open_by_url(url)
        except gspread.exceptions.APIError as e:
            logging.error(f"Không thể mở bảng. Lỗi: {e}")
            return None
            
    with open('link', 'r') as file:
        link_spreadsheet_url = file.read().strip()
    with open('master', 'r') as file:
        master_spreadsheet_url = file.read().strip()
        
    # Mở spreadsheet chứa danh sách các link
    link_spreadsheet = open_spreadsheet_by_url(link_spreadsheet_url)
    if link_spreadsheet is None:
        raise Exception("Không thể mở bảng chứa danh sách các link. Kiểm tra quyền truy cập và URL.")

    link_sheet = link_spreadsheet.worksheet("Overview")

    # Lấy dữ liệu từ cột D (Link) và các cột F, G, H (Tên sheet)
    data = link_sheet.get_all_records()
    df_links = pd.DataFrame(data)
    sheet_urls = df_links['Link'].tolist()
    sheet_names = df_links[['Sheet 1', 'Sheet 2', 'Sheet 3','Sheet 4','Sheet 5']].values.tolist()

    # Mở spreadsheet tổng
    master_spreadsheet = open_spreadsheet_by_url(master_spreadsheet_url)
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
    "fullname_ob", "phone_ob", "id_code_ob", "pic", "channel_by_prod"
    ]

    # Hàm để đọc dữ liệu từ một sheet và trả về DataFrame
    def get_sheet_data(url, sheet_name, schema):
        sheet = open_spreadsheet_by_url(url)
        if sheet is None:
            return pd.DataFrame(columns=schema)
        try:
            worksheet = sheet.worksheet(sheet_name)
            data = worksheet.get('B8:AP')
            df = pd.DataFrame(data)  # Chuyển dữ liệu thành DataFrame
            
            # Chỉ loại bỏ các dòng mà tất cả các ô từ cột B đến cột E trống
            df.dropna(subset=df.columns[1:5], how='all', inplace=True)

            # Đảm bảo rằng DataFrame có các cột theo schema
            df.columns = schema[:len(df.columns)]
            df = df.reindex(columns=schema)
            return df
        except gspread.exceptions.WorksheetNotFound:
            logging.error(f"Không tìm thấy sheet với tên {sheet_name}")
            return pd.DataFrame()

    # Tổng hợp dữ liệu từ tất cả các sheet
    all_data = pd.DataFrame(columns=schema)
    api_call_count = 0

    for url, names in zip(sheet_urls, sheet_names):
        for name in names:
            if name:  # Chỉ lấy dữ liệu nếu tên sheet không rỗng
                logging.info(f"Đang xử lý sheet '{name}'")
                sheet_data = get_sheet_data(url, name, schema)
                all_data = pd.concat([all_data, sheet_data], ignore_index=True)
                api_call_count += 1
                
                # Delay sau mỗi 20 lần gọi API
                if api_call_count % 15 == 0:
                    logging.info("Đã gọi API 15 lần, chờ 1 phút trước khi tiếp tục...")
                    time.sleep(70)  # Chờ 1 phút

    # Ghi dữ liệu tổng hợp vào sheet tổng
    for col in ["date_update", "date_cdd_applied", "recruiter_call_date", "hm_interview_date", "offering_date", "accept_date", "onboard_date"]:
        all_data[col] = pd.to_datetime(all_data[col], errors='coerce').dt.strftime('%Y-%m-%d')
    all_data.replace([float('inf'), float('-inf')], '', inplace=True)
    all_data.fillna('', inplace=True)
    master_sheet.clear()  # Xóa dữ liệu cũ
    master_sheet.update([all_data.columns.values.tolist()] + all_data.values.tolist())
    master_sheet.update_cell(1, 42, '=ARRAYFORMULA(ifna(XLOOKUP(D1:D,Source!$A:$A,Source!$C:$C)))')
    
    logging.info("Dữ liệu đã được tổng hợp thành công vào Master Spreadsheet!")

if __name__ == "__main__":
    main()
