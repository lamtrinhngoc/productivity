import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
import logging
import time

# Thông tin xác thực
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
            logging.error(f"Không thể mở bảng với URL {url}. Lỗi: {e}")
            return None

    # Data sheet all member to dataframe
    all_member_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1VlXicEr1FGrpdDcRpuv1aE2TAG-7QHEfWKNtFJF4nc8/edit?gid=0#gid=0')
    all_member_productivity = all_member_spreadsheet.worksheet("Productivity")
    master_data = all_member_productivity.get_all_records()
    df_all_member_productivity = pd.DataFrame(master_data)
    df_all_member_productivity = df_all_member_productivity.astype(str)
    df_all_member_productivity['phone'] = df_all_member_productivity.apply(lambda row: row['phone_ob'][-9:] if row['phone_ob'] else row['phone'][-9:], axis=1)
    df_all_member_productivity['position'] = df_all_member_productivity['position'].apply(lambda x: 'Rider' if 'Rider' in x else 'FTE Staff' if 'Staff' in x else 'Driver' if 'Driver' in x else None)
    date_columns = ['date_update', 'recruiter_call_date', 'hm_interview_date', 'offering_date', 'accept_date', 'onboard_date']
    filter_datetime = df_all_member_productivity[
    (df_all_member_productivity['team'] == "Hoa Bui") |
    (df_all_member_productivity['team'] == "Gia Han") |
    (df_all_member_productivity['position'].str.contains("Driver", na=False))
    ]
    for col in date_columns:
        filter_datetime[col] = filter_datetime[col].dt.strftime('%Y-%m-%d')
    filter_datetime = filter_datetime.replace({np.nan: '', np.inf: '', -np.inf: ''})
    # [WFA] Performance Management | Nationwide
    
    # Open the target spreadsheet
nationwide_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1zHEWFEwyZ6zq88hUvQfEzqwipCnHpdaovIl8hXFhPPw/edit?gid=714896083#gid=714896083')
nationwide_sheet = nationwide_spreadsheet.worksheet("Test")

# Update the sheet with the filtered data
nationwide_sheet.update(
    [filter_datetime.columns.values.tolist()] + filter_datetime.values.tolist(),
    value_input_option=gspread.utils.ValueInputOption.user_entered
)
if __name__ == "__main__":
    main()
