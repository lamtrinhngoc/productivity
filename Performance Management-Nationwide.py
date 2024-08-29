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
    df_all_member_productivity['area'] = df_all_member_productivity['area'].apply(lambda x: 'South' if x in ['SE', 'SW'] else x)
    df_all_member_productivity['station_type'] = np.where(
        df_all_member_productivity['station_name'].str.contains('SOC', na=False), 'SOC',
        np.where(
            df_all_member_productivity['station_name'] != '', 'HUB',
            np.where(
                df_all_member_productivity['position'].str.contains('Driver', case=False, na=False), 'LH',
                np.where(
                    df_all_member_productivity['note'].str.contains('SOC', na=False) & 
                    df_all_member_productivity['position'].str.contains('Staff', na=False), 'SOC',
                    'HUB'
                )
            )
        )
    )
    for col in ["date_update", "date_cdd_applied", "recruiter_call_date", "hm_interview_date", "offering_date", "accept_date", "onboard_date"]:
        df_all_member_productivity[col] = pd.to_datetime(df_all_member_productivity[col], errors='coerce').dt.strftime('%Y-%m-%d')

    # [WFA] Performance Management | Nationwide
    data = df_all_member_productivity[['station_name', 'fullname', 'phone', 'date_update', 'storage', 'recruiter_call', 'recruiter_call_date', 'hm_interview', 'hm_interview_date', 'offering', 'offering_date', 'accept_date', 'accept', 'onboard_date', 'onboard', 'channel_by_prod', 'pic', 'position', 'area', 'station_type']]
    nationwide_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1rBfFxs8fsidwV0RbspHiTSQHuE-AoNPDMFfOfTsFbyQ/edit?gid=1531624287#gid=1531624287')
    nationwide = nationwide_spreadsheet.worksheet("Raw Tracker")
    nationwide.clear()  # Xóa dữ liệu cũ
    nationwide.update([data.columns.values.tolist()] + data.values.tolist())
if __name__ == "__main__":
    main()
