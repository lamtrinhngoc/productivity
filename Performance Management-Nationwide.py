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
    date_columns = ['date_update', 'recruiter_call_date', 'hm_interview_date', 'offering_date', 'accept_date', 'onboard_date']
    for col in date_columns:
        df_all_member_productivity[col] = pd.to_datetime(df_all_member_productivity[col], errors='coerce')
    two_months_ago = pd.Timestamp.today().replace(day=1) - pd.DateOffset(months=2)
    filter_datetime = df_all_member_productivity[
    (df_all_member_productivity['date_update'] >= two_months_ago) |
    (df_all_member_productivity['recruiter_call_date'] >= two_months_ago) |
    (df_all_member_productivity['hm_interview_date'] >= two_months_ago) |
    (df_all_member_productivity['offering_date'] >= two_months_ago) |
    (df_all_member_productivity['accept_date'] >= two_months_ago) |
    (df_all_member_productivity['onboard_date'] >= two_months_ago)
    ]
    for col in date_columns:
        filter_datetime[col] = filter_datetime[col].dt.strftime('%Y-%m-%d')
    filter_datetime = filter_datetime.replace({np.nan: '', np.inf: '', -np.inf: ''})
    # [WFA] Performance Management | Nationwide
    
    data_nationwide = filter_datetime[['station_name', 'fullname', 'phone', 'date_update', 'storage', 'recruiter_call', 'recruiter_call_date', 'hm_interview', 'hm_interview_date', 'offering', 'offering_date', 'accept_date', 'accept', 'onboard_date', 'onboard', 'channel_by_prod', 'pic', 'position', 'area', 'station_type']]
    nationwide_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1rBfFxs8fsidwV0RbspHiTSQHuE-AoNPDMFfOfTsFbyQ/edit?gid=1531624287#gid=1531624287')
    nationwide_sheet = nationwide_spreadsheet.worksheet("Raw Tracker")
    nationwide_sheet.clear()  # Xóa dữ liệu cũ
    nationwide_sheet.update([data_nationwide.columns.values.tolist()] + data_nationwide.values.tolist(),value_input_option=gspread.utils.ValueInputOption.user_entered)
    nationwide_sheet.update_cell(2, 21, '=ARRAYFORMULA(IF(R2:R = "Rider", XLOOKUP(A2:A, \'Priority - Rider\'!$C:$C, \'Priority - Rider\'!$AR:$AR), XLOOKUP(A2:A & R2:R, \'Priority - FTE\'!$A:$A, \'Priority - FTE\'!$AS:$AS)))')
    nationwide_sheet.update_cell(1, 21, 'priority')
if __name__ == "__main__":
    main()
