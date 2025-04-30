import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

def main():
    # Authenticate and create a client for gspread
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    client = gspread.authorize(creds)

    def open_spreadsheet_by_url(url):
        try:
            return client.open_by_url(url)
        except gspread.exceptions.APIError as e:
            logging.error(f"Cannot open spreadsheet with URL {url}. Error: {e}")
            return None

    # Load data from the "Productivity" worksheet into a DataFrame
    all_member_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1VlXicEr1FGrpdDcRpuv1aE2TAG-7QHEfWKNtFJF4nc8/edit?gid=0#gid=0')
    if all_member_spreadsheet is None:
        return

    all_member_productivity = all_member_spreadsheet.worksheet("Productivity")
    master_data = all_member_productivity.get_all_records()
    df_all_member_productivity = pd.DataFrame(master_data)
    df_all_member_productivity = df_all_member_productivity.astype(str)

    # Process phone numbers and positions
    # df_all_member_productivity['phone'] = df_all_member_productivity.apply(
    #     lambda row: row['phone_ob'][-9:] if row['phone_ob'] else row['phone'][-9:], axis=1
    # )
    # df_all_member_productivity['position'] = df_all_member_productivity['position'].apply(
    #     lambda x: 'Rider' if 'Rider' in x else 'FTE Staff' if 'Staff' in x else 'Driver' if 'Driver' in x else None
    # )

    # Define date columns and filter data
    date_columns = ['date_update', 'recruiter_call_date', 'hm_interview_date', 'offering_date', 'accept_date', 'onboard_date']
    for col in date_columns:
        df_all_member_productivity[col] = pd.to_datetime(df_all_member_productivity[col], errors='coerce')

    # File SOC & LineHaul Nationwide (c Hoa + c Hân)

    soc_linehaul = df_all_member_productivity[
        (df_all_member_productivity['team'] == "Hoa Bui") |
        (df_all_member_productivity['team'] == "Gia Han") |
        (df_all_member_productivity['position'].str.contains("Driver", na=False))
    ]

    for col in date_columns:
        soc_linehaul[col] = soc_linehaul[col].dt.strftime('%Y-%m-%d')

    soc_linehaul = soc_linehaul.replace({np.nan: '', np.inf: '', -np.inf: ''})

    # Open the target spreadsheet and update with filtered data
    soc_linehaul_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1zHEWFEwyZ6zq88hUvQfEzqwipCnHpdaovIl8hXFhPPw/edit?gid=714896083#gid=714896083')
    if soc_linehaul_spreadsheet is None:
        return

    soc_linehaul_sheet = soc_linehaul_spreadsheet.worksheet("Raw Productivity")
    soc_linehaul_sheet.clear()
    soc_linehaul_sheet.update(
        [soc_linehaul.columns.values.tolist()] + soc_linehaul.values.tolist(),
        value_input_option='USER_ENTERED'
    )

    # File BD Projection (c Hân)

    binh_duong = df_all_member_productivity[
        (df_all_member_productivity['team'] == "Gia Han") |
        (df_all_member_productivity['station_name'].str.contains("Binh Duong 1 SOC", na=False))
    ]

    for col in date_columns:
        binh_duong[col] = binh_duong[col].dt.strftime('%Y-%m-%d')

    binh_duong = binh_duong.replace({np.nan: '', np.inf: '', -np.inf: ''})

    # Open the target spreadsheet and update with filtered data
    binh_duong_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1A7hwKMN74dMCFUJ_QF1cGy41FdGAOQhga19l_I3O01Y/edit?gid=0#gid=0')
    if binh_duong_spreadsheet is None:
        return

    binh_duong_sheet = binh_duong_spreadsheet.worksheet("Raw Productivity'")
    binh_duong_sheet.clear()
    binh_duong_sheet.update(
        [binh_duong.columns.values.tolist()] + binh_duong.values.tolist(),
        value_input_option='USER_ENTERED'
    )

    # File Nationwide Scheme Efficiency

    two_months_ago = pd.Timestamp.today().replace(day=1) - pd.DateOffset(months=2)

    scheme_efficiency = df_all_member_productivity[df_all_member_productivity['date_update'] >= two_months_ago]

    scheme_efficiency['position'] = scheme_efficiency['position'].apply(
        lambda x: 'Rider' if 'Rider' in x else 'FTE Staff' if 'Staff' in x else 'Driver' if 'Driver' in x else None
    )

    for col in date_columns:
        scheme_efficiency[col] = scheme_efficiency[col].dt.strftime('%Y-%m-%d')

    scheme_efficiency = scheme_efficiency.replace({np.nan: '', np.inf: '', -np.inf: ''})

    # Open the target spreadsheet and update with filtered data
    scheme_efficiency_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1ZghVwm_7cniD1CdFNwXz9gOjoggUqzxxznqT0hTDLk0/edit?gid=0#gid=0')
    if scheme_efficiency_spreadsheet is None:
        return

    scheme_efficiency_sheet = scheme_efficiency_spreadsheet.worksheet("Raw Productivity")
    scheme_efficiency_sheet.clear()
    scheme_efficiency_sheet.update(
        [scheme_efficiency.columns.values.tolist()] + scheme_efficiency.values.tolist(),
        value_input_option='USER_ENTERED'
    )

if __name__ == "__main__":
    main()
