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

    # Define date columns and filter data
    date_columns = ['date_update', 'recruiter_call_date', 'hm_interview_date', 'offering_date', 'accept_date', 'onboard_date']
    for col in date_columns:
        df_all_member_productivity[col] = pd.to_datetime(df_all_member_productivity[col], errors='coerce')
        

    df_all_member_productivity['station_name'] = df_all_member_productivity['station_name'].apply(
    lambda x: 'BD A Mega SOC' if 'Binh Duong' in x and 'SOC' in x else x
    )

    mapping = {
        "3. Staff": "FTE Staff",
        "3. Staff (DC)": "FTE Staff (DC)",
        "4. Rider": "Rider",
        "4. Rider - Lơ xe": "Rider - Lơ xe",
        "4. Rider Freelancer": "Rider Freelancer",
        "4. Rider Part-time": "Rider Part-time",
        "4. Rider SDD": "Rider SDD",
        "6. Driver": "Driver",
        "6. Driver - 1T25": "Driver - 1T25",
        "6. Driver - 2T": "Driver - 2T",
        "6. Driver - 5T": "Driver - 5T",
        "6. Driver - 8T": "Driver - 8T",
        "6. Driver (Bulky)": "Driver - Bulky (Có xe)",
        "6. Driver - Bulky (Có xe)": "Driver - Bulky (Có xe)",
        "6. Driver - Bulky (Không xe)": "Driver - Bulky (Không xe)",
        "6. Driver (Van)": "Driver - Van",
        "6. Driver - Van": "Driver - Van",
        "6. Driver (LH X-metro)": "Driver - 8T",
        "7. Freelancer Rider": "Rider Freelancer",
        "8. Part-time Rider": "Rider Part-time",
    }

    df_all_member_productivity['position'] = df_all_member_productivity['position'].replace(mapping)

    
    df_all_member_productivity['area'] = df_all_member_productivity['area'].apply(
    lambda x: 'South' if x in ['SE', 'SW'] else ('HNI' if x == 'HN' else x)
    )


    for col in date_columns:
        df_all_member_productivity[col] = df_all_member_productivity[col].dt.strftime('%Y-%m-%d')

    df_all_member_productivity = df_all_member_productivity.replace({np.nan: '', np.inf: '', -np.inf: ''})

    # File [WFA] Performance Management | Yen Phan

    yen_phan = df_all_member_productivity[
        (df_all_member_productivity['team'] == "Gia Han") |
        (df_all_member_productivity['team'] == "Yen Phan") |
        (df_all_member_productivity['team'] == "Cam Giang") |
        (df_all_member_productivity['team'] == "Yen Nhi")
    ]
    
    yen_phan_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1E-kFjoHSmOnrDK_O4tpegxBh5qh4wTxfhvMXoL-p5O4')
    if yen_phan_spreadsheet is None:
        return

    yen_phan_sheet = yen_phan_spreadsheet.worksheet("Raw Productivity")
    yen_phan_sheet.clear()
    yen_phan_sheet.update(
        [yen_phan.columns.values.tolist()] + yen_phan.values.tolist(),
        value_input_option='USER_ENTERED'
    )

    # File [WFA] Performance Management | Minh Nguyet

    minh_nguyet = df_all_member_productivity[
        (df_all_member_productivity['team'] == "Minh Nguyet")
    ]
    
    minh_nguyet_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1DCcJycFigvCWZz51jnyZtvMInHnJ0AGXAfg0B6WBq40')
    if minh_nguyet_spreadsheet is None:
        return

    minh_nguyet_sheet = minh_nguyet_spreadsheet.worksheet("Raw Productivity")
    minh_nguyet_sheet.clear()
    minh_nguyet_sheet.update(
        [minh_nguyet.columns.values.tolist()] + minh_nguyet.values.tolist(),
        value_input_option='USER_ENTERED'
    )

    # File [WFA] Performance Management | Trinh Phan

    trinh_phan = df_all_member_productivity[
        (df_all_member_productivity['team'] == "Trinh Phan")
    ]
    
    trinh_phan_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1Iwt_1x_KHzRAQZ9FEi0hBeGBfTrvbwxDyrjjGkz6VRU')
    if trinh_phan_spreadsheet is None:
        return

    trinh_phan_sheet = trinh_phan_spreadsheet.worksheet("Raw Productivity")
    trinh_phan_sheet.clear()
    trinh_phan_sheet.update(
        [trinh_phan.columns.values.tolist()] + trinh_phan.values.tolist(),
        value_input_option='USER_ENTERED'
    )

    # File [WFA] Performance Management | Hoa Bui

    hoa_bui = df_all_member_productivity[
        (df_all_member_productivity['team'] == "Hoa Bui")
    ]
    
    hoa_bui_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1oJ_UHIbolyFI616oyS_df1yv0NczeRputMtCjAis5AY')
    if hoa_bui_spreadsheet is None:
        return

    hoa_bui_sheet = hoa_bui_spreadsheet.worksheet("Raw Productivity")
    hoa_bui_sheet.clear()
    hoa_bui_sheet.update(
        [hoa_bui.columns.values.tolist()] + hoa_bui.values.tolist(),
        value_input_option='USER_ENTERED'
    )

    # File [WFA] Performance Management | Huyen Trang

    huyen_trang = df_all_member_productivity[
        (df_all_member_productivity['team'] == "Huyen Trang")
    ]
    
    huyen_trang_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1IT1rHY369YLNRLZ5UNQbVa3k2yeMiEvFXImEd9LmVJo')
    if huyen_trang_spreadsheet is None:
        return

    huyen_trang_sheet = huyen_trang_spreadsheet.worksheet("Raw Productivity")
    huyen_trang_sheet.clear()
    huyen_trang_sheet.update(
        [huyen_trang.columns.values.tolist()] + huyen_trang.values.tolist(),
        value_input_option='USER_ENTERED'
    )

    # File [WFA] Performance Management | Thu Hien

    thu_hien = df_all_member_productivity[
        (df_all_member_productivity['team'] == "Thu Hien")
    ]
    
    thu_hien_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1muT6hNa3uKPxiTjGCtQhCBGqPD0DsIYX4vS0yXSKVs8')
    if thu_hien_spreadsheet is None:
        return

    thu_hien_sheet = thu_hien_spreadsheet.worksheet("Raw Productivity")
    thu_hien_sheet.clear()
    thu_hien_sheet.update(
        [thu_hien.columns.values.tolist()] + thu_hien.values.tolist(),
        value_input_option='USER_ENTERED'
    )

    # File Linehaul project

    soc_linehaul = df_all_member_productivity[
        (df_all_member_productivity['position'].str.contains("Driver", na=False))
    ]

    soc_linehaul_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1y12mSMS03JCWRDkVojJWNt93R7E_p3poUa5xEGIUmgk')
    if soc_linehaul_spreadsheet is None:
        return

    soc_linehaul_sheet = soc_linehaul_spreadsheet.worksheet("Raw Productivity")
    soc_linehaul_sheet.clear()
    soc_linehaul_sheet.update(
        [soc_linehaul.columns.values.tolist()] + soc_linehaul.values.tolist(),
        value_input_option='USER_ENTERED'
    )
    
    # File Linehaul-Bulky project

    soc_linehaul_bulky = df_all_member_productivity[
        (df_all_member_productivity['position'].str.contains("Bulky", na=False)) |
        (df_all_member_productivity['position'].str.contains("Rider - Lơ xe", na=False)) |
        (df_all_member_productivity['position'].str.contains("DC", na=False))
    ]

    soc_linehaul_bulky_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1d8Q_r7PP9URrODzF0QoqfHsGkobrucHxsxTzgzzeft0')
    if soc_linehaul_bulky_spreadsheet is None:
        return

    soc_linehaul_bulky_sheet = soc_linehaul_bulky_spreadsheet.worksheet("Raw Productivity")
    soc_linehaul_bulky_sheet.clear()
    soc_linehaul_bulky_sheet.update(
        [soc_linehaul_bulky.columns.values.tolist()] + soc_linehaul_bulky.values.tolist(),
        value_input_option='USER_ENTERED'
    )
      

    # File track SDD

    rider_sdd = df_all_member_productivity[
        df_all_member_productivity['position'].str.contains("Rider SDD", na=False)
    ]

    rider_sdd_hn = rider_sdd[(rider_sdd['team'] == 'Huyen Trang') | (rider_sdd['team'] == 'Thu Hien')]

    rider_sdd_spreadsheet = open_spreadsheet_by_url('https://docs.google.com/spreadsheets/d/1sItVLyDOaGWx2eWzxdJBygRnwLmJ4h5RiKBDb6glnJI')
    if rider_sdd_spreadsheet is None:
        return
    rider_sdd_sheet = rider_sdd_spreadsheet.worksheet("Data team")
    rider_sdd_sheet.clear()
    rider_sdd_sheet.update(
        [rider_sdd.columns.values.tolist()] + rider_sdd.values.tolist(),
        value_input_option='USER_ENTERED'
    )
    

if __name__ == "__main__":
    main()
