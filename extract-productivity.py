import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
import logging
import re
import time
import random

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

# ---------- Retry helper ----------
HARD_RETRY_STATUSES = (429, 500, 502, 503, 504)
SOFT_RETRY_STATUSES = (403, 404)
MAX_RETRIES = 6
SOFT_MAX = 2


def with_retry(func, *args, max_retries=MAX_RETRIES, **kwargs):
    """
    Retry phân tầng:
    - Hard errors (429, 5xx): retry tới max_retries lần, backoff exponential + jitter
    - Soft errors (403, 404): chỉ retry SOFT_MAX lần với backoff dài (có thể là lỗi thật)
    - Lỗi khác: raise ngay
    """
    soft_attempts = 0

    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            status = getattr(e.response, "status_code", None)

            if status in HARD_RETRY_STATUSES and attempt < max_retries - 1:
                wait = min(2 ** attempt, 30) + random.uniform(0, 1)
                log.warning(
                    f"  ↻ APIError {status} on {func.__name__}, "
                    f"hard-retry {attempt+1}/{max_retries} sau {wait:.1f}s"
                )
                time.sleep(wait)

            elif status in SOFT_RETRY_STATUSES and soft_attempts < SOFT_MAX:
                soft_attempts += 1
                wait = 15 * soft_attempts + random.uniform(0, 2)
                log.warning(
                    f"  ↻ APIError {status} on {func.__name__} (có thể transient), "
                    f"soft-retry {soft_attempts}/{SOFT_MAX} sau {wait:.1f}s"
                )
                time.sleep(wait)

            else:
                raise

    raise RuntimeError(f"{func.__name__} failed sau {max_retries} lần retry")


def main():
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    client = gspread.authorize(creds)

    # ---------- Tracking results ----------
    results = []   # list of dict: {name, status, rows, error, duration}

    def write_df_to_sheet(name, url, worksheet_name, df,
                          use_batch_clear=False, clear_range="A:AT"):
        """Ghi DataFrame vào sheet, log + lưu kết quả."""
        start = time.time()
        rows = len(df)
        log.info(f"→ Bắt đầu [{name}] - {rows} dòng - sheet '{worksheet_name}'")

        try:
            spreadsheet = with_retry(client.open_by_url, url)
            sheet = with_retry(spreadsheet.worksheet, worksheet_name)

            if use_batch_clear:
                with_retry(sheet.batch_clear, [clear_range])
                with_retry(
                    sheet.update,
                    range_name="A1",
                    values=[df.columns.values.tolist()] + df.values.tolist(),
                    value_input_option='USER_ENTERED'
                )
            else:
                with_retry(sheet.clear)
                with_retry(
                    sheet.update,
                    [df.columns.values.tolist()] + df.values.tolist(),
                    value_input_option='USER_ENTERED'
                )

            duration = time.time() - start
            log.info(f"✓ [{name}] OK - {rows} dòng - {duration:.1f}s")
            results.append({
                "name": name, "status": "OK", "rows": rows,
                "duration": duration, "error": ""
            })

        except APIError as e:
            duration = time.time() - start
            status = getattr(e.response, "status_code", "?")
            err_msg = f"APIError {status}: {str(e)[:120]}"
            log.error(f"✗ [{name}] FAILED - {err_msg}")
            results.append({
                "name": name, "status": "FAIL", "rows": rows,
                "duration": duration, "error": err_msg
            })

        except Exception as e:
            duration = time.time() - start
            err_msg = f"{type(e).__name__}: {str(e)[:120]}"
            log.error(f"✗ [{name}] FAILED - {err_msg}")
            results.append({
                "name": name, "status": "FAIL", "rows": rows,
                "duration": duration, "error": err_msg
            })

    # ---------- Load master data ----------
    log.info("=" * 60)
    log.info("Loading master data...")
    master_url = 'https://docs.google.com/spreadsheets/d/1VlXicEr1FGrpdDcRpuv1aE2TAG-7QHEfWKNtFJF4nc8/edit?gid=0#gid=0'
    try:
        spreadsheet = with_retry(client.open_by_url, master_url)
        ws = with_retry(spreadsheet.worksheet, "Productivity")
        master_data = with_retry(ws.get_all_records)
        log.info(f"✓ Loaded {len(master_data)} dòng từ master sheet")
    except Exception as e:
        log.error(f"✗ KHÔNG load được master sheet: {e}")
        return

    df = pd.DataFrame(master_data).astype(str)

    # ---------- Preprocess (giữ nguyên logic cũ) ----------
    def preprocess_date_string(val):
        if pd.isna(val):
            return None
        val = str(val).strip()
        if not val:
            return None
        m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", val)
        if m:
            mm, dd, yy = m.groups()
            return f"{yy}-{int(mm):02d}-{int(dd):02d}"
        return val

    date_columns = ['date_update', 'recruiter_call_date', 'hm_interview_date',
                    'offering_date', 'accept_date', 'onboard_date']
    for col in date_columns:
        df[col] = df[col].apply(preprocess_date_string)
        df[col] = pd.to_datetime(df[col], errors='coerce')

    df['station_name'] = df['station_name'].apply(
        lambda x: 'BD A Mega SOC' if 'Binh Duong 1' in x and 'SOC' in x else x
    )

    mapping = {
        "3. Staff": "FTE Staff",
        "3. Staff SOC": "Staff SOC",
        "3. Staff FLM": "Staff FLM",
        "3. Staff (DC)": "FTE Staff (DC)",
        "4. Rider": "Rider",
        "4. Rider - Lơ xe": "Rider Bulky (Lơ)",
        "4. Rider Freelancer": "Rider Freelancer",
        "4. Rider Part-time": "Rider Part-time",
        "4. Rider SDD": "Rider SDD",
        "6. Driver": "Driver",
        "6. Driver - 1T25": "Driver - 1T25",
        "6. Driver - 2T": "Driver - 2T",
        "6. Driver - 5T": "Driver - 5T",
        "6. Driver - 8T": "Driver - 8T",
        "6. Driver (Bulky)": "Driver Bulky (Có xe)",
        "6. Driver - Bulky (Có xe)": "Driver Bulky (Có xe)",
        "6. Driver - Bulky (Không xe)": "Driver Bulky (Không xe)",
        "6. Driver (Van)": "Driver - Van",
        "6. Driver - Van": "Driver - Van",
        "6. Driver (LH X-metro)": "Driver - 8T",
        "7. Freelancer Rider": "Rider Freelancer",
        "8. Part-time Rider": "Rider Part-time",
        "4. Rider SDD (PT)": "Rider SDD (PT)",
        "3. Staff - WH": "Warehouse Staff",
        "WH - Inbound": "Warehouse Staff",
        "WH - Outbound": "Warehouse Staff",
        "WH - Inventory": "Warehouse Staff",
        "WH - Return": "Warehouse Staff",
        "BD contractor": "BD contractor",
        "Telesale": "BD contractor",
        "BD Satellite Sales": "BD contractor",
        "KAM": "BD contractor",
        "S.BPO": "S-BPO",
    }
    df['position'] = df['position'].replace(mapping)

    df['area'] = df.apply(
        lambda row: (
            'South' if row['area'] in ['SE', 'SW'] else
            'HNI' if row['area'] == 'HN' else
            'HNI' if 'Hà Nội' in str(row['address']) else
            'HCM' if 'Hồ Chí Minh' in str(row['address']) else
            row['area']
        ),
        axis=1
    )

    for col in date_columns:
        df[col] = df[col].dt.strftime('%Y-%m-%d')

    df = df.replace({np.nan: '', np.inf: '', -np.inf: ''})

    # ---------- Ghi các file ----------
    log.info("=" * 60)
    log.info("Bắt đầu ghi các file...")
    log.info("=" * 60)

    team_files = [
        ("Van Anh",     'https://docs.google.com/spreadsheets/d/1E-kFjoHSmOnrDK_O4tpegxBh5qh4wTxfhvMXoL-p5O4',
            lambda d: d[d['team'] == "Van Anh"]),
        ("Viet Vuong", 'https://docs.google.com/spreadsheets/d/1DCcJycFigvCWZz51jnyZtvMInHnJ0AGXAfg0B6WBq40',
            lambda d: d[d['team'] == "Viet Vuong"]),
        ("Hoai Phuong",  'https://docs.google.com/spreadsheets/d/1Iwt_1x_KHzRAQZ9FEi0hBeGBfTrvbwxDyrjjGkz6VRU',
            lambda d: d[d['team'] == 'Hoai Phuong']),
        ("Hoa Bui",     'https://docs.google.com/spreadsheets/d/1oJ_UHIbolyFI616oyS_df1yv0NczeRputMtCjAis5AY',
            lambda d: d[d['team'] == "Hoa Bui"]),
        ("Quynh Trang", 'https://docs.google.com/spreadsheets/d/1fQHpixWzd6Ho-Zci5mHWE0klXIGcLGZAEG5g6LwBF90',
            lambda d: d[d['team'] == "Quynh Trang"]),
        ("Huyen Trang", 'https://docs.google.com/spreadsheets/d/1IT1rHY369YLNRLZ5UNQbVa3k2yeMiEvFXImEd9LmVJo',
            lambda d: d[d['team'] == "Huyen Trang"]),
        ("Thu Hien",    'https://docs.google.com/spreadsheets/d/1muT6hNa3uKPxiTjGCtQhCBGqPD0DsIYX4vS0yXSKVs8',
            lambda d: d[d['team'] == "Thu Hien"]),
        ("Nhi Tran",    'https://docs.google.com/spreadsheets/d/1rduwnpSBfEpZk0Tk0D3ATDlLaYOFFT5fsUU5Maqx2wU',
            lambda d: d[d['team'] == "Nhi Tran"]),
        ("Trinh Phan",  'https://docs.google.com/spreadsheets/d/1BIjhc47fc2mtXbHFK2Y_edqzu_pgrQjDVXE56MRwdpU',
            lambda d: d[d['team'] == 'Trinh Phan']),
        ("Hoai Phuong",  'https://docs.google.com/spreadsheets/d/19MLD_rTpMD4zke0pPp855KUCgsWr2yggMdPlagbprG0',
            lambda d: d[d['team'] == 'Hoai Phuong']),
    ]
    for name, url, filter_fn in team_files:
        write_df_to_sheet(name, url, "Raw Productivity", filter_fn(df))

    write_df_to_sheet(
        "External Referral",
        'https://docs.google.com/spreadsheets/d/18ojJoncFCt35Bf9H9uHAUmOOr8U61sPZXgk_yvEtpuI/edit?gid=1701019638#gid=1701019638',
        "Raw Productivity",
        df[df['source'].str.contains("SPX-Referral Program-RP-External", na=False)]
    )

    write_df_to_sheet(
        "Linehaul",
        'https://docs.google.com/spreadsheets/d/1y12mSMS03JCWRDkVojJWNt93R7E_p3poUa5xEGIUmgk',
        "Raw Productivity",
        df[df['position'].str.contains("Driver", na=False)]
    )

    write_df_to_sheet(
        "Linehaul-Bulky",
        'https://docs.google.com/spreadsheets/d/1d8Q_r7PP9URrODzF0QoqfHsGkobrucHxsxTzgzzeft0',
        "Raw Productivity",
        df[
            df['position'].str.contains("Bulky", na=False) |
            df['position'].str.contains("Rider - Lơ xe", na=False) |
            df['position'].str.contains("DC", na=False)
        ]
    )

    write_df_to_sheet(
        "Rider SDD",
        'https://docs.google.com/spreadsheets/d/1sItVLyDOaGWx2eWzxdJBygRnwLmJ4h5RiKBDb6glnJI',
        "Data team",
        df[df['position'].str.contains("Rider SDD", na=False)]
    )

    write_df_to_sheet(
        "S-BPO",
        'https://docs.google.com/spreadsheets/d/1mUOEC77iRXnTqfuaN5IiE1yA6EJZkOZlaRBPlWYUiV0',
        "Raw Productivity",
        df[df['position'].str.contains("S-BPO", na=False)]
    )

    write_df_to_sheet(
        "Binh Duong SOC",
        'https://docs.google.com/spreadsheets/d/1-uKjt-NamVr3eOwAycYicJnFkTF5SkzQeI0PX7O_r9k',
        "Raw Productivity",
        df[
            df['station_name'].isin(["Binh Duong 1 SOC", "BD A Mega SOC", "BD B Mega SOC"])
            & df['position'].str.contains("FTE Staff", na=False)
        ],
        use_batch_clear=True,
        clear_range="A:AT"
    )

    # ---------- Bảng tổng kết ----------
    log.info("=" * 60)
    log.info("KẾT QUẢ TỔNG KẾT")
    log.info("=" * 60)

    ok = [r for r in results if r["status"] == "OK"]
    fail = [r for r in results if r["status"] == "FAIL"]

    log.info(f"Tổng số file: {len(results)} | Thành công: {len(ok)} | Lỗi: {len(fail)}")
    log.info("-" * 60)
    log.info(f"{'#':<3} {'STATUS':<6} {'NAME':<20} {'ROWS':>6}  {'TIME':>6}  ERROR")
    log.info("-" * 60)
    for i, r in enumerate(results, 1):
        status_icon = "✓" if r["status"] == "OK" else "✗"
        log.info(
            f"{i:<3} {status_icon} {r['status']:<4} {r['name']:<20} "
            f"{r['rows']:>6}  {r['duration']:>5.1f}s  {r['error']}"
        )
    log.info("=" * 60)

    if fail:
        log.warning(f"⚠ Có {len(fail)} file lỗi - cần chạy lại:")
        for r in fail:
            log.warning(f"   - {r['name']}: {r['error']}")
    else:
        log.info("🎉 Tất cả file đã ghi thành công!")


if __name__ == "__main__":
    main()
