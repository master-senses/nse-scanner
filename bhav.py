from datetime import datetime, timedelta
from nse import NSE
import pandas as pd
from pathlib import Path
import time

# Folder to store daily bhavcopies
download_folder = Path("daily_bhavcopies")
download_folder.mkdir(exist_ok=True)

# Initialize NSE object
for i in range(5):
    try:
        nse = NSE(download_folder=download_folder)
        break
    except Exception as e:
        print("Retrying NSE init...", e)
        time.sleep(5)
else:
    raise Exception("NSE site blocked all attempts")

# -------------------------
# Auto-calculate last 50 trading days
# -------------------------
def get_last_n_trading_days(n: int) -> list[datetime]:
    """Walk backwards from yesterday, collecting weekdays until we have n days."""
    trading_days = []
    curr = datetime.today() - timedelta(days=1)  # start from yesterday
    while len(trading_days) < n:
        if curr.weekday() < 5:  # Mon–Fri only
            trading_days.append(curr)
        curr -= timedelta(days=1)
    return sorted(trading_days)  # oldest → newest

trading_days = get_last_n_trading_days(50)
print(f"Fetching bhavcopies from {trading_days[0].date()} to {trading_days[-1].date()}")

# -------------------------
# Download loop
# -------------------------
for curr in trading_days:
    try:
        path = nse.equityBhavcopy(date=curr)
        print(f"Downloaded: {path}")

        try:
            df = pd.read_csv(path, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(path, encoding="latin1")

        df["DATE"] = curr.strftime("%Y-%m-%d")
        output_file = download_folder / f"bhavcopy_{curr.strftime('%Y%m%d')}.csv"
        df.to_csv(output_file, index=False, encoding="utf-8")
        print(f"Saved: {output_file}")
        path.unlink()

    except Exception as e:
        print(f"No bhavcopy for {curr.date()} ({e})")

print("Finished downloading daily bhavcopies.")
import subprocess

# Step 1: Compute avg vol & turnover (already filters BE/BZ/EQ internally)
subprocess.run(["python", "fetch_avg_vol.py"], check=True)

# Step 2: Filter pct_chg JSON to BE/BZ/EQ only (still needs build_series_jsons)
# But only run if fetch_pct_chg.json exists
import os
if os.path.exists("fetch_pct_chg.json"):
    subprocess.run(["python", "build_series_jsons.py"], check=True)
else:
    print("Skipping build_series_jsons.py — fetch_pct_chg.json not found")

print("All done.")