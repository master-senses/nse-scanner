import pandas as pd
import requests
import time
import random
from datetime import datetime, timedelta
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Paths — resolved relative to this script's own folder ───────────────────
# Works on any machine regardless of username or OS. Just place all .py files
# in the same folder and run from there — no path editing needed.
import os as _os
_HERE       = _os.path.dirname(_os.path.abspath(__file__))
input_file  = _os.path.join(_HERE, "EQUITY_L.csv")
output_file = _os.path.join(_HERE, "final_output.csv")

# ── Config ───────────────────────────────────────────────────────────────────
MAX_WORKERS = 8
DELAY_MIN   = 0.3
DELAY_MAX   = 0.7
RETRY_TOTAL = 3
BATCH_SIZE  = 100

# ── Session factory (one per thread) ─────────────────────────────────────────
def make_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept":          "application/json",
        "Referer":         "https://www.nseindia.com/",
        "Accept-Language": "en-US,en;q=0.9",
    })
    retry = Retry(
        total=RETRY_TOTAL,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    try:
        session.get("https://www.nseindia.com", timeout=8)
    except Exception:
        pass
    return session

# ── Step 1: Earnings data (single request, no need to parallelize) ────────────
def fetch_earnings(session):
    today     = datetime.today()
    future    = today + timedelta(days=90)
    from_date = today.strftime("%d-%m-%Y")
    to_date   = future.strftime("%d-%m-%Y")
    url = (
        f"https://www.nseindia.com/api/corporate-board-meetings"
        f"?index=equities&from_date={from_date}&to_date={to_date}"
    )
    try:
        bm_data = session.get(url, timeout=10).json()
        earnings_dict = {}
        for item in bm_data:
            if "Financial Results" in item.get("bm_purpose", ""):
                symbol = item.get("bm_symbol")
                date   = item.get("bm_date")
                if symbol and symbol not in earnings_dict:
                    earnings_dict[symbol] = date
        print(f"📅 Earnings fetched for {len(earnings_dict)} symbols")
        return earnings_dict
    except Exception as e:
        print(f"⚠️  Could not fetch earnings data: {e}")
        return {}

# ── Step 2: Per-symbol fetch ──────────────────────────────────────────────────
def fetch_symbol(symbol, series, session, earnings_dict):
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    # Try the given series first, then fallback series if data is empty
    series_to_try = [series] + [s for s in ["EQ", "BE", "BZ"] if s != series]

    for attempt_series in series_to_try:
        try:
            url = (
                "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
                f"?functionName=getSymbolData&marketType=N"
                f"&series={attempt_series}&symbol={quote(symbol.strip())}"
            )
            resp = session.get(url, timeout=12)
            resp.raise_for_status()
            data = resp.json()

            if "equityResponse" not in data or not data["equityResponse"]:
                continue  # try next series

            equity     = data["equityResponse"][0]
            trade_info = equity.get("tradeInfo") or {}
            price_info = equity.get("priceInfo") or {}
            sec_info   = equity.get("secInfo")   or {}

            ffmc = trade_info.get("ffmc")
            mcap = trade_info.get("totalMarketCap")

            # Skip if both key fields are empty — try next series
            if ffmc is None and mcap is None:
                continue

            # listingDate arrives as "18-Apr-2000 00:00:00" — strip time
            # suffix at fetch time so pandas never sees the space-delimited format
            # that causes silent NaT coercion in the final output step.
            raw_listing  = sec_info.get("listingDate") or ""
            listing_date = raw_listing.split(" ")[0].strip() or None

            return {
                "symbol":         symbol,
                "ffmc_cr":        round(ffmc / 1e7, 2) if ffmc is not None else None,
                "mcap_cr":        round(mcap / 1e7, 2) if mcap is not None else None,
                "macro":          sec_info.get("macro"),
                "sector":         sec_info.get("sector"),
                "industryInfo":   sec_info.get("basicIndustry"),
                "priceband":      price_info.get("ppriceBand"),
                "series_used":    attempt_series,
                "EARNINGS DATE":  earnings_dict.get(symbol),
                "LISTING DATE":   listing_date,
                "_status":        "ok",
            }

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                print(f"  ⚠️  Rate limited on {symbol}, backing off 10s…")
                time.sleep(10)
            continue  # try next series
        except Exception:
            continue  # try next series

    return {"symbol": symbol, "_status": "no data across all series (EQ/BE/BZ)"}
# ── Auto-download EQUITY_L.csv from NSE if not present ───────────────────────
# NSE publishes the full equity master at this URL — no login required.
# We download it once automatically so users don't have to fetch it manually.
_EQUITY_URL = "https://www.nseindia.com/content/equities/EQUITY_L.csv"

def _download_equity_master(dest: str) -> bool:
    """Download EQUITY_L.csv from NSE into dest. Returns True on success."""
    print("📥 EQUITY_L.csv not found — downloading from NSE...")
    try:
        _sess = make_session()
        resp  = _sess.get(_EQUITY_URL, timeout=30)
        resp.raise_for_status()
        with open(dest, "wb") as _f:
            _f.write(resp.content)
        print(f"✅ EQUITY_L.csv downloaded ({len(resp.content)//1024} KB)")
        return True
    except Exception as _e:
        print(f"❌ Could not download EQUITY_L.csv: {_e}")
        print("   Please download it manually from:")
        print("   https://www.nseindia.com/market-data/securities-available-for-trading")
        print(f"   and place it in: {_os.path.dirname(dest)}")
        return False

if not _os.path.exists(input_file):
    if not _download_equity_master(input_file):
        raise SystemExit(1)

# ── Load & filter ─────────────────────────────────────────────────────────────
df = pd.read_csv(input_file)
df.columns = df.columns.str.strip().str.upper()
df["SERIES"] = df["SERIES"].astype(str).str.strip().str.upper()
df = df[df["SERIES"].isin(["EQ", "BE", "BZ"])]

symbol_series_map = df.groupby("SYMBOL")["SERIES"].first().to_dict()
symbols = list(symbol_series_map.keys())
print(f"📋 Total symbols (EQ/BE/BZ): {len(symbols)}")

# ── Warm up sessions + fetch earnings in parallel ─────────────────────────────
print(f"🔥 Warming up {MAX_WORKERS} sessions…")
sessions = [make_session() for _ in range(MAX_WORKERS)]

# Reuse first session for earnings (already warmed up)
earnings_dict = fetch_earnings(sessions[0])

# ── Parallel fetch ────────────────────────────────────────────────────────────
results = []
errors  = []
done    = 0

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {
        executor.submit(
            fetch_symbol,
            sym,
            symbol_series_map[sym],
            sessions[i % MAX_WORKERS],
            earnings_dict,
        ): sym
        for i, sym in enumerate(symbols)
    }

    for future in as_completed(futures):
        row   = future.result()
        done += 1

        if row["_status"] == "ok":
            results.append({k: v for k, v in row.items() if k != "_status"})
            print(f"  ✔ [{done}/{len(symbols)}] {row.get('series_used', '?')} | {row['symbol']}")
        else:
            errors.append(row)
            print(f"  ❌ [{done}/{len(symbols)}] {row['symbol']} → {row['_status']}")

        # Save progress every BATCH_SIZE symbols
        if done % BATCH_SIZE == 0:
            pd.DataFrame(results).to_csv(output_file, index=False)
            print(f"\n  💾 Progress saved ({len(results)} ok, {len(errors)} errors)\n")

# ── Final output ──────────────────────────────────────────────────────────────
output_df = pd.DataFrame(results)
output_df["EARNINGS DATE"] = pd.to_datetime(output_df["EARNINGS DATE"], errors="coerce")
output_df = output_df.sort_values(by="EARNINGS DATE", ascending=True)
output_df["EARNINGS DATE"] = output_df["EARNINGS DATE"].dt.strftime("%d-%m-%Y")
# LISTING DATE is already clean ("18-Apr-2000") from fetch_symbol — just parse and reformat safely.
output_df["LISTING DATE"]  = pd.to_datetime(output_df["LISTING DATE"], format="%d-%b-%Y", errors="coerce").dt.strftime("%d-%m-%Y")
output_df.to_csv(output_file, index=False)

error_file = output_file.replace(".csv", "_errors.csv")
pd.DataFrame(errors).to_csv(error_file, index=False)

print(f"\n✅ Done! {len(results)} symbols saved → {output_file}")
print(f"⚠️  {len(errors)} errors logged  → {error_file}")