"""
build_series_jsons.py
---------------------
Reads the daily bhavcopies downloaded by the NSE script, builds a
SYMBOL → SERIES lookup, then produces two filtered JSON files
(pct_chg and avg_vol) containing only BE, BZ, and EQ series stocks.

Output files:
  daily_bhavcopies/fetch_pct_chg_BE_BZ_EQ.json
  daily_bhavcopies/fetch_avg_vol_BE_BZ_EQ.json
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
DOWNLOAD_FOLDER  = Path("daily_bhavcopies")
PCT_CHG_INPUT    = Path("fetch_pct_chg.json")        # change to your actual path
AVG_VOL_INPUT  = DOWNLOAD_FOLDER / "fetch_avg_vol.json"
AVG_VOL_OUTPUT = DOWNLOAD_FOLDER / "fetch_avg_vol_BE_BZ_EQ.json"
TARGET_SERIES    = {"BE", "BZ", "EQ"}

PCT_CHG_OUTPUT   = DOWNLOAD_FOLDER / "fetch_pct_chg_BE_BZ_EQ.json"


# ── Step 1: Build SYMBOL → SERIES map from all available bhavcopies ──────────

def detect_columns(df: pd.DataFrame) -> tuple[str, str]:
    """
    Return (symbol_col, series_col) for both old and new NSE bhavcopy formats.
    Old format : SYMBOL, SERIES
    New format : TckrSymb, SctySrs
    """
    cols = set(df.columns.str.strip())
    if "SYMBOL" in cols and "SERIES" in cols:
        return "SYMBOL", "SERIES"
    if "TckrSymb" in cols and "SctySrs" in cols:
        return "TckrSymb", "SctySrs"
    raise ValueError(f"Unrecognised bhavcopy format. Columns: {list(df.columns)}")


def build_symbol_series_map(folder: Path) -> dict[str, str]:
    """
    Reads every bhavcopy_*.csv in the folder and builds a dict
    { SYMBOL: SERIES }.  Later files overwrite earlier ones for the same symbol
    (series rarely changes, but this keeps the map fresh).
    """
    csv_files = sorted(folder.glob("bhavcopy_*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"No bhavcopy_*.csv files found in '{folder}'. "
            "Run the download script first."
        )

    symbol_series: dict[str, str] = {}
    for csv_path in csv_files:
        try:
            try:
                df = pd.read_csv(csv_path, encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(csv_path, encoding="latin1")

            # Strip whitespace from column names & string values
            df.columns = df.columns.str.strip()
            sym_col, ser_col = detect_columns(df)
            df[sym_col] = df[sym_col].astype(str).str.strip()
            df[ser_col] = df[ser_col].astype(str).str.strip().str.upper()

            for _, row in df[[sym_col, ser_col]].iterrows():
                symbol_series[row[sym_col]] = row[ser_col]

        except Exception as e:
            print(f"  ⚠  Skipping {csv_path.name}: {e}")

    print(f"✓ Built SYMBOL→SERIES map from {len(csv_files)} bhavcopy file(s)"
          f" ({len(symbol_series)} unique symbols)")
    return symbol_series


# ── Step 2: Filter a JSON data-dict to only target series ────────────────────

def filter_json(
    input_path: Path,
    symbol_series: dict[str, str],
    target: set[str],
    output_path: Path,
) -> None:
    with open(input_path, encoding="utf-8") as f:
        original = json.load(f)

    all_data: dict = original["data"]

    filtered_data = {
        symbol: values
        for symbol, values in all_data.items()
        if symbol_series.get(symbol, "").upper() in target
    }

    output = {
        "date":    original.get("date", ""),
        "fetched": original.get("fetched", ""),
        "series_filter": sorted(target),
        "count":   len(filtered_data),
        "data":    filtered_data,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    total = len(all_data)
    kept  = len(filtered_data)
    print(f"✓ {output_path.name}: {kept}/{total} symbols kept "
          f"({total - kept} excluded)  →  {output_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Building Series-Filtered JSON files")
    print(f"  Target series : {sorted(TARGET_SERIES)}")
    print(f"  Bhavcopies    : {DOWNLOAD_FOLDER}")
    print("=" * 60)

    # 1. Build the symbol → series lookup
    symbol_series = build_symbol_series_map(DOWNLOAD_FOLDER)

    # Quick sanity: how many of each target series
    from collections import Counter
    counts = Counter(
        s for s in symbol_series.values() if s in TARGET_SERIES
    )
    for series in sorted(TARGET_SERIES):
        print(f"   {series}: {counts.get(series, 0)} symbols")
    print()

    # 2. Filter pct_chg JSON
    print("Processing fetch_pct_chg …")
    filter_json(PCT_CHG_INPUT, symbol_series, TARGET_SERIES, PCT_CHG_OUTPUT)

    # 3. Filter avg_vol JSON
    print("Processing fetch_avg_vol …")
    filter_json(AVG_VOL_INPUT, symbol_series, TARGET_SERIES, AVG_VOL_OUTPUT)

    print()
    print("Done ✓")
    print(f"  {PCT_CHG_OUTPUT}")
    print(f"  {AVG_VOL_OUTPUT}")


if __name__ == "__main__":
    main()
