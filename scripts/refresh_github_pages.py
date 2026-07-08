"""
Fetch Angel One quotes and rebuild docs/index.html for GitHub Pages.

Used by .github/workflows/quote-refresh.yml. Reads credentials from env vars:
  ANGEL_API_KEY, ANGEL_CLIENT_CODE, ANGEL_PIN, ANGEL_TOTP_SECRET
"""

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyotp
import requests

ROOT = Path(__file__).resolve().parent.parent
QUOTES_PATH = ROOT / "docs" / "quotes.json"
INDEX_HTML = ROOT / "docs" / "index.html"
SCRIP_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
ANGEL_BASE = "https://apiconnect.angelbroking.com"
LOGIN_URL = f"{ANGEL_BASE}/rest/auth/angelbroking/user/v1/loginByPassword"
QUOTE_URL = f"{ANGEL_BASE}/rest/secure/angelbroking/market/v1/quote/"
IST = timezone(timedelta(hours=5, minutes=30))
BATCH_SIZE = 50
RATE_DELAY = 0.25


def is_market_hours() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    return 9 * 60 + 15 <= minutes <= 15 * 60 + 30


def load_config() -> dict:
    fields = {
        "api_key": "ANGEL_API_KEY",
        "client_code": "ANGEL_CLIENT_CODE",
        "pin": "ANGEL_PIN",
        "totp_secret": "ANGEL_TOTP_SECRET",
    }
    cfg = {k: os.environ.get(env, "").strip() for k, env in fields.items()}
    missing = [env for k, env in fields.items() if not cfg[k]]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")
    return cfg


def base_headers(api_key: str) -> dict:
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress": "00:00:00:00:00:00",
        "X-PrivateKey": api_key,
    }


def login(cfg: dict) -> str:
    totp = pyotp.TOTP(cfg["totp_secret"]).now()
    r = requests.post(
        LOGIN_URL,
        json={"clientcode": cfg["client_code"], "password": cfg["pin"], "totp": totp},
        headers=base_headers(cfg["api_key"]),
        timeout=20,
    )
    data = r.json()
    if not data.get("status"):
        raise RuntimeError(f"Angel login failed: {data.get('message', data)}")
    return data["data"]["jwtToken"]


def load_script_map() -> dict[str, str]:
    smap: dict[str, str] = {}
    for inst in requests.get(SCRIP_URL, timeout=60).json():
        if inst.get("exch_seg") == "NSE" and inst.get("instrumenttype") == "":
            sym = inst["symbol"].upper()
            clean = sym.replace("-EQ", "").replace("-BE", "").replace("-IL", "")
            smap[clean] = inst["token"]
            smap[sym] = inst["token"]
    return smap


def load_symbols() -> list[str]:
    if not INDEX_HTML.exists():
        raise FileNotFoundError(f"{INDEX_HTML} not found — run prepare_pages.py once first")
    html = INDEX_HTML.read_text(encoding="utf-8")
    match = re.search(r"const STOCKS_RAW = (\[.*?\]);", html, re.DOTALL)
    if not match:
        raise RuntimeError("Could not parse STOCKS_RAW from docs/index.html")
    return [s["symbol"] for s in json.loads(match.group(1)) if s.get("symbol")]


def fetch_quotes(symbols: list[str], jwt: str, api_key: str, script_map: dict[str, str]) -> dict:
    pairs = [(sym, script_map.get(sym) or script_map.get(f"{sym}-EQ"))
             for sym in symbols]
    pairs = [(sym, tok) for sym, tok in pairs if tok]

    results: dict[str, dict] = {}
    headers = base_headers(api_key)
    headers["Authorization"] = f"Bearer {jwt}"

    print(f"Fetching quotes for {len(pairs)}/{len(symbols)} symbols…")
    for i in range(0, len(pairs), BATCH_SIZE):
        batch = pairs[i : i + BATCH_SIZE]
        token_to_sym = {tok: sym for sym, tok in batch}
        payload = {"mode": "FULL", "exchangeTokens": {"NSE": [tok for _, tok in batch]}}
        try:
            data = requests.post(QUOTE_URL, json=payload, headers=headers, timeout=20).json()
            for q in data.get("data", {}).get("fetched", []):
                sym = token_to_sym.get(q.get("symbolToken", ""), "")
                if not sym:
                    continue
                ltp = float(q.get("ltp", 0) or 0)
                prev_close = float(q.get("close", 0) or 0)
                results[sym] = {
                    "ltp": ltp,
                    "prev_close": prev_close,
                    "pchg": round((ltp - prev_close) / prev_close * 100, 2) if prev_close else None,
                    "open": float(q.get("open", 0) or 0),
                    "high": float(q.get("high", 0) or 0),
                    "low": float(q.get("low", 0) or 0),
                    "volume": int(q.get("tradeVolume", 0) or 0),
                }
        except Exception as exc:
            print(f"  batch {i // BATCH_SIZE + 1} failed: {exc}")
        time.sleep(RATE_DELAY)

    return results


def main() -> int:
    if os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("GITHUB_EVENT_NAME") == "schedule":
        if not is_market_hours():
            print("Outside NSE market hours — skipping")
            return 0

    cfg = load_config()
    jwt = login(cfg)
    symbols = load_symbols()
    quotes = fetch_quotes(symbols, jwt, cfg["api_key"], load_script_map())

    now = datetime.now(timezone.utc)
    QUOTES_PATH.parent.mkdir(parents=True, exist_ok=True)
    QUOTES_PATH.write_text(json.dumps({
        "ts": now.timestamp(),
        "fetched_at_ist": now.astimezone(IST).strftime("%d-%b-%Y %H:%M IST"),
        "count": len(quotes),
        "data": quotes,
    }, indent=2), encoding="utf-8")
    print(f"Wrote {QUOTES_PATH} ({len(quotes)} quotes)")

    result = subprocess.run([sys.executable, str(ROOT / "prepare_pages.py")], cwd=ROOT)
    return result.returncode


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"refresh failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
