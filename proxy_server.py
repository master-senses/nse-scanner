"""
NSE Scanner - Angel Broking SmartAPI Proxy Server
--------------------------------------------------
Runs locally on http://localhost:8765
* Handles login + TOTP generation automatically
* Relays all Angel Broking API calls with proper headers
* Serves the market_scanner.html dashboard
* Adds CORS headers so the browser can call the API freely
* Auto-refreshes token when expired
* Provides a /quotes endpoint that batches all 2125 stocks efficiently

Usage:
    python proxy_server.py               # reads config.json automatically
    python proxy_server.py --config path/to/config.json
"""

import json
import time
import logging
import argparse
import threading
import webbrowser
import os
import sys
from pathlib import Path
from collections import deque
import sys as _sys

# PyInstaller bundles files into a temp folder (_MEIPASS) when running as .exe
# This ensures we find market_scanner.html, final_output.csv, config.json correctly
def _base_path() -> Path:
    if getattr(_sys, "frozen", False):
        return Path(_sys._MEIPASS)
    return Path(__file__).parent

BASE_PATH = _base_path()
from typing import Optional, Dict, Any

import pyotp
import requests
from requests.adapters import HTTPAdapter
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from contextlib import asynccontextmanager
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

# Angel One SmartAPI WebSocket - powers the live 1m/3m/5m OR tracker.
# Optional: if the package isn't installed, /live_or simply stays empty
# and everything else in this file keeps working as before.
try:
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
except ImportError:
    SmartWebSocketV2 = None

# ----------------------- logging -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scanner")

# ----------------------- constants -----------------------
# Shared session with large connection pool for parallel OR candle fetching
_angel_session = requests.Session()
_angel_adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30)
_angel_session.mount("https://", _angel_adapter)
_angel_session.mount("http://",  _angel_adapter)

ANGEL_BASE   = "https://apiconnect.angelbroking.com"
SCRIP_URL    = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
LOGIN_URL    = f"{ANGEL_BASE}/rest/auth/angelbroking/user/v1/loginByPassword"
QUOTE_URL    = f"{ANGEL_BASE}/rest/secure/angelbroking/market/v1/quote/"
PROFILE_URL  = f"{ANGEL_BASE}/rest/secure/angelbroking/user/v1/getProfile"
REFRESH_URL  = f"{ANGEL_BASE}/rest/auth/angelbroking/jwt/v1/generateTokens"

PORT = 8765
BATCH_SIZE = 50          # Angel API max symbols per quote call
RATE_DELAY = 0.25        # seconds between batch calls

# RSI(14) + Williams-fractal Be Ready/Fired signal (long) and the mirrored
# Short Be Ready/Short Fired (short), fed by the same websocket tick stream
# that already powers the live OR tracker -- no extra connection needed.
RSI_PERIOD = 14
RSI_LOW, RSI_HIGH = 40, 60
FRACTAL_WING = 2   # bars on each side -> 5-bar window

# ----------------------- state -----------------------
class State:
    jwt_token:     Optional[str] = None
    refresh_token: Optional[str] = None
    token_expiry:  float = 0          # epoch seconds
    config:        Dict  = {}
    script_map:    Dict  = {}         # symbol -> token string
    script_loaded: bool  = False
    last_quotes:   Dict  = {}         # symbol -> {ltp, pchg, prev_close, ...}
    quotes_ts:     float = 0          # when last_quotes was fetched
    fetching:      bool  = False      # guard against concurrent fetches
    or_data:       Dict  = {}         # symbol -> {or_high, or_low, or_open} - 5-min candle
    or_data_ts:    float = 0          # when or_data was last fetched
    or_fetching:   bool  = False      # guard against concurrent OR fetches
    avg_vol:       Dict  = {}         # symbol -> {avg_vol_20, avg_vol_50, avg_to_20, roc10, roc21, roc63}
    avg_vol_date:  str   = ""         # YYYY-MM-DD of last avg_vol fetch
    avg_vol_fetching: bool = False    # guard against concurrent fetches

    # ---- Live multi-timeframe OR tracker (WebSocket-fed, zero-delay) ----
    feed_token:      Optional[str] = None   # needed alongside jwt_token for SmartWebSocketV2
    live_or_tracker: Dict = {}              # token -> {symbol, last_ltp, 1m/3m/5m: {high, low, complete}}
    or_lock = threading.Lock()              # guards live_or_tracker across the WS thread + request handlers
    ws_connected:    bool = False
    last_or_reset_date = None               # date object; used to detect day rollover and flush stale ranges

    # ---- RSI(14) + Williams-fractal Be Ready/Fired tracker (same WS feed) ----
    rsi_tracker: Dict = {}                  # token -> {symbol, bars, rsi, long_status, short_status, ...}
    rsi_lock = threading.Lock()             # separate from or_lock -- independent state, avoid contention

state = State()

# ----------------------- config -----------------------
def load_config(path: str = "config.json") -> dict:
    p = Path(path)
    if not p.exists():
        log.warning(f"Config not found at {path}, using empty config")
        return {}
    with open(p) as f:
        cfg = json.load(f)
    log.info(f"Config loaded from {path}")
    return cfg

# ----------------------- TOTP -----------------------
def get_totp(secret: str) -> str:
    return pyotp.TOTP(secret).now()

# ----------------------- login -----------------------
def angel_login(cfg: dict) -> bool:
    api_key = cfg.get("api_key", "")
    client  = cfg.get("client_code", "")
    pin     = cfg.get("pin", "")
    totp_s  = cfg.get("totp_secret", "")

    if not all([api_key, client, pin, totp_s]):
        log.error("Missing credentials in config (need api_key, client_code, pin, totp_secret)")
        return False

    totp = get_totp(totp_s)
    log.info(f"Logging in as {client} | TOTP={totp}")

    headers = _base_headers(api_key)
    payload = {"clientcode": client, "password": pin, "totp": totp}

    try:
        r = requests.post(LOGIN_URL, json=payload, headers=headers, timeout=15)
        data = r.json()
    except Exception as e:
        log.error(f"Login request failed: {e}")
        return False

    if data.get("status") and data.get("data", {}).get("jwtToken"):
        state.jwt_token     = data["data"]["jwtToken"]
        state.refresh_token = data["data"].get("refreshToken", "")
        state.feed_token    = data["data"].get("feedToken", "")
        state.token_expiry  = time.time() + 3600   # tokens valid ~1h
        log.info("Login successful OK")
        # Kick off script map download for mid-session re-logins.
        # On startup, on_startup() owns the avg vol thread spawn after executor returns.
        if not state.script_loaded:
            threading.Thread(target=load_script_map, daemon=True).start()
        return True
    else:
        log.error(f"Login failed: {data.get('message', data)}")
        return False

def ensure_logged_in() -> bool:
    """Re-login if token is missing or expired."""
    if state.jwt_token and time.time() < state.token_expiry - 60:
        return True
    log.info("Token missing or expiring, re-logging in...")
    return angel_login(state.config)

# ----------------------- headers -----------------------
def _base_headers(api_key: str) -> dict:
    return {
        "Content-Type":       "application/json",
        "Accept":             "application/json",
        "X-UserType":         "USER",
        "X-SourceID":         "WEB",
        "X-ClientLocalIP":    "127.0.0.1",
        "X-ClientPublicIP":   "127.0.0.1",
        "X-MACAddress":       "00:00:00:00:00:00",
        "X-PrivateKey":       api_key,
    }

def _auth_headers() -> dict:
    h = _base_headers(state.config.get("api_key", ""))
    h["Authorization"] = f"Bearer {state.jwt_token}"
    return h

# ----------------------- script map -----------------------
def load_script_map():
    """Download Angel's master instrument list and build symbol->token map."""
    if state.script_loaded:
        return
    log.info("Downloading instrument master...")
    try:
        r = requests.get(SCRIP_URL, timeout=30)
        instruments = r.json()
        smap = {}
        for inst in instruments:
            if inst.get("exch_seg") == "NSE" and inst.get("instrumenttype") == "":
                sym = inst["symbol"].upper()
                # strip -EQ / -BE suffix
                clean = sym.replace("-EQ", "").replace("-BE", "").replace("-IL", "")
                smap[clean] = inst["token"]
                smap[sym]   = inst["token"]   # keep original too
        state.script_map   = smap
        state.script_loaded = True
        log.info(f"Instrument master loaded - {len(smap)} entries")
    except Exception as e:
        log.error(f"Failed to load instrument master: {e}")

# ----------------------- live multi-timeframe OR tracker -----------------------
# Streams ticks over a WebSocket and tracks the 1m / 3m / 5m opening range live,
# with zero polling delay. Complements the REST-based /ordata (static 5-min
# candle, fetched once after 9:20) with continuously updating ranges and
# breakout/breakdown status for the active session.

def init_live_or_tracker(target_date):
    """(Re)builds the token->node tracking map for `target_date` and stamps the
    reset date so update_live_or_tick() can detect the next day's rollover."""
    with state.or_lock:
        state.live_or_tracker.clear()
        for sym in state.symbols:
            tok = state.script_map.get(sym) or state.script_map.get(sym + "-EQ")
            if not tok:
                continue
            state.live_or_tracker[tok] = {
                "symbol": sym,
                "last_ltp": None,
                "1m": {"open": None, "high": None, "low": None, "close": None, "complete": False},
                "3m": {"open": None, "high": None, "low": None, "close": None, "complete": False},
                "5m": {"open": None, "high": None, "low": None, "close": None, "complete": False},
            }
        state.last_or_reset_date = target_date
    log.info(f"[LIVE-OR] Tracker (re)initialized for {len(state.live_or_tracker)} scrips - date bound: {target_date}")

def update_live_or_tick(token: str, ltp: float):
    """Folds one incoming tick into the relevant 1m/3m/5m windows. Detects a new
    trading day and flushes stale ranges before touching the tracker, so this
    can run unattended across midnight without carrying over yesterday's high/low.
    Tracks open/high/low/close per window (not just high/low) so /ordata can use
    this as a drop-in replacement for the REST 5-min candle, not just a status check."""
    from datetime import datetime as _dt
    now   = _dt.now()
    today = now.date()

    if state.last_or_reset_date != today:
        log.info(f"[LIVE-OR] New day detected ({today}) - flushing stale ranges...")
        init_live_or_tracker(today)
        init_rsi_tracker(today)

    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    elapsed = (now - market_open).total_seconds()
    if elapsed < 0:
        return  # pre-market tick - ignore

    with state.or_lock:
        node = state.live_or_tracker.get(token)
        if node is None:
            return
        node["last_ltp"] = ltp
        for label, limit_sec in (("1m", 60), ("3m", 180), ("5m", 300)):
            win = node[label]
            if elapsed <= limit_sec:
                if win["open"] is None:
                    win["open"] = ltp          # first tick inside the window
                if win["high"] is None or ltp > win["high"]:
                    win["high"] = ltp
                if win["low"] is None or ltp < win["low"]:
                    win["low"] = ltp
                win["close"] = ltp             # last tick inside the window so far;
                                                # naturally freezes once elapsed crosses limit_sec
            else:
                win["complete"] = True

# ----------------------- RSI(14) + Williams-fractal Be Ready/Fired tracker -----------------------
# Fed by the exact same websocket tick stream as the OR tracker above (see the
# on_data() callback inside run_angel_websocket(), which calls both update
# functions per tick) -- no second websocket connection needed.
#
# Unlike the OR tracker's 1m/3m/5m windows (which freeze once their time
# limit passes), this builds an all-day ROLLING series of 1-minute bars,
# since RSI(14) and the fractal pivot lookback both need a continuous
# series, not a one-shot opening window.
#
#   Be Ready (long):  a confirmed Williams 2-period fractal HIGH (5-bar
#                      window: 2 bars each side) whose RSI, at the bar where
#                      it formed, was between RSI_LOW-RSI_HIGH. The pivot's
#                      price becomes the level to watch.
#   Fired (long):      live price crosses above that pivot.
#   Short Be Ready:    the mirror image, on a fractal LOW.
#   Short Fired:        live price crosses below that pivot.
#
# Fractal confirmation has a built-in 2-bar lag (we only know a bar was a
# pivot once we see the 2 bars after it), so "Be Ready" always fires ~2
# minutes after the actual pivot bar, not the instant it happens.

def _new_rsi_node(sym: str) -> dict:
    return {
        "symbol": sym,
        "cur_minute": None, "cur_bar": None,
        "bars": deque(maxlen=300), "rsi_hist": deque(maxlen=300),
        "_gains": [], "_losses": [], "_avg_gain": None, "_avg_loss": None,
        "_prev_close_1m": None, "rsi": None,
        "active_pivot": None, "long_status": "",
        "active_pivot_low": None, "short_status": "",
    }

def init_rsi_tracker(target_date):
    """(Re)builds the token->node map for `target_date`. Called from the
    same day-rollover check as init_live_or_tracker, so both stay in sync."""
    with state.rsi_lock:
        state.rsi_tracker.clear()
        for sym in state.symbols:
            tok = state.script_map.get(sym) or state.script_map.get(sym + "-EQ")
            if not tok:
                continue
            state.rsi_tracker[tok] = _new_rsi_node(sym)
    log.info(f"[RSI] Tracker (re)initialized for {len(state.rsi_tracker)} scrips - date bound: {target_date}")

def _rsi_update(node: dict, close: float):
    """Wilder's RSI(14): needs 15 closes (14 deltas) before it produces a
    first value; recursively smoothed after that."""
    if node["_prev_close_1m"] is None:
        node["_prev_close_1m"] = close
        return
    delta = close - node["_prev_close_1m"]
    node["_prev_close_1m"] = close
    gain, loss = max(delta, 0.0), max(-delta, 0.0)

    if node["_avg_gain"] is None:
        node["_gains"].append(gain)
        node["_losses"].append(loss)
        if len(node["_gains"]) == RSI_PERIOD:
            node["_avg_gain"] = sum(node["_gains"]) / RSI_PERIOD
            node["_avg_loss"] = sum(node["_losses"]) / RSI_PERIOD
            _rsi_set(node)
        return

    node["_avg_gain"] = (node["_avg_gain"] * (RSI_PERIOD - 1) + gain) / RSI_PERIOD
    node["_avg_loss"] = (node["_avg_loss"] * (RSI_PERIOD - 1) + loss) / RSI_PERIOD
    _rsi_set(node)

def _rsi_set(node: dict):
    if node["_avg_loss"] == 0:
        node["rsi"] = 100.0
    elif node["_avg_gain"] == 0:
        node["rsi"] = 0.0
    else:
        rs = node["_avg_gain"] / node["_avg_loss"]
        node["rsi"] = 100 - (100 / (1 + rs))

def _check_fractal_high(node: dict):
    """Long side: confirmed fractal HIGH + RSI 40-60 at that bar -> Be Ready."""
    bars = node["bars"]
    n = len(bars)
    if n < 2 * FRACTAL_WING + 1:
        return
    idx = n - 1 - FRACTAL_WING  # candidate pivot, confirmed 2 bars later
    candidate = bars[idx]
    neighbors = [bars[idx - w] for w in range(1, FRACTAL_WING + 1)]
    neighbors += [bars[idx + w] for w in range(1, FRACTAL_WING + 1)]
    if all(candidate["high"] > b["high"] for b in neighbors):
        rsi_then = node["rsi_hist"][idx]
        if rsi_then is not None and RSI_LOW <= rsi_then <= RSI_HIGH:
            pivot_price = candidate["high"]
            if node["active_pivot"] != pivot_price:
                node["active_pivot"] = pivot_price
                node["long_status"] = "Be Ready"
                log.info(f"[RSI] {node['symbol']} Be Ready - pivot={pivot_price:.2f} rsi={rsi_then:.1f}")

def _check_fractal_low(node: dict):
    """Short side: mirror of _check_fractal_high, on a fractal LOW."""
    bars = node["bars"]
    n = len(bars)
    if n < 2 * FRACTAL_WING + 1:
        return
    idx = n - 1 - FRACTAL_WING
    candidate = bars[idx]
    neighbors = [bars[idx - w] for w in range(1, FRACTAL_WING + 1)]
    neighbors += [bars[idx + w] for w in range(1, FRACTAL_WING + 1)]
    if all(candidate["low"] < b["low"] for b in neighbors):
        rsi_then = node["rsi_hist"][idx]
        if rsi_then is not None and RSI_LOW <= rsi_then <= RSI_HIGH:
            pivot_price = candidate["low"]
            if node["active_pivot_low"] != pivot_price:
                node["active_pivot_low"] = pivot_price
                node["short_status"] = "Short Be Ready"
                log.info(f"[RSI] {node['symbol']} Short Be Ready - pivot={pivot_price:.2f} rsi={rsi_then:.1f}")

def _finalize_rsi_bar(node: dict, bar: dict):
    node["bars"].append(bar)
    _rsi_update(node, bar["close"])
    node["rsi_hist"].append(node["rsi"])
    _check_fractal_high(node)
    _check_fractal_low(node)

def update_rsi_tracker_tick(token: str, ltp: float):
    """Mirrors update_live_or_tick()'s tick-folding into 1-min bars, but
    keeps an all-day rolling series instead of a one-shot window."""
    from datetime import datetime as _dt
    with state.rsi_lock:
        node = state.rsi_tracker.get(token)
        if node is None:
            return

        minute = _dt.now().replace(second=0, microsecond=0)
        if node["cur_minute"] is None:
            node["cur_minute"] = minute
            node["cur_bar"] = {"high": ltp, "low": ltp, "close": ltp}
        elif minute == node["cur_minute"]:
            b = node["cur_bar"]
            b["high"] = max(b["high"], ltp)
            b["low"] = min(b["low"], ltp)
            b["close"] = ltp
        else:
            _finalize_rsi_bar(node, node["cur_bar"])
            node["cur_minute"] = minute
            node["cur_bar"] = {"high": ltp, "low": ltp, "close": ltp}

        # Breakout/breakdown check on every tick (not just at bar close) for
        # the fastest possible Fired / Short Fired signal.
        if node["long_status"] == "Be Ready" and node["active_pivot"] is not None and ltp > node["active_pivot"]:
            pivot = node["active_pivot"]
            node["long_status"] = "Fired"
            node["active_pivot"] = None
            log.info(f"[RSI] {node['symbol']} Fired - crossed pivot {pivot:.2f} @ {ltp:.2f}")

        if node["short_status"] == "Short Be Ready" and node["active_pivot_low"] is not None and ltp < node["active_pivot_low"]:
            pivot = node["active_pivot_low"]
            node["short_status"] = "Short Fired"
            node["active_pivot_low"] = None
            log.info(f"[RSI] {node['symbol']} Short Fired - crossed pivot {pivot:.2f} @ {ltp:.2f}")


def run_angel_websocket():
    """Background loop: waits for auth + instrument master, then keeps a
    SmartWebSocketV2 connection alive, resubscribing on every (re)connect.
    Reconnects with a short pause on both clean closes and errors so a flaky
    connection can't spin-reconnect rapidly."""
    if not SmartWebSocketV2:
        log.error("[LIVE-OR] SmartApi package not installed - live OR tracking disabled (REST /ordata still works).")
        return

    # ---- Patch a known upstream bug in smartapi-python -------------------
    # SmartWebSocketV2._on_close(self, wsapp) only declares one argument, but
    # every websocket-client release calls it as on_close(wsapp, status, msg).
    # That mismatch crashes the close handler on every disconnect (widely
    # reported on Angel One's own SmartAPI forum, unresolved for years).
    # We patch it once, process-wide, to accept and forward the extra args.
    if not getattr(SmartWebSocketV2, "_or_close_patched", False):
        def _patched_on_close(self, wsapp, close_status_code=None, close_msg=None):
            if self.on_close:
                self.on_close(wsapp, close_status_code, close_msg)
        SmartWebSocketV2._on_close = _patched_on_close
        SmartWebSocketV2._or_close_patched = True
        log.info("[LIVE-OR] Patched SmartWebSocketV2._on_close (upstream arg-count bug).")

    while True:
        if not (state.jwt_token and state.feed_token):
            time.sleep(5)
            continue
        if not state.script_loaded:
            time.sleep(2)
            continue
        if not state.live_or_tracker:
            from datetime import date as _d
            init_live_or_tracker(_d.today())
        if not state.rsi_tracker:
            from datetime import date as _d
            init_rsi_tracker(_d.today())

        try:
            log.info("[LIVE-OR] Spawning WebSocket feed instance...")
            sws = SmartWebSocketV2(
                state.jwt_token,
                state.config.get("api_key", ""),
                state.config.get("client_code", ""),
                state.feed_token,
            )

            def on_data(wsapp, message):
                token   = message.get("token")
                raw_ltp = message.get("last_traded_price")
                if token and raw_ltp is not None:
                    ltp = float(raw_ltp) / 100.0 if isinstance(raw_ltp, int) else float(raw_ltp)
                    update_live_or_tick(token, ltp)
                    update_rsi_tracker_tick(token, ltp)

            def on_open(wsapp):
                log.info("[LIVE-OR] WebSocket connected - subscribing...")
                state.ws_connected = True
                with state.or_lock:
                    tokens = list(state.live_or_tracker.keys())
                if not tokens:
                    log.warning("[LIVE-OR] No tracked tokens to subscribe.")
                    return
                # subscribe(correlation_id, mode, token_list) - three separate
                # positional args (NOT a single combined payload dict).
                # mode=2 -> Quote mode; high/low are still computed locally from ticks.
                chunk_size = 500   # safe chunk boundary for Angel's subscription payloads
                for i in range(0, len(tokens), chunk_size):
                    chunk = tokens[i:i + chunk_size]
                    correlation_id = f"or{i:08d}"  # fixed 10-char alphanumeric id, per Angel's docs
                    try:
                        sws.subscribe(correlation_id, 2, [{"exchangeType": 1, "tokens": chunk}])
                    except Exception as e:
                        log.error(f"[LIVE-OR] subscribe failed for chunk {i}: {e}")
                    time.sleep(0.2)

            def on_close(wsapp, code, reason):
                log.warning(f"[LIVE-OR] WebSocket closed: {code} - {reason}")
                state.ws_connected = False

            def on_error(wsapp, err):
                log.error(f"[LIVE-OR] WebSocket error: {err}")

            sws.on_data  = on_data
            sws.on_open  = on_open
            sws.on_close = on_close
            sws.on_error = on_error

            sws.connect()  # blocks until the connection drops

            log.info("[LIVE-OR] WebSocket runtime stopped - pausing before reconnect...")
            state.ws_connected = False
            time.sleep(5)
        except Exception as ex:
            log.error(f"[LIVE-OR] WebSocket runtime crashed: {ex} - retrying in 10s...")
            state.ws_connected = False
            time.sleep(10)

# ----------------------- quote fetching -----------------------
def fetch_quotes_for_symbols(symbols: list) -> dict:
    """
    Fetch LTP + prev_close for a list of NSE symbols.
    Returns dict: symbol -> {ltp, prev_close, pchg, volume, high, low}
    """
    if not ensure_logged_in():
        raise RuntimeError("Not authenticated")

    load_script_map()
    results = {}

    # Build (symbol, token) pairs - skip unknowns
    pairs = []
    for sym in symbols:
        tok = state.script_map.get(sym) or state.script_map.get(sym + "-EQ")
        if tok:
            pairs.append((sym, tok))
        else:
            pass  # silently skip - very small-cap / SME stocks may be missing

    log.info(f"Fetching quotes for {len(pairs)}/{len(symbols)} mapped symbols in batches of {BATCH_SIZE}...")

    for i in range(0, len(pairs), BATCH_SIZE):
        batch  = pairs[i : i + BATCH_SIZE]
        tokens = [tok for _, tok in batch]
        token_to_sym = {tok: sym for sym, tok in batch}

        payload = {"mode": "FULL", "exchangeTokens": {"NSE": tokens}}
        try:
            r = requests.post(QUOTE_URL, json=payload, headers=_auth_headers(), timeout=15)
            data = r.json()
            # One-time debug: log all keys from first batch first quote
            if i == 0 and data.get("data", {}).get("fetched"):
                sample = data["data"]["fetched"][0]
                log.info(f"FULL mode sample keys: {list(sample.keys())}")
                log.info(f"FULL mode sample: { {k:v for k,v in sample.items() if k in ['symbol','ltp','tradedVolume','tradeVolume','totTradedVol','volume','totTradedQty']} }")

            if not data.get("status"):
                # Token might have expired mid-fetch
                if "token" in data.get("message", "").lower():
                    log.warning("Token expired mid-fetch, re-logging in...")
                    if angel_login(state.config):
                        r = requests.post(QUOTE_URL, json=payload, headers=_auth_headers(), timeout=15)
                        data = r.json()

            fetched = data.get("data", {}).get("fetched", [])
            for q in fetched:
                tok = q.get("symbolToken", "")
                sym = token_to_sym.get(tok, "")
                if not sym:
                    continue
                ltp        = float(q.get("ltp", 0) or 0)
                prev_close = float(q.get("close", 0) or 0)
                pchg = round((ltp - prev_close) / prev_close * 100, 2) if prev_close else None
                results[sym] = {
                    "ltp":        ltp,
                    "prev_close": prev_close,
                    "pchg":       pchg,
                    "open":       float(q.get("open", 0) or 0),
                    "high":       float(q.get("high", 0) or 0),
                    "low":        float(q.get("low", 0) or 0),
                    "volume":     int(q.get("tradeVolume", 0) or 0),
                }

        except Exception as e:
            log.warning(f"Batch {i//BATCH_SIZE + 1} error: {e}")

        time.sleep(RATE_DELAY)

    log.info(f"Quotes fetched: {len(results)} symbols")
    return results

# ----------------------- FastAPI app -----------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    await on_startup()
    yield

app = FastAPI(title="NSE Scanner Proxy", version="1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the dashboard HTML
DASHBOARD_PATH = BASE_PATH / "market_scanner.html"

def build_stocks_raw() -> str:
    """
    Reads CSV and returns a JS-safe JSON array string for STOCKS_RAW.
    Supports both column naming conventions:
      - lowercase: symbol, ffmc_cr, mcap_cr, macro, sector, industryInfo, priceband
      - uppercase: SYMBOL, FFMC_CR, MCAP_CR, MACRO, SECTOR, INDUSTRY, PRICE_BAND
    fno = True when priceband == 'No Band' (F&O eligible stocks)
    """
    import csv as _csv, json as _json
    # Accept either filename
    for fname in ("output_nse_data.csv", "final_output.csv"):
        csv_path = BASE_PATH / fname
        if csv_path.exists():
            break
    else:
        return "[]"

    def g(r, *keys):
        """Get first matching key (case-insensitive fallback)."""
        for k in keys:
            if k in r: return r[k]
        # case-insensitive fallback
        kl = {x.lower(): x for x in r}
        for k in keys:
            if k.lower() in kl: return r[kl[k.lower()]]
        return ""

    rows = []
    with open(csv_path) as f:
        for r in _csv.DictReader(f):
            try: mcap = float(g(r, "mcap_cr", "MCAP_CR") or 0)
            except: mcap = 0.0
            try: ffmc = float(g(r, "ffmc_cr", "FFMC_CR") or mcap)
            except: ffmc = mcap
            priceband = g(r, "priceband", "PRICE_BAND", "priceBand").strip()
            rows.append({
                "symbol":   g(r, "symbol", "SYMBOL"),
                "mcap_cr":  mcap,
                "ffmc_cr":  ffmc,
                "priceband": priceband,
                "macro":    g(r, "macro", "MACRO"),
                "sector":   g(r, "sector", "SECTOR"),
                "industry": g(r, "industryInfo", "industry", "INDUSTRY"),
                "fno":      priceband == "No Band",
            })
    log.info(f"Built STOCKS_RAW: {len(rows)} stocks from {csv_path.name}")
    return _json.dumps(rows, separators=(',', ':'))

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    if not DASHBOARD_PATH.exists():
        return HTMLResponse("<h2>market_scanner.html not found next to proxy_server.py</h2>", status_code=404)
    html = DASHBOARD_PATH.read_text(encoding="utf-8")
    # Inject rebuilt STOCKS_RAW from CSV (includes ffmc_cr, fno, latest data)
    stocks_json = build_stocks_raw()
    html = html.replace('/*STOCKS_RAW_PLACEHOLDER*/', f'const STOCKS_RAW = {stocks_json};')
    return HTMLResponse(html, media_type="text/html")

@app.get("/health")
async def health():
    return {
        "status":        "ok",
        "authenticated": bool(state.jwt_token),
        "token_expiry":  state.token_expiry,
        "script_loaded": state.script_loaded,
        "last_quotes_age_s": round(time.time() - state.quotes_ts) if state.quotes_ts else None,
    }

@app.post("/login")
async def login_endpoint(body: dict):
    """
    Manual login - POST {api_key, client_code, pin, totp_secret}
    Or just trigger auto-login from config.
    """
    if body:
        state.config.update(body)
    ok = angel_login(state.config)
    if ok:
        return {"status": True, "message": "Login successful", "token": state.jwt_token}
    raise HTTPException(status_code=401, detail="Login failed - check credentials / TOTP")

@app.get("/quotes")
async def get_all_quotes(force: bool = False):
    """
    Returns cached quotes if < 55s old, otherwise fetches fresh.
    Add ?force=1 to bypass cache and fetch immediately.
    Response: { "ts": epoch, "data": { "SYMBOL": {ltp, pchg, ...} } }
    """
    age = time.time() - state.quotes_ts
    if not force and age < 55 and state.last_quotes:
        log.info(f"Serving cached quotes (age={age:.0f}s)")
        return {"ts": state.quotes_ts, "cached": True, "data": state.last_quotes}

    if state.fetching:
        # Another request is already fetching - return stale data
        return {"ts": state.quotes_ts, "cached": True, "fetching": True, "data": state.last_quotes}

    state.fetching = True
    try:
        # Read stock list from CSV embedded at startup
        symbols = list(state.symbols) if hasattr(state, "symbols") else []
        if not symbols:
            raise HTTPException(status_code=500, detail="No symbols loaded")
        quotes = fetch_quotes_for_symbols(symbols)
        state.last_quotes = quotes
        state.quotes_ts   = time.time()
        return {"ts": state.quotes_ts, "cached": False, "data": quotes}
    except Exception as e:
        log.error(f"/quotes error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        state.fetching = False

@app.get("/quotes/partial")
async def get_partial_quotes(syms: str):
    """Fetch quotes for a comma-separated list of symbols (for on-demand drilldown)."""
    symbols = [s.strip().upper() for s in syms.split(",") if s.strip()]
    if not symbols:
        raise HTTPException(status_code=400, detail="No symbols provided")
    try:
        quotes = fetch_quotes_for_symbols(symbols)
        return {"ts": time.time(), "data": quotes}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/token_status")
async def token_status():
    remaining = max(0, state.token_expiry - time.time())
    return {
        "authenticated": bool(state.jwt_token),
        "expires_in_s":  round(remaining),
        "script_loaded": state.script_loaded,
    }

# Generic Angel API proxy - forwards any path under /angel/
@app.api_route("/angel/{path:path}", methods=["GET", "POST"])
async def angel_proxy(path: str, request: Request):
    """
    Transparent proxy to Angel Broking API.
    GET/POST /angel/rest/secure/... -> forwards to apiconnect.angelbroking.com/rest/secure/...
    """
    if not ensure_logged_in():
        raise HTTPException(status_code=401, detail="Not authenticated")

    url = f"{ANGEL_BASE}/{path}"
    method  = request.method
    headers = _auth_headers()

    try:
        body = await request.body()
        if method == "POST":
            r = requests.post(url, data=body, headers=headers, timeout=15)
        else:
            r = requests.get(url, headers=headers, timeout=15)
        return JSONResponse(content=r.json(), status_code=r.status_code)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

# ----------------------- OR candle fetching -----------------------
CANDLE_URL = f"{ANGEL_BASE}/rest/secure/angelbroking/historical/v1/getCandleData"

def fetch_or_candles(symbols: list, trade_date: str = None) -> dict:
    """
    Fetch the 9:15-9:20 AM (first 5-min) candle for all symbols.
    Uses a thread pool for parallel fetching — ~10x faster than sequential.
    trade_date: "YYYY-MM-DD" - defaults to today.
    Returns dict: symbol -> {or_open, or_high, or_low, or_close}
    """
    import concurrent.futures
    import threading

    if not ensure_logged_in():
        raise RuntimeError("Not authenticated")

    load_script_map()

    from datetime import datetime, date
    if trade_date:
        try:
            datetime.strptime(trade_date, "%Y-%m-%d")
            use_date = trade_date
        except ValueError:
            raise ValueError(f"Invalid date format '{trade_date}' - use YYYY-MM-DD")
    else:
        use_date = date.today().strftime("%Y-%m-%d")

    from_time = f"{use_date} 09:15"
    to_time   = f"{use_date} 09:20"

    # Build (sym, token) pairs
    pairs = [(sym, state.script_map.get(sym) or state.script_map.get(sym + "-EQ"))
             for sym in symbols]
    pairs = [(sym, tok) for sym, tok in pairs if tok]
    total = len(pairs)
    log.info(f"Fetching OR candles: {total} symbols in parallel (date={use_date})")

    results = {}
    results_lock = threading.Lock()
    done_count   = [0]
    errors       = [0]

    # Rate limiter: max 20 concurrent workers, ~25 req/sec total
    # Angel One allows 3 req/sec per connection but multiple connections are fine
    WORKERS    = 20
    REQ_DELAY  = 0.05   # 50ms between requests per thread = 20*20 = ~400 req/s headroom
                         # actual Angel limit is per-IP ~30 req/sec, we stay safe

    def fetch_one(sym_tok):
        sym, tok = sym_tok
        payload = {
            "exchange":    "NSE",
            "symboltoken": tok,
            "interval":    "FIVE_MINUTE",
            "fromdate":    from_time,
            "todate":      to_time,
        }
        BACKOFF = [1, 3, 6]
        for attempt in range(3):
            try:
                r   = _angel_session.post(CANDLE_URL, json=payload, headers=_auth_headers(), timeout=10)
                raw = r.text.strip()
                if not raw:
                    time.sleep(BACKOFF[attempt])
                    continue
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    time.sleep(BACKOFF[attempt])
                    continue
                candles = data.get("data", [])
                if candles:
                    c = candles[0]
                    with results_lock:
                        results[sym] = {
                            "or_open":  float(c[1]),
                            "or_high":  float(c[2]),
                            "or_low":   float(c[3]),
                            "or_close": float(c[4]),
                        }
                break
            except Exception as e:
                if attempt == 2:
                    with results_lock:
                        errors[0] += 1
                else:
                    time.sleep(BACKOFF[attempt])
        time.sleep(REQ_DELAY)
        with results_lock:
            done_count[0] += 1
            if done_count[0] % 200 == 0:
                pct = done_count[0] / total * 100
                log.info(f"OR candles: {done_count[0]}/{total} ({pct:.0f}%) — {len(results)} with data, {errors[0]} errors")

    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        executor.map(fetch_one, pairs)

    elapsed = time.time() - start
    log.info(f"OR candles done: {len(results)}/{total} symbols in {elapsed:.1f}s ({errors[0]} errors)")
    return results

@app.get("/ordata")
async def get_or_data(force: bool = False, date: str = None):
    """
    Returns the Opening Range (9:15-9:20 first 5-min candle) for all symbols.

    Source: for "today", prefers the live websocket-tracked 5m window (free,
    instant, available the moment 9:20 passes) and only falls back to Angel's
    REST historical-candle API for symbols the live tracker doesn't have full
    data for (e.g. websocket was down, or a symbol simply hasn't traded yet).
    Each entry carries "source": "live" or "rest" so you can see which path it
    came from. ?force=1 skips live data entirely and re-fetches everything
    from REST - useful to cross-check against the official exchange candle.
    Past dates (?date=) always use REST; the live tracker only knows "today".

    Parameters:
      ?force=1          - bypass cache AND live data, re-fetch everything from REST
      ?date=YYYY-MM-DD  - fetch a specific past date (for testing after market hours)

    Cached for the entire trading day (per date). A new date automatically re-fetches.
    Response: { "ts": epoch, "date": "YYYY-MM-DD", "live_count": N, "rest_count": M,
                 "data": { "SYMBOL": {or_open, or_high, or_low, or_close, status, ltp, source} } }
    """
    from datetime import date as _date, datetime

    # Determine which date to use
    today_str    = _date.today().strftime("%Y-%m-%d")
    request_date = date if date else today_str  # ?date= overrides today

    # Validate custom date format
    if date:
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid date '{date}' - use YYYY-MM-DD")

    # Check cache - serve if same date and not forced
    cached_date = ""
    if state.or_data_ts:
        cached_date = datetime.fromtimestamp(state.or_data_ts).strftime("%Y-%m-%d")

    # Use cache only if same requested date
    if not force and state.or_data and cached_date == request_date:
        return {"ts": state.or_data_ts, "date": request_date, "cached": True, "data": state.or_data}

    if state.or_fetching:
        return {"ts": state.or_data_ts, "date": request_date, "cached": True, "fetching": True, "data": state.or_data}

    is_today = (request_date == today_str)

    # For today, only fetch after 9:20 AM (skip this check for past dates)
    if is_today:
        now = datetime.now()
        if now.hour == 9 and now.minute < 20:
            return {"ts": 0, "date": today_str, "cached": False,
                    "error": "Market opens at 9:15. OR data available after 9:20 AM.", "data": {}}

    state.or_fetching = True
    try:
        symbols = list(state.symbols) if hasattr(state, "symbols") else []
        if not symbols:
            raise HTTPException(status_code=500, detail="No symbols loaded")

        or_enriched  = {}
        rest_targets = symbols   # default: REST-fetch everyone (past dates, or ?force=1)

        # ---- Prefer live websocket 5m data for "today", unless ?force=1 ----
        if is_today and not force:
            with state.or_lock:
                live_snapshot = {}
                for node in state.live_or_tracker.values():
                    win = dict(node["5m"])
                    win["last_ltp"] = node["last_ltp"]
                    live_snapshot[node["symbol"]] = win

            still_missing = []
            for sym in symbols:
                win = live_snapshot.get(sym)
                has_full_candle = (
                    win is not None
                    and win["open"] is not None and win["high"] is not None
                    and win["low"] is not None and win["close"] is not None
                )
                if has_full_candle:
                    ltp = win["last_ltp"] if win["last_ltp"] is not None else state.last_quotes.get(sym, {}).get("ltp")
                    status = "inside"
                    if ltp is not None:
                        if ltp > win["high"]:  status = "breakout"
                        elif ltp < win["low"]: status = "breakdown"
                    or_enriched[sym] = {
                        "or_open": win["open"], "or_high": win["high"],
                        "or_low": win["low"], "or_close": win["close"],
                        "status": status, "ltp": ltp, "date": request_date, "source": "live",
                    }
                else:
                    still_missing.append(sym)
            rest_targets = still_missing

            if rest_targets:
                log.info(f"/ordata: {len(or_enriched)} symbols from live tracker, "
                         f"{len(rest_targets)} need REST fallback")
            else:
                log.info(f"/ordata: all {len(or_enriched)} symbols served from live tracker - no REST calls needed")

        # ---- REST: only for whatever live data didn't cover (or everyone, for past dates / ?force=1) ----
        if rest_targets:
            candles = fetch_or_candles(rest_targets, trade_date=request_date)

            if not candles and not or_enriched:
                raise HTTPException(status_code=404,
                    detail=f"No OR candle data found for {request_date}. "
                           f"Market may have been closed or date is a holiday/weekend.")

            for sym, c in candles.items():
                if is_today:
                    ltp = state.last_quotes.get(sym, {}).get("ltp")
                else:
                    ltp = c["or_open"]   # use open as reference for past dates
                status = "inside"
                if ltp is not None:
                    if ltp > c["or_high"]:  status = "breakout"
                    elif ltp < c["or_low"]: status = "breakdown"
                or_enriched[sym] = {**c, "status": status, "ltp": ltp, "date": request_date, "source": "rest"}

        if not or_enriched:
            raise HTTPException(status_code=404,
                detail=f"No OR data found for {request_date}. "
                       f"Market may have been closed or date is a holiday/weekend.")

        state.or_data    = or_enriched
        state.or_data_ts = time.time()
        live_n = sum(1 for v in or_enriched.values() if v.get("source") == "live")
        rest_n = len(or_enriched) - live_n
        log.info(f"OR data ready: {len(or_enriched)} symbols for {request_date} ({live_n} live, {rest_n} rest)")
        return {"ts": state.or_data_ts, "date": request_date, "cached": False,
                "live_count": live_n, "rest_count": rest_n, "data": or_enriched}
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"/ordata error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        state.or_fetching = False

@app.get("/ordata/status")
async def get_or_status():
    """Quick summary - breakout/breakdown/inside counts + market regime."""
    if not state.or_data:
        return {"ready": False, "message": "OR data not fetched yet. Call /ordata first."}

    breakouts  = sum(1 for v in state.or_data.values() if v.get("status") == "breakout")
    breakdowns = sum(1 for v in state.or_data.values() if v.get("status") == "breakdown")
    inside     = sum(1 for v in state.or_data.values() if v.get("status") == "inside")
    total      = breakouts + breakdowns + inside
    bo_pct     = round(breakouts / total * 100, 1) if total else 0

    if   bo_pct >= 70: regime = "Strong Bull"
    elif bo_pct >= 55: regime = "Mild Bull"
    elif bo_pct >= 45: regime = "Neutral"
    elif bo_pct >= 30: regime = "Mild Bear"
    else:              regime = "Strong Bear"

    return {
        "ready":      True,
        "breakouts":  breakouts,
        "breakdowns": breakdowns,
        "inside":     inside,
        "total":      total,
        "bo_pct":     bo_pct,
        "regime":     regime,
        "ts":         state.or_data_ts,
    }

@app.get("/live_or")
async def get_live_or_status():
    """
    Live (WebSocket-fed) 1m / 3m / 5m opening range, updated tick-by-tick with
    zero polling delay. Unlike /ordata (a single static 5-min candle fetched
    once after 9:20), this keeps updating through the session and each
    timeframe flags its own breakout/breakdown/inside status independently.

    Also carries %chg and %gap per symbol (same fields/computation as the
    Stock Scanner's /scan/stocks) so the Live OR table can show and sort by
    them without a second request.

    Also carries the RSI(14) + Williams-fractal Be Ready/Fired (long) and
    Short Be Ready/Short Fired (short) signal -- see update_rsi_tracker_tick()
    -- plus 10D/21D/63D price ROC (from fetch_avg_volume / the avg-volume
    cache), so the table has everything in one response.

    Response: { "ws_active": bool, "data": { "SYMBOL": {
        "ltp", "pchg", "gap", "rsi", "long_status", "short_status",
        "roc10", "roc21", "roc63",
        "1m"/"3m"/"5m": {open, high, low, close, status, complete}
    } } }
    """
    # Snapshot just the scalar rsi/status fields under their own lock first,
    # so the main OR loop below doesn't need to hold two locks at once.
    with state.rsi_lock:
        rsi_summary = {
            tok: (node.get("rsi"), node.get("long_status", ""), node.get("short_status", ""))
            for tok, node in state.rsi_tracker.items()
        }

    output = {}
    with state.or_lock:
        for token, node in state.live_or_tracker.items():
            sym = node["symbol"]
            ltp = node["last_ltp"]
            quote = state.last_quotes.get(sym, {})
            roc = state.avg_vol.get(sym) or {}
            rsi_val, long_status, short_status = rsi_summary.get(token, (None, "", ""))
            entry = {
                "ltp":  ltp,
                "pchg": quote.get("pchg"),
                "gap":  _compute_gap(sym),
                "rsi":          rsi_val,
                "long_status":  long_status,
                "short_status": short_status,
                "roc10": roc.get("roc10"),
                "roc21": roc.get("roc21"),
                "roc63": roc.get("roc63"),
            }
            for frame in ("1m", "3m", "5m"):
                fdata = node[frame]
                high, low = fdata["high"], fdata["low"]
                status = "waiting"
                if high is not None and low is not None and ltp is not None:
                    if ltp > high:
                        status = "breakout"
                    elif ltp < low:
                        status = "breakdown"
                    else:
                        status = "inside"
                entry[frame] = {
                    "open": fdata["open"], "high": high, "low": low, "close": fdata["close"],
                    "status": status, "complete": fdata["complete"],
                }
            output[sym] = entry
    return {"ws_active": state.ws_connected, "data": output}

# ----------------------- avg volume (20D / 50D) -----------------------

def fetch_avg_volume(symbols: list) -> dict:
    """
    Fetch daily candles per symbol and compute avg_vol_20d/avg_vol_50d volume PLUS
    10D/21D/63D price ROC, all from the same candle fetch (no second pass).
    Uses getCandleData with interval=ONE_DAY.
    Returns dict: symbol -> {avg_vol_20, avg_vol_50, roc10, roc21, roc63}
    (no avg_to_20/turnover here -- see the comment at the call site below)

    ROC = (latest_close - close_N_bars_ago) / close_N_bars_ago * 100.

    Note: 'tradeVolume' in FULL-mode quotes is today's cumulative volume
    (market open -> now), so no separate today-volume fetch needed.
    """
    if not ensure_logged_in():
        raise RuntimeError("Not authenticated")

    # Script map must already be loaded by caller (_avg_vol_after_login waits for it).
    # Log a hard error if it somehow isn't - helps diagnose the race condition.
    if not state.script_loaded:
        log.error("[AVG-VOL] fetch_avg_volume called before instrument master is ready!")
        return {}

    from datetime import date, timedelta
    # Fetch last 130-calendar-day window: ~90 trading sessions after holidays,
    # comfortably covering both avg50 (needs 50) and roc63 (needs 64) from a
    # single fetch per symbol.
    to_date   = date.today().strftime("%Y-%m-%d")
    from_date = (date.today() - timedelta(days=130)).strftime("%Y-%m-%d")
    from_time = f"{from_date} 09:15"
    to_time   = f"{to_date} 15:30"

    # Count how many symbols have tokens before we start
    mapped = [s for s in symbols if state.script_map.get(s) or state.script_map.get(s + "-EQ")]
    log.info(f"[AVG-VOL] {len(mapped)}/{len(symbols)} symbols mapped . date window: {from_date} -> {to_date}")
    if not mapped:
        log.error("[AVG-VOL] 0 symbols have tokens - instrument master may be incomplete")
        return {}

    results = {}
    total   = len(mapped)
    done    = 0
    errors  = 0
    _diag_done = False   # log one sample response for diagnosis

    for sym in symbols:
        tok = state.script_map.get(sym) or state.script_map.get(sym + "-EQ")
        if not tok:
            continue
        payload = {
            "exchange":    "NSE",
            "symboltoken": tok,
            "interval":    "ONE_DAY",
            "fromdate":    from_time,
            "todate":      to_time,
        }
        for attempt in range(3):
            try:
                r = requests.post(CANDLE_URL, json=payload, headers=_auth_headers(), timeout=15)
                raw = r.text.strip()
                if not raw:
                    log.warning(f"[AVG-VOL] Empty response for {sym} (attempt {attempt+1})")
                    time.sleep(0.5 * (attempt + 1))
                    continue
                data    = r.json()

                # One-time diagnostic: log first response in full
                if not _diag_done:
                    _diag_done = True
                    log.info(f"[AVG-VOL] First API response for {sym}: status={data.get('status')} "                             f"msg={data.get('message','')} candles={len(data.get('data') or [])}")
                    if data.get('data'):
                        log.info(f"[AVG-VOL] Sample candle: {data['data'][0]}")

                if not data.get("status"):
                    msg = data.get('message', '')
                    # Token expired - re-login and retry
                    if 'token' in msg.lower() or 'session' in msg.lower():
                        log.warning("[AVG-VOL] Token expired mid-fetch - re-logging in")
                        if ensure_logged_in():
                            continue
                    errors += 1
                    if errors <= 3:
                        log.warning(f"[AVG-VOL] API error for {sym}: {msg}")
                    break

                candles = data.get("data") or []
                today_str = date.today().strftime("%Y-%m-%d")
                hist   = [c for c in candles if not str(c[0]).startswith(today_str)]
                vols   = [int(c[5])   for c in hist if len(c) > 5 and c[5]]
                closes = [float(c[4]) for c in hist if len(c) > 4 and c[4]]

                if vols or closes:
                    entry = {}
                    if vols:
                        entry["avg_vol_20"] = round(sum(vols[-20:]) / min(len(vols), 20))
                        entry["avg_vol_50"] = round(sum(vols[-50:]) / min(len(vols), 50))
                        # avg_to_20 (turnover) isn't computed here -- Angel's candle data has
                        # no turnover field, only volume, so the cache file's avg_to_20 (from
                        # NSE's reported turnover via your external script) can't be replicated
                        # exactly here. Nothing in this app currently reads avg_to_20 from
                        # state.avg_vol though, so this gap is currently inert either way.

                    # closes is oldest-first (Angel returns ascending), so
                    # closes[-1] is the latest close and closes[-(N+1)] is
                    # N trading days back.
                    latest_close = closes[-1] if closes else None
                    for n, key in ((10, "roc10"), (21, "roc21"), (63, "roc63")):
                        if latest_close is not None and len(closes) > n and closes[-(n + 1)]:
                            ref = closes[-(n + 1)]
                            entry[key] = round((latest_close - ref) / ref * 100, 2)
                        else:
                            entry[key] = None

                    results[sym] = entry
                break
            except Exception as e:
                if attempt == 2:
                    errors += 1
                    log.warning(f"[AVG-VOL] Exception for {sym}: {e}")
                else:
                    time.sleep(0.3 * (attempt + 1))

        done += 1
        if done % 200 == 0:
            log.info(f"[AVG-VOL] Progress: {done}/{total} processed, {len(results)} with data, {errors} errors")
        time.sleep(0.15)   # ~6-7 req/sec - stays within Angel rate limits

    log.info(f"[AVG-VOL] Done: {len(results)}/{total} symbols got avg volume data ({errors} errors)")
    return results


def _merge_roc_from_pct_chg_cache():
    """Backfills roc10/21/63 into state.avg_vol from fetch_pct_chg.json -- a
    SEPARATE cache file (different schema: chg10/chg21/chg63, written by a
    separate fetch_pct_chg.py script) from fetch_avg_vol.json. Called after
    state.avg_vol is populated, regardless of whether that came from the
    fast cache path or the live fallback, so ROC is available either way.
    Overwrites any roc10/21/63 the live fallback may have self-computed --
    this file is the authoritative source when present."""
    pct_chg_file = BASE_PATH / "fetch_pct_chg.json"
    if not pct_chg_file.exists():
        log.info("fetch_pct_chg.json not found -- ROC will rely on the live-fallback's own calc, if any")
        return
    try:
        with open(pct_chg_file) as f:
            cached = json.load(f)
    except Exception as e:
        log.warning(f"fetch_pct_chg.json read error: {e}")
        return

    data = cached.get("data") or {}
    for sym, chg in data.items():
        if sym not in state.avg_vol:
            state.avg_vol[sym] = {}
        state.avg_vol[sym]["roc10"] = chg.get("chg10")
        state.avg_vol[sym]["roc21"] = chg.get("chg21")
        state.avg_vol[sym]["roc63"] = chg.get("chg63")
    log.info(
        f"ROC merged from fetch_pct_chg.json for {len(data)} symbols "
        f"(file date={cached.get('date')}, fetched={cached.get('fetched')})"
    )


def _do_fetch_avg_volume():
    """
    Blocking worker - runs in a background thread so FastAPI stays responsive.
    1. First checks fetch_avg_vol.json in same folder — loads instantly if today's file exists.
    2. Falls back to live Angel One API fetch (~8-10 min) if cache is missing or stale.
    3. Either way, _merge_roc_from_pct_chg_cache() backfills roc10/21/63 from
       the SEPARATE fetch_pct_chg.json file afterward.

    Cache file schema (confirmed from the actual file in use):
        { "date": "YYYY-MM-DD", "fetched": "...", "count": N,
          "data": { "SYMBOL": { "avg_vol_20": float, "avg_vol_50": float, "avg_to_20": float } } }
    """
    from datetime import date as _date
    today_str = _date.today().strftime("%Y-%m-%d")
    if not state.avg_vol_fetching:
        state.avg_vol_fetching = True
    try:
        # ── Step 1: load pre-fetched cache file if available ──
        cache_file = BASE_PATH / "fetch_avg_vol.json"
        if cache_file.exists():
            try:
                with open(cache_file) as f:
                    cached = json.load(f)
                if cached.get("date") == today_str and cached.get("data"):
                    state.avg_vol      = cached["data"]
                    state.avg_vol_date = today_str
                    log.info(f"Avg volume loaded from cache ✓  "
                             f"{len(state.avg_vol)} symbols  "
                             f"(fetched at {cached.get('fetched', '?')})")
                    _merge_roc_from_pct_chg_cache()
                    return   # skip live fetch entirely
                else:
                    log.info(f"Cache date={cached.get('date')} != today={today_str} — falling back to live fetch")
            except Exception as e:
                log.warning(f"Cache read error: {e} — falling back to live fetch")
        else:
            log.info("fetch_avg_vol.json not found — fetching live from Angel One")
            log.info("Tip: run fetch_avg_vol.py at 8:45 AM to pre-cache and skip this step")

        # ── Step 2: live fetch (fallback) ──
        symbols = list(state.symbols) if hasattr(state, "symbols") else []
        if not symbols:
            log.warning("Avg volume: no symbols loaded yet - skipping")
            return
        log.info(f"=== Starting 20D/50D avg volume fetch for {len(symbols)} symbols ===")
        avgs = fetch_avg_volume(symbols)
        if avgs:
            state.avg_vol      = avgs
            state.avg_vol_date = today_str
            log.info(f"Avg volume ready OK  {len(avgs)} symbols cached for {today_str}")
            _merge_roc_from_pct_chg_cache()
        else:
            log.error("Avg volume fetch returned 0 results - check token mapping and API response")
    except Exception as e:
        log.error(f"Avg volume fetch error: {e}")
    finally:
        state.avg_vol_fetching = False


@app.get("/avgvolume")
async def get_avg_volume(force: bool = False):
    """
    Returns 20D and 50D average daily volume per symbol.
    Cached for the whole trading day; re-fetches automatically on a new day.

    Response: { "date": "YYYY-MM-DD", "fetching": bool, "data": { "SYMBOL": { "avg_vol_20": N, "avg_vol_50": N, "avg_to_20": N, "roc10": N, "roc21": N, "roc63": N } } }

    The frontend combines this with today's cumulative volume from /quotes
    (tradeVolume field) to compute Relative Volume:
        relVol20 = (todayVol / avg20 - 1) * 100
        relVol50 = (todayVol / avg50 - 1) * 100
    """
    from datetime import date as _date
    today_str = _date.today().strftime("%Y-%m-%d")

    # Already cached for today
    if not force and state.avg_vol and state.avg_vol_date == today_str:
        return {"date": today_str, "cached": True, "fetching": False, "data": state.avg_vol}

    # Background thread already running - return stale/empty data immediately
    if state.avg_vol_fetching:
        return {"date": today_str, "cached": False, "fetching": True, "data": state.avg_vol}

    # Stale date or forced refresh - kick off a new background thread
    state.avg_vol_fetching = True   # set BEFORE spawning to prevent race
    t = threading.Thread(target=_do_fetch_avg_volume, daemon=True)
    t.start()
    return {"date": today_str, "cached": False, "fetching": True, "data": state.avg_vol}


# ─────────────────────── NSE proxy ───────────────────────

NSE_HEADERS = {
    "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-US,en;q=0.9",
    "Accept-Encoding":  "gzip, deflate",   # deliberately exclude 'br' — requests can't decode Brotli natively
    "Referer":          "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
    "sec-fetch-dest":   "empty",
    "sec-fetch-mode":   "cors",
    "sec-fetch-site":   "same-origin",
    "Connection":       "keep-alive",
}
_nse_session: requests.Session | None = None

def _get_nse_session(force_new: bool = False) -> requests.Session:
    """Return a warmed-up NSE session with cookies seeded for the announcements API."""
    global _nse_session
    if _nse_session is None or force_new:
        s = requests.Session()
        # Large pool — ann check fires many parallel NSE requests
        _nse_adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
        s.mount("https://", _nse_adapter)
        s.mount("http://",  _nse_adapter)
        s.headers.update(NSE_HEADERS)
        # NSE's Akamai protection requires visiting the page that owns the API
        # before the API cookies are valid. Two-step warmup:
        # 1. Homepage → seeds base bm_sz / bm_sv / AKA_A2 cookies
        # 2. Corporate filings page → seeds the specific API session cookies
        warmup_pages = [
            "https://www.nseindia.com/",
            "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
        ]
        for page in warmup_pages:
            try:
                resp = s.get(page, timeout=12)
                log.info(f"NSE warmup {page} → {resp.status_code}, cookies: {list(s.cookies.keys())}")
                time.sleep(0.5)
            except Exception as ex:
                log.warning(f"NSE warmup failed for {page}: {ex}")
        _nse_session = s
        log.info(f"NSE session ready — {len(s.cookies)} cookies")
    return _nse_session


# ----------------------- NSE listing-date master -----------------------
# output_nse_data.csv has no listing_date column, so the IPO scanner can
# never know which stocks are recent listings from that file alone.
# NSE publishes an official equity master list with a "DATE OF LISTING"
# column for every symbol — we fetch that instead and merge it in.
_LISTING_DATE_URL    = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
_listing_date_cache: dict = {}     # symbol -> date
_listing_date_fetched_on = None    # date we last successfully fetched

def _fetch_nse_listing_dates() -> dict:
    """Download NSE's equity master list and return {symbol: listing_date}.
    Cached for the day; returns the stale cache on failure rather than empty."""
    global _listing_date_cache, _listing_date_fetched_on
    from datetime import date as _d, datetime as _dt2
    today = _d.today()
    if _listing_date_fetched_on == today and _listing_date_cache:
        return _listing_date_cache

    try:
        s = _get_nse_session()
        r = s.get(_LISTING_DATE_URL, timeout=20)
        r.raise_for_status()
        r.encoding = 'utf-8'
        import csv as _csv3, io as _io
        reader = _csv3.DictReader(_io.StringIO(r.text))
        # NSE's CSV has leading spaces in header names (", DATE OF LISTING")
        fieldmap = {fn.strip().upper(): fn for fn in (reader.fieldnames or [])}
        sym_key, ld_key = fieldmap.get("SYMBOL"), fieldmap.get("DATE OF LISTING")
        out = {}
        if sym_key and ld_key:
            for row in reader:
                sym    = (row.get(sym_key) or "").strip().upper()
                ld_str = (row.get(ld_key) or "").strip()
                if not sym or not ld_str:
                    continue
                try:
                    out[sym] = _dt2.strptime(ld_str, "%d-%b-%Y").date()
                except Exception:
                    continue
        if out:
            _listing_date_cache, _listing_date_fetched_on = out, today
            log.info(f"[IPO] Fetched {len(out)} listing dates from NSE master list")
            return out
        log.warning(f"[IPO] NSE master list parsed 0 rows (headers: {list(fieldmap.keys())}) — keeping stale cache")
        return _listing_date_cache
    except Exception as e:
        log.warning(f"[IPO] Failed to fetch NSE listing-date master: {e} — using stale cache if any")
        return _listing_date_cache


@app.get("/nse_proxy")
async def nse_proxy(url: str):
    """
    Relay GET requests to NSE APIs from the browser (bypasses CORS).
    Used by the Gap Scanner and Stock Scanner to fetch corporate announcements.
    The browser calls: GET /nse_proxy?url=<encoded NSE API URL>
    """
    import asyncio
    if not url.startswith("https://www.nseindia.com/"):
        raise HTTPException(status_code=400, detail="Only nseindia.com URLs allowed")

    def _fetch(session: requests.Session):
        r = session.get(url, timeout=15)
        r.raise_for_status()
        # Force UTF-8 decode — NSE sometimes omits charset in content-type
        r.encoding = 'utf-8'
        content = r.text.strip()
        if not content:
            raise ValueError("NSE returned an empty response — session may need re-warming")
        if content[0] not in ('{', '['):
            # Show first 200 printable chars to help diagnose
            preview = ''.join(c if c.isprintable() else '?' for c in content[:200])
            raise ValueError(f"NSE returned non-JSON (status {r.status_code}): {preview}")
        import json as _json
        return _json.loads(content)

    try:
        loop = asyncio.get_event_loop()
        s = _get_nse_session()
        data = await loop.run_in_executor(None, _fetch, s)
        return data
    except Exception as e:
        log.warning(f"NSE proxy first attempt failed for {url}: {e} — re-warming session and retrying")
        # Force a fresh session warmup and retry once
        global _nse_session
        _nse_session = None
        try:
            loop = asyncio.get_event_loop()
            s = _get_nse_session(force_new=True)
            data = await loop.run_in_executor(None, _fetch, s)
            return data
        except Exception as e2:
            log.error(f"NSE proxy error for {url}: {e2}")
            raise HTTPException(status_code=502, detail=str(e2))


# ----------------------- startup -----------------------
async def on_startup():
    log.info("=== NSE Industry Scanner Proxy starting ===")
    cfg = load_config(state._config_path if hasattr(state, "_config_path") else "config.json")
    state.config = cfg

    # Load symbols from CSV (supports both output_nse_data.csv and final_output.csv)
    import csv as _csv
    csv_path = None
    for _fname in ("output_nse_data.csv", "final_output.csv"):
        _p = BASE_PATH / _fname
        if _p.exists():
            csv_path = _p
            break
    if csv_path:
        def _gcol(r, *keys):
            for k in keys:
                if k in r: return r[k]
            kl = {x.lower(): x for x in r}
            for k in keys:
                if k.lower() in kl: return r[kl[k.lower()]]
            return ""
        with open(csv_path) as f:
            rows = list(_csv.DictReader(f))
        state.symbols = [_gcol(r, "symbol", "SYMBOL") for r in rows]
        state.ffmc_map = {}
        for r in rows:
            sym = _gcol(r, "symbol", "SYMBOL")
            try: ffmc = float(_gcol(r, "ffmc_cr", "FFMC_CR") or _gcol(r, "mcap_cr", "MCAP_CR") or 0)
            except: ffmc = 0.0
            state.ffmc_map[sym] = ffmc
        log.info(f"Loaded {len(state.symbols)} symbols from {csv_path.name}")

        # Build stocks_meta with parsed listing dates for server-side scan endpoints
        from datetime import date as _sd, datetime as _sdt
        state.stocks_meta = []
        with open(csv_path) as f2:
            for r in __import__('csv').DictReader(f2):
                def _g(*keys):
                    for k in keys:
                        if k in r: return r[k]
                    kl = {x.lower(): x for x in r}
                    for k in keys:
                        if k.lower() in kl: return r[kl[k.lower()]]
                    return ""
                try: mcap = float(_g("mcap_cr","MCAP_CR") or 0)
                except: mcap = 0.0
                try: ffmc = float(_g("ffmc_cr","FFMC_CR") or mcap)
                except: ffmc = mcap
                pb = _g("priceband","PRICE_BAND","priceBand").strip()
                ld_str = _g("listing_date","LISTING_DATE","listingDate","date_of_listing",
                              "Listing Date","listing date","Date of Listing","DateOfListing")
                ld_obj = None
                for fmt in ("%Y-%m-%d","%d-%b-%Y","%d/%m/%Y","%d-%m-%Y","%b %d, %Y","%d %b %Y"):
                    try:
                        ld_obj = _sdt.strptime(ld_str.strip(), fmt).date()
                        break
                    except: pass
                if not ld_obj and ld_str:
                    log.warning(f"[IPO] Could not parse listing_date '{ld_str}' for {_g('symbol','SYMBOL')}")
                state.stocks_meta.append({
                    "symbol":           _g("symbol","SYMBOL"),
                    "mcap_cr":          mcap,
                    "ffmc_cr":          ffmc,
                    "priceband":        pb,
                    "macro":            _g("macro","MACRO"),
                    "sector":           _g("sector","SECTOR"),
                    "industry":         _g("industryInfo","industry","INDUSTRY"),
                    "fno":              pb == "No Band",
                    "listing_date_obj": ld_obj,
                })
        # Diagnostic: log how many stocks have a parsed listing date
        with_ld = sum(1 for s in state.stocks_meta if s.get("listing_date_obj"))
        without_ld = len(state.stocks_meta) - with_ld
        log.info(f"stocks_meta: {len(state.stocks_meta)} total, {with_ld} with listing_date, {without_ld} without")
        if without_ld == len(state.stocks_meta):
            # None parsed — log the CSV headers so we can see the actual column name
            with open(csv_path) as _f:
                import csv as _csv2
                headers = next(_csv2.reader(_f))
            log.warning(f"[IPO] NO listing dates parsed! CSV headers: {headers}")

        # CSV had no/incomplete listing dates — backfill from NSE's official
        # master list in the background so /scan/ipo self-heals without
        # needing a CSV pipeline change. Runs once at startup, ~10-20s.
        def _startup_listing_dates():
            log.info("[IPO] Fetching NSE listing-date master in background...")
            ld_map = _fetch_nse_listing_dates()
            if not ld_map:
                log.warning("[IPO] No listing dates available from NSE master list")
                return
            matched = 0
            for s in state.stocks_meta:
                if s.get("listing_date_obj"):
                    continue  # CSV already had a usable date for this row
                ld = ld_map.get(s["symbol"])
                if ld:
                    s["listing_date_obj"] = ld
                    matched += 1
            log.info(f"[IPO] Merged NSE listing dates — {matched}/{len(state.stocks_meta)} symbols now have a listing date")
        threading.Thread(target=_startup_listing_dates, name="listing-dates-startup", daemon=True).start()
    else:
        log.warning("No CSV found (output_nse_data.csv or final_output.csv) - /quotes will return empty")
        state.symbols = []
        state.ffmc_map = {}

    # Login - run in executor so blocking HTTP call doesn't stall async loop
    if cfg.get("api_key"):
        import asyncio
        loop = asyncio.get_event_loop()
        ok = await loop.run_in_executor(None, angel_login, cfg)
    else:
        log.warning("No api_key in config - call POST /login to authenticate")
        ok = False

    log.info(f"Dashboard -> http://localhost:{PORT}/")
    log.info(f"Quotes    -> http://localhost:{PORT}/quotes")
    log.info(f"Health    -> http://localhost:{PORT}/health")

    # Spawn avg vol thread HERE on the main async thread, NOT inside angel_login.
    # angel_login runs in an executor worker thread on Windows; daemon threads
    # spawned from executor workers are silently killed during uvicorn startup.
    if ok and not state.avg_vol_fetching:
        state.avg_vol_fetching = True
        def _startup_avg_vol():
            log.info("[AVG-VOL] Thread started - waiting for instrument master...")
            waited = 0
            while not state.script_loaded and waited < 60:
                time.sleep(2)
                waited += 2
            if not state.script_loaded:
                log.error("[AVG-VOL] Instrument master never loaded - aborting")
                state.avg_vol_fetching = False
                return
            log.info(f"[AVG-VOL] Instrument master ready after {waited}s - starting fetch")
            _do_fetch_avg_volume()
        t = threading.Thread(target=_startup_avg_vol, name="avg-vol-startup", daemon=True)
        t.start()
        log.info(f"[AVG-VOL] Background fetch thread spawned (alive={t.is_alive()})")
    elif not ok:
        log.warning("[AVG-VOL] Skipped - login did not succeed")

    # Live 1m/3m/5m OR tracker - background WebSocket thread. It self-waits for
    # jwt_token/feed_token + instrument master before connecting, so it's safe
    # to spawn unconditionally even if login above failed or is still pending
    # (e.g. user later calls POST /login).
    if SmartWebSocketV2:
        wt = threading.Thread(target=run_angel_websocket, name="live-or-websocket", daemon=True)
        wt.start()
        log.info(f"[LIVE-OR] WebSocket thread spawned (alive={wt.is_alive()})")
    else:
        log.warning("[LIVE-OR] SmartApi package not installed - /live_or will stay empty. "
                     "Install with: pip install smartapi-python")

# =====================================================================
# SERVER-SIDE SCAN ENDPOINTS
# All scan/filter/aggregation logic lives here — HTML is a dumb UI only
# =====================================================================

from datetime import date as _date, datetime as _datetime

# ── helpers ──────────────────────────────────────────────────────────

def _compute_gap(sym: str) -> float | None:
    p = state.last_quotes.get(sym)
    if not p:
        return None
    o, pc = p.get("open"), p.get("prev_close")
    if o is None or pc is None or pc == 0:
        return None
    return round((o - pc) / pc * 100, 2)

def _rel_vol(vol: int, sym: str, days: int) -> float | None:
    av = state.avg_vol.get(sym)
    if not av:
        return None
    avg = av.get(f"avg_vol_{days}", 0)
    if avg <= 0 or vol <= 0:
        return None
    return round((vol / avg - 1) * 100, 1)

def _fmt_stock(s: dict, p: dict, av: dict | None) -> dict:
    """Build a unified stock result dict from metadata + live quote."""
    ltp        = p.get("ltp")
    pchg       = p.get("pchg")
    vol        = p.get("volume", 0) or 0
    prev_close = p.get("prev_close")
    sym        = s["symbol"]
    turnover   = round(ltp * vol / 1e7, 2) if ltp and vol else None
    gap        = _compute_gap(sym)
    rv20       = _rel_vol(vol, sym, 20)
    rv50       = _rel_vol(vol, sym, 50)
    or_rec     = state.or_data.get(sym, {})
    or_close   = or_rec.get("or_close")
    or5_chg    = round((or_close - prev_close) / prev_close * 100, 2)                  if or_close and prev_close else None
    ld         = s.get("listing_date_obj")
    days_listed = (_date.today() - ld).days if ld else None
    return {
        "symbol":       sym,
        "industry":     s.get("industry", ""),
        "sector":       s.get("sector", ""),
        "macro":        s.get("macro", ""),
        "priceband":    s.get("priceband", ""),
        "ffmc_cr":      s.get("ffmc_cr", 0),
        "mcap_cr":      s.get("mcap_cr", 0),
        "fno":          s.get("fno", False),
        "ltp":          ltp,
        "pchg":         pchg,
        "prev_close":   prev_close,
        "volume":       vol,
        "turnover":     turnover,
        "gap":          gap,
        "relVol20":     rv20,
        "relVol50":     rv50,
        "days_listed":  days_listed,
        "or5_chg":      or5_chg,
    }


# ── /scan/stocks ─────────────────────────────────────────────────────

@app.get("/scan/stocks")
async def scan_stocks(
    chg_min:    float = None, chg_max:   float = None,
    gap_min:    float = None, gap_max:   float = None,
    mcap_min:   float = None, mcap_max:  float = None,
    ffmc_min:   float = None,
    turn_min:   float = None, turn_max:  float = None,
    price_min:  float = None, price_max: float = None,
    rv20_min:   float = None, rv50_min:  float = None,
    band:       str   = "all",
    fno_only:   bool  = False,
):
    """
    Server-side stock scanner. All filter params are optional.
    Returns filtered + enriched stock list with live quote data.
    """
    if not state.last_quotes:
        raise HTTPException(status_code=503, detail="Quotes not loaded yet")

    rows = state.stocks_meta  # list of dicts from CSV
    results = []
    for s in rows:
        sym = s["symbol"]
        if fno_only and not s.get("fno"):
            continue

        p = state.last_quotes.get(sym)
        if not p:
            continue

        ltp    = p.get("ltp")
        pchg   = p.get("pchg")
        vol    = p.get("volume", 0) or 0
        turnover = round(ltp * vol / 1e7, 2) if ltp and vol else None
        gap    = _compute_gap(sym)
        rv20   = _rel_vol(vol, sym, 20)
        rv50   = _rel_vol(vol, sym, 50)
        ffmc   = s.get("ffmc_cr", 0) or 0
        mcap   = s.get("mcap_cr", 0) or 0
        pb     = (s.get("priceband") or "").strip()

        # Apply filters
        if chg_min  is not None and (pchg is None or pchg < chg_min):   continue
        if chg_max  is not None and (pchg is None or pchg > chg_max):   continue
        if gap_min  is not None and (gap  is None or gap  < gap_min):   continue
        if gap_max  is not None and (gap  is None or gap  > gap_max):   continue
        if mcap_min is not None and mcap < mcap_min:                     continue
        if mcap_max is not None and mcap > mcap_max:                     continue
        if ffmc_min is not None and ffmc < ffmc_min:                     continue
        if turn_min is not None and (turnover is None or turnover < turn_min): continue
        if turn_max is not None and (turnover is None or turnover > turn_max): continue
        if price_min is not None and (ltp is None or ltp < price_min):  continue
        if price_max is not None and (ltp is None or ltp > price_max):  continue
        if rv20_min is not None and (rv20 is None or rv20 < rv20_min):  continue
        if rv50_min is not None and (rv50 is None or rv50 < rv50_min):  continue
        if band != "all" and pb != band:                                 continue

        results.append(_fmt_stock(s, p, state.avg_vol.get(sym)))

    return {
        "ts":      state.quotes_ts,
        "count":   len(results),
        "results": results,
    }


# ── /scan/ipo ─────────────────────────────────────────────────────────

def _get_ipo_stocks(days_max: int = 365) -> list:
    """Return stocks from CSV listed within days_max days."""
    today = _date.today()
    out = []
    for s in state.stocks_meta:
        ld = s.get("listing_date_obj")
        if not ld:
            continue
        days = (today - ld).days
        if 0 <= days <= days_max:
            out.append({**s, "days_listed": days,
                        "listing_date_display": ld.strftime("%d-%b-%Y")})
    return out

def _get_ipo_symbols(days_max: int = 365) -> set:
    return {s["symbol"] for s in _get_ipo_stocks(days_max)}

@app.get("/scan/ipo")
async def scan_ipo(
    chg_min:    float = None, chg_max:   float = None,
    gap_min:    float = None, gap_max:   float = None,
    mcap_min:   float = None, mcap_max:  float = None,
    ffmc_min:   float = None, ffmc_max:  float = None,
    turn_min:   float = None,
    price_min:  float = None, price_max: float = None,
    days_max:   int   = 365,
    band:       str   = "all",
    rv20_min:   float = None,
):
    """
    Server-side IPO scanner. Filters stocks listed within days_max days.
    """
    if not state.last_quotes:
        raise HTTPException(status_code=503, detail="Quotes not loaded yet")

    ipo_stocks = _get_ipo_stocks(days_max)
    results = []

    for s in ipo_stocks:
        sym  = s["symbol"]
        p    = state.last_quotes.get(sym, {})
        ltp  = p.get("ltp")
        pchg = p.get("pchg")
        vol  = p.get("volume", 0) or 0
        turnover = round(ltp * vol / 1e7, 2) if ltp and vol else None
        gap  = _compute_gap(sym)
        rv20 = _rel_vol(vol, sym, 20)
        rv50 = _rel_vol(vol, sym, 50)
        ffmc = s.get("ffmc_cr", 0) or 0
        mcap = s.get("mcap_cr", 0) or 0
        pb   = (s.get("priceband") or "").strip()

        if chg_min  is not None and (pchg is None or pchg < chg_min):   continue
        if chg_max  is not None and (pchg is None or pchg > chg_max):   continue
        if gap_min  is not None and (gap  is None or gap  < gap_min):   continue
        if gap_max  is not None and (gap  is None or gap  > gap_max):   continue
        if mcap_min is not None and mcap < mcap_min:                     continue
        if mcap_max is not None and mcap > mcap_max:                     continue
        if ffmc_min is not None and ffmc < ffmc_min:                     continue
        if ffmc_max is not None and ffmc > ffmc_max:                     continue
        if turn_min is not None and (turnover is None or turnover < turn_min): continue
        if price_min is not None and (ltp is None or ltp < price_min):  continue
        if price_max is not None and (ltp is None or ltp > price_max):  continue
        if rv20_min is not None and (rv20 is None or rv20 < rv20_min):  continue
        if band != "all" and pb != band:                                 continue

        results.append({
            **_fmt_stock(s, p, state.avg_vol.get(sym)),
            "days_listed":    s["days_listed"],
            "listing_date":   s["listing_date_display"],
            "relVol50":       rv50,
        })

    return {
        "ts":      state.quotes_ts,
        "count":   len(results),
        "results": results,
    }


# ── /scan/industries ──────────────────────────────────────────────────

@app.get("/scan/industries")
async def scan_industries(group_by: str = "industry"):
    """
    Aggregate stocks by industry/sector/macro.
    Returns sorted list with avg pchg, stock count, advancing/declining counts.
    group_by: 'industry' | 'sector' | 'macro' | 'fno'
    """
    if not state.last_quotes:
        raise HTTPException(status_code=503, detail="Quotes not loaded yet")

    groups: dict = {}
    ipo_syms = _get_ipo_symbols()

    for s in state.stocks_meta:
        sym = s["symbol"]
        if sym in ipo_syms:
            continue
        p = state.last_quotes.get(sym)
        if not p:
            continue
        pchg = p.get("pchg")
        ltp  = p.get("ltp", 0) or 0
        vol  = p.get("volume", 0) or 0

        key = "F&O" if group_by == "fno" else s.get(group_by, "Unknown") or "Unknown"
        if key not in groups:
            groups[key] = {"name": key, "stocks": [], "pchgs": [],
                           "advancing": 0, "declining": 0, "total_turnover": 0}
        g = groups[key]
        g["stocks"].append(sym)
        if pchg is not None:
            g["pchgs"].append(pchg)
            if pchg > 0: g["advancing"] += 1
            elif pchg < 0: g["declining"] += 1
        g["total_turnover"] += round(ltp * vol / 1e7, 2)

    result = []
    for g in groups.values():
        avg_pchg = round(sum(g["pchgs"]) / len(g["pchgs"]), 2) if g["pchgs"] else None
        result.append({
            "name":           g["name"],
            "stock_count":    len(g["stocks"]),
            "covered":        len(g["pchgs"]),
            "avg_pchg":       avg_pchg,
            "advancing":      g["advancing"],
            "declining":      g["declining"],
            "total_turnover": round(g["total_turnover"], 1),
        })

    result.sort(key=lambda x: (x["avg_pchg"] or 0), reverse=True)
    return {"ts": state.quotes_ts, "group_by": group_by, "groups": result}


# ── /scan/industry_stocks ─────────────────────────────────────────────

@app.get("/scan/industry_stocks")
async def scan_industry_stocks(name: str, group_by: str = "industry"):
    """
    Return all stocks in a specific industry/sector/macro group with live data.
    """
    if not state.last_quotes:
        raise HTTPException(status_code=503, detail="Quotes not loaded yet")

    ipo_syms = _get_ipo_symbols()
    results = []
    for s in state.stocks_meta:
        sym = s["symbol"]
        if sym in ipo_syms:
            continue
        key = "F&O" if group_by == "fno" else s.get(group_by, "") or ""
        if key != name:
            continue
        p = state.last_quotes.get(sym, {})
        results.append(_fmt_stock(s, p, state.avg_vol.get(sym)))

    results.sort(key=lambda x: (x["pchg"] or 0), reverse=True)
    return {"name": name, "count": len(results), "results": results}



# =====================================================================
# ANNOUNCEMENT WINDOW ENGINE
# Handles weekends + NSE holidays to compute correct "previous trading
# session" window. Results are cached per symbol per window.
# =====================================================================

import threading as _threading
from datetime import date as _date, datetime as _dt, timedelta as _td

_ann_lock    = _threading.Lock()
_ann_cache   = {}          # symbol -> {window_key, count, rows}
_holiday_cache = []        # list of date objects from NSE
_holiday_fetched_on = None # date we last fetched holidays


def _fetch_nse_holidays() -> list[_date]:
    """Fetch NSE equity market holidays for current year."""
    global _holiday_cache, _holiday_fetched_on
    today = _date.today()
    if _holiday_fetched_on == today and _holiday_cache:
        return _holiday_cache

    url = f"https://www.nseindia.com/api/holiday-master?type=trading"
    try:
        s = _get_nse_session()
        r = s.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        # NSE returns {CM: [{tradingDate: "14-Aug-2025", ...}, ...], ...}
        rows = data.get("CM", [])
        holidays = []
        for row in rows:
            dt_str = row.get("tradingDate", "")
            for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y"):
                try:
                    holidays.append(_dt.strptime(dt_str.strip(), fmt).date())
                    break
                except: pass
        _holiday_cache = holidays
        _holiday_fetched_on = today
        log.info(f"[ANN] Fetched {len(holidays)} NSE holidays")
        return holidays
    except Exception as e:
        log.warning(f"[ANN] Holiday fetch failed: {e} — using empty list")
        return _holiday_cache  # use stale if available


def _is_trading_day(d: _date, holidays: list[_date]) -> bool:
    return d.weekday() < 5 and d not in holidays


def _prev_trading_day(d: _date, holidays: list[_date]) -> _date:
    """Return the most recent trading day before d."""
    prev = d - _td(days=1)
    while not _is_trading_day(prev, holidays):
        prev -= _td(days=1)
    return prev


def _ann_window() -> tuple[_dt, _dt, str]:
    """
    Compute (from_dt, to_dt, window_key) for announcement lookup.

    Rules:
    - to_dt   = today 09:00 IST
    - from_dt = previous_trading_day 09:00 IST
    - Handles weekends, holidays, long weekends automatically.
    """
    holidays = _fetch_nse_holidays()
    now_ist  = _dt.utcnow() + _td(hours=5, minutes=30)
    today    = now_ist.date()
    prev     = _prev_trading_day(today, holidays)

    # If today itself is not a trading day, still look back from today
    from_dt  = _dt(prev.year, prev.month, prev.day, 15, 30, 0)
    to_dt    = _dt(today.year, today.month, today.day, 9, 0, 0)
    key      = f"{prev.isoformat()}_{today.isoformat()}"
    return from_dt, to_dt, key


def _fetch_ann_for_symbol(symbol: str, from_dt: _dt, to_dt: _dt) -> list[dict]:
    """Fetch corporate announcements for one symbol from NSE."""
    pad = lambda n: str(n).zfill(2)
    fmt = lambda d: f"{pad(d.day)}-{pad(d.month)}-{d.year}"
    url = (
        f"https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
        f"?functionName=getCorporateAnnouncement"
        f"&symbol={requests.utils.quote(symbol)}"
        f"&marketApiType=equities&subject="
        f"&fromDate={fmt(from_dt)}&toDate={fmt(to_dt)}"
    )
    try:
        s = _get_nse_session()
        r = s.get(url, timeout=10)
        r.encoding = 'utf-8'
        data = r.json()
        rows = data if isinstance(data, list) else data.get("data", data.get("results", []))
        # Filter strictly within window
        result = []
        for row in rows:
            dt_str = row.get("an_dt") or row.get("dt") or row.get("sort_date") or ""
            for fmt2 in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    ann_dt = _dt.strptime(dt_str.strip()[:19], fmt2)
                    if from_dt <= ann_dt <= to_dt:
                        result.append(row)
                    break
                except: pass
            # else: date unparseable — skip rather than blindly include
        return result
    except Exception as e:
        log.warning(f"[ANN] Fetch failed for {symbol}: {e}")
        return []


@app.get("/ann/window")
async def ann_window_info():
    """Return the current announcement window dates."""
    from_dt, to_dt, key = _ann_window()
    return {
        "from":  from_dt.strftime("%d-%b-%Y %H:%M"),
        "to":    to_dt.strftime("%d-%b-%Y %H:%M"),
        "key":   key,
    }


@app.get("/ann/check")
async def ann_check(symbols: str):
    """
    Check which symbols have announcements in the current window.
    symbols = comma-separated list e.g. ?symbols=INFY,TCS,RELIANCE
    Returns: {symbol: count, ...}
    """
    import asyncio
    sym_list  = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    from_dt, to_dt, win_key = _ann_window()

    result = {}
    to_fetch = []

    with _ann_lock:
        for sym in sym_list:
            cached = _ann_cache.get(sym)
            if cached and cached.get("window_key") == win_key:
                result[sym] = cached["count"]
            else:
                to_fetch.append(sym)

    # Fetch missing symbols concurrently
    if to_fetch:
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(None, _fetch_ann_for_symbol, sym, from_dt, to_dt)
            for sym in to_fetch
        ]
        fetched = await asyncio.gather(*tasks, return_exceptions=True)
        with _ann_lock:
            for sym, rows in zip(to_fetch, fetched):
                if isinstance(rows, Exception):
                    rows = []
                _ann_cache[sym] = {"window_key": win_key, "count": len(rows), "rows": rows}
                result[sym] = len(rows)

    return {"window_key": win_key, "from": from_dt.strftime("%d-%b-%Y"), "to": to_dt.strftime("%d-%b-%Y"), "counts": result}


@app.get("/ann/detail")
async def ann_detail(symbol: str):
    """
    Return full announcement rows for one symbol in the current window.
    Served from cache if available, otherwise fetches fresh.
    """
    import asyncio
    symbol = symbol.upper()
    from_dt, to_dt, win_key = _ann_window()

    with _ann_lock:
        cached = _ann_cache.get(symbol)
        if cached and cached.get("window_key") == win_key:
            return {
                "symbol": symbol,
                "from":   from_dt.strftime("%d-%b-%Y"),
                "to":     to_dt.strftime("%d-%b-%Y"),
                "count":  cached["count"],
                "rows":   cached["rows"],
            }

    # Not cached — fetch now
    loop   = asyncio.get_event_loop()
    rows   = await loop.run_in_executor(None, _fetch_ann_for_symbol, symbol, from_dt, to_dt)
    with _ann_lock:
        _ann_cache[symbol] = {"window_key": win_key, "count": len(rows), "rows": rows}

    return {
        "symbol": symbol,
        "from":   from_dt.strftime("%d-%b-%Y"),
        "to":     to_dt.strftime("%d-%b-%Y"),
        "count":  len(rows),
        "rows":   rows,
    }

# ----------------------- entry point -----------------------
def main():
    parser = argparse.ArgumentParser(description="NSE Scanner Proxy")
    parser.add_argument("--config", default="config.json", help="Path to config.json")
    parser.add_argument("--port",   default=PORT, type=int)
    parser.add_argument("--no-browser", action="store_true", help="Don't auto-open browser")
    args = parser.parse_args()

    # Set config path on state BEFORE uvicorn starts so on_startup() sees it
    state._config_path = args.config

    if not args.no_browser:
        # Open browser after a short delay
        def _open():
            time.sleep(2.5)
            webbrowser.open(f"http://localhost:{args.port}/")
        threading.Thread(target=_open, daemon=True).start()

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=args.port,
        log_level="warning",  # suppress uvicorn noise; our logger handles it
        reload=False,
    )

if __name__ == "__main__":
    main()
