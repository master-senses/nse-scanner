"""
live_scanner.py

Combines:
  - The cached daily 10D/21D/63D ROC (fetch_pct_chg.json) and the price-band
    screen (final_output.csv): only "20%" and "No Band" symbols are tracked.
    The ENTIRE band-filtered universe is tracked live (no further
    pre-selection), so the gainers/losers list below reflects the real
    universe, not a pre-filtered subset.
  - A live Angel One SmartAPI websocket feed (QUOTE mode, split across
    multiple connections -- see MAX_TOKENS_PER_WS / MAX_WS_CONNECTIONS --
    since the universe is typically well over the 1000-token-per-connection
    cap) for live %chg, gap%, 1-minute RSI(14), and a base/breakout signal:

      * Williams Fractal (2-period, i.e. 5-bar window: 2 bars each side)
        identifies a pivot high once it's confirmed (2 bars after it forms).
      * If that pivot's RSI (at the time it formed) was between 40-60,
        the symbol is flagged "Be Ready" with that pivot price as the level
        to watch; once price crosses above it, "Fired".
      * A mirrored check on fractal LOWS does the same for the short side:
        "Short Be Ready" / "Short Fired" on a breakdown below the pivot low.

  - The page itself is just two fixed lists: the current Top N gainers and
    Top N losers by live %chg (config.TOP_GAINERS_LOSERS_N, default 30),
    combined across both price bands (the Band column is informational only
    now, not a selection criterion).

The live table is written to an auto-refreshing HTML file (config.SCANNER_HTML)
rather than a console table, since a terminal window can only show as many
rows as its height -- the HTML page has no such limit.

Run locally (needs internet access to nseindia-independent Angel One
endpoints, and a live Angel One session during market hours):

    pip install smartapi-python pyotp websocket-client rich
    python live_scanner.py

Fill in config.py with your credentials before running.

ASSUMPTIONS (documented since the original spec left these open):
  1. RSI 40-60 is checked using the RSI value AT THE BAR where the pivot
     formed (not the RSI 2 minutes later when the fractal gets confirmed).
  2. The breakout/breakdown check is done on every live tick, not just on
     1-min bar close, for the fastest possible "Fired" signal.
  3. After "Fired" / "Short Fired", that side's state resets so a new base
     can be tracked again. Long and short states are independent -- a
     symbol can carry both at once.
  4. The full price-band-filtered universe (typically 1500-1900+ symbols)
     is tracked live, split across multiple websocket connections (Angel
     One allows up to 3 concurrent connections per client, 1000 tokens
     each = ~3000 capacity). If the universe ever exceeds that, it's
     truncated with a warning rather than silently dropped.
  5. At startup each day, each symbol is seeded with the previous trading
     session's last SEED_BARS (default 15) one-minute candles, fetched
     fresh from Angel One's historical candle API -- not stored locally,
     so there's nothing to go stale. With ~1800+ symbols this is run with
     a small thread pool (config.SEED_WORKERS) instead of one-at-a-time,
     since Angel One's historical API can stall for 60-100s on individual
     calls under sustained load -- sequentially, a single stuck call would
     block every symbol behind it. Symbols whose seed call fails just skip
     the head start and warm up live instead (config.ENABLE_SEEDING can
     turn this off entirely if it's still unreliable at this scale).
  6. Top N gainers/losers are computed fresh from live %chg every refresh;
     a symbol moves between/out of the lists automatically as %chg changes.
  7. The price band / mcap / ffmcap screen uses final_output.csv; the
     turnover screen (MIN_AVG_TURNOVER, 20-day average of NSE's own daily
     CH_TOT_TRADED_VAL, not an approximation) uses fetch_avg_vol.json. Both
     are applied once at startup (static reference data); symbols missing
     from either file are excluded rather than guessed at.
  8. 1-minute bars are bucketed using the local system clock.
"""

import csv
import json
import os
import threading
import time
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pyotp
from rich.console import Console
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

import config

SCRIP_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
NIFTY50_TOKEN = "99926000"  # NSE index token, used only to detect the last trading session
RSI_PERIOD = 14
RSI_LOW, RSI_HIGH = 40, 60
FRACTAL_WING = 2  # bars on each side -> 5-bar window

MAX_TOKENS_PER_WS = 1000   # Angel One's cap per websocket connection
MAX_WS_CONNECTIONS = 3     # Angel One's cap on concurrent connections per client

lock = threading.Lock()
console = Console()
states = {}          # symbol -> SymbolState
token_to_symbol = {}  # angel token -> symbol


# ---------------------------------------------------------------------------
# 1. Load the cached daily scanner data (10D/21D/63D ROC), sorted
# ---------------------------------------------------------------------------

def load_daily_table():
    with open(config.PCT_CHG_JSON) as f:
        pct_chg = json.load(f)["data"]
    with open(config.AVG_VOL_JSON) as f:
        avg_vol = json.load(f)["data"]

    symbols = []
    with open(config.CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sym = (row.get("SYMBOL") or "").strip()
            if sym:
                symbols.append(sym)

    rows = []
    for sym in symbols:
        chg = pct_chg.get(sym, {})
        vol = avg_vol.get(sym, {})
        rows.append(
            {
                "symbol": sym,
                "roc10": chg.get("chg10"),
                "roc21": chg.get("chg21"),
                "roc63": chg.get("chg63"),
                "avg_vol_20": vol.get("avg_vol_20"),
                "avg_vol_50": vol.get("avg_vol_50"),
                "avg_turnover20": vol.get("avg_to_20"),
            }
        )

    rows.sort(key=lambda r: (r["roc21"] is None, -(r["roc21"] or 0)))
    return rows


def load_meta(csv_path):
    """Read mcap_cr / ffmc_cr / priceband from final_output.csv."""
    meta = {}
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sym = (row.get("symbol") or "").strip()
            if not sym:
                continue

            def to_float(x):
                try:
                    return float(x)
                except (TypeError, ValueError):
                    return None

            meta[sym] = {
                "mcap": to_float(row.get("mcap_cr")),
                "ffmc": to_float(row.get("ffmc_cr")),
                "band": (row.get("priceband") or "").strip(),
            }
    return meta


def filter_universe(rows, meta):
    """Apply the price-band / mcap / ffmc / turnover screens. Order is
    preserved, so the input should already be ROC-sorted."""
    filtered = []
    for r in rows:
        m = meta.get(r["symbol"])
        if m is None:
            continue  # no reference row for this symbol -- skip rather than guess
        if config.ALLOWED_PRICEBANDS and m["band"] not in config.ALLOWED_PRICEBANDS:
            continue
        if config.MIN_MCAP_CR is not None and (m["mcap"] is None or m["mcap"] < config.MIN_MCAP_CR):
            continue
        if config.MIN_FFMC_CR is not None and (m["ffmc"] is None or m["ffmc"] < config.MIN_FFMC_CR):
            continue
        if config.MIN_AVG_TURNOVER is not None and (
            r["avg_turnover20"] is None or r["avg_turnover20"] <= config.MIN_AVG_TURNOVER
        ):
            continue
        r["mcap"] = m["mcap"]
        r["ffmc"] = m["ffmc"]
        r["band"] = m["band"]
        filtered.append(r)
    return filtered


# ---------------------------------------------------------------------------
# 2. Scrip master -> Angel One token mapping
# ---------------------------------------------------------------------------

def load_token_map(symbols):
    console.print("Downloading Angel One scrip master (this is a few MB)...")
    with urllib.request.urlopen(SCRIP_MASTER_URL, timeout=60) as resp:
        instruments = json.loads(resp.read())

    by_symbol = {}
    for inst in instruments:
        if inst.get("exch_seg") == "NSE" and inst.get("symbol", "").endswith("-EQ"):
            base = inst["symbol"][:-3]  # strip "-EQ"
            by_symbol[base] = inst["token"]

    mapping, missing = {}, []
    for sym in symbols:
        token = by_symbol.get(sym)
        if token:
            mapping[sym] = token
        else:
            missing.append(sym)
    return mapping, missing


# ---------------------------------------------------------------------------
# 3. Angel One login
# ---------------------------------------------------------------------------

def angel_login():
    obj = SmartConnect(api_key=config.API_KEY)
    totp = pyotp.TOTP(config.TOTP_SECRET).now()
    data = obj.generateSession(config.CLIENT_CODE, config.PASSWORD, totp)
    if not data.get("status"):
        raise RuntimeError(f"Angel One login failed: {data}")
    feed_token = obj.getfeedToken()
    return obj, data["data"]["jwtToken"], feed_token


# ---------------------------------------------------------------------------
# 3b. Previous-day seed: continuous RSI/fractal state across days
# ---------------------------------------------------------------------------

def get_last_trading_day(obj):
    """Find the most recent session with data, using NIFTY 50 as a liquid
    reference (so we don't need our own holiday calendar)."""
    today = datetime.now().date()
    params = {
        "exchange": "NSE",
        "symboltoken": NIFTY50_TOKEN,
        "interval": "ONE_DAY",
        "fromdate": (today - timedelta(days=10)).strftime("%Y-%m-%d 09:15"),
        "todate": today.strftime("%Y-%m-%d 15:30"),
    }
    try:
        res = obj.getCandleData(params)
        candles = res.get("data") or []
    except Exception as e:
        console.print(f"[red]Could not determine last trading day: {e}[/red]")
        return None
    if not candles:
        return None

    last_date = candles[-1][0][:10]
    if last_date == today.strftime("%Y-%m-%d"):
        # today already has a daily candle (e.g. script restarted mid-session) --
        # we want the PREVIOUS session, so step back one more candle if we can
        if len(candles) > 1:
            last_date = candles[-2][0][:10]
        else:
            return None
    return last_date


class _RateLimiter:
    """Enforces a minimum gap between calls across ALL worker threads, not
    just between submissions. A plain submission-stagger doesn't actually
    bound the real request rate once several workers are running -- as soon
    as any worker finishes (including a fast failure), it immediately grabs
    the next queued item, so the true call rate can spike well above what
    the stagger implied."""

    def __init__(self, min_interval):
        self.min_interval = min_interval
        self._lock = threading.Lock()
        self._last_call = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            delay = self._last_call + self.min_interval - now
            if delay > 0:
                time.sleep(delay)
            self._last_call = time.monotonic()


_seed_rate_limiter = _RateLimiter(config.SEED_MIN_INTERVAL)


def fetch_prev_day_candles(obj, token, day, num_bars):
    params = {
        "exchange": "NSE",
        "symboltoken": token,
        "interval": "ONE_MINUTE",
        "fromdate": f"{day} 09:15",
        "todate": f"{day} 15:30",
    }
    for attempt in range(2):  # one retry -- transient throttling often clears in a second
        _seed_rate_limiter.wait()
        try:
            res = obj.getCandleData(params)
            candles = res.get("data") or []
            if candles:
                return candles[-num_bars:]
        except Exception:
            pass
        if attempt == 0:
            time.sleep(1.5)
    return []  # quietly skip -- seeding is a nice-to-have, not critical


def seed_all_symbols(obj, token_map):
    """Concurrent (small pool) so one slow call doesn't block everything
    behind it, but the REAL request rate is bounded by _seed_rate_limiter,
    not by worker count -- worker count alone doesn't cap throughput once
    fast calls (including fast failures) let threads cycle through the
    queue quickly."""
    if not config.ENABLE_SEEDING:
        console.print("[yellow]Seeding disabled (config.ENABLE_SEEDING = False) -- starting cold.[/yellow]")
        return

    last_day = get_last_trading_day(obj)
    if not last_day:
        console.print("[yellow]Could not find a previous session -- starting cold (no seed).[/yellow]")
        return

    items = [(sym, st, token_map[sym]) for sym, st in states.items() if sym in token_map]
    est_minutes = len(items) * config.SEED_MIN_INTERVAL / 60
    console.print(
        f"Seeding from {last_day} (last {config.SEED_BARS} 1-min bars/symbol, "
        f"{config.SEED_WORKERS} workers, paced at 1 call/{config.SEED_MIN_INTERVAL}s -- "
        f"est. ~{est_minutes:.1f} min)..."
    )

    seeded = 0
    failed_symbols = []
    done = 0

    def seed_one(item):
        sym, st, token = item
        candles = fetch_prev_day_candles(obj, token, last_day, config.SEED_BARS)
        return sym, st, candles

    with ThreadPoolExecutor(max_workers=config.SEED_WORKERS) as executor:
        futures = [executor.submit(seed_one, item) for item in items]

        for future in as_completed(futures):
            sym, st, candles = future.result()
            for c in candles:
                # candle = [timestamp, open, high, low, close, volume]
                bar = {"high": c[2], "low": c[3], "close": c[4]}
                st._finalize_bar(bar, silent=True)
            if candles:
                seeded += 1
            else:
                failed_symbols.append(sym)
            done += 1
            if done % 200 == 0 or done == len(items):
                console.print(f"...seeded {done}/{len(items)} ({seeded} ok, {len(failed_symbols)} failed so far)")

    console.print(f"[green]Seeded {seeded}/{len(items)} symbols from the previous session.[/green]")
    if failed_symbols:
        console.print(
            f"[yellow]{len(failed_symbols)} symbols had no seed data (timed out or no history) -- "
            f"they'll just warm up live instead: {failed_symbols[:20]}{' ...' if len(failed_symbols) > 20 else ''}[/yellow]"
        )


# ---------------------------------------------------------------------------
# 4. Signal log helper
# ---------------------------------------------------------------------------

def log_signal(symbol, status, detail):
    line = f"{datetime.now():%Y-%m-%d %H:%M:%S} | {symbol:<12} | {status:<9} | {detail}"
    with open(config.SIGNALS_LOG, "a") as f:
        f.write(line + "\n")
    console.print(f"[bold yellow]{line}[/bold yellow]")


# ---------------------------------------------------------------------------
# 5. Per-symbol live state: 1-min bars, RSI(14), Williams fractal, signals
# ---------------------------------------------------------------------------

class SymbolState:
    def __init__(self, symbol, roc_row):
        self.symbol = symbol
        self.roc10 = roc_row.get("roc10")
        self.roc21 = roc_row.get("roc21")
        self.roc63 = roc_row.get("roc63")
        self.mcap = roc_row.get("mcap")
        self.ffmc = roc_row.get("ffmc")
        self.band = roc_row.get("band")

        self.ltp = None
        self.open = None
        self.prev_close = None

        # 1-minute bar building
        self.cur_minute = None
        self.cur_bar = None
        self.bars = deque(maxlen=300)
        self.rsi_hist = deque(maxlen=300)  # rsi aligned 1:1 with self.bars

        # Wilder RSI state
        self._gains, self._losses = [], []
        self._avg_gain = self._avg_loss = None
        self._prev_close_1m = None
        self.rsi = None

        # Base / fractal state machine -- long (breakout above a pivot high)
        self.active_pivot = None
        self.status = ""  # "", "Be Ready", "Fired"

        # Base / fractal state machine -- short (breakdown below a pivot low)
        self.active_pivot_low = None
        self.short_status = ""  # "", "Short Be Ready", "Short Fired"

    def on_tick(self, ltp, open_, prev_close):
        self.ltp = ltp
        if open_:
            self.open = open_
        if prev_close:
            self.prev_close = prev_close

        minute = datetime.now().replace(second=0, microsecond=0)
        if self.cur_minute is None:
            self.cur_minute = minute
            self.cur_bar = {"high": ltp, "low": ltp, "close": ltp}
        elif minute == self.cur_minute:
            b = self.cur_bar
            b["high"] = max(b["high"], ltp)
            b["low"] = min(b["low"], ltp)
            b["close"] = ltp
        else:
            self._finalize_bar(self.cur_bar)
            self.cur_minute = minute
            self.cur_bar = {"high": ltp, "low": ltp, "close": ltp}

        # Breakout check on every tick for a fast "Fired" trigger
        if self.status == "Be Ready" and self.active_pivot is not None:
            if ltp > self.active_pivot:
                pivot = self.active_pivot
                self.status = "Fired"
                self.active_pivot = None
                log_signal(self.symbol, "Fired", f"crossed pivot {pivot:.2f} @ {ltp:.2f}")

        # Breakdown check (short side) -- mirror of the above, below a pivot low
        if self.short_status == "Short Be Ready" and self.active_pivot_low is not None:
            if ltp < self.active_pivot_low:
                pivot = self.active_pivot_low
                self.short_status = "Short Fired"
                self.active_pivot_low = None
                log_signal(self.symbol, "Short Fired", f"crossed pivot {pivot:.2f} @ {ltp:.2f}")

    def _finalize_bar(self, bar, silent=False):
        self.bars.append(bar)
        self._update_rsi(bar["close"])
        self.rsi_hist.append(self.rsi)
        self._check_fractal(silent=silent)
        self._check_fractal_low(silent=silent)

    def _update_rsi(self, close):
        if self._prev_close_1m is None:
            self._prev_close_1m = close
            return
        delta = close - self._prev_close_1m
        self._prev_close_1m = close
        gain, loss = max(delta, 0.0), max(-delta, 0.0)

        if self._avg_gain is None:
            self._gains.append(gain)
            self._losses.append(loss)
            if len(self._gains) == RSI_PERIOD:
                self._avg_gain = sum(self._gains) / RSI_PERIOD
                self._avg_loss = sum(self._losses) / RSI_PERIOD
                self._set_rsi()
            return

        self._avg_gain = (self._avg_gain * (RSI_PERIOD - 1) + gain) / RSI_PERIOD
        self._avg_loss = (self._avg_loss * (RSI_PERIOD - 1) + loss) / RSI_PERIOD
        self._set_rsi()

    def _set_rsi(self):
        if self._avg_loss == 0:
            self.rsi = 100.0
        elif self._avg_gain == 0:
            self.rsi = 0.0
        else:
            rs = self._avg_gain / self._avg_loss
            self.rsi = 100 - (100 / (1 + rs))

    def _check_fractal(self, silent=False):
        n = len(self.bars)
        if n < 2 * FRACTAL_WING + 1:
            return
        idx = n - 1 - FRACTAL_WING  # candidate pivot, confirmed 2 bars later
        candidate = self.bars[idx]
        neighbors = [self.bars[idx - w] for w in range(1, FRACTAL_WING + 1)]
        neighbors += [self.bars[idx + w] for w in range(1, FRACTAL_WING + 1)]

        if all(candidate["high"] > b["high"] for b in neighbors):
            rsi_then = self.rsi_hist[idx]
            if rsi_then is not None and RSI_LOW <= rsi_then <= RSI_HIGH:
                pivot_price = candidate["high"]
                if self.active_pivot != pivot_price:
                    self.active_pivot = pivot_price
                    self.status = "Be Ready"
                    if not silent:
                        log_signal(self.symbol, "Be Ready", f"pivot={pivot_price:.2f} rsi={rsi_then:.1f}")

    def _check_fractal_low(self, silent=False):
        """Mirror of _check_fractal: a confirmed fractal LOW with RSI 40-60
        at the time it formed arms the short side."""
        n = len(self.bars)
        if n < 2 * FRACTAL_WING + 1:
            return
        idx = n - 1 - FRACTAL_WING
        candidate = self.bars[idx]
        neighbors = [self.bars[idx - w] for w in range(1, FRACTAL_WING + 1)]
        neighbors += [self.bars[idx + w] for w in range(1, FRACTAL_WING + 1)]

        if all(candidate["low"] < b["low"] for b in neighbors):
            rsi_then = self.rsi_hist[idx]
            if rsi_then is not None and RSI_LOW <= rsi_then <= RSI_HIGH:
                pivot_price = candidate["low"]
                if self.active_pivot_low != pivot_price:
                    self.active_pivot_low = pivot_price
                    self.short_status = "Short Be Ready"
                    if not silent:
                        log_signal(self.symbol, "Short Be Ready", f"pivot={pivot_price:.2f} rsi={rsi_then:.1f}")


# ---------------------------------------------------------------------------
# 6. Websocket callback
# ---------------------------------------------------------------------------

def on_data(wsapp, message):
    token = message.get("token")
    sym = token_to_symbol.get(token)
    if not sym:
        return
    ltp = message.get("last_traded_price")
    if ltp is None:
        return
    open_ = message.get("open_price_of_the_day")
    prev_close = message.get("closed_price")

    ltp = ltp / 100
    open_ = (open_ / 100) if open_ else None
    prev_close = (prev_close / 100) if prev_close else None

    with lock:
        st = states.get(sym)
        if st:
            st.on_tick(ltp, open_, prev_close)


# ---------------------------------------------------------------------------
# 7. HTML output (the cmd window can only show as many rows as its height --
#    writing a self-refreshing HTML file avoids that truncation entirely)
# ---------------------------------------------------------------------------

def fmt(v, decimals=2):
    return f"{v:.{decimals}f}" if v is not None else "-"


def gather_rows():
    with lock:
        snapshot = list(states.values())

    rows = []
    for st in snapshot:
        pct_chg = (st.ltp - st.prev_close) / st.prev_close * 100 if st.ltp and st.prev_close else None
        gap = (st.open - st.prev_close) / st.prev_close * 100 if st.open and st.prev_close else None
        rows.append(
            {
                "symbol": st.symbol,
                "pct_chg": pct_chg,
                "gap": gap,
                "roc10": st.roc10,
                "roc21": st.roc21,
                "roc63": st.roc63,
                "rsi": st.rsi,
                "mcap": st.mcap,
                "ffmc": st.ffmc,
                "band": st.band,
                "status": st.status,
                "short_status": st.short_status,
            }
        )
    return rows


def build_top_lists_by_band(rows, n):
    """Gainers/losers ranked separately within each price band -- a 20%-band
    mover and a No-Band mover never compete for the same slot."""
    by_band = {}
    for r in rows:
        by_band.setdefault(r["band"], []).append(r)

    result = {}
    for band, band_rows in by_band.items():
        gainers = [r for r in band_rows if r["pct_chg"] is not None and r["pct_chg"] > 0]
        gainers.sort(key=lambda r: -r["pct_chg"])
        losers = [r for r in band_rows if r["pct_chg"] is not None and r["pct_chg"] < 0]
        losers.sort(key=lambda r: r["pct_chg"])
        result[band] = (gainers[:n], losers[:n])
    return result


COLS = (
    "Symbol", "%chg", "Gap%", "10D ROC", "21D ROC", "63D ROC", "RSI(1m)",
    "MCap(cr)", "FFMCap(cr)", "Band", "Long Status", "Short Status",
)

BAND_DISPLAY_ORDER = ("20", "No Band")
BAND_LABELS = {"20": "20% Band", "No Band": "No Band"}


def render_row(r):
    cls_parts = []
    if r["status"] == "Fired":
        cls_parts.append("fired")
    elif r["status"] == "Be Ready":
        cls_parts.append("be-ready")
    if r["short_status"] == "Short Fired":
        cls_parts.append("short-fired")
    elif r["short_status"] == "Short Be Ready":
        cls_parts.append("short-be-ready")

    cells = (
        r["symbol"], fmt(r["pct_chg"]), fmt(r["gap"]), fmt(r["roc10"]), fmt(r["roc21"]),
        fmt(r["roc63"]), fmt(r["rsi"], 1), fmt(r["mcap"], 0), fmt(r["ffmc"], 0),
        r["band"] or "-", r["status"] or "", r["short_status"] or "",
    )
    return f'<tr class="{" ".join(cls_parts)}">' + "".join(f"<td>{c}</td>" for c in cells) + "</tr>"


def render_table_block(title, rows):
    parts = [f"<h3>{title} ({len(rows)})</h3>", "<table><tr>"]
    parts += [f"<th>{c}</th>" for c in COLS]
    parts.append("</tr>")
    parts += [render_row(r) for r in rows]
    parts.append("</table>")
    return "".join(parts)


def build_html(by_band):
    now = datetime.now().strftime("%H:%M:%S")
    parts = [
        "<html><head>",
        f'<meta http-equiv="refresh" content="{config.HTML_REFRESH_SECONDS}">',
        "<title>Live Scanner</title>",
        "<style>",
        "body{background:#0d0d0d;color:#e6e6e6;font-family:Consolas,monospace;padding:20px;}",
        "table{border-collapse:collapse;width:100%;font-size:14px;margin-bottom:28px;}",
        "th,td{border:1px solid #333;padding:5px 10px;text-align:right;}",
        "th:first-child,td:first-child{text-align:left;}",
        "th{background:#1c1c1c;position:sticky;top:0;}",
        "tr.be-ready{background:#3a3a10;}",
        "tr.fired{background:#0f3a1c;color:#8effb0;font-weight:bold;}",
        "tr.short-be-ready{background:#3a1a1a;}",
        "tr.short-fired{background:#3a0f14;color:#ff8e8e;font-weight:bold;}",
        "h2{color:#8ecbff;margin-bottom:4px;}",
        "h2.band-heading{margin-top:34px;border-top:1px solid #333;padding-top:16px;}",
        "h3{color:#8ecbff;margin:18px 0 6px;}",
        "</style></head><body>",
        f"<h2>Live Scanner &mdash; {now}</h2>",
        "<p>Ranked by live %chg, separately per price band</p>",
    ]
    for band in BAND_DISPLAY_ORDER:
        gainers, losers = by_band.get(band, ([], []))
        label = BAND_LABELS.get(band, band)
        parts.append(f'<h2 class="band-heading">{label}</h2>')
        parts.append(render_table_block(f"Top {config.TOP_GAINERS_LOSERS_N} Gainers", gainers))
        parts.append(render_table_block(f"Top {config.TOP_GAINERS_LOSERS_N} Losers", losers))
    parts.append("</body></html>")
    return "\n".join(parts)


def write_scanner_html():
    rows = gather_rows()
    by_band = build_top_lists_by_band(rows, config.TOP_GAINERS_LOSERS_N)
    html = build_html(by_band)
    tmp_path = config.SCANNER_HTML + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(html)
    os.replace(tmp_path, config.SCANNER_HTML)  # atomic, so the browser never reads a half-written file


# ---------------------------------------------------------------------------
# 8. Multi-connection websocket (the universe is usually well over the
#    1000-token-per-connection cap, so it's split across up to 3 connections)
# ---------------------------------------------------------------------------

def _make_ws_callbacks(sws, tokens_for_conn, idx, total):
    def on_open(wsapp):
        sws.subscribe(f"sub_{idx}", 2, [{"exchangeType": 1, "tokens": tokens_for_conn}])
        console.print(f"[green]Connection {idx + 1}/{total}: subscribed {len(tokens_for_conn)} symbols.[/green]")

    def on_error(wsapp, error):
        console.print(f"[red]WebSocket {idx + 1}/{total} error: {error}[/red]")

    def on_close(wsapp):
        console.print(f"[red]WebSocket {idx + 1}/{total} closed.[/red]")

    return on_open, on_error, on_close


def start_websockets(jwt_token, feed_token, token_map):
    tokens = list(token_map.values())
    chunks = [tokens[i : i + MAX_TOKENS_PER_WS] for i in range(0, len(tokens), MAX_TOKENS_PER_WS)]

    if len(chunks) > MAX_WS_CONNECTIONS:
        console.print(
            f"[red]{len(tokens)} tokens need {len(chunks)} connections, but Angel One allows only "
            f"{MAX_WS_CONNECTIONS} per client. Truncating to the first "
            f"{MAX_WS_CONNECTIONS * MAX_TOKENS_PER_WS} tokens.[/red]"
        )
        chunks = chunks[:MAX_WS_CONNECTIONS]

    sockets = []
    for idx, tok_chunk in enumerate(chunks):
        sws = SmartWebSocketV2(jwt_token, config.API_KEY, config.CLIENT_CODE, feed_token, max_retry_attempt=5)
        on_open, on_error, on_close = _make_ws_callbacks(sws, tok_chunk, idx, len(chunks))
        sws.on_open = on_open
        sws.on_data = on_data
        sws.on_error = on_error
        sws.on_close = on_close
        sockets.append(sws)

    for sws in sockets:
        threading.Thread(target=sws.connect, daemon=True).start()
        time.sleep(1)  # stagger connection attempts slightly

    return sockets


# ---------------------------------------------------------------------------
# 9. Main
# ---------------------------------------------------------------------------

def main():
    daily_rows = load_daily_table()
    meta = load_meta(config.FINAL_OUTPUT_CSV)
    universe = filter_universe(daily_rows, meta)
    console.print(
        f"Universe after price band / mcap / ffmcap / turnover screen: {len(universe)} "
        f"(from {len(daily_rows)} total symbols)."
    )

    # No ROC-based pre-selection anymore -- track the ENTIRE filtered universe
    # so the top gainers/losers list is accurate, not drawn from a subset.
    top_rows = universe
    symbols = [r["symbol"] for r in top_rows]

    token_map, missing = load_token_map(symbols)
    if missing:
        console.print(
            f"[yellow]No Angel One token found for {len(missing)} symbols "
            f"(possibly delisted/renamed): {missing[:20]}{' ...' if len(missing) > 20 else ''}[/yellow]"
        )

    token_to_symbol.update({v: k for k, v in token_map.items()})

    for row in top_rows:
        sym = row["symbol"]
        if sym in token_map:
            states[sym] = SymbolState(sym, row)

    band_counts = {}
    for st in states.values():
        band_counts[st.band] = band_counts.get(st.band, 0) + 1
    console.print(f"Tracking {len(states)} symbols live. Band breakdown: {band_counts}")

    n_connections = -(-len(states) // MAX_TOKENS_PER_WS)  # ceil division
    console.print(f"This needs {n_connections} websocket connection(s) ({MAX_TOKENS_PER_WS} tokens each).")

    console.print("Logging in to Angel One...")
    obj, jwt_token, feed_token = angel_login()
    console.print("[green]Login OK.[/green]")

    seed_all_symbols(obj, token_map)

    start_websockets(jwt_token, feed_token, token_map)

    abs_path = os.path.abspath(config.SCANNER_HTML)
    console.print(f"[green]Writing live scanner to: {abs_path}[/green]")
    console.print("[green]Open that file in a browser -- it auto-refreshes, no row limit.[/green]")

    try:
        while True:
            write_scanner_html()
            time.sleep(config.HTML_REFRESH_SECONDS)
    except KeyboardInterrupt:
        console.print("Stopping...")


if __name__ == "__main__":
    main()