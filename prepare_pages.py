"""
Build docs/index.html for GitHub Pages (demo mode only).

Live quotes, opening range, and NSE data require proxy_server.py — that cannot
run on GitHub Pages. This script embeds stock metadata and enables client-side
demo mode when hosted on github.io.
"""

import json
import re
from pathlib import Path

ROOT = Path(__file__).parent
SRC = ROOT / "market_scanner.html"
OUT = ROOT / "docs" / "index.html"

DEMO_BOOTSTRAP = r"""
// ── GitHub Pages demo (no backend) ──────────────────────────────────────────
const IS_GITHUB_PAGES = /\.github\.io$/i.test(location.hostname);

function demoFmtStock(s) {
  const p = prices[s.symbol] || {};
  const ltp = p.ltp;
  const vol = p.volume || p.vol || 0;
  const prev = p.prev_close;
  const gap = (p.open != null && prev) ? Math.round((p.open - prev) / prev * 10000) / 100 : null;
  const turnover = (ltp && vol) ? Math.round(ltp * vol / 1e7 * 100) / 100 : null;
  return {
    symbol: s.symbol,
    industry: s.industry || '',
    sector: s.sector || '',
    macro: s.macro || '',
    priceband: s.priceband || '',
    ffmc_cr: s.ffmc_cr || 0,
    mcap_cr: s.mcap_cr || 0,
    fno: !!s.fno,
    ltp, pchg: p.pchg, prev_close: prev,
    volume: vol, turnover, gap,
    relVol20: null, relVol50: null,
    days_listed: null, or5_chg: null,
  };
}

function demoScanStocks(filters, universe) {
  const rows = [];
  for (const s of universe) {
    if (filters.fno_only && !s.fno) continue;
    const p = prices[s.symbol];
    if (!p) continue;
    const row = demoFmtStock(s);
    const { ltp, pchg, gap, turnover, mcap_cr: mcap, ffmc_cr: ffmc, priceband: pb } = row;
    if (filters.chg_min != null && (pchg == null || pchg < filters.chg_min)) continue;
    if (filters.chg_max != null && (pchg == null || pchg > filters.chg_max)) continue;
    if (filters.gap_min != null && (gap == null || gap < filters.gap_min)) continue;
    if (filters.gap_max != null && (gap == null || gap > filters.gap_max)) continue;
    if (filters.mcap_min != null && mcap < filters.mcap_min) continue;
    if (filters.mcap_max != null && mcap > filters.mcap_max) continue;
    if (filters.ffmc_min != null && ffmc < filters.ffmc_min) continue;
    if (filters.ffmc_max != null && ffmc > filters.ffmc_max) continue;
    if (filters.turn_min != null && (turnover == null || turnover < filters.turn_min)) continue;
    if (filters.turn_max != null && (turnover == null || turnover > filters.turn_max)) continue;
    if (filters.price_min != null && (ltp == null || ltp < filters.price_min)) continue;
    if (filters.price_max != null && (ltp == null || ltp > filters.price_max)) continue;
    if (filters.band && filters.band !== 'all' && pb !== filters.band) continue;
    rows.push(row);
  }
  return rows;
}

function demoScanIndustries(groupBy) {
  const groups = {};
  for (const s of STOCKS_RAW) {
    const p = prices[s.symbol];
    if (!p) continue;
    const key = groupBy === 'fno' ? (s.fno ? 'F&O' : null) : (s[groupBy] || 'Unknown');
    if (!key) continue;
    if (!groups[key]) groups[key] = { name: key, stocks: [], pchgs: [], advancing: 0, declining: 0, total_turnover: 0 };
    const g = groups[key];
    g.stocks.push(s.symbol);
    const pchg = p.pchg;
    if (pchg != null) {
      g.pchgs.push(pchg);
      if (pchg > 0) g.advancing++;
      else if (pchg < 0) g.declining++;
    }
    const ltp = p.ltp || 0;
    const vol = p.volume || p.vol || 0;
    g.total_turnover += Math.round(ltp * vol / 1e7 * 10) / 10;
  }
  return Object.values(groups).map(g => ({
    name: g.name,
    stock_count: g.stocks.length,
    covered: g.pchgs.length,
    avg_pchg: g.pchgs.length ? Math.round(g.pchgs.reduce((a, b) => a + b, 0) / g.pchgs.length * 100) / 100 : null,
    advancing: g.advancing,
    declining: g.declining,
    total_turnover: Math.round(g.total_turnover * 10) / 10,
  })).sort((a, b) => (b.avg_pchg || 0) - (a.avg_pchg || 0));
}

function renderIndustriesDemo() {
  const search = document.getElementById('searchInput').value.toLowerCase();
  let entries = demoScanIndustries(groupBy);
  if (search) entries = entries.filter(e => e.name.toLowerCase().includes(search));
  document.getElementById('indCount').textContent = entries.length;
  const maxAbsChg = Math.max(...entries.filter(e => e.avg_pchg !== null).map(e => Math.abs(e.avg_pchg || 0)), 1);
  let html = '';
  for (const g of entries) {
    const chg = g.avg_pchg;
    const isActive = g.name === selectedGroup ? 'active' : '';
    const chgStr = chg !== null ? (chg >= 0 ? '+' : '') + chg.toFixed(2) + '%' : '--';
    const cls = chg === null ? 'flat' : (chg >= 0 ? 'up' : 'down');
    const barW = chg !== null ? Math.round((Math.abs(chg) / maxAbsChg) * 56) : 0;
    html += `
      <div class="industry-row ${isActive}" onclick="selectGroup('${g.name.replace(/'/g, "\\'")}')">
        <div>
          <div class="ind-name" title="${g.name}">${g.name}</div>
          <div class="ind-meta">${g.stock_count} stocks · ${g.covered} live</div>
        </div>
        <div class="chg-bar-wrap"><div class="mini-bar ${cls}" style="width:${barW}px"></div></div>
        <div class="chg-badge ${cls}">${chgStr}</div>
      </div>`;
  }
  document.getElementById('industryList').innerHTML = html;
}

function renderRightDemo() {
  if (!selectedGroup) {
    document.getElementById('rightPanel').innerHTML = `
      <div class="right-empty">
        <div class="icon">📊</div>
        <p>Select an industry to view stocks</p>
      </div>`;
    return;
  }
  const stocks = STOCKS_RAW.filter(s => {
    const key = groupBy === 'fno' ? (s.fno ? 'F&O' : '') : (s[groupBy] || '');
    return key === selectedGroup && prices[s.symbol];
  }).map(demoFmtStock).sort((a, b) => (b.pchg || 0) - (a.pchg || 0));
  let rows = '';
  stocks.forEach((s, i) => {
    const ltpStr = s.ltp ? '₹' + s.ltp.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '--';
    const pchgStr = s.pchg !== null ? (s.pchg >= 0 ? '+' : '') + s.pchg.toFixed(2) + '%' : '--';
    const color = s.pchg === null ? 'color:var(--muted)' : (s.pchg >= 0 ? 'color:var(--up)' : 'color:var(--down)');
    rows += `<tr>
      <td class="rank-cell">${i + 1}</td>
      <td class="sym-cell"><span class="sym-name">${s.symbol}</span></td>
      <td style="font-family:'JetBrains Mono',monospace">${ltpStr}</td>
      <td style="font-family:'JetBrains Mono',monospace;${color}">${pchgStr}</td>
      <td style="color:var(--muted);font-size:11px">${formatMcap(s.ffmc_cr || s.mcap_cr)}</td>
    </tr>`;
  });
  document.getElementById('rightPanel').innerHTML = `
    <div class="right-header">
      <div class="rh-title">${selectedGroup}</div>
      <div class="rh-sub">${stocks.length} stocks · demo data</div>
    </div>
    <table class="stock-table"><thead><tr>
      <th>#</th><th>Symbol</th><th>LTP</th><th>%Chg</th><th>FF MCap</th>
    </tr></thead><tbody>${rows}</tbody></table>`;
}

function startGithubPagesDemo() {
  demoMode = true;
  document.getElementById('configModal')?.classList.add('hidden');
  setStatus('demo', 'Demo (GitHub Pages)');
  loadDemoData();
  startRefreshCycle();
  const banner = document.createElement('div');
  banner.style.cssText = 'background:#1e3a5f;border:1px solid #3b82f6;border-radius:7px;padding:10px 14px;font-size:12px;color:#93c5fd;margin:8px 24px;line-height:1.5;';
  banner.innerHTML = '<b>Demo site.</b> Prices are simulated. For live NSE data, run <code>python proxy_server.py</code> locally and open <a href="http://localhost:8765" style="color:#bfdbfe">localhost:8765</a>.';
  document.querySelector('.header')?.after(banner);
}
"""


def extract_ipo_raw(html: str) -> list:
    marker = "const IPO_RAW_ALL = "
    start = html.index(marker) + len(marker)
    depth = 0
    i = start
    while i < len(html):
        ch = html[i]
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return json.loads(html[start : i + 1])
        i += 1
    raise ValueError("Could not parse IPO_RAW_ALL")


def ipo_to_stocks_raw(ipo_rows: list) -> list:
    out = []
    for row in ipo_rows:
        pb = (row.get("priceband") or "").strip()
        out.append({
            "symbol": row["symbol"],
            "mcap_cr": row.get("mcap_cr", 0),
            "ffmc_cr": row.get("ffmc_cr", row.get("mcap_cr", 0)),
            "priceband": pb,
            "macro": row.get("macro", ""),
            "sector": row.get("sector", ""),
            "industry": row.get("industry", ""),
            "fno": pb == "No Band",
        })
    return out


def main():
    html = SRC.read_text(encoding="utf-8")
    ipo_rows = extract_ipo_raw(html)
    stocks_raw = ipo_to_stocks_raw(ipo_rows)
    stocks_js = "const STOCKS_RAW = " + json.dumps(stocks_raw, separators=(",", ":")) + ";"

    html = html.replace("/*STOCKS_RAW_PLACEHOLDER*/", stocks_js)
    html = html.replace(
        "function renderIndustries() {\n  const search = document.getElementById('searchInput').value.toLowerCase();",
        "function renderIndustries() {\n  if (demoMode) { renderIndustriesDemo(); return; }\n  const search = document.getElementById('searchInput').value.toLowerCase();",
    )
    html = html.replace(
        "function renderRight() {\n  if (!selectedGroup) {",
        "function renderRight() {\n  if (demoMode) { renderRightDemo(); return; }\n  if (!selectedGroup) {",
    )

    onload_old = """window.onload = async () => {
  renderIndustries();
  setStatus('loading', 'Connecting to proxy…');
  const connected = await checkProxyHealth();
  if (!connected) {
    if (location.protocol === 'file:') {
      const warn = document.createElement('div');
      warn.style.cssText = 'background:#7c1d1d;border:1px solid #ef4444;border-radius:7px;padding:10px 12px;font-size:11px;color:#fca5a5;margin-bottom:14px;line-height:1.6;';
      warn.innerHTML = '⚠️ <b>Open from proxy, not as a file.</b> You\\'re running this as <code>file://</code> — the browser blocks requests to localhost. Visit <a href="http://localhost:8765" style="color:#f87171">http://localhost:8765</a> instead.';
      const modal = document.querySelector('.modal');
      if (modal) modal.insertBefore(warn, modal.firstChild);
    }
    showConfig();
  }
};"""

    onload_new = """window.onload = async () => {
  if (IS_GITHUB_PAGES) { startGithubPagesDemo(); return; }
  renderIndustries();
  setStatus('loading', 'Connecting to proxy…');
  const connected = await checkProxyHealth();
  if (!connected) {
    if (location.protocol === 'file:') {
      const warn = document.createElement('div');
      warn.style.cssText = 'background:#7c1d1d;border:1px solid #ef4444;border-radius:7px;padding:10px 12px;font-size:11px;color:#fca5a5;margin-bottom:14px;line-height:1.6;';
      warn.innerHTML = '⚠️ <b>Open from proxy, not as a file.</b> You\\'re running this as <code>file://</code> — the browser blocks requests to localhost. Visit <a href="http://localhost:8765" style="color:#f87171">http://localhost:8765</a> instead.';
      const modal = document.querySelector('.modal');
      if (modal) modal.insertBefore(warn, modal.firstChild);
    }
    showConfig();
  }
};"""

    if onload_old not in html:
        raise SystemError("Could not find window.onload block to patch")
    html = html.replace(onload_old, onload_new)

    # Inject demo helpers after STOCKS_RAW
    html = html.replace(
        stocks_js,
        stocks_js + "\n" + DEMO_BOOTSTRAP.strip(),
    )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(html, encoding="utf-8")
    print(f"Wrote {OUT} ({len(stocks_raw)} stocks, demo mode for GitHub Pages)")


if __name__ == "__main__":
    main()
