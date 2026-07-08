# NSE Industry Scanner

Live market scanner for NSE equities — industry heatmaps, stock filters, IPO tracking, and opening-range monitoring.

## Live use (real market data)

GitHub Pages **cannot** run the backend. For live quotes you need the proxy on your machine:

```bash
pip install -r requirements.txt
# Create config.json with your Angel One SmartAPI credentials (never commit this file)
python final_output.py          # once — builds the stock universe CSV
python proxy_server.py          # during market hours
```

Open **http://localhost:8765** in your browser.

`config.json` example (keep local only):

```json
{
  "api_key": "your-api-key",
  "client_code": "your-client-id",
  "pin": "1234",
  "totp_secret": "your-totp-secret"
}
```

## GitHub Pages (cached prices)

The hosted site shows **real price snapshots** from Angel One, refreshed **every 30 minutes** on trading days (9:15 AM–3:30 PM IST). Reload the page to see the latest.

Add these **GitHub Secrets** (Settings → Secrets → Actions):

| Secret | Value |
|--------|--------|
| `ANGEL_API_KEY` | SmartAPI key from [smartapi.angelone.in](https://smartapi.angelone.in) |
| `ANGEL_CLIENT_CODE` | Client ID |
| `ANGEL_PIN` | 4-digit Angel One login PIN |
| `ANGEL_TOTP_SECRET` | TOTP secret from SmartAPI portal |

Workflow: `.github/workflows/quote-refresh.yml` — runs `scripts/refresh_github_pages.py`.

Manual test: **Actions → Quote refresh → Run workflow**

For live streaming during market hours, run `proxy_server.py` locally.

To rebuild the demo page after editing `market_scanner.html`:

```bash
python prepare_pages.py
```
