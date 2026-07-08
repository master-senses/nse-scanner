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

## GitHub Pages (demo only)

The hosted site uses **simulated prices** so you can preview the UI. It does not connect to Angel One or NSE.

To rebuild the demo page after editing `market_scanner.html`:

```bash
python prepare_pages.py
```
