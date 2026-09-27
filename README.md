# Sharks OKX Recommendation — clean rebuild

## Architecture
- Primary market-data source: OKX public market data.
- Cloudflare Worker: `worker/worker.js` (OKX-only public market-data proxy).
- Render web service: `server.js`.
- Optional PostgreSQL persistence: set `DATABASE_URL`.
- `APP_MODE=crypto` keeps the application focused on crypto Spot + Futures.

## Deploy order
1. Deploy `worker/worker.js` to the Cloudflare Worker and keep its URL.
2. Set Render `OKX_BASE_URL` to that Worker URL.
3. Deploy the root project to Render.
4. Set `ADMIN_PASSWORD` and `DATABASE_URL` in Render.
5. Open `/api/health` and confirm `dataSource: OKX`.

## Search behavior
- `BTC`, `BTCUSDT`, `BTC-USDT`, `BTC-USDT-SWAP` are normalized for the selected market.
- Search is direct and does not depend on the recommendation cache.
- Enter submits coin search.
- Enter on the access-code field submits login.

## Important
This rebuild does not use Binance as the primary source. Binance Pay can remain a payment method; it is unrelated to market-data sourcing.
