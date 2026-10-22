# a_bot

Streams every Binance and Chainlink price available through Polymarket RTDS, without symbol filters. A dedicated writer thread commits queued updates in one transaction every second. After each nonempty commit, prints the number of prices, distinct assets, per-feed/symbol counts, and write duration instead of individual ticks.

```sh
cargo run
# Optional database location:
DATABASE_URL=sqlite://custom.db cargo run
```

SQLite defaults to `prices.db` in the working directory. SQLx creates it and initializes the latest schema automatically (no migrations). Existing databases must already use the latest schema, including `received_ms` and `assets.provider`. Ctrl+C drains the bounded writer queue and commits the final partial batch before exiting. Receipt timestamps are captured when updates arrive, not at batch commit time. No credentials required. The sibling `../rs-clob-client-v2` SDK is a path dependency.

- `assets`: unique `(provider, symbol)` pairs, with provider `binance` or `chainlink` and source-native symbols (e.g. Binance `btcusdt`, Chainlink `btc/usd`); no normalization or symbol filtering.
- `binance_prices`: Binance history.
- `chainlink_prices`: Chainlink history (created automatically alongside existing Binance tables).

Both history tables store every received update, linked to its asset, with source `timestamp_ms` and local `received_ms`, both Unix milliseconds. Prices are exact decimal TEXT, not floating point. Repeated ticks are retained.

```sql
SELECT a.symbol, p.timestamp_ms, p.price
FROM binance_prices p JOIN assets a ON a.id = p.asset_id
ORDER BY p.timestamp_ms DESC, p.id DESC
LIMIT 20;
```

The SDK reconnects automatically, but this is a live feed, not a historical backfill: updates during disconnections or database stalls may be missed. RTDS determines available assets for each feed; these are not necessarily Binance's entire exchange listing. Binance uses `crypto_prices`/`update`; Chainlink uses `crypto_prices_chainlink`/`*`. Both subscriptions omit the filters field. Storage grows with price history; no retention policy is applied.

## Crypto five-minute Up/Down markets

`src/markets.rs` uses the SDK's Gamma and CLOB WebSocket clients (public; no credentials).

- Every 30 seconds, paginates Gamma's open markets ending within the next 15 minutes, selecting source slugs matching `<asset>-updown-5m-<window-start-seconds>`.
- Discovers assets dynamically, validates Up/Down outcomes, and maps each outcome to its Gamma token ID. Tracks current and upcoming windows, refreshes subscriptions as windows roll over, and ignores expired-window ticks.
- Subscribes to `best_bid_ask` with the SDK's custom-feature flag. These are change events, not periodic snapshots; a quiet token may have no rows until its best prices change. No fabricated initial quotes or historical backfill.
- Discovery runs separately from quote consumption, retries HTTP errors, and preserves subscriptions on discovery failures. The SDK handles WebSocket reconnects.
- Quote writes share the dedicated one-second writer, WAL database, and bounded queue. RTDS and CLOB batches commit in separate transactions. Ctrl+C stops discovery/collection, drains queued updates, and commits before closing SQLite.

Schema is created directly without migration files:

- `crypto_5m_markets`: Gamma market ID, condition ID, slug, crypto asset, question, and window start/end in Unix milliseconds.
- `crypto_5m_tokens`: decimal-text token ID, market foreign key, and `up`/`down` outcome.
- `crypto_5m_quote_tables`: asset-to-table registry.
- `crypto_5m_<asset>_quotes`: one table per discovered asset; token foreign key, `timestamp_ms`, `received_ms`, exact-decimal-text `best_bid` and `best_ask`, indexed by token/time. Both outcomes and successive windows are retained in the asset's table.

Observed live assets: BTC, ETH, SOL, XRP, DOGE, BNB, HYPE, ZEC (availability may change).

```sql
SELECT m.slug, t.outcome, q.timestamp_ms, q.received_ms, q.best_bid, q.best_ask
FROM crypto_5m_btc_quotes q
JOIN crypto_5m_tokens t ON t.token_id = q.token_id
JOIN crypto_5m_markets m ON m.market_id = t.market_id
ORDER BY q.id DESC LIMIT 20;
```

```sh
cargo test
```
