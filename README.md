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

## Crypto Up/Down markets: 5m, 15m, 1h, 4h

`src/markets.rs` uses the SDK's Gamma and CLOB WebSocket clients (public; no credentials).

- Every 30 seconds, paginates Gamma's open markets in 100-row pages. Queries end times out to 4h15m, then selects supported markets whose start is no later than 15 minutes from now. This includes all currently active 4h/1h/15m/5m windows, not just markets closing in 15 minutes.
- 5m/15m/4h markets use `<asset>-updown-<duration>-<start-seconds>` slugs; hourly markets use descriptive slugs such as `bitcoin-up-or-down-october-22-2026-12am-et`. Hourly windows are validated as exactly one hour using Gamma `eventStartTime`/`endDate`, with the crypto symbol derived from the Binance pair in the description. `startDate` is a listing date and is not used as the trading-window start. Timestamp slugs are cross-checked against Gamma's timing fields where present, and ET-aligned 4h windows are supported.
- Discovers assets dynamically, validates Up/Down outcomes, and maps each outcome to its Gamma token ID. Tracks current and upcoming windows and refreshes subscriptions as windows roll over. Discovery/subscriptions retain the 15-minute lookahead, but persistence requires both the source timestamp and captured receipt timestamp to be within `[start_ms, end_ms)`. Metadata and per-asset tables are created with the first eligible quote, never for upcoming windows alone. A final batch can commit after close if its ticks arrived during the active window. Existing history is not retroactively deleted.
- Subscribes to `best_bid_ask` with the SDK's custom-feature flag. These are change events, not periodic snapshots; a quiet token may have no rows until its best prices change. No fabricated initial quotes or historical backfill.
- Discovery runs separately from quote consumption, retries HTTP errors, and preserves subscriptions on discovery failures. The SDK handles WebSocket reconnects.
- Quote writes share the dedicated one-second writer, WAL database, and bounded queue. RTDS and CLOB batches commit in separate transactions. Ctrl+C stops discovery/collection, drains queued updates, and commits before closing SQLite.

Schema is created directly without migration files:

For each `<duration>` in `5m`, `15m`, `1h`, `4h`:

- `crypto_<duration>_markets`: Gamma market ID, condition ID, slug, crypto asset, question, and window start/end in Unix milliseconds.
- `crypto_<duration>_tokens`: decimal-text token ID, market foreign key, and `up`/`down` outcome.
- `crypto_<duration>_quote_tables`: asset-to-table registry.
- `<asset>_<duration>_quotes` (e.g. `btc_15m_quotes`, `btc_1h_quotes`, `btc_4h_quotes`): one table per discovered asset; token foreign key, `timestamp_ms`, `received_ms`, exact-decimal-text `best_bid` and `best_ask`, indexed by token/time. Both outcomes and successive windows are retained in the asset's table.

Live research found 5m/15m markets for BTC, ETH, SOL, XRP, DOGE, BNB, HYPE, ZEC; hourly listings for all of those except ZEC; and 4h listings as well. Availability and quote activity vary. Tables appear only after an eligible quote arrives. Existing 5m schema/data remain compatible.

```sql
SELECT m.slug, t.outcome, q.timestamp_ms, q.received_ms, q.best_bid, q.best_ask
FROM btc_5m_quotes q
JOIN crypto_5m_tokens t ON t.token_id = q.token_id
JOIN crypto_5m_markets m ON m.market_id = t.market_id
ORDER BY q.id DESC LIMIT 20;
```

```sh
cargo test
```
