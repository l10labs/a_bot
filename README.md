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

`src/markets.rs` handles discovery/subscriptions; `src/market_data.rs` handles depth, executions, and final results. Both use the SDK's public Gamma/CLOB APIs (no credentials).

- Every 30 seconds, paginates Gamma's open markets in 100-row pages. Queries end times out to 4h15m, then selects supported markets whose start is no later than 15 minutes from now. This includes all currently active 4h/1h/15m/5m windows, not just markets closing in 15 minutes.
- 5m/15m/4h markets use `<asset>-updown-<duration>-<start-seconds>` slugs; hourly markets use descriptive slugs such as `bitcoin-up-or-down-october-22-2026-12am-et`. Hourly windows are validated as exactly one hour using Gamma `eventStartTime`/`endDate`, with the crypto symbol derived from the Binance pair in the description. `startDate` is a listing date and is not used as the trading-window start. Timestamp slugs are cross-checked against Gamma's timing fields where present, and ET-aligned 4h windows are supported.
- Discovers assets dynamically, validates Up/Down outcomes, and maps each outcome to its Gamma token ID. Tracks current and upcoming windows and refreshes subscriptions as windows roll over. Discovery/subscriptions retain the 15-minute lookahead, but persistence requires both the source timestamp and captured receipt timestamp to be within `[start_ms, end_ms)`. Metadata and per-asset tables are created with the first eligible quote/book/change/trade, never for upcoming windows alone. A final batch can commit after close if its ticks arrived during the active window. Existing history is not retroactively deleted.
- Subscribes to best bid/ask, full order-book snapshots, price-level changes, public last-trade events, and market resolutions. Best bid/ask and resolution subscriptions enable the SDK's custom features. Quotes are change events, not periodic samples.
- At active-window transitions, refreshes subscriptions to request a fresh initial book; pre-window snapshots are not stored. Book levels preserve wire order and exact decimal price/size. Changes with size zero remove that price level; missing size remains NULL, not zero.
- Public `last_trade_price` is the library's live public execution feed, not authenticated user trades. Every received eligible tick is retained, including repeats; the SDK does not supply an execution ID for reliable deduplication. This is not a guaranteed complete historical trade tape. Disconnects, SDK broadcast lag, and subscription refreshes can cause gaps. SDK warnings are enabled (`RUST_LOG=warn` by default); snapshots re-anchor depth after refresh/reconnect. There is no sequence-number guarantee for reconstructing an uninterrupted book.
- Discovery runs separately from event consumption, retries HTTP errors, and preserves subscriptions on discovery failures. The SDK handles WebSocket reconnects.
- Expired unresolved markets retain subscriptions. Resolution events are explicitly exempt from active-window filtering and must match the tracked winning token/outcome/condition. Every 60 seconds, reconciliation polls Gamma for persisted expired unresolved markets, accepting only `closed=true`, UMA status `resolved`, and an exact 1/0 payout pair. It does not guess a winner from near-1 prices or assume expiry means resolution. Persisted pending markets are restored on restart. Gamma-fallback `timestamp_ms` is the observation time, not an invented resolution-event time. Pending results can remain unresolved until Gamma confirms finality.
- Quote writes share the dedicated one-second writer, WAL database, and bounded queue. RTDS and CLOB batches commit in separate transactions. Ctrl+C stops discovery/collection, drains queued updates, and commits before closing SQLite.

Schema is created directly without migration files:

For each `<duration>` in `5m`, `15m`, `1h`, `4h`:

- `crypto_<duration>_markets`: Gamma market ID, condition ID, slug, crypto asset, question, and window start/end in Unix milliseconds.
- `crypto_<duration>_tokens`: decimal-text token ID, market foreign key, and `up`/`down` outcome.
- `crypto_<duration>_quote_tables`: quote-table registry.
- `crypto_<duration>_data_tables`: asset/kind/table-name registry for depth and trades.
- `crypto_<duration>_resolutions`: one final result per market, with winning token/outcome, source (`clob_ws` or `gamma_finalized`), event/observation timestamp and receipt timestamp. Duplicate notifications are idempotent.
- `<asset>_<duration>_quotes` (e.g. `btc_15m_quotes`, `btc_1h_quotes`, `btc_4h_quotes`): one table per discovered asset; token foreign key, `timestamp_ms`, `received_ms`, exact-decimal-text `best_bid` and `best_ask`, indexed by token/time. Both outcomes and successive windows are retained in the asset's table.

Additional tables for every active asset/duration (created when the relevant event first arrives):

- `<asset>_<duration>_books`: snapshot header, token ID, source/receipt timestamps, hash. Empty snapshots retain their header.
- `<asset>_<duration>_book_levels`: snapshot foreign key, bid/ask side, level position, exact price and size.
- `<asset>_<duration>_book_changes`: token, timestamps, BUY/SELL side, price, optional size/hash/resulting best prices.
- `<asset>_<duration>_trades`: token, timestamps, execution price, optional size/side/fee basis points.

Token foreign keys link all histories to the market and Up/Down outcome. Numeric market values remain decimal TEXT. Snapshots and their levels, metadata, trades, and resolutions commit atomically within each CLOB batch. Schema is initialized with `CREATE TABLE IF NOT EXISTS`; existing quote tables remain compatible.

Live research found 5m/15m markets for BTC, ETH, SOL, XRP, DOGE, BNB, HYPE, ZEC; hourly listings for all of those except ZEC; and 4h listings as well. Availability and quote activity vary. Tables appear only after an eligible quote arrives. Existing 5m schema/data remain compatible.

```sql
SELECT m.slug, t.outcome, q.timestamp_ms, q.received_ms, q.best_bid, q.best_ask
FROM btc_5m_quotes q
JOIN crypto_5m_tokens t ON t.token_id = q.token_id
JOIN crypto_5m_markets m ON m.market_id = t.market_id
ORDER BY q.id DESC LIMIT 20;
```

```sql
-- Final results, including those received after expiry:
SELECT m.slug, r.winning_outcome, r.source, r.received_ms
FROM crypto_5m_resolutions r JOIN crypto_5m_markets m USING (market_id);

-- Public execution history with Up/Down labels:
SELECT t.outcome, x.timestamp_ms, x.price, x.size, x.side
FROM btc_5m_trades x JOIN crypto_5m_tokens t USING (token_id)
ORDER BY x.id DESC LIMIT 20;
```

```sh
cargo test
```
