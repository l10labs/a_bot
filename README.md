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

```sh
cargo test
```
