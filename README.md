# a_bot

Streams every Binance price available through Polymarket RTDS, without symbol filters. A dedicated writer thread commits queued updates in one transaction every second. After each nonempty commit, prints the number of prices, distinct assets, per-symbol counts, and write duration instead of individual ticks.

```sh
cargo run
# Optional database location:
DATABASE_URL=sqlite://custom.db cargo run
```

SQLite defaults to `prices.db` in the working directory. SQLx creates it and initializes the latest schema automatically (no migrations). Existing databases must already use the latest schema, including `received_ms`. Ctrl+C drains the bounded writer queue and commits the final partial batch before exiting. Receipt timestamps are captured when updates arrive, not at batch commit time. No credentials required. The sibling `../rs-clob-client-v2` SDK is a path dependency.

- `assets`: unique trading-pair symbols (e.g. `btcusdt`).
- `prices`: every received update, linked to its asset, with source `timestamp_ms` and local `received_ms`, both Unix milliseconds. Prices are exact decimal TEXT, not floating point. Repeated ticks are retained.

```sql
SELECT a.symbol, p.timestamp_ms, p.price
FROM prices p JOIN assets a ON a.id = p.asset_id
ORDER BY p.timestamp_ms DESC, p.id DESC
LIMIT 20;
```

The SDK reconnects automatically, but this is a live feed, not a historical backfill: updates during disconnections or database stalls may be missed. RTDS's available assets are not necessarily Binance's entire exchange listing. Storage grows with price history; no retention policy is applied.

```sh
cargo test
```
