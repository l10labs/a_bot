# a_bot — min5-slim

This branch collects only:

- Five-minute crypto Up/Down **best bid/ask** updates for both outcome tokens.
- Five-minute crypto Up/Down **public executed-trade ticks**.
- **Final resolutions** for five-minute markets, including results received after expiry.
- All **Binance** and **Chainlink** reference-price updates available through Polymarket RTDS, without asset filters.

It does **not** store order books, book changes, or any 15m/1h/4h market data.

## Run

```sh
cargo run
# Optional fresh database location:
DATABASE_URL=sqlite://custom-slim.db cargo run
```

Defaults to `min5-slim.db` in the working directory. The full-data branch's `prices.db` is not modified. Schema is created directly at startup, without migration files. Use a fresh database: pointing this branch at an old database does not remove or migrate its existing tables/history.

No credentials are required. The bot uses the published `polymarket_client_sdk_v2` crate from crates.io, not a local path or patched SDK. The sibling `../rs-clob-client-v2` checkout is read-only reference material. It was added with `cargo add polymarket_client_sdk_v2 --registry crates-io --features rtds,clob,ws,gamma,tracing`.

## Discovery and timing

Every 30 seconds, paginates Gamma in 100-row pages for open markets ending within the next 15 minutes. Accepts only `<asset>-updown-5m-<window-start-seconds>` markets with exactly Up/Down outcomes and two distinct token IDs. Checks available Gamma timing fields against the five-minute slug window. Assets are discovered dynamically.

Current and upcoming markets are pre-subscribed, but **both source and receipt timestamps must lie in `[start_ms, end_ms)` before a quote/trade is stored**. Active market/token metadata is registered on the one-second maintenance tick, even if no quotes or trades arrive, so quiet markets can still be tracked to settlement. Upcoming metadata is not persisted before the window starts. Expired unresolved markets retain subscriptions until their result is stored, then are removed on the next discovery update.

Resolution events are exempt from active-window filtering and are accepted after expiry only when condition ID, winning token and Up/Down mapping match. Every 60 seconds, Gamma reconciliation checks persisted expired unresolved markets, accepting only explicitly finalized results (`closed=true`, UMA status `resolved`, exact 1/0 payouts). It never guesses from near-1 prices. Pending markets resume on restart. For Gamma fallback, `timestamp_ms` is the observation time; for WebSocket results it is the event timestamp.

No full-universe or opening-window snapshot refreshes are needed on this branch. Best bid/ask is a change feed, not periodic sampling: a quiet token may have no quote row until prices change. Books, depth changes, and other irrelevant messages arriving on the exchange's shared market channel are discarded before the raw queue. This reduces processing/storage, but does not prevent the exchange from transmitting those message types.

## Slim schema

Shared tables:

| Table | Purpose |
|---|---|
| `assets` | RTDS symbols, unique by `(provider, symbol)`; provider is Binance or Chainlink |
| `binance_prices` | Source `timestamp_ms`, local `received_ms`, exact decimal price, asset foreign key |
| `chainlink_prices` | Same structure for Chainlink |
| `markets` | Gamma market ID, condition ID, slug, crypto asset, window start/end; enforces five-minute duration |
| `tokens` | Token ID, market foreign key, Up/Down outcome |
| `resolutions` | One final result per market: winning token/outcome, event or observation time, receipt time and source |
| `collection_events` | Small operational session/gap/connection log |

For each recorded crypto asset:

- `<asset>_5m_quotes`: token ID, source/receipt Unix milliseconds, exact decimal `best_bid` and `best_ask`.
- `<asset>_5m_trades`: token ID, source/receipt Unix milliseconds, price, optional size/side/fee basis points.

For example: `btc_5m_quotes`, `btc_5m_trades`, `eth_5m_quotes`, `eth_5m_trades`. Both outcomes and successive windows share the asset's tables. Tables are created when the first eligible event for that asset is stored. There are no timeframe registries or depth tables. All five-minute outcomes use the single `resolutions` table, with market/token foreign keys and idempotent inserts. All prices/sizes are decimal TEXT, not floating point.

```sql
SELECT m.slug, t.outcome, q.timestamp_ms, q.best_bid, q.best_ask
FROM btc_5m_quotes q
JOIN tokens t USING (token_id)
JOIN markets m USING (market_id)
ORDER BY q.id DESC LIMIT 20;

SELECT m.slug, t.outcome, x.timestamp_ms, x.price, x.size, x.side
FROM btc_5m_trades x
JOIN tokens t USING (token_id)
JOIN markets m USING (market_id)
ORDER BY x.id DESC LIMIT 20;
```

## Pipeline and durability

Independent Tokio tasks handle Gamma discovery, per-asset WebSocket ingestion, and per-asset filtering/normalization on the multi-thread runtime. Each market uses the stock SDK's typed best-bid/ask, last-trade-price, and market-resolution subscriptions, merged by the bot. All three SDK subscription references are released together when a market is removed. No custom SDK APIs are required.

One dedicated SQLite writer batches all persisted records every second, or earlier at 8,192 records, using chunked multi-row inserts. Raw queues hold up to 8,192 relevant messages per asset; the shared writer queue holds 65,536 records. SQLite uses WAL and two pooled connections so discovery/reconciliation can read pending results independently of the single database writer.

Ctrl+C stops ingress, drains accepted raw records and writer queues, commits the final batch, verifies `PRAGMA wal_checkpoint(TRUNCATE)`, then closes SQLite. Never manually remove nonempty WAL files.

Memory is bounded by item counts, not a fixed byte budget. A full raw queue drops new messages without blocking ingestion and records the loss in `collection_events`. Returned stream errors, connection changes, and session boundaries are also recorded. The unmodified SDK logs broadcast-lag warnings internally but does not return them as typed-stream errors; those warnings remain visible in the console and are **not** automatically persisted as `sdk_lag` rows. Counts from multiple SDK receivers can overlap and are not unique missing trades. Telemetry timestamps are observation times, not exact source gap boundaries.

Public `last_trade_price` messages lack reliable execution IDs for deduplication. Repeated received ticks are retained. Disconnects/lag can cause missing trades, and this branch does not backfill them. Final outcomes are retained in `resolutions`; unresolved markets remain pending until the WebSocket or Gamma confirms finality.

## Logs and tests

Logs use aligned UTC time/level/component columns. Default filtering is `warn,a_bot=info`. Each committed batch reports `batch`, RTDS `ref`, CLOB `quote`, `trade`, final `res`, metadata/telemetry `evt`, write `ms`, and writer `queue` depth at flush start. Batch counts refer to queued records, not all SQL rows.

```sh
RUST_LOG=warn,a_bot=debug cargo run  # include ingress queue diagnostics
RUST_LOG=warn cargo run            # warnings/errors only
cargo test
```
