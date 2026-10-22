use anyhow::Context;
use futures::StreamExt as _;
use polymarket_client_sdk_v2::rtds::{Client, types::response::CryptoPrice};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

#[derive(Clone, Copy)]
enum Feed {
    Binance,
    Chainlink,
}

impl Feed {
    fn name(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Chainlink => "chainlink",
        }
    }

    fn insert_sql(self) -> &'static str {
        match self {
            Self::Binance => {
                "INSERT INTO binance_prices (asset_id, timestamp_ms, price, received_ms) SELECT id, ?, ?, ? FROM assets WHERE symbol = ? AND provider = ?"
            }
            Self::Chainlink => {
                "INSERT INTO chainlink_prices (asset_id, timestamp_ms, price, received_ms) SELECT id, ?, ?, ? FROM assets WHERE symbol = ? AND provider = ?"
            }
        }
    }
}

struct ReceivedPrice {
    feed: Feed,
    price: CryptoPrice,
    received_ms: i64,
}

impl ReceivedPrice {
    fn new(feed: Feed, price: CryptoPrice) -> Self {
        Self {
            feed,
            price,
            received_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock precedes Unix epoch")
                .as_millis() as i64,
        }
    }
}

async fn initialize_schema(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS assets (
            id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL,
            provider TEXT NOT NULL CHECK (provider IN ('binance', 'chainlink')),
            UNIQUE(provider, symbol)
        );
        CREATE TABLE IF NOT EXISTS binance_prices (
            id INTEGER PRIMARY KEY,
            asset_id INTEGER NOT NULL REFERENCES assets(id),
            timestamp_ms INTEGER NOT NULL,
            price TEXT NOT NULL,
            received_ms INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
        );
        CREATE INDEX IF NOT EXISTS binance_prices_asset_timestamp ON binance_prices(asset_id, timestamp_ms);
        CREATE TABLE IF NOT EXISTS chainlink_prices (
            id INTEGER PRIMARY KEY,
            asset_id INTEGER NOT NULL REFERENCES assets(id),
            timestamp_ms INTEGER NOT NULL,
            price TEXT NOT NULL,
            received_ms INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER))
        );
        CREATE INDEX IF NOT EXISTS chainlink_prices_asset_timestamp ON chainlink_prices(asset_id, timestamp_ms);",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn store_batch(pool: &SqlitePool, batch: &[ReceivedPrice]) -> anyhow::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let started = std::time::Instant::now();
    let mut tx = pool.begin().await?;
    let mut counts = BTreeMap::new();
    for update in batch {
        let price = &update.price;
        sqlx::query("INSERT INTO assets (symbol, provider) VALUES (?, ?) ON CONFLICT(provider, symbol) DO NOTHING")
            .bind(&price.symbol)
            .bind(update.feed.name())
            .execute(&mut *tx)
            .await?;
        sqlx::query(update.feed.insert_sql())
            .bind(price.timestamp)
            .bind(price.value.to_string())
            .bind(update.received_ms)
            .bind(&price.symbol)
            .bind(update.feed.name())
            .execute(&mut *tx)
            .await?;
        *counts
            .entry((update.feed.name(), price.symbol.as_str()))
            .or_insert(0usize) += 1;
    }
    tx.commit().await?;
    let assets = counts
        .iter()
        .map(|((feed, symbol), count)| format!("{feed}:{symbol}={count}"))
        .collect::<Vec<_>>()
        .join(", ");
    println!(
        "Wrote {} prices across {} assets in {} ms: {}",
        batch.len(),
        counts.len(),
        started.elapsed().as_millis(),
        assets
    );
    Ok(())
}

async fn writer(pool: SqlitePool, mut rx: mpsc::Receiver<ReceivedPrice>) -> anyhow::Result<()> {
    let period = Duration::from_secs(1);
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut batch = Vec::new();
    loop {
        tokio::select! {
            // Prioritize the timer so a busy stream cannot starve commits.
            biased;
            _ = interval.tick() => {
                store_batch(&pool, &batch).await?;
                batch.clear();
            }
            update = rx.recv() => match update {
                Some(update) => batch.push(update),
                None => {
                    store_batch(&pool, &batch).await?;
                    return Ok(());
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://prices.db".into());
    let options = SqliteConnectOptions::from_str(&database_url)?
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .context("opening SQLite database")?;
    initialize_schema(&pool)
        .await
        .context("initializing schema")?;

    let client = Client::default();
    // None omits filters for both feeds. Keep source-native symbols unchanged.
    let binance = client
        .subscribe_crypto_prices(None)?
        .map(|update| update.map(|price| ReceivedPrice::new(Feed::Binance, price)));
    let chainlink = client.subscribe_chainlink_prices(None)?.map(|update| {
        update.map(|price| {
            ReceivedPrice::new(
                Feed::Chainlink,
                CryptoPrice::builder()
                    .symbol(price.symbol)
                    .timestamp(price.timestamp)
                    .value(price.value)
                    .build(),
            )
        })
    });
    let stream = futures::stream::select(binance, chainlink);
    tokio::pin!(stream);
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    // Bounded queue provides backpressure instead of unbounded memory growth.
    let (tx, rx) = mpsc::channel(16_384);
    let writer_pool = pool.clone();
    // A dedicated blocking thread runs the async SQLx writer on its own runtime.
    let mut writer_handle = tokio::task::spawn_blocking(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(writer(writer_pool, rx))
    });
    eprintln!(
        "Streaming all RTDS Binance and Chainlink prices into {database_url}; committing every second. Ctrl+C to stop."
    );

    let mut writer_joined = false;
    let result = async {
        loop {
            let update = tokio::select! {
                result = &mut writer_handle => {
                    writer_joined = true;
                    result.context("writer thread panicked")??;
                    anyhow::bail!("writer stopped unexpectedly");
                }
                signal = &mut shutdown => {
                    signal.context("listening for Ctrl+C")?;
                    return Ok(());
                }
                update = stream.next() => update,
            };
            match update {
                Some(Ok(update)) => {
                    tx.send(update).await.context("writer queue closed")?;
                }
                Some(Err(error)) => eprintln!("RTDS stream error: {error}"),
                None => anyhow::bail!("RTDS price stream ended unexpectedly"),
            }
        }
    }
    .await;
    drop(tx);
    // Drain queued updates and flush the final partial batch before exiting.
    let writer_result = if writer_joined {
        Ok(())
    } else {
        writer_handle.await.context("writer thread panicked")?
    };
    pool.close().await;
    result?;
    writer_result.context("persisting price batch")
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn database() -> anyhow::Result<SqlitePool> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        initialize_schema(&pool).await?;
        Ok(pool)
    }

    fn updates() -> anyhow::Result<Vec<ReceivedPrice>> {
        [
            ("btcusdt", 1000, "12345.123456789"),
            ("btcusdt", 1001, "12346.123456789"),
            ("ethusdt", 1000, "2345.987654321"),
        ]
        .into_iter()
        .map(|(symbol, timestamp, value)| {
            Ok(ReceivedPrice {
                feed: Feed::Binance,
                price: CryptoPrice::builder()
                    .symbol(symbol.to_owned())
                    .timestamp(timestamp)
                    .value(value.parse()?)
                    .build(),
                received_ms: 1_700_000_000_123,
            })
        })
        .collect()
    }

    #[tokio::test]
    async fn stores_exact_history_and_receipt_times() -> anyhow::Result<()> {
        let pool = database().await?;
        store_batch(&pool, &updates()?).await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM assets")
                .fetch_one(&pool)
                .await?,
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM binance_prices")
                .fetch_one(&pool)
                .await?,
            3
        );
        let row: (String, i64) =
            sqlx::query_as("SELECT price, received_ms FROM binance_prices WHERE id = 1")
                .fetch_one(&pool)
                .await?;
        assert_eq!(row, ("12345.123456789".into(), 1_700_000_000_123));
        Ok(())
    }

    #[tokio::test]
    async fn stores_both_feeds_without_mixing_history() -> anyhow::Result<()> {
        let pool = database().await?;
        let mut batch = updates()?;
        let mut chainlink = ReceivedPrice::new(
            Feed::Chainlink,
            CryptoPrice::builder()
                .symbol("btc/usd".to_owned())
                .timestamp(2000)
                .value("98765.123456789".parse()?)
                .build(),
        );
        chainlink.received_ms = 1_700_000_000_456;
        batch.push(chainlink);
        store_batch(&pool, &batch).await?;
        let row: (String, i64, String, i64) = sqlx::query_as(
            "SELECT a.symbol, p.timestamp_ms, p.price, p.received_ms FROM chainlink_prices p JOIN assets a ON a.id = p.asset_id"
        ).fetch_one(&pool).await?;
        assert_eq!(
            row,
            (
                "btc/usd".into(),
                2000,
                "98765.123456789".into(),
                1_700_000_000_456
            )
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM binance_prices")
                .fetch_one(&pool)
                .await?,
            3
        );
        Ok(())
    }

    #[tokio::test]
    async fn writer_flushes_on_queue_close() -> anyhow::Result<()> {
        let pool = database().await?;
        let (tx, rx) = mpsc::channel(10);
        for update in updates()? {
            tx.send(update).await?;
        }
        drop(tx);
        writer(pool.clone(), rx).await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM binance_prices")
                .fetch_one(&pool)
                .await?,
            3
        );
        Ok(())
    }
}
