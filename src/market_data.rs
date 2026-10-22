//! Order-book, public execution, and final-resolution persistence.

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_client_sdk_v2::{
        clob::{types::Side, ws::types::response::OrderBookLevel},
        types::B256,
    };

    fn token(frame: Timeframe) -> Arc<Token> {
        Arc::new(Token {
            asset: "btc".into(),
            timeframe: frame,
            market_id: frame.name().into(),
            condition_id: B256::ZERO.to_string(),
            slug: format!("btc-updown-{}-1800000000", frame.name()),
            question: "test".into(),
            start_ms: 1800000000000,
            end_ms: 1800000300000,
            token_id: U256::from(1),
            outcome: "up".into(),
        })
    }

    #[tokio::test]
    async fn stores_depth_changes_trades_and_post_expiry_results() -> anyhow::Result<()> {
        let pool = SqlitePool::connect("sqlite::memory:").await?;
        crate::markets::initialize_schema(&pool).await?;
        for frame in [Timeframe::M5, Timeframe::M15, Timeframe::H1, Timeframe::H4] {
            let t = token(frame);
            let timestamp = t.start_ms + 1;
            let book = BookUpdate::builder()
                .asset_id(t.token_id)
                .market(B256::ZERO)
                .timestamp(timestamp)
                .bids(vec![
                    OrderBookLevel::builder()
                        .price("0.400000001".parse()?)
                        .size("12.345".parse()?)
                        .build(),
                ])
                .asks(Vec::new())
                .build();
            let trade = LastTradePrice::builder()
                .asset_id(t.token_id)
                .market(B256::ZERO)
                .timestamp(timestamp)
                .price("0.400000001".parse()?)
                .size("2.5".parse()?)
                .side(Side::Buy)
                .build();
            let entry = PriceChangeBatchEntry::builder()
                .asset_id(t.token_id)
                .price("0.4".parse()?)
                .size("0".parse()?)
                .side(Side::Buy)
                .build();
            let mut batch = vec![
                Write::MarketData(Record {
                    token: t.clone(),
                    received_ms: timestamp,
                    data: Data::Book(book),
                }),
                Write::MarketData(Record {
                    token: t.clone(),
                    received_ms: timestamp,
                    data: Data::Trade(trade),
                }),
                Write::MarketData(Record {
                    token: t.clone(),
                    received_ms: timestamp,
                    data: Data::Change {
                        timestamp_ms: timestamp,
                        entry,
                    },
                }),
            ];
            crate::markets::store_batch(&pool, &batch).await?;
            batch.clear();
            for _ in 0..2 {
                batch.push(Write::MarketData(Record {
                    token: t.clone(),
                    received_ms: t.end_ms + 100,
                    data: Data::Resolution {
                        winning_token: t.token_id,
                        outcome: "up".into(),
                        timestamp_ms: t.end_ms + 100,
                        source: "test".into(),
                    },
                }));
            }
            crate::markets::store_batch(&pool, &batch).await?;
            let prefix = format!("btc_{}", frame.name());
            let level: (String, String) =
                sqlx::query_as(&format!("SELECT price,size FROM {prefix}_book_levels"))
                    .fetch_one(&pool)
                    .await?;
            assert_eq!(level, ("0.400000001".into(), "12.345".into()));
            assert_eq!(
                sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM {prefix}_trades"))
                    .fetch_one(&pool)
                    .await?,
                1
            );
            assert_eq!(
                sqlx::query_scalar::<_, String>(&format!("SELECT size FROM {prefix}_book_changes"))
                    .fetch_one(&pool)
                    .await?,
                "0"
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>(&format!(
                    "SELECT COUNT(*) FROM crypto_{}_resolutions",
                    frame.name()
                ))
                .fetch_one(&pool)
                .await?,
                1
            );
        }
        assert!(pending(&pool).await?.is_empty());
        assert!(
            sqlx::query("PRAGMA foreign_key_check")
                .fetch_all(&pool)
                .await?
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn bulk_inserts_cross_chunk_boundaries_without_losing_repeats() -> anyhow::Result<()> {
        let pool = SqlitePool::connect("sqlite::memory:").await?;
        crate::markets::initialize_schema(&pool).await?;
        let t = token(Timeframe::M5);
        let mut batch = Vec::new();
        for _ in 0..225 {
            let trade = LastTradePrice::builder()
                .asset_id(t.token_id)
                .market(polymarket_client_sdk_v2::types::B256::ZERO)
                .timestamp(t.start_ms + 1)
                .price("0.123456789".parse()?)
                .size("3.5".parse()?)
                .build();
            batch.push(Write::MarketData(Record {
                token: t.clone(),
                received_ms: t.start_ms + 2,
                data: Data::Trade(trade),
            }));
        }
        crate::markets::store_batch(&pool, &batch).await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM btc_5m_trades WHERE price='0.123456789'"
            )
            .fetch_one(&pool)
            .await?,
            225
        );
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_flushes_market_records_and_restores_pending() -> anyhow::Result<()> {
        let pool = SqlitePool::connect("sqlite::memory:").await?;
        crate::markets::initialize_schema(&pool).await?;
        let t = token(Timeframe::M5);
        let trade = LastTradePrice::builder()
            .asset_id(t.token_id)
            .market(polymarket_client_sdk_v2::types::B256::ZERO)
            .timestamp(t.start_ms + 1)
            .price("0.5".parse()?)
            .build();
        let (tx, rx) = mpsc::channel(4);
        tx.send(Write::MarketData(Record {
            token: t.clone(),
            received_ms: t.start_ms + 1,
            data: Data::Trade(trade),
        }))
        .await?;
        drop(tx);
        crate::writer(pool.clone(), rx).await?;
        assert_eq!(pending(&pool).await?.len(), 1);
        let resolution = Record {
            token: t.clone(),
            received_ms: t.end_ms + 1,
            data: Data::Resolution {
                winning_token: t.token_id,
                outcome: "up".into(),
                timestamp_ms: t.end_ms + 1,
                source: "test".into(),
            },
        };
        crate::markets::store_batch(&pool, &[Write::MarketData(resolution)]).await?;
        assert!(pending(&pool).await?.is_empty());
        Ok(())
    }

    #[test]
    fn resolutions_bypass_quote_window_but_not_preexpiry() {
        let t = token(Timeframe::M5);
        let mut record = Record {
            token: t.clone(),
            received_ms: t.end_ms - 1,
            data: Data::Resolution {
                winning_token: t.token_id,
                outcome: "up".into(),
                timestamp_ms: t.end_ms + 1,
                source: "test".into(),
            },
        };
        assert!(!record.eligible());
        record.received_ms = t.end_ms + 1;
        assert!(record.eligible());
        record.data = Data::Trade(
            LastTradePrice::builder()
                .asset_id(t.token_id)
                .market(B256::ZERO)
                .timestamp(t.end_ms)
                .price("0.5".parse().unwrap())
                .build(),
        );
        assert!(!record.eligible());
    }
}
use crate::{
    Write,
    markets::{Timeframe, Token, in_active_window},
    now_ms,
};
use anyhow::Context;
use polymarket_client_sdk_v2::{
    clob::ws::types::response::{BookUpdate, LastTradePrice, PriceChangeBatchEntry},
    gamma::{Client, types::request::MarketByIdRequest},
    types::U256,
};
use sqlx::{Sqlite, SqlitePool, Transaction};
use std::sync::Arc;
use tokio::sync::mpsc;

pub enum Data {
    Book(BookUpdate),
    Change {
        timestamp_ms: i64,
        entry: PriceChangeBatchEntry,
    },
    Trade(LastTradePrice),
    Resolution {
        winning_token: U256,
        outcome: String,
        timestamp_ms: i64,
        source: String,
    },
}
pub struct Record {
    pub token: Arc<Token>,
    pub received_ms: i64,
    pub data: Data,
}
impl Record {
    pub fn eligible(&self) -> bool {
        let timestamp = match &self.data {
            Data::Book(x) => x.timestamp,
            Data::Change { timestamp_ms, .. } => *timestamp_ms,
            Data::Trade(x) => x.timestamp,
            Data::Resolution { .. } => return self.received_ms >= self.token.end_ms,
        };
        in_active_window(&self.token, timestamp, self.received_ms)
    }
}

pub async fn initialize_schema(pool: &SqlitePool) -> anyhow::Result<()> {
    for frame in ["5m", "15m", "1h", "4h"] {
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS crypto_{frame}_data_tables (
            asset TEXT NOT NULL, kind TEXT NOT NULL, table_name TEXT NOT NULL UNIQUE,
            PRIMARY KEY(asset,kind)
        )"
        ))
        .execute(pool)
        .await?;
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS crypto_{frame}_resolutions (
            market_id TEXT PRIMARY KEY REFERENCES crypto_{frame}_markets(market_id),
            winning_token_id TEXT NOT NULL REFERENCES crypto_{frame}_tokens(token_id),
            winning_outcome TEXT NOT NULL CHECK(winning_outcome IN ('up','down')),
            timestamp_ms INTEGER NOT NULL, received_ms INTEGER NOT NULL, source TEXT NOT NULL
        )"
        ))
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub async fn store(
    tx: &mut Transaction<'_, Sqlite>,
    record: &Record,
    initialize: bool,
) -> anyhow::Result<()> {
    let token = &record.token;
    let frame = token.timeframe.name();
    let prefix = format!("{}_{frame}", token.asset);
    let id = token.token_id.to_string();
    if initialize {
        let kinds: &[&str] = match record.data {
            Data::Book(_) => &["books", "book_levels"],
            Data::Change { .. } => &["book_changes"],
            Data::Trade(_) => &["trades"],
            Data::Resolution { .. } => &[],
        };
        for kind in kinds {
            sqlx::query(&format!("INSERT INTO crypto_{frame}_data_tables(asset,kind,table_name) VALUES (?,?,?) ON CONFLICT(asset,kind) DO NOTHING"))
                .bind(&token.asset).bind(kind).bind(format!("{prefix}_{kind}")).execute(&mut **tx).await?;
        }
    }
    match &record.data {
        Data::Resolution {
            winning_token,
            outcome,
            timestamp_ms,
            source,
        } => {
            anyhow::ensure!(
                *winning_token == token.token_id && outcome.eq_ignore_ascii_case(&token.outcome),
                "resolution winner does not match Gamma token mapping"
            );
            sqlx::query(&format!("INSERT INTO crypto_{frame}_resolutions(market_id,winning_token_id,winning_outcome,timestamp_ms,received_ms,source) VALUES (?,?,?,?,?,?) ON CONFLICT(market_id) DO NOTHING"))
                .bind(&token.market_id).bind(id).bind(outcome.to_ascii_lowercase()).bind(timestamp_ms).bind(record.received_ms).bind(source).execute(&mut **tx).await?;
        }
        Data::Book(book) => {
            if initialize {
                sqlx::raw_sql(&format!("CREATE TABLE IF NOT EXISTS {prefix}_books (
                id INTEGER PRIMARY KEY, token_id TEXT NOT NULL REFERENCES crypto_{frame}_tokens(token_id),
                timestamp_ms INTEGER NOT NULL, received_ms INTEGER NOT NULL, hash TEXT
            );
            CREATE TABLE IF NOT EXISTS {prefix}_book_levels (
                book_id INTEGER NOT NULL REFERENCES {prefix}_books(id), side TEXT NOT NULL CHECK(side IN ('bid','ask')),
                level INTEGER NOT NULL, price TEXT NOT NULL, size TEXT NOT NULL, PRIMARY KEY(book_id,side,level)
            );
            CREATE INDEX IF NOT EXISTS {prefix}_books_token_time ON {prefix}_books(token_id,timestamp_ms);")).execute(&mut **tx).await?;
            }
            let book_id = sqlx::query(&format!("INSERT INTO {prefix}_books(token_id,timestamp_ms,received_ms,hash) VALUES (?,?,?,?)"))
                .bind(id).bind(book.timestamp).bind(record.received_ms).bind(&book.hash).execute(&mut **tx).await?.last_insert_rowid();
            for (side, levels) in [("bid", &book.bids), ("ask", &book.asks)] {
                for (chunk_index, chunk) in levels.chunks(150).enumerate() {
                    let mut query = sqlx::QueryBuilder::<Sqlite>::new(format!(
                        "INSERT INTO {prefix}_book_levels(book_id,side,level,price,size) "
                    ));
                    query.push_values(chunk.iter().enumerate(), |mut row, (index, value)| {
                        row.push_bind(book_id)
                            .push_bind(side)
                            .push_bind((chunk_index * 150 + index) as i64)
                            .push_bind(value.price.to_string())
                            .push_bind(value.size.to_string());
                    });
                    query.build().execute(&mut **tx).await?;
                }
            }
        }
        Data::Change {
            timestamp_ms,
            entry,
        } => {
            if initialize {
                sqlx::raw_sql(&format!("CREATE TABLE IF NOT EXISTS {prefix}_book_changes (
                id INTEGER PRIMARY KEY, token_id TEXT NOT NULL REFERENCES crypto_{frame}_tokens(token_id),
                timestamp_ms INTEGER NOT NULL, received_ms INTEGER NOT NULL,
                side TEXT NOT NULL, price TEXT NOT NULL, size TEXT, hash TEXT, best_bid TEXT, best_ask TEXT
            ); CREATE INDEX IF NOT EXISTS {prefix}_changes_token_time ON {prefix}_book_changes(token_id,timestamp_ms);")).execute(&mut **tx).await?;
            }
            sqlx::query(&format!("INSERT INTO {prefix}_book_changes(token_id,timestamp_ms,received_ms,side,price,size,hash,best_bid,best_ask) VALUES (?,?,?,?,?,?,?,?,?)"))
                .bind(id).bind(timestamp_ms).bind(record.received_ms).bind(entry.side.to_string()).bind(entry.price.to_string())
                .bind(entry.size.map(|x| x.to_string())).bind(&entry.hash).bind(entry.best_bid.map(|x| x.to_string())).bind(entry.best_ask.map(|x| x.to_string())).execute(&mut **tx).await?;
        }
        Data::Trade(trade) => {
            if initialize {
                sqlx::raw_sql(&format!("CREATE TABLE IF NOT EXISTS {prefix}_trades (
                id INTEGER PRIMARY KEY, token_id TEXT NOT NULL REFERENCES crypto_{frame}_tokens(token_id),
                timestamp_ms INTEGER NOT NULL, received_ms INTEGER NOT NULL,
                price TEXT NOT NULL, size TEXT, side TEXT, fee_rate_bps TEXT
            ); CREATE INDEX IF NOT EXISTS {prefix}_trades_token_time ON {prefix}_trades(token_id,timestamp_ms);")).execute(&mut **tx).await?;
            }
            sqlx::query(&format!("INSERT INTO {prefix}_trades(token_id,timestamp_ms,received_ms,price,size,side,fee_rate_bps) VALUES (?,?,?,?,?,?,?)"))
                .bind(id).bind(trade.timestamp).bind(record.received_ms).bind(trade.price.to_string()).bind(trade.size.map(|x| x.to_string()))
                .bind(trade.side.map(|x| x.to_string())).bind(trade.fee_rate_bps.map(|x| x.to_string())).execute(&mut **tx).await?;
        }
    }
    Ok(())
}

/// Rows are already validated/grouped by asset, duration and event kind by the writer.
pub async fn bulk_store(
    tx: &mut Transaction<'_, Sqlite>,
    records: &[&Record],
) -> anyhow::Result<()> {
    let Some(first) = records.first() else {
        return Ok(());
    };
    let prefix = format!("{}_{}", first.token.asset, first.token.timeframe.name());
    for chunk in records.chunks(100) {
        let suffix = match first.data {
            Data::Change { .. } => {
                "book_changes(token_id,timestamp_ms,received_ms,side,price,size,hash,best_bid,best_ask)"
            }
            Data::Trade(_) => {
                "trades(token_id,timestamp_ms,received_ms,price,size,side,fee_rate_bps)"
            }
            _ => anyhow::bail!("unsupported bulk event"),
        };
        let mut query =
            sqlx::QueryBuilder::<Sqlite>::new(format!("INSERT INTO {prefix}_{suffix} "));
        query.push_values(chunk.iter(), |mut row, record| {
            row.push_bind(record.token.token_id.to_string());
            match &record.data {
                Data::Change {
                    timestamp_ms,
                    entry,
                } => {
                    row.push_bind(*timestamp_ms)
                        .push_bind(record.received_ms)
                        .push_bind(entry.side.to_string())
                        .push_bind(entry.price.to_string())
                        .push_bind(entry.size.map(|x| x.to_string()))
                        .push_bind(entry.hash.as_deref())
                        .push_bind(entry.best_bid.map(|x| x.to_string()))
                        .push_bind(entry.best_ask.map(|x| x.to_string()));
                }
                Data::Trade(trade) => {
                    row.push_bind(trade.timestamp)
                        .push_bind(record.received_ms)
                        .push_bind(trade.price.to_string())
                        .push_bind(trade.size.map(|x| x.to_string()))
                        .push_bind(trade.side.map(|x| x.to_string()))
                        .push_bind(trade.fee_rate_bps.map(|x| x.to_string()));
                }
                _ => unreachable!("caller groups bulk events"),
            }
        });
        query.build().execute(&mut **tx).await?;
    }
    Ok(())
}

pub async fn pending(pool: &SqlitePool) -> anyhow::Result<Vec<Arc<Token>>> {
    let mut tokens = Vec::new();
    for timeframe in [Timeframe::M5, Timeframe::M15, Timeframe::H1, Timeframe::H4] {
        let frame = timeframe.name();
        type Row = (
            String,
            String,
            String,
            String,
            String,
            i64,
            i64,
            String,
            String,
        );
        let rows: Vec<Row> = sqlx::query_as(&format!("SELECT m.asset,m.market_id,m.condition_id,m.slug,m.question,m.start_ms,m.end_ms,t.token_id,t.outcome
            FROM crypto_{frame}_markets m JOIN crypto_{frame}_tokens t USING(market_id)
            LEFT JOIN crypto_{frame}_resolutions r USING(market_id) WHERE r.market_id IS NULL"))
            .fetch_all(pool).await?;
        for (asset, market_id, condition_id, slug, question, start_ms, end_ms, id, outcome) in rows
        {
            tokens.push(Arc::new(Token {
                asset,
                timeframe,
                market_id,
                condition_id,
                slug,
                question,
                start_ms,
                end_ms,
                token_id: id.parse()?,
                outcome,
            }));
        }
    }
    Ok(tokens)
}

/// Finalized Gamma results only: never infer a winner merely from a high price.
pub async fn reconcile(pool: SqlitePool, tx: mpsc::Sender<Write>) -> anyhow::Result<()> {
    let gamma = Client::default();
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let pending = pending(&pool).await?;
        let mut seen = std::collections::BTreeSet::new();
        for token in pending {
            if token.end_ms > now_ms() || !seen.insert(token.market_id.clone()) {
                continue;
            }
            let request = MarketByIdRequest::builder()
                .id(token.market_id.clone())
                .build();
            let market = match gamma.market_by_id(&request).await {
                Ok(market) => market,
                Err(error) => {
                    crate::console::warn(
                        "RESOLVE",
                        format!("market={} lookup failed: {error}", token.market_id),
                    );
                    continue;
                }
            };
            if market.closed != Some(true)
                || market.uma_resolution_status.as_deref() != Some("resolved")
            {
                continue;
            }
            let (Some(outcomes), Some(ids), Some(prices)) = (
                market.outcomes,
                market.clob_token_ids,
                market.outcome_prices,
            ) else {
                continue;
            };
            if outcomes.len() != 2 || ids.len() != 2 || prices.len() != 2 {
                continue;
            }
            let Some(index) = prices
                .iter()
                .position(|p| *p == polymarket_client_sdk_v2::types::Decimal::ONE)
            else {
                continue;
            };
            if prices[1 - index] != polymarket_client_sdk_v2::types::Decimal::ZERO {
                continue;
            }
            let mut winner = (*token).clone();
            winner.token_id = ids[index];
            winner.outcome = outcomes[index].to_ascii_lowercase();
            if !["up", "down"].contains(&winner.outcome.as_str()) {
                continue;
            }
            let timestamp = now_ms();
            tx.send(Write::MarketData(Record {
                token: Arc::new(winner),
                received_ms: timestamp,
                data: Data::Resolution {
                    winning_token: ids[index],
                    outcome: outcomes[index].to_ascii_lowercase(),
                    timestamp_ms: timestamp,
                    source: "gamma_finalized".into(),
                },
            }))
            .await
            .context("writer queue closed")?;
        }
    }
}
