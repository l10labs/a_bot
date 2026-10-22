//! Gamma discovery and public CLOB best-bid/ask collection for rolling 5m markets.
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::Context;
use futures::{StreamExt, stream::BoxStream};
use polymarket_client_sdk_v2::{
    clob::ws::{Client as WsClient, types::response::BestBidAsk},
    gamma::{
        Client as GammaClient,
        types::{request::MarketsRequest, response::Market},
    },
    types::U256,
};
use sqlx::SqlitePool;
use tokio::sync::{mpsc, watch};

use crate::{Write, now_ms};

#[derive(Clone, Debug)]
pub struct Token {
    pub asset: String,
    pub market_id: String,
    pub condition_id: String,
    pub slug: String,
    pub question: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub token_id: U256,
    pub outcome: String,
}

pub struct Quote {
    pub token: Arc<Token>,
    pub bid: String,
    pub ask: String,
    pub timestamp_ms: i64,
    pub received_ms: i64,
}

fn slug_parts(slug: &str) -> Option<(&str, i64)> {
    let (asset, start) = slug.split_once("-updown-5m-")?;
    // Table identifiers come only from this strictly validated symbol.
    if asset.is_empty()
        || !asset
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    {
        return None;
    }
    let start: i64 = start.parse().ok()?;
    (start > 0 && start % 300 == 0).then_some((asset, start))
}

fn tokens(market: Market, now: i64) -> Option<Vec<Arc<Token>>> {
    let slug = market.slug.as_deref()?;
    let (asset, start) = slug_parts(slug)?;
    let end_ms = (start.checked_add(300)?).checked_mul(1000)?;
    if end_ms <= now
        || market.closed == Some(true)
        || market.active == Some(false)
        || market.enable_order_book == Some(false)
    {
        return None;
    }
    let outcomes = market.outcomes.as_ref()?;
    let ids = market.clob_token_ids.as_ref()?;
    if outcomes.len() != 2
        || ids.len() != 2
        || ids[0] == ids[1]
        || !outcomes.iter().any(|s| s.eq_ignore_ascii_case("up"))
        || !outcomes.iter().any(|s| s.eq_ignore_ascii_case("down"))
    {
        return None;
    }
    let condition = market.condition_id?;
    Some(
        outcomes
            .iter()
            .zip(ids)
            .map(|(outcome, id)| {
                Arc::new(Token {
                    asset: asset.to_owned(),
                    market_id: market.id.clone(),
                    condition_id: condition.to_string(),
                    slug: slug.to_owned(),
                    question: market.question.clone().unwrap_or_default(),
                    start_ms: start * 1000,
                    end_ms,
                    token_id: *id,
                    outcome: outcome.to_ascii_lowercase(),
                })
            })
            .collect(),
    )
}

async fn discover(client: &GammaClient) -> anyhow::Result<BTreeMap<U256, Arc<Token>>> {
    let now = chrono::Utc::now();
    let mut result = BTreeMap::new();
    let mut offset = 0;
    loop {
        let request = MarketsRequest::builder()
            .closed(false)
            .end_date_min(now)
            .end_date_max(now + chrono::Duration::minutes(15))
            .order("id".to_owned())
            .ascending(true)
            .limit(500)
            .offset(offset)
            .build();
        let page = client
            .markets(&request)
            .await
            .context("querying Gamma markets")?;
        let len = page.len();
        for market in page {
            if let Some(tokens) = tokens(market, now.timestamp_millis()) {
                for token in tokens {
                    result.insert(token.token_id, token);
                }
            }
        }
        if len < 500 {
            break;
        }
        offset += 500;
    }
    Ok(result)
}

pub async fn run(
    tx: mpsc::Sender<Write>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let (discovery_tx, mut discovery_rx) = watch::channel(BTreeMap::new());
    // Discovery must not block consuming quotes while HTTP requests are in flight.
    let discovery = tokio::spawn(async move {
        let gamma = GammaClient::default();
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            match discover(&gamma).await {
                Ok(tokens) => {
                    if discovery_tx.send(tokens).is_err() {
                        break;
                    }
                }
                Err(error) => eprintln!("Gamma discovery failed (retrying): {error:#}"),
            }
        }
    });
    let result = async {
        let ws = WsClient::default();
        let mut known: BTreeMap<U256, Arc<Token>> = BTreeMap::new();
        let mut quotes: BoxStream<'static, polymarket_client_sdk_v2::Result<BestBidAsk>> = Box::pin(futures::stream::pending());
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => return Ok(()),
                changed = discovery_rx.changed() => {
                    changed.context("Gamma discovery task stopped")?;
                    let next = discovery_rx.borrow_and_update().clone();
                    if known.keys().eq(next.keys()) { continue; }
                    let ids = next.keys().copied().collect::<Vec<_>>();
                    let new_stream: BoxStream<'static, _> = if ids.is_empty() {
                        Box::pin(futures::stream::pending())
                    } else {
                        Box::pin(ws.subscribe_best_bid_ask(ids)?)
                    };
                    if !known.is_empty() {
                        ws.unsubscribe_orderbook(&known.keys().copied().collect::<Vec<_>>())?;
                    }
                    for token in next.values() {
                        tx.send(Write::Market(token.clone())).await.context("writer queue closed")?;
                    }
                    eprintln!("Gamma: tracking {} five-minute Up/Down markets ({} tokens)", next.len()/2, next.len());
                    known = next;
                    quotes = new_stream;
                }
                update = quotes.next() => match update {
                    Some(Ok(quote)) => {
                        if let Some(token) = known.get(&quote.asset_id) {
                            if token.end_ms <= now_ms() { continue; }
                            tx.send(Write::Quote(Quote {
                                token: token.clone(), bid: quote.best_bid.to_string(), ask: quote.best_ask.to_string(),
                                timestamp_ms: quote.timestamp, received_ms: now_ms(),
                            })).await.context("writer queue closed")?;
                        }
                    }
                    Some(Err(error)) => eprintln!("CLOB quote stream error: {error}"),
                    None => anyhow::bail!("CLOB quote stream ended unexpectedly"),
                }
            }
        }
    }.await;
    discovery.abort();
    let _ = discovery.await;
    result
}

pub async fn initialize_schema(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::raw_sql("CREATE TABLE IF NOT EXISTS crypto_5m_markets (
        market_id TEXT PRIMARY KEY, condition_id TEXT NOT NULL, slug TEXT NOT NULL,
        asset TEXT NOT NULL, question TEXT NOT NULL, start_ms INTEGER NOT NULL, end_ms INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS crypto_5m_tokens (
        token_id TEXT PRIMARY KEY, market_id TEXT NOT NULL REFERENCES crypto_5m_markets(market_id),
        outcome TEXT NOT NULL CHECK(outcome IN ('up','down'))
    );
    CREATE TABLE IF NOT EXISTS crypto_5m_quote_tables (
        asset TEXT PRIMARY KEY, table_name TEXT NOT NULL UNIQUE
    );").execute(pool).await?;
    Ok(())
}

pub async fn store_batch(pool: &SqlitePool, batch: &[Write]) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    let mut counts = BTreeMap::new();
    for write in batch {
        let (token, quote) = match write {
            Write::Market(token) => (token, None),
            Write::Quote(quote) => (&quote.token, Some(quote)),
            Write::Price(_) => continue,
        };
        // Validate again at the SQL boundary; identifiers cannot be bound as parameters.
        anyhow::ensure!(
            slug_parts(&token.slug).map(|(a, _)| a) == Some(token.asset.as_str()),
            "invalid asset identifier"
        );
        let table = format!("crypto_5m_{}_quotes", token.asset);
        sqlx::query("INSERT INTO crypto_5m_markets (market_id, condition_id, slug, asset, question, start_ms, end_ms) VALUES (?,?,?,?,?,?,?) ON CONFLICT(market_id) DO NOTHING")
            .bind(&token.market_id).bind(&token.condition_id).bind(&token.slug).bind(&token.asset)
            .bind(&token.question).bind(token.start_ms).bind(token.end_ms).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO crypto_5m_tokens (token_id,market_id,outcome) VALUES (?,?,?) ON CONFLICT(token_id) DO NOTHING")
            .bind(token.token_id.to_string()).bind(&token.market_id).bind(&token.outcome).execute(&mut *tx).await?;
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
            id INTEGER PRIMARY KEY, token_id TEXT NOT NULL REFERENCES crypto_5m_tokens(token_id),
            timestamp_ms INTEGER NOT NULL, received_ms INTEGER NOT NULL,
            best_bid TEXT NOT NULL, best_ask TEXT NOT NULL
        )"
        ))
        .execute(&mut *tx)
        .await?;
        sqlx::query(&format!(
            "CREATE INDEX IF NOT EXISTS {table}_token_timestamp ON {table}(token_id,timestamp_ms)"
        ))
        .execute(&mut *tx)
        .await?;
        sqlx::query("INSERT INTO crypto_5m_quote_tables(asset,table_name) VALUES (?,?) ON CONFLICT(asset) DO NOTHING")
            .bind(&token.asset).bind(&table).execute(&mut *tx).await?;
        if let Some(quote) = quote {
            sqlx::query(&format!("INSERT INTO {table}(token_id,timestamp_ms,received_ms,best_bid,best_ask) VALUES (?,?,?,?,?)"))
                .bind(token.token_id.to_string()).bind(quote.timestamp_ms).bind(quote.received_ms)
                .bind(&quote.bid).bind(&quote.ask).execute(&mut *tx).await?;
            *counts.entry(token.asset.as_str()).or_insert(0usize) += 1;
        }
    }
    tx.commit().await?;
    if !counts.is_empty() {
        println!(
            "Wrote CLOB 5m best bid/ask: {}",
            counts
                .iter()
                .map(|(asset, n)| format!("{asset}={n}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_five_minute_slugs() {
        assert_eq!(
            slug_parts("btc-updown-5m-1800000000"),
            Some(("btc", 1800000000))
        );
        assert!(slug_parts("btc-updown-15m-1800000000").is_none());
        assert!(slug_parts("btc;drop-updown-5m-1800000000").is_none());
    }
    #[test]
    fn maps_gamma_outcomes_by_position_and_rejects_expired_markets() {
        let market = Market::builder()
            .id("test".to_owned())
            .slug("btc-updown-5m-1800000000".to_owned())
            .condition_id(polymarket_client_sdk_v2::types::B256::ZERO)
            .outcomes(vec!["Down".to_owned(), "Up".to_owned()])
            .clob_token_ids(vec![U256::from(7), U256::from(8)])
            .build();
        let found = tokens(market.clone(), 1_800_000_001_000).unwrap();
        assert_eq!(found[0].outcome, "down");
        assert_eq!(found[0].token_id, U256::from(7));
        assert_eq!(found[1].outcome, "up");
        assert!(tokens(market, 1_800_000_300_000).is_none());
    }

    #[tokio::test]
    async fn stores_separate_asset_tables_and_outcomes() -> anyhow::Result<()> {
        let pool = SqlitePool::connect("sqlite::memory:").await?;
        initialize_schema(&pool).await?;
        let mut batch = Vec::new();
        for (asset, id, outcome) in [("btc", 1, "up"), ("btc", 2, "down"), ("eth", 3, "up")] {
            batch.push(Write::Quote(Quote {
                token: Arc::new(Token {
                    asset: asset.into(),
                    market_id: asset.into(),
                    condition_id: "condition".into(),
                    slug: format!("{asset}-updown-5m-1800000000"),
                    question: "test".into(),
                    start_ms: 1800000000000,
                    end_ms: 1800000300000,
                    token_id: U256::from(id),
                    outcome: outcome.into(),
                }),
                bid: "0.123456789".into(),
                ask: "0.55".into(),
                timestamp_ms: 1,
                received_ms: 2,
            }));
        }
        store_batch(&pool, &batch).await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM crypto_5m_btc_quotes")
                .fetch_one(&pool)
                .await?,
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM crypto_5m_eth_quotes")
                .fetch_one(&pool)
                .await?,
            1
        );
        let rows: Vec<(String,String)> = sqlx::query_as("SELECT t.outcome,q.best_bid FROM crypto_5m_btc_quotes q JOIN crypto_5m_tokens t USING(token_id) ORDER BY q.id").fetch_all(&pool).await?;
        assert_eq!(
            rows,
            vec![
                ("up".into(), "0.123456789".into()),
                ("down".into(), "0.123456789".into())
            ]
        );
        Ok(())
    }
}
