//! Discovery and batched persistence for five-minute Up/Down quotes and trades only.
use crate::{Write, market_data::Trade};
use anyhow::Context;
use polymarket_client_sdk_v2::{
    gamma::{
        Client,
        types::{request::MarketsRequest, response::Market},
    },
    types::U256,
};
use sqlx::{Sqlite, SqlitePool};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Debug)]
pub struct Token {
    pub asset: String,
    pub market_id: String,
    pub condition_id: String,
    pub slug: String,
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
pub(crate) fn in_active_window(token: &Token, timestamp_ms: i64, received_ms: i64) -> bool {
    (token.start_ms..token.end_ms).contains(&timestamp_ms)
        && (token.start_ms..token.end_ms).contains(&received_ms)
}
fn slug_parts(slug: &str) -> Option<(&str, i64)> {
    let (asset, start) = slug.split_once("-updown-5m-")?;
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
    let start_ms = start.checked_mul(1000)?;
    let end_ms = start_ms.checked_add(300_000)?;
    if end_ms <= now
        || start_ms > now + 900_000
        || market.closed == Some(true)
        || market.active == Some(false)
        || market.enable_order_book == Some(false)
        || market
            .end_date
            .is_some_and(|t| t.timestamp_millis() != end_ms)
        || market
            .event_start_time
            .is_some_and(|t| t.timestamp_millis() != start_ms)
    {
        return None;
    }
    let outcomes = market.outcomes.as_ref()?;
    let ids = market.clob_token_ids.as_ref()?;
    if outcomes.len() != 2
        || ids.len() != 2
        || ids[0] == ids[1]
        || !outcomes.iter().any(|o| o.eq_ignore_ascii_case("up"))
        || !outcomes.iter().any(|o| o.eq_ignore_ascii_case("down"))
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
                    asset: asset.into(),
                    market_id: market.id.clone(),
                    condition_id: condition.to_string(),
                    slug: slug.into(),
                    start_ms,
                    end_ms,
                    token_id: *id,
                    outcome: outcome.to_ascii_lowercase(),
                })
            })
            .collect(),
    )
}
pub(crate) async fn discover(client: &Client) -> anyhow::Result<BTreeMap<U256, Arc<Token>>> {
    let now = chrono::Utc::now();
    let mut result = BTreeMap::new();
    let mut offset = 0;
    loop {
        let request = MarketsRequest::builder()
            .closed(false)
            .end_date_min(now)
            .end_date_max(now + chrono::Duration::minutes(15))
            .order("id".into())
            .ascending(true)
            .limit(100)
            .offset(offset)
            .build();
        let page = client
            .markets(&request)
            .await
            .context("querying Gamma five-minute markets")?;
        let len = page.len();
        for market in page {
            if let Some(tokens) = tokens(market, now.timestamp_millis()) {
                for token in tokens {
                    result.insert(token.token_id, token);
                }
            }
        }
        if len < 100 {
            break;
        }
        offset += 100;
    }
    Ok(result)
}
pub async fn initialize_schema(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS markets (
        market_id TEXT PRIMARY KEY, condition_id TEXT NOT NULL, slug TEXT NOT NULL UNIQUE,
        asset TEXT NOT NULL, start_ms INTEGER NOT NULL, end_ms INTEGER NOT NULL,
        CHECK(end_ms-start_ms=300000)
    );
    CREATE TABLE IF NOT EXISTS tokens (
        token_id TEXT PRIMARY KEY, market_id TEXT NOT NULL REFERENCES markets(market_id),
        outcome TEXT NOT NULL CHECK(outcome IN ('up','down')), UNIQUE(market_id,outcome)
    );
    CREATE TABLE IF NOT EXISTS resolutions (
        market_id TEXT PRIMARY KEY REFERENCES markets(market_id),
        winning_token_id TEXT NOT NULL REFERENCES tokens(token_id),
        winning_outcome TEXT NOT NULL CHECK(winning_outcome IN ('up','down')),
        timestamp_ms INTEGER NOT NULL, received_ms INTEGER NOT NULL,
        source TEXT NOT NULL CHECK(source IN ('clob_ws','gamma_finalized'))
    );
    CREATE INDEX IF NOT EXISTS markets_asset_start ON markets(asset,start_ms);",
    )
    .execute(pool)
    .await?;
    Ok(())
}
pub async fn store_batch(pool: &SqlitePool, batch: &[Write]) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    let mut prepared_tokens = BTreeSet::new();
    let mut prepared_assets = BTreeSet::new();
    let mut quotes: BTreeMap<String, Vec<&Quote>> = BTreeMap::new();
    let mut trades: BTreeMap<String, Vec<&Trade>> = BTreeMap::new();
    for write in batch {
        let token = match write {
            Write::Quote(q) if in_active_window(&q.token, q.timestamp_ms, q.received_ms) => {
                &q.token
            }
            Write::Trade(t) if t.eligible() => &t.token,
            Write::Resolution(r) if r.eligible() => &r.token,
            Write::Market { token, received_ms }
                if (token.start_ms..token.end_ms).contains(received_ms) =>
            {
                token
            }
            _ => continue,
        };
        anyhow::ensure!(
            slug_parts(&token.slug).map(|(a, s)| (a, s * 1000))
                == Some((token.asset.as_str(), token.start_ms))
                && token.end_ms - token.start_ms == 300_000,
            "invalid five-minute market metadata"
        );
        if prepared_tokens.insert(token.token_id) {
            sqlx::query("INSERT INTO markets(market_id,condition_id,slug,asset,start_ms,end_ms) VALUES (?,?,?,?,?,?) ON CONFLICT(market_id) DO NOTHING")
                .bind(&token.market_id).bind(&token.condition_id).bind(&token.slug).bind(&token.asset).bind(token.start_ms).bind(token.end_ms).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO tokens(token_id,market_id,outcome) VALUES (?,?,?) ON CONFLICT(token_id) DO NOTHING")
                .bind(token.token_id.to_string()).bind(&token.market_id).bind(&token.outcome).execute(&mut *tx).await?;
        }
        if matches!(write, Write::Market { .. }) {
            continue;
        }
        if let Write::Resolution(result) = write {
            sqlx::query("INSERT INTO resolutions(market_id,winning_token_id,winning_outcome,timestamp_ms,received_ms,source) VALUES (?,?,?,?,?,?) ON CONFLICT(market_id) DO NOTHING")
                .bind(&token.market_id).bind(token.token_id.to_string()).bind(&token.outcome)
                .bind(result.timestamp_ms).bind(result.received_ms).bind(&result.source).execute(&mut *tx).await?;
            continue;
        }
        if prepared_assets.insert(token.asset.clone()) {
            let asset = &token.asset;
            sqlx::raw_sql(&format!("CREATE TABLE IF NOT EXISTS \"{asset}_5m_quotes\" (
                id INTEGER PRIMARY KEY,token_id TEXT NOT NULL REFERENCES tokens(token_id),timestamp_ms INTEGER NOT NULL,received_ms INTEGER NOT NULL,best_bid TEXT NOT NULL,best_ask TEXT NOT NULL
            ); CREATE INDEX IF NOT EXISTS \"{asset}_5m_quotes_token_time\" ON \"{asset}_5m_quotes\"(token_id,timestamp_ms);
            CREATE TABLE IF NOT EXISTS \"{asset}_5m_trades\" (
                id INTEGER PRIMARY KEY,token_id TEXT NOT NULL REFERENCES tokens(token_id),timestamp_ms INTEGER NOT NULL,received_ms INTEGER NOT NULL,
                price TEXT NOT NULL,size TEXT,side TEXT,fee_rate_bps TEXT
            ); CREATE INDEX IF NOT EXISTS \"{asset}_5m_trades_token_time\" ON \"{asset}_5m_trades\"(token_id,timestamp_ms);")).execute(&mut *tx).await?;
        }
        match write {
            Write::Quote(q) => quotes.entry(token.asset.clone()).or_default().push(q),
            Write::Trade(t) => trades.entry(token.asset.clone()).or_default().push(t),
            _ => unreachable!(),
        }
    }
    for (asset, rows) in quotes {
        for chunk in rows.chunks(150) {
            let mut query = sqlx::QueryBuilder::<Sqlite>::new(format!(
                "INSERT INTO \"{asset}_5m_quotes\"(token_id,timestamp_ms,received_ms,best_bid,best_ask) "
            ));
            query.push_values(chunk.iter(), |mut row, q| {
                row.push_bind(q.token.token_id.to_string())
                    .push_bind(q.timestamp_ms)
                    .push_bind(q.received_ms)
                    .push_bind(q.bid.as_str())
                    .push_bind(q.ask.as_str());
            });
            query.build().execute(&mut *tx).await?;
        }
    }
    for (asset, rows) in trades {
        crate::market_data::store_trades(&mut tx, &asset, &rows).await?;
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    pub(crate) fn token() -> Arc<Token> {
        Arc::new(Token {
            asset: "btc".into(),
            market_id: "test".into(),
            condition_id: polymarket_client_sdk_v2::types::B256::ZERO.to_string(),
            slug: "btc-updown-5m-1800000000".into(),
            start_ms: 1800000000000,
            end_ms: 1800000300000,
            token_id: U256::from(1),
            outcome: "up".into(),
        })
    }
    #[test]
    fn accepts_only_five_minute_markets() {
        assert!(slug_parts("btc-updown-5m-1800000000").is_some());
        for slug in [
            "btc-updown-15m-1800000000",
            "btc-updown-4h-1800000000",
            "bitcoin-up-or-down-october-22-2026-12am-et",
            "btc;drop-updown-5m-1800000000",
        ] {
            assert!(slug_parts(slug).is_none());
        }
        let market = Market::builder()
            .id("test".into())
            .slug("btc-updown-5m-1800000000".into())
            .condition_id(polymarket_client_sdk_v2::types::B256::ZERO)
            .outcomes(vec!["Down".into(), "Up".into()])
            .clob_token_ids(vec![U256::from(7), U256::from(8)])
            .build();
        let found = tokens(market.clone(), 1800000000001).unwrap();
        assert_eq!(found[0].outcome, "down");
        assert_eq!(found[0].token_id, U256::from(7));
        assert!(tokens(market, 1800000300000).is_none());
    }
    #[test]
    fn active_window_is_start_inclusive_end_exclusive() {
        let t = token();
        assert!(in_active_window(&t, t.start_ms, t.start_ms));
        assert!(!in_active_window(&t, t.start_ms - 1, t.start_ms));
        assert!(!in_active_window(&t, t.start_ms, t.start_ms - 1));
        assert!(!in_active_window(&t, t.end_ms, t.end_ms));
    }
    #[tokio::test]
    async fn schema_contains_only_slim_market_tables() -> anyhow::Result<()> {
        let pool = SqlitePool::connect("sqlite::memory:").await?;
        initialize_schema(&pool).await?;
        let t = token();
        store_batch(
            &pool,
            &[Write::Quote(Quote {
                token: t.clone(),
                bid: "0.123456789".into(),
                ask: "0.55".into(),
                timestamp_ms: t.start_ms + 1,
                received_ms: t.start_ms + 2,
            })],
        )
        .await?;
        let tables: Vec<String> =
            sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                .fetch_all(&pool)
                .await?;
        assert_eq!(
            tables,
            vec![
                "btc_5m_quotes",
                "btc_5m_trades",
                "markets",
                "resolutions",
                "tokens"
            ]
        );
        let price: String = sqlx::query_scalar("SELECT best_bid FROM btc_5m_quotes")
            .fetch_one(&pool)
            .await?;
        assert_eq!(price, "0.123456789");
        Ok(())
    }
    #[tokio::test]
    async fn future_quotes_do_not_create_metadata() -> anyhow::Result<()> {
        let pool = SqlitePool::connect("sqlite::memory:").await?;
        initialize_schema(&pool).await?;
        let t = token();
        store_batch(
            &pool,
            &[Write::Quote(Quote {
                token: t.clone(),
                bid: "0.4".into(),
                ask: "0.6".into(),
                timestamp_ms: t.start_ms - 1,
                received_ms: t.start_ms - 1,
            })],
        )
        .await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM markets")
                .fetch_one(&pool)
                .await?,
            0
        );
        Ok(())
    }
}
