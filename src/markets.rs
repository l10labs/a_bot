//! Gamma discovery and CLOB best-bid/ask collection for rolling crypto markets.
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::Context;
use futures::{StreamExt, stream::BoxStream};
use polymarket_client_sdk_v2::{
    clob::ws::{
        Client as WsClient,
        types::response::{BestBidAsk, BookUpdate, LastTradePrice, MarketResolved, PriceChange},
    },
    gamma::{
        Client as GammaClient,
        types::{request::MarketsRequest, response::Market},
    },
    types::U256,
};
use sqlx::SqlitePool;
use tokio::sync::{mpsc, watch};

use crate::{Write, now_ms};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Timeframe {
    M5,
    M15,
    H1,
    H4,
}

impl Timeframe {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::H1 => "1h",
            Self::H4 => "4h",
        }
    }
    fn seconds(self) -> i64 {
        match self {
            Self::M5 => 300,
            Self::M15 => 900,
            Self::H1 => 3600,
            Self::H4 => 14400,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Token {
    pub asset: String,
    pub timeframe: Timeframe,
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

pub(crate) fn in_active_window(token: &Token, timestamp_ms: i64, received_ms: i64) -> bool {
    (token.start_ms..token.end_ms).contains(&timestamp_ms)
        && (token.start_ms..token.end_ms).contains(&received_ms)
}

fn slug_parts(slug: &str) -> Option<(&str, i64, Timeframe)> {
    let (asset, suffix) = slug.split_once("-updown-")?;
    let (frame, start) = suffix.split_once('-')?;
    let timeframe = match frame {
        "5m" => Timeframe::M5,
        "15m" => Timeframe::M15,
        "1h" => Timeframe::H1,
        "4h" => Timeframe::H4,
        _ => return None,
    };
    // Table identifiers come only from this strictly validated symbol.
    if asset.is_empty()
        || !asset
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    {
        return None;
    }
    let start: i64 = start.parse().ok()?;
    // Four-hour windows are ET-aligned, not necessarily UTC-epoch 4h aligned.
    (start > 0 && start % 300 == 0).then_some((asset, start, timeframe))
}

fn market_window(market: &Market) -> Option<(String, i64, i64, Timeframe)> {
    let slug = market.slug.as_deref()?;
    if let Some((asset, start, timeframe)) = slug_parts(slug) {
        let start_ms = start.checked_mul(1000)?;
        let end_ms = start_ms.checked_add(timeframe.seconds() * 1000)?;
        if market
            .end_date
            .is_some_and(|end| end.timestamp_millis() != end_ms)
            || market
                .event_start_time
                .is_some_and(|begin| begin.timestamp_millis() != start_ms)
        {
            return None;
        }
        return Some((asset.into(), start_ms, end_ms, timeframe));
    }
    let (name, _) = slug.split_once("-up-or-down-")?;
    let start = market.event_start_time?;
    let end = market.end_date?;
    if end.timestamp_millis() - start.timestamp_millis() != 3_600_000 {
        return None;
    }
    // Hourly crypto markets use Binance candles. Derive the asset from the pair,
    // rather than maintaining a fixed list of supported cryptocurrencies.
    let description = market.description.as_deref()?;
    let prefix = description.split_once("/USDT")?.0;
    let asset = prefix
        .rsplit(|c: char| !c.is_ascii_uppercase() && !c.is_ascii_digit())
        .next()?;
    if name.is_empty() || asset.is_empty() || !description.contains("Binance") {
        return None;
    }
    Some((
        asset.to_ascii_lowercase(),
        start.timestamp_millis(),
        end.timestamp_millis(),
        Timeframe::H1,
    ))
}

fn tokens(market: Market, now: i64) -> Option<Vec<Arc<Token>>> {
    let slug = market.slug.as_deref()?;
    let (asset, start_ms, end_ms, timeframe) = market_window(&market)?;
    if start_ms > now + 15 * 60 * 1000 {
        return None;
    }
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
                    start_ms,
                    timeframe,
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
            .end_date_max(now + chrono::Duration::minutes(255))
            .order("id".to_owned())
            .ascending(true)
            .limit(100)
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
        if len < 100 {
            break;
        }
        offset += 100;
    }
    Ok(result)
}

enum Wire {
    Quote(BestBidAsk),
    Book(BookUpdate),
    Change(PriceChange),
    Trade(LastTradePrice),
    Resolution(MarketResolved),
}

fn subscribe(
    ws: &WsClient,
    ids: Vec<U256>,
) -> anyhow::Result<BoxStream<'static, polymarket_client_sdk_v2::Result<Wire>>> {
    if ids.is_empty() {
        return Ok(Box::pin(futures::stream::pending()));
    }
    let streams: Vec<BoxStream<'static, _>> = vec![
        ws.subscribe_best_bid_ask(ids.clone())?
            .map(|r| r.map(Wire::Quote))
            .boxed(),
        ws.subscribe_orderbook(ids.clone())?
            .map(|r| r.map(Wire::Book))
            .boxed(),
        ws.subscribe_prices(ids.clone())?
            .map(|r| r.map(Wire::Change))
            .boxed(),
        ws.subscribe_last_trade_price(ids.clone())?
            .map(|r| r.map(Wire::Trade))
            .boxed(),
        ws.subscribe_market_resolutions(ids)?
            .map(|r| r.map(Wire::Resolution))
            .boxed(),
    ];
    Ok(futures::stream::select_all(streams).boxed())
}

pub async fn run(
    tx: mpsc::Sender<Write>,
    mut shutdown: watch::Receiver<bool>,
    pool: SqlitePool,
) -> anyhow::Result<()> {
    let (discovery_tx, mut discovery_rx) =
        watch::channel((BTreeMap::new(), std::collections::BTreeSet::new()));
    let discovery_pool = pool.clone();
    // Discovery must not block consuming quotes while HTTP requests are in flight.
    let discovery = tokio::spawn(async move {
        let gamma = GammaClient::default();
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            match discover(&gamma).await {
                Ok(tokens) => match crate::market_data::pending(&discovery_pool).await {
                    Ok(pending) => {
                        let pending = pending.into_iter().map(|t| t.market_id.clone()).collect();
                        if discovery_tx.send((tokens, pending)).is_err() {
                            break;
                        }
                    }
                    Err(error) => eprintln!("Pending-resolution refresh failed: {error:#}"),
                },
                Err(error) => eprintln!("Gamma discovery failed (retrying): {error:#}"),
            }
        }
    });
    let mut reconcile = tokio::spawn(crate::market_data::reconcile(pool.clone(), tx.clone()));
    let result = async {
        let ws = WsClient::default();
        // Restore unresolved markets after restart; retain their subscriptions after expiry.
        let mut known: BTreeMap<U256, Arc<Token>> = crate::market_data::pending(&pool).await?
            .into_iter().map(|t| (t.token_id,t)).collect();
        let mut quotes = subscribe(&ws, known.keys().copied().collect())?;
        let mut resolved = std::collections::BTreeSet::new();
        let mut activated: std::collections::BTreeSet<U256> = known.values()
            .filter(|t| t.start_ms <= now_ms() && now_ms() < t.end_ms).map(|t| t.token_id).collect();
        let mut activation = tokio::time::interval(Duration::from_secs(1));
        activation.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => return Ok(()),
                result = &mut reconcile => {
                    result.context("resolution reconciliation task panicked")??;
                    anyhow::bail!("resolution reconciliation task stopped unexpectedly");
                }
                _ = activation.tick() => {
                    let now = now_ms();
                    let active: std::collections::BTreeSet<_> = known.values()
                        .filter(|t| t.start_ms <= now && now < t.end_ms).map(|t| t.token_id).collect();
                    if active.difference(&activated).next().is_some() {
                        // Refresh initial depth after the window opens, rather than persisting
                        // the pre-window snapshot sent when we first subscribed ahead of time.
                        let ids = known.keys().copied().collect::<Vec<_>>();
                        if !ids.is_empty() {
                            for _ in 0..5 { ws.unsubscribe_orderbook(&ids)?; }
                            quotes = subscribe(&ws, ids)?;
                        }
                    }
                    activated = active;
                }
                changed = discovery_rx.changed() => {
                    changed.context("Gamma discovery task stopped")?;
                    let (mut next, pending) = discovery_rx.borrow_and_update().clone();
                    for (id, token) in &known {
                        if !resolved.contains(&token.market_id) && (token.end_ms > now_ms() || pending.contains(&token.market_id)) {
                            next.entry(*id).or_insert_with(|| token.clone());
                        }
                    }
                    next.retain(|_, token| !resolved.contains(&token.market_id));
                    if known.keys().eq(next.keys()) { continue; }
                    let ids = next.keys().copied().collect::<Vec<_>>();
                    if !known.is_empty() {
                        // Each of the five typed streams owns one subscription reference.
                        for _ in 0..5 {
                            ws.unsubscribe_orderbook(&known.keys().copied().collect::<Vec<_>>())?;
                        }
                    }
                    eprintln!("Gamma: tracking {} crypto Up/Down markets (5m/15m/1h/4h) ({} tokens)", next.len()/2, next.len());
                    quotes = subscribe(&ws, ids)?;
                    known = next;
                    let now = now_ms();
                    activated = known.values().filter(|t| t.start_ms <= now && now < t.end_ms).map(|t| t.token_id).collect();
                }
                update = quotes.next() => match update {
                    Some(Ok(Wire::Quote(quote))) => {
                        if let Some(token) = known.get(&quote.asset_id) {
                            let received_ms = now_ms();
                            if !in_active_window(token, quote.timestamp, received_ms) { continue; }
                            tx.send(Write::Quote(Quote {
                                token: token.clone(), bid: quote.best_bid.to_string(), ask: quote.best_ask.to_string(),
                                timestamp_ms: quote.timestamp, received_ms,
                            })).await.context("writer queue closed")?;
                        }
                    }
                    Some(Ok(event)) => {
                        use crate::market_data::{Data, Record};
                        let received_ms = now_ms();
                        let mut records = Vec::new();
                        match event {
                            Wire::Book(book) => if let Some(token) = known.get(&book.asset_id) {
                                records.push(Record { token:token.clone(), received_ms, data:Data::Book(book) });
                            },
                            Wire::Trade(trade) => if let Some(token) = known.get(&trade.asset_id) {
                                records.push(Record { token:token.clone(), received_ms, data:Data::Trade(trade) });
                            },
                            Wire::Change(change) => for entry in change.price_changes {
                                if let Some(token) = known.get(&entry.asset_id) {
                                    records.push(Record { token:token.clone(), received_ms, data:Data::Change { timestamp_ms:change.timestamp, entry } });
                                }
                            },
                            Wire::Resolution(result) => if let Some(token) = known.get(&result.winning_asset_id) {
                                if token.end_ms <= received_ms && result.market.to_string() == token.condition_id
                                    && result.winning_outcome.eq_ignore_ascii_case(&token.outcome) {
                                    resolved.insert(token.market_id.clone());
                                    records.push(Record { token:token.clone(), received_ms, data:Data::Resolution {
                                        winning_token:result.winning_asset_id, outcome:result.winning_outcome.to_ascii_lowercase(),
                                        timestamp_ms:result.timestamp, source:"clob_ws".into(),
                                    }});
                                }
                            },
                            Wire::Quote(_) => unreachable!(),
                        }
                        for record in records {
                            if record.eligible() {
                                tx.send(Write::MarketData(record)).await.context("writer queue closed")?;
                            }
                        }
                    }
                    Some(Err(error)) => eprintln!("CLOB stream error (possible data gap): {error}"),
                    None => anyhow::bail!("CLOB quote stream ended unexpectedly"),
                }
            }
        }
    }.await;
    discovery.abort();
    let _ = discovery.await;
    reconcile.abort();
    // Its join result may already have been consumed in the select above.
    if !reconcile.is_finished() {
        let _ = reconcile.await;
    }
    result
}

pub async fn initialize_schema(pool: &SqlitePool) -> anyhow::Result<()> {
    for timeframe in [Timeframe::M5, Timeframe::M15, Timeframe::H1, Timeframe::H4] {
        let frame = timeframe.name();
        sqlx::raw_sql(&format!("CREATE TABLE IF NOT EXISTS crypto_{frame}_markets (
        market_id TEXT PRIMARY KEY, condition_id TEXT NOT NULL, slug TEXT NOT NULL,
        asset TEXT NOT NULL, question TEXT NOT NULL, start_ms INTEGER NOT NULL, end_ms INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS crypto_{frame}_tokens (
        token_id TEXT PRIMARY KEY, market_id TEXT NOT NULL REFERENCES crypto_{frame}_markets(market_id),
        outcome TEXT NOT NULL CHECK(outcome IN ('up','down'))
    );
    CREATE TABLE IF NOT EXISTS crypto_{frame}_quote_tables (
        asset TEXT PRIMARY KEY, table_name TEXT NOT NULL UNIQUE
    );")).execute(pool).await?;
    }
    crate::market_data::initialize_schema(pool).await?;
    Ok(())
}

pub async fn store_batch(pool: &SqlitePool, batch: &[Write]) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    let mut counts = BTreeMap::new();
    let mut prepared_tokens = std::collections::BTreeSet::new();
    let mut prepared_tables = std::collections::BTreeSet::new();
    let mut prepared_data = std::collections::BTreeSet::new();
    for write in batch {
        let (token, quote, data) = match write {
            Write::Quote(quote)
                if in_active_window(&quote.token, quote.timestamp_ms, quote.received_ms) =>
            {
                (&quote.token, Some(quote), None)
            }
            Write::MarketData(record) if record.eligible() => (&record.token, None, Some(record)),
            _ => continue,
        };
        // Validate again at the SQL boundary; identifiers cannot be bound as parameters.
        anyhow::ensure!(
            !token.asset.is_empty()
                && token
                    .asset
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()),
            "invalid asset identifier"
        );
        let frame = token.timeframe.name();
        let table = format!("{}_{frame}_quotes", token.asset);
        if prepared_tokens.insert((frame, token.token_id)) {
            sqlx::query(&format!("INSERT INTO crypto_{frame}_markets (market_id, condition_id, slug, asset, question, start_ms, end_ms) VALUES (?,?,?,?,?,?,?) ON CONFLICT(market_id) DO NOTHING"))
            .bind(&token.market_id).bind(&token.condition_id).bind(&token.slug).bind(&token.asset)
            .bind(&token.question).bind(token.start_ms).bind(token.end_ms).execute(&mut *tx).await?;
            sqlx::query(&format!("INSERT INTO crypto_{frame}_tokens (token_id,market_id,outcome) VALUES (?,?,?) ON CONFLICT(token_id) DO NOTHING"))
            .bind(token.token_id.to_string()).bind(&token.market_id).bind(&token.outcome).execute(&mut *tx).await?;
        }
        if prepared_tables.insert(table.clone()) {
            sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
            id INTEGER PRIMARY KEY, token_id TEXT NOT NULL REFERENCES crypto_{frame}_tokens(token_id),
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
            sqlx::query(&format!("INSERT INTO crypto_{frame}_quote_tables(asset,table_name) VALUES (?,?) ON CONFLICT(asset) DO NOTHING"))
            .bind(&token.asset).bind(&table).execute(&mut *tx).await?;
        }
        if let Some(quote) = quote {
            sqlx::query(&format!("INSERT INTO {table}(token_id,timestamp_ms,received_ms,best_bid,best_ask) VALUES (?,?,?,?,?)"))
                .bind(token.token_id.to_string()).bind(quote.timestamp_ms).bind(quote.received_ms)
                .bind(&quote.bid).bind(&quote.ask).execute(&mut *tx).await?;
            *counts
                .entry(format!("{}:{frame}", token.asset))
                .or_insert(0usize) += 1;
        }
        if let Some(record) = data {
            let kind = match record.data {
                crate::market_data::Data::Book(_) => "books",
                crate::market_data::Data::Change { .. } => "changes",
                crate::market_data::Data::Trade(_) => "trades",
                crate::market_data::Data::Resolution { .. } => "resolutions",
            };
            let initialize = prepared_data.insert((table.clone(), kind));
            crate::market_data::store(&mut tx, record, initialize).await?;
            *counts
                .entry(format!("{}:{frame}:{kind}", token.asset))
                .or_insert(0usize) += 1;
        }
    }
    tx.commit().await?;
    if !counts.is_empty() {
        println!(
            "Wrote CLOB data: {}",
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
            Some(("btc", 1800000000, Timeframe::M5))
        );
        assert_eq!(
            slug_parts("btc-updown-15m-1800000000").unwrap().2,
            Timeframe::M15
        );
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
        let token = &found[0];
        assert!(in_active_window(token, token.start_ms, token.start_ms));
        assert!(in_active_window(token, token.end_ms - 1, token.end_ms - 1));
        assert!(!in_active_window(token, token.start_ms - 1, token.start_ms));
        assert!(!in_active_window(token, token.start_ms, token.start_ms - 1));
        assert!(!in_active_window(token, token.end_ms, token.end_ms - 1));
        assert!(!in_active_window(token, token.end_ms - 1, token.end_ms));
        assert!(tokens(market, 1_800_000_300_000).is_none());
    }

    #[test]
    fn recognizes_all_windows_and_hourly_event_start() {
        for (name, frame) in [("15m", Timeframe::M15), ("4h", Timeframe::H4)] {
            let market = Market::builder()
                .id(name.to_owned())
                .slug(format!("btc-updown-{name}-1800000000"))
                .build();
            let (_, start, end, actual) = market_window(&market).unwrap();
            assert_eq!(actual, frame);
            assert_eq!(end - start, frame.seconds() * 1000);
        }
        let begin = chrono::DateTime::from_timestamp(1800000000, 0).unwrap();
        let market = Market::builder()
            .id("hourly".to_owned())
            .slug("bitcoin-up-or-down-january-15-2027-8am-et".to_owned())
            .description("Binance BTC/USDT 1 hour candle".to_owned())
            .start_date(begin - chrono::Duration::days(2))
            .event_start_time(begin)
            .end_date(begin + chrono::Duration::hours(1))
            .build();
        let (asset, start, end, frame) = market_window(&market).unwrap();
        assert_eq!(asset, "btc");
        assert_eq!(frame, Timeframe::H1);
        assert_eq!(start, begin.timestamp_millis());
        assert_eq!(end - start, 3_600_000);
    }

    #[tokio::test]
    async fn stores_all_timeframes_separately() -> anyhow::Result<()> {
        let pool = SqlitePool::connect("sqlite::memory:").await?;
        initialize_schema(&pool).await?;
        let mut batch = Vec::new();
        for (id, frame) in [Timeframe::M5, Timeframe::M15, Timeframe::H1, Timeframe::H4]
            .into_iter()
            .enumerate()
        {
            let start = 1800000000000;
            batch.push(Write::Quote(Quote {
                token: Arc::new(Token {
                    asset: "btc".into(),
                    timeframe: frame,
                    market_id: frame.name().into(),
                    condition_id: "test".into(),
                    slug: format!("btc-updown-{}-1800000000", frame.name()),
                    question: "test".into(),
                    start_ms: start,
                    end_ms: start + frame.seconds() * 1000,
                    token_id: U256::from(id + 1),
                    outcome: "up".into(),
                }),
                bid: "0.41".into(),
                ask: "0.59".into(),
                timestamp_ms: start + 1,
                received_ms: start + 2,
            }));
        }
        store_batch(&pool, &batch).await?;
        for frame in ["5m", "15m", "1h", "4h"] {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM btc_{frame}_quotes"))
                    .fetch_one(&pool)
                    .await?,
                1
            );
        }
        Ok(())
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
                    timeframe: Timeframe::M5,
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
                timestamp_ms: 1800000000001,
                received_ms: 1800000000002,
            }));
        }
        store_batch(&pool, &batch).await?;
        // Future-window quotes must not create even metadata/table-registry rows.
        for write in &mut batch {
            if let Write::Quote(quote) = write {
                quote.timestamp_ms = quote.token.start_ms - 1;
                quote.received_ms = quote.token.start_ms - 1;
            }
        }
        let empty_pool = SqlitePool::connect("sqlite::memory:").await?;
        initialize_schema(&empty_pool).await?;
        store_batch(&empty_pool, &batch).await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM crypto_5m_markets")
                .fetch_one(&empty_pool)
                .await?,
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM crypto_5m_quote_tables")
                .fetch_one(&empty_pool)
                .await?,
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM btc_5m_quotes")
                .fetch_one(&pool)
                .await?,
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM eth_5m_quotes")
                .fetch_one(&pool)
                .await?,
            1
        );
        let rows: Vec<(String,String)> = sqlx::query_as("SELECT t.outcome,q.best_bid FROM btc_5m_quotes q JOIN crypto_5m_tokens t USING(token_id) ORDER BY q.id").fetch_all(&pool).await?;
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
