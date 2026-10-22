//! Per-asset ingestion -> bounded queues -> processors -> one batched SQLite writer.
use crate::{
    Write,
    market_data::{self, Resolution, Trade},
    markets::{self, Quote, Token},
    now_ms,
};
use anyhow::Context;
use futures::{StreamExt, stream::BoxStream};
use polymarket_client_sdk_v2::{
    clob::ws::{ChannelType, Client, types::response::WsMessage},
    types::U256,
};
use sqlx::SqlitePool;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};
use tokio_stream::StreamMap;

type Tokens = BTreeMap<U256, Arc<Token>>;
const RAW_CAPACITY: usize = 8192;
pub struct Event {
    pub session: String,
    pub asset: String,
    pub kind: String,
    pub at_ms: i64,
    pub count: i64,
    pub detail: String,
}
impl Event {
    fn new(session: &str, asset: &str, kind: &str, count: u64, detail: impl Into<String>) -> Self {
        Self {
            session: session.into(),
            asset: asset.into(),
            kind: kind.into(),
            at_ms: now_ms(),
            count: count as i64,
            detail: detail.into(),
        }
    }
}
pub async fn initialize_schema(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS collection_events (
        id INTEGER PRIMARY KEY,session TEXT NOT NULL,asset TEXT NOT NULL,kind TEXT NOT NULL,
        at_ms INTEGER NOT NULL,count INTEGER NOT NULL,detail TEXT NOT NULL
    ); CREATE INDEX IF NOT EXISTS collection_events_asset_time ON collection_events(asset,at_ms);",
    )
    .execute(pool)
    .await?;
    Ok(())
}
pub async fn store(pool: &SqlitePool, batch: &[Write]) -> anyhow::Result<()> {
    let events: Vec<_> = batch
        .iter()
        .filter_map(|x| {
            if let Write::Collection(e) = x {
                Some(e)
            } else {
                None
            }
        })
        .collect();
    if events.is_empty() {
        return Ok(());
    }
    let mut tx = pool.begin().await?;
    for e in events {
        sqlx::query("INSERT INTO collection_events(session,asset,kind,at_ms,count,detail) VALUES (?,?,?,?,?,?)")
            .bind(&e.session).bind(&e.asset).bind(&e.kind).bind(e.at_ms).bind(e.count).bind(&e.detail).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}
struct Envelope {
    message: WsMessage,
    tokens: Arc<Tokens>,
    received_ms: i64,
}
#[derive(Default)]
struct Stats {
    overflow: AtomicU64,
    errors: AtomicU64,
    disconnects: AtomicU64,
    connections: AtomicU64,
    received: AtomicU64,
}
fn wanted(message: &WsMessage) -> bool {
    matches!(
        message,
        WsMessage::BestBidAsk(_) | WsMessage::LastTradePrice(_) | WsMessage::MarketResolved(_)
    )
}
fn offer(tx: &mpsc::Sender<Envelope>, stats: &Stats, item: Envelope) -> anyhow::Result<()> {
    // Discard books, depth changes and unrelated message types before queueing.
    if !wanted(&item.message) {
        return Ok(());
    }
    stats.received.fetch_add(1, Ordering::Relaxed);
    match tx.try_send(item) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(_)) => {
            stats.overflow.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(mpsc::error::TrySendError::Closed(_)) => anyhow::bail!("processor stopped"),
    }
}
async fn report(
    stats: &Stats,
    tx: &mpsc::Sender<Write>,
    session: &str,
    asset: &str,
) -> anyhow::Result<()> {
    for (kind, counter) in [
        ("ingress_overflow", &stats.overflow),
        ("stream_error", &stats.errors),
        ("connection_down", &stats.disconnects),
        ("connection_up", &stats.connections),
    ] {
        let count = counter.swap(0, Ordering::Relaxed);
        if count == 0 {
            continue;
        }
        let component = format!("CLOB/{asset}");
        let message = format!("{kind} count={count}");
        if kind == "connection_up" {
            crate::console::debug(&component, message)
        } else {
            crate::console::warn(&component, message)
        }
        tx.send(Write::Collection(Event::new(
            session,
            asset,
            kind,
            count,
            "Observed since preceding report; exact missing source interval unknown",
        )))
        .await
        .context("writer queue closed")?;
    }
    Ok(())
}
async fn process(
    mut rx: mpsc::Receiver<Envelope>,
    tx: mpsc::Sender<Write>,
    stats: Arc<Stats>,
    session: String,
    asset: String,
    control: watch::Receiver<Arc<Tokens>>,
) -> anyhow::Result<()> {
    let mut registered = std::collections::BTreeSet::new();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let item = tokio::select! {
            _=tick.tick()=>{
                report(&stats,&tx,&session,&asset).await?;
                let current=control.borrow().clone();
                registered.retain(|id| current.contains_key(id));
                let received_ms=now_ms();
                for token in current.values() {
                    if (token.start_ms..token.end_ms).contains(&received_ms) && registered.insert(token.token_id) {
                        tx.send(Write::Market {token:token.clone(),received_ms}).await.context("writer closed")?;
                    }
                }
                continue;
            },
            item=rx.recv()=>match item{Some(x)=>x,None=>break},
        };
        let Envelope {
            message,
            tokens,
            received_ms,
        } = item;
        match message {
            WsMessage::BestBidAsk(q) => {
                if let Some(t) = tokens.get(&q.asset_id) {
                    if q.market.to_string() == t.condition_id
                        && markets::in_active_window(t, q.timestamp, received_ms)
                    {
                        tx.send(Write::Quote(Quote {
                            token: t.clone(),
                            bid: q.best_bid.to_string(),
                            ask: q.best_ask.to_string(),
                            timestamp_ms: q.timestamp,
                            received_ms,
                        }))
                        .await
                        .context("writer closed")?;
                    }
                }
            }
            WsMessage::LastTradePrice(trade) => {
                if let Some(t) = tokens.get(&trade.asset_id) {
                    if trade.market.to_string() == t.condition_id
                        && markets::in_active_window(t, trade.timestamp, received_ms)
                    {
                        tx.send(Write::Trade(Trade {
                            token: t.clone(),
                            received_ms,
                            trade,
                        }))
                        .await
                        .context("writer closed")?;
                    }
                }
            }
            WsMessage::MarketResolved(result) => {
                if let Some(t) = tokens.get(&result.winning_asset_id) {
                    if result.market.to_string() == t.condition_id
                        && result.winning_outcome.eq_ignore_ascii_case(&t.outcome)
                        && received_ms >= t.end_ms
                    {
                        tx.send(Write::Resolution(Resolution {
                            token: t.clone(),
                            timestamp_ms: result.timestamp,
                            received_ms,
                            source: "clob_ws".into(),
                        }))
                        .await
                        .context("writer closed")?;
                    }
                }
            }
            _ => {}
        }
    }
    report(&stats, &tx, &session, &asset).await?;
    tx.send(Write::Collection(Event::new(
        &session,
        &asset,
        "session_stop",
        0,
        "Ingress stopped and raw queue drained",
    )))
    .await
    .context("writer closed")?;
    Ok(())
}
// Use only the SDK's public, unmodified typed subscriptions. Each typed stream
// owns one SDK subscription reference; release all three when removing a market.
fn subscribe_market(
    ws: &Client,
    ids: Vec<U256>,
) -> anyhow::Result<BoxStream<'static, polymarket_client_sdk_v2::Result<WsMessage>>> {
    let streams: Vec<BoxStream<'static, _>> = vec![
        ws.subscribe_best_bid_ask(ids.clone())?
            .map(|r| r.map(WsMessage::BestBidAsk))
            .boxed(),
        ws.subscribe_last_trade_price(ids.clone())?
            .map(|r| r.map(WsMessage::LastTradePrice))
            .boxed(),
        ws.subscribe_market_resolutions(ids)?
            .map(|r| r.map(WsMessage::MarketResolved))
            .boxed(),
    ];
    Ok(futures::stream::select_all(streams).boxed())
}
fn unsubscribe_market(ws: &Client, ids: &[U256]) -> anyhow::Result<()> {
    for _ in 0..3 {
        ws.unsubscribe_prices(ids)?;
    }
    Ok(())
}

async fn ingest(
    mut control: watch::Receiver<Arc<Tokens>>,
    mut shutdown: watch::Receiver<bool>,
    raw: mpsc::Sender<Envelope>,
    stats: Arc<Stats>,
) -> anyhow::Result<()> {
    let ws = Client::default();
    let mut streams: StreamMap<
        String,
        BoxStream<'static, polymarket_client_sdk_v2::Result<WsMessage>>,
    > = StreamMap::new();
    let mut subscriptions: BTreeMap<String, Vec<U256>> = BTreeMap::new();
    let mut tokens = Arc::new(Tokens::new());
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_connected = false;
    let mut samples = 0;
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            biased;
            _=shutdown.changed()=>break,
            changed=control.changed()=>{
                if changed.is_err(){break;}tokens=control.borrow_and_update().clone();
                let mut groups:BTreeMap<String,Vec<U256>>=BTreeMap::new();
                for token in tokens.values(){groups.entry(token.market_id.clone()).or_default().push(token.token_id);}
                let removed:Vec<_>=subscriptions.keys().filter(|id|!groups.contains_key(*id)).cloned().collect();
                for id in removed{streams.remove(&id);if let Some(ids)=subscriptions.remove(&id){unsubscribe_market(&ws,&ids)?;}}
                for (id,ids) in groups {
                    if !subscriptions.contains_key(&id){streams.insert(id.clone(),subscribe_market(&ws,ids.clone())?);subscriptions.insert(id,ids);}
                }
            },
            _=tick.tick()=>{
                // Discovery retains expired unresolved markets until their final result is stored.
                let connected=ws.is_connected(ChannelType::Market);
                if last_connected && !connected{stats.disconnects.fetch_add(1,Ordering::Relaxed);}
                if !last_connected && connected{stats.connections.fetch_add(1,Ordering::Relaxed);}last_connected=connected;samples+=1;
                if samples%10==0{
                    let asset=tokens.values().next().map(|t|t.asset.as_str()).unwrap_or("idle");
                    crate::console::debug(&format!("INGRESS/{asset}"),format!("received={:>8} queue={:>5}/{}",stats.received.load(Ordering::Relaxed),RAW_CAPACITY-raw.capacity(),RAW_CAPACITY));
                }
                // Recover ended streams without rebuilding unrelated subscriptions.
                let ended:Vec<_>=subscriptions.keys().filter(|id|!streams.contains_key(*id)).cloned().collect();
                for id in ended{let ids=&subscriptions[&id];unsubscribe_market(&ws,ids)?;streams.insert(id,subscribe_market(&ws,ids.clone())?);}
            },
            event=streams.next(),if !streams.is_empty()=>if let Some((id,result))=event {
                match result {
                    Ok(message)=>offer(&raw,&stats,Envelope{message,tokens:tokens.clone(),received_ms:now_ms()})?,
                    Err(error)=>{
                        // SDK broadcast lag is logged internally, not returned by its
                        // typed streams. Only actual returned errors are counted here.
                        stats.errors.fetch_add(1,Ordering::Relaxed);
                        crate::console::debug("CLOB",format!("market={id} error={error}"));
                    }
                }
            },
        }
    }
    Ok(())
}
async fn shard(
    control: watch::Receiver<Arc<Tokens>>,
    shutdown: watch::Receiver<bool>,
    tx: mpsc::Sender<Write>,
    asset: String,
    session: String,
) -> anyhow::Result<()> {
    let (raw_tx, raw_rx) = mpsc::channel(RAW_CAPACITY);
    let stats = Arc::new(Stats::default());
    tx.send(Write::Collection(Event::new(
        &session,
        &asset,
        "session_start",
        0,
        "5m quotes/trades/resolutions; raw capacity=8192",
    )))
    .await?;
    let mut processor = tokio::spawn(process(
        raw_rx,
        tx,
        stats.clone(),
        session,
        asset,
        control.clone(),
    ));
    let mut ingestion = tokio::spawn(ingest(control, shutdown, raw_tx, stats));
    let result = tokio::select! {
        result=&mut ingestion=>result.context("ingestion panicked")?,
        result=&mut processor=>{
            let outcome=result.context("processor panicked").and_then(|result|result);
            if outcome.is_err(){ingestion.abort();let _=ingestion.await;return outcome;}
            // A normal processor exit means ingress closed its sender. On a quiet
            // stream both tasks may finish together during shutdown; that is not a failure.
            return ingestion.await.context("ingestion panicked")?;
        },
    };
    let processed = processor.await.context("processor panicked")?;
    result?;
    processed
}
pub async fn run(
    tx: mpsc::Sender<Write>,
    mut shutdown: watch::Receiver<bool>,
    pool: SqlitePool,
) -> anyhow::Result<()> {
    let session = format!("{}-{}", std::process::id(), now_ms());
    let initial: Tokens = market_data::pending(&pool)
        .await?
        .into_iter()
        .map(|t| (t.token_id, t))
        .collect();
    let (updates_tx, mut updates) = watch::channel(Arc::new(Tokens::new()));
    updates_tx.send(Arc::new(initial))?;
    let discovery_pool = pool.clone();
    let discovery = tokio::spawn(async move {
        let gamma = polymarket_client_sdk_v2::gamma::Client::default();
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            match markets::discover(&gamma).await {
                Ok(mut next) => match market_data::pending(&discovery_pool).await {
                    Ok(pending) => {
                        for token in pending {
                            next.entry(token.token_id).or_insert(token);
                        }
                        if updates_tx.send(Arc::new(next)).is_err() {
                            break;
                        }
                    }
                    Err(error) => crate::console::warn(
                        "DISCOVERY",
                        format!("pending refresh failed: {error:#}"),
                    ),
                },
                Err(e) => crate::console::warn("DISCOVERY", format!("Gamma request failed: {e:#}")),
            }
        }
    });
    let mut reconcile = tokio::spawn(market_data::reconcile(pool, tx.clone()));
    let mut workers = JoinSet::new();
    let mut controls: BTreeMap<String, watch::Sender<Arc<Tokens>>> = BTreeMap::new();
    let (stop, stop_rx) = watch::channel(false);
    let result=async{
        loop{
            if *shutdown.borrow(){return Ok(())}
            tokio::select!{
                biased;
                _=shutdown.changed()=>return Ok(()),
                result=&mut reconcile=>{result.context("reconciler panicked")??;anyhow::bail!("reconciler stopped unexpectedly")},
                result=workers.join_next(),if !workers.is_empty()=>{result.unwrap().context("shard panicked")??;anyhow::bail!("shard stopped unexpectedly")},
                changed=updates.changed()=>{
                    changed.context("discovery stopped")?;let tokens=updates.borrow_and_update().clone();
                    let mut by_asset:BTreeMap<String,Tokens>=BTreeMap::new();for (id,t) in tokens.iter(){by_asset.entry(t.asset.clone()).or_default().insert(*id,t.clone());}
                    for (asset,control) in &controls{control.send(Arc::new(by_asset.remove(asset).unwrap_or_default())).context("shard control closed")?;}
                    for (asset,tokens) in by_asset{let(control,rx)=watch::channel(Arc::new(Tokens::new()));control.send(Arc::new(tokens))?;controls.insert(asset.clone(),control);workers.spawn(shard(rx,stop_rx.clone(),tx.clone(),asset,session.clone()));}
                    crate::console::info("DISCOVERY",format!("5m assets={:>2} markets={:>3} tokens={:>3}",controls.len(),tokens.len()/2,tokens.len()));
                }
            }
        }
    }.await;
    discovery.abort();
    let _ = discovery.await;
    reconcile.abort();
    if !reconcile.is_finished() {
        let _ = reconcile.await;
    }
    let _ = stop.send(true);
    let mut drained = Ok(());
    while let Some(joined) = workers.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => drained = Err(e),
            Err(e) => drained = Err(e.into()),
        }
    }
    result?;
    drained
}
#[cfg(test)]
mod tests {
    use super::*;
    fn trade_message(t: &Token) -> WsMessage {
        WsMessage::LastTradePrice(
            polymarket_client_sdk_v2::clob::ws::types::response::LastTradePrice::builder()
                .asset_id(t.token_id)
                .market(polymarket_client_sdk_v2::types::B256::ZERO)
                .timestamp(t.start_ms + 1)
                .price("0.5".parse().unwrap())
                .build(),
        )
    }
    #[tokio::test]
    async fn drains_accepted_trades_and_persists_loss_markers() -> anyhow::Result<()> {
        let pool = SqlitePool::connect("sqlite::memory:").await?;
        markets::initialize_schema(&pool).await?;
        initialize_schema(&pool).await?;
        let t = markets::tests::token();
        let tokens = Arc::new([(t.token_id, t.clone())].into_iter().collect());
        let stats = Arc::new(Stats::default());
        let (raw_tx, raw_rx) = mpsc::channel(2);
        for _ in 0..3 {
            offer(
                &raw_tx,
                &stats,
                Envelope {
                    message: trade_message(&t),
                    tokens: Arc::clone(&tokens),
                    received_ms: t.start_ms + 2,
                },
            )?;
        }
        drop(raw_tx);
        let (tx, rx) = mpsc::channel(16);
        let processor = tokio::spawn(process(
            raw_rx,
            tx,
            stats,
            "test".into(),
            "btc".into(),
            watch::channel(Arc::new(Tokens::new())).1,
        ));
        crate::writer(pool.clone(), rx).await?;
        processor.await??;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM btc_5m_trades")
                .fetch_one(&pool)
                .await?,
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count FROM collection_events WHERE kind='ingress_overflow'"
            )
            .fetch_one(&pool)
            .await?,
            1
        );
        Ok(())
    }
    #[tokio::test]
    async fn quiet_shards_shutdown_without_false_worker_failure() -> anyhow::Result<()> {
        for _ in 0..20 {
            let (_control, rx) = watch::channel(Arc::new(Tokens::new()));
            let (stop, stop_rx) = watch::channel(false);
            let (tx, _records) = mpsc::channel(16);
            let task = tokio::spawn(shard(rx, stop_rx, tx, "btc".into(), "test".into()));
            tokio::task::yield_now().await;
            stop.send(true)?;
            tokio::time::timeout(Duration::from_secs(2), task).await???;
        }
        Ok(())
    }

    #[test]
    fn ignores_books_before_queueing() {
        let (tx, mut rx) = mpsc::channel(1);
        let stats = Stats::default();
        let t = markets::tests::token();
        let message = WsMessage::Book(
            polymarket_client_sdk_v2::clob::ws::types::response::BookUpdate::builder()
                .asset_id(t.token_id)
                .market(polymarket_client_sdk_v2::types::B256::ZERO)
                .timestamp(t.start_ms)
                .bids(Vec::new())
                .asks(Vec::new())
                .build(),
        );
        offer(
            &tx,
            &stats,
            Envelope {
                message,
                tokens: Arc::new(Tokens::new()),
                received_ms: t.start_ms,
            },
        )
        .unwrap();
        assert!(rx.try_recv().is_err());
        assert_eq!(stats.received.load(Ordering::Relaxed), 0);
    }
}
