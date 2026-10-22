//! Sharded ingestion -> bounded raw queues -> processors -> single batched SQLite writer.
use crate::{
    Write,
    market_data::{self, Data, Record},
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
    lag: AtomicU64,
    errors: AtomicU64,
    disconnects: AtomicU64,
    connections: AtomicU64,
    refreshes: AtomicU64,
    received: AtomicU64,
}

fn offer(tx: &mpsc::Sender<Envelope>, stats: &Stats, item: Envelope) -> anyhow::Result<bool> {
    stats.received.fetch_add(1, Ordering::Relaxed);
    match tx.try_send(item) {
        Ok(()) => Ok(true),
        Err(mpsc::error::TrySendError::Full(_)) => {
            stats.overflow.fetch_add(1, Ordering::Relaxed);
            Ok(false)
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
        ("sdk_lag", &stats.lag),
        ("stream_error", &stats.errors),
        ("connection_down", &stats.disconnects),
        ("connection_up", &stats.connections),
        ("snapshot_refresh", &stats.refreshes),
    ] {
        let count = counter.swap(0, Ordering::Relaxed);
        if count > 0 {
            let component = format!("CLOB/{asset}");
            let message = format!("{kind} count={count}");
            if matches!(
                kind,
                "ingress_overflow" | "sdk_lag" | "stream_error" | "connection_down"
            ) {
                crate::console::warn(&component, message);
            } else {
                crate::console::debug(&component, message);
            }
            tx.send(Write::Collection(Event::new(
                session,
                asset,
                kind,
                count,
                match kind {
                    "snapshot_refresh" => "Market subscription refreshed at opening or after loss for a new snapshot; adjacent events may have a gap",
                    "connection_up" | "connection_down" => "Connection state sampled once a second",
                    _ => "Observed since preceding report; exact missing source interval unknown",
                },
            )))
            .await
            .context("writer queue closed")?;
        }
    }
    Ok(())
}

async fn process(
    mut rx: mpsc::Receiver<Envelope>,
    tx: mpsc::Sender<Write>,
    stats: Arc<Stats>,
    session: String,
    asset: String,
) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let envelope = tokio::select! {
            _=tick.tick()=>{report(&stats,&tx,&session,&asset).await?;continue;},
            envelope=rx.recv()=>match envelope{Some(e)=>e,None=>break},
        };
        let Envelope {
            message,
            tokens,
            received_ms,
        } = envelope;
        let mut records = Vec::new();
        match message {
            WsMessage::BestBidAsk(q) => {
                if let Some(t) = tokens.get(&q.asset_id) {
                    if markets::in_active_window(t, q.timestamp, received_ms) {
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
            WsMessage::Book(book) => {
                if let Some(t) = tokens.get(&book.asset_id) {
                    records.push(Record {
                        token: t.clone(),
                        received_ms,
                        data: Data::Book(book),
                    })
                }
            }
            WsMessage::LastTradePrice(trade) => {
                if let Some(t) = tokens.get(&trade.asset_id) {
                    records.push(Record {
                        token: t.clone(),
                        received_ms,
                        data: Data::Trade(trade),
                    })
                }
            }
            WsMessage::PriceChange(change) => {
                for entry in change.price_changes {
                    if let Some(t) = tokens.get(&entry.asset_id) {
                        records.push(Record {
                            token: t.clone(),
                            received_ms,
                            data: Data::Change {
                                timestamp_ms: change.timestamp,
                                entry,
                            },
                        })
                    }
                }
            }
            WsMessage::MarketResolved(r) => {
                if let Some(t) = tokens.get(&r.winning_asset_id) {
                    if r.market.to_string() == t.condition_id
                        && r.winning_outcome.eq_ignore_ascii_case(&t.outcome)
                    {
                        records.push(Record {
                            token: t.clone(),
                            received_ms,
                            data: Data::Resolution {
                                winning_token: r.winning_asset_id,
                                outcome: r.winning_outcome.to_ascii_lowercase(),
                                timestamp_ms: r.timestamp,
                                source: "clob_ws".into(),
                            },
                        })
                    }
                }
            }
            _ => {}
        }
        for record in records {
            if record.eligible() {
                tx.send(Write::MarketData(record))
                    .await
                    .context("writer closed")?;
            }
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
    let mut subscriptions: BTreeMap<String, (Vec<U256>, i64, bool)> = BTreeMap::new();
    let mut tokens = Arc::new(Tokens::new());
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_connected = false;
    let mut samples = 0u64;
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            biased;
            _=shutdown.changed()=>break,
            changed=control.changed()=>{
                if changed.is_err(){break;}
                tokens=control.borrow_and_update().clone();
                let mut groups:BTreeMap<String,(Vec<U256>,i64)>=BTreeMap::new();
                for token in tokens.values(){let group=groups.entry(token.market_id.clone()).or_insert_with(||(Vec::new(),token.start_ms));group.0.push(token.token_id);}
                let removed:Vec<_>=subscriptions.keys().filter(|id|!groups.contains_key(*id)).cloned().collect();
                for id in removed {
                    streams.remove(&id);
                    if let Some((ids,_,_))=subscriptions.remove(&id){ws.unsubscribe_orderbook(&ids)?;}
                }
                for (id,(ids,start)) in groups {
                    if !subscriptions.contains_key(&id){
                        streams.insert(id.clone(),ws.subscribe_market_events(ids.clone())?.boxed());
                        subscriptions.insert(id,(ids,start,start<=now_ms()));
                    }
                }
            },
            _=tick.tick()=>{
                // Only refresh markets that just opened; other subscriptions stay untouched.
                for (id,(ids,start,activated)) in &mut subscriptions {
                    if !*activated && *start<=now_ms() && raw.capacity() > RAW_CAPACITY/2 {
                        streams.remove(id);ws.unsubscribe_orderbook(ids)?;
                        streams.insert(id.clone(),ws.subscribe_market_events(ids.clone())?.boxed());
                        *activated=true;
                        stats.refreshes.fetch_add(1,Ordering::Relaxed);
                    }
                }
                let connected=ws.is_connected(ChannelType::Market);
                if last_connected && !connected { stats.disconnects.fetch_add(1,Ordering::Relaxed); }
                if !last_connected && connected { stats.connections.fetch_add(1,Ordering::Relaxed); }
                last_connected=connected;
                samples += 1;
                if samples % 10 == 0 {
                    let asset=tokens.values().next().map(|t|t.asset.as_str()).unwrap_or("idle");
                    crate::console::debug(&format!("INGRESS/{asset}"),format!("received={:>8} queue={:>5}/{}",stats.received.load(Ordering::Relaxed),RAW_CAPACITY-raw.capacity(),RAW_CAPACITY));
                }
            },
            event=streams.next(),if !streams.is_empty()=>{
                if let Some((id,result))=event {
                    match result {
                        Ok(message)=>{
                            if !offer(&raw,&stats,Envelope{message,tokens:tokens.clone(),received_ms:now_ms()})? {
                                if let Some((_,_,activated))=subscriptions.get_mut(&id){*activated=false;}
                            }
                        },
                        Err(error)=>{
                            use std::error::Error as _;
                            let missed=error.source().and_then(|s|s.downcast_ref::<polymarket_client_sdk_v2::ws::error::WsError>()).and_then(|e|match e{polymarket_client_sdk_v2::ws::error::WsError::Lagged(n)=>Some(*n),_=>None});
                            if let Some(n)=missed{stats.lag.fetch_add(n,Ordering::Relaxed);}else{stats.errors.fetch_add(1,Ordering::Relaxed);}
                            if let Some((_,_,activated))=subscriptions.get_mut(&id){*activated=false;}
                            crate::console::debug("CLOB", format!("market={id} error={error}"));
                        }
                    }
                }
                // Ended streams are removed by StreamMap. Recreate on next control refresh.
                let ended:Vec<_>=subscriptions.keys().filter(|id|!streams.contains_key(*id)).cloned().collect();
                for id in ended {
                    if let Some((ids,_,_))=subscriptions.remove(&id){ws.unsubscribe_orderbook(&ids)?;stats.errors.fetch_add(1,Ordering::Relaxed);}
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
    let mut processor = tokio::spawn(process(
        raw_rx,
        tx.clone(),
        stats.clone(),
        session.clone(),
        asset.clone(),
    ));
    tx.send(Write::Collection(Event::new(
        &session,
        &asset,
        "session_start",
        0,
        "Per-asset raw queue capacity=8192; processing isolated from ingestion",
    )))
    .await?;
    let mut ingestion = tokio::spawn(ingest(control, shutdown, raw_tx, stats));
    let result = tokio::select! {
        result=&mut ingestion=>result.context("ingestion panicked")?,
        result=&mut processor=>{
            ingestion.abort();
            let _=ingestion.await;
            result.context("processor panicked")??;
            anyhow::bail!("processor exited before ingestion");
        },
    };
    // Ingestion has dropped its sender; processor drains all accepted envelopes.
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
    // Finish fallible startup reads before spawning background tasks.
    let pending = market_data::pending(&pool).await?;
    let initial: Tokens = pending.into_iter().map(|t| (t.token_id, t)).collect();
    let (updates_tx, mut updates) = watch::channel(Arc::new(Tokens::new()));
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
                    Err(e) => {
                        crate::console::warn("DISCOVERY", format!("pending refresh failed: {e:#}"))
                    }
                },
                Err(e) => crate::console::warn("DISCOVERY", format!("Gamma request failed: {e:#}")),
            }
        }
    });
    let mut reconcile = tokio::spawn(market_data::reconcile(pool.clone(), tx.clone()));
    let mut workers = JoinSet::new();
    let mut controls: BTreeMap<String, watch::Sender<Arc<Tokens>>> = BTreeMap::new();
    // Start persisted unresolved subscriptions without waiting for Gamma.
    let (stop, stop_rx) = watch::channel(false);
    let result=async {
        let mut snapshot=Some(Arc::new(initial));
        loop {
            if let Some(tokens)=snapshot.take(){
                let mut by_asset:BTreeMap<String,Tokens>=BTreeMap::new();
                for (id,token) in tokens.iter(){by_asset.entry(token.asset.clone()).or_default().insert(*id,token.clone());}
                for (asset,control) in &controls {let next=by_asset.remove(asset).unwrap_or_default();control.send(Arc::new(next)).context("shard control closed")?;}
                for (asset,tokens) in by_asset {
                    let (control,rx)=watch::channel(Arc::new(Tokens::new()));
                    control.send(Arc::new(tokens))?;
                    controls.insert(asset.clone(),control);
                    workers.spawn(shard(rx,stop_rx.clone(),tx.clone(),asset,session.clone()));
                }
                crate::console::info("DISCOVERY",format!("assets={:>2} markets={:>4} tokens={:>4}",controls.len(),tokens.len()/2,tokens.len()));
            }
            tokio::select! {
                biased;
                _=shutdown.changed()=>return Ok(()),
                result=&mut reconcile=>{result.context("reconciler panicked")??;anyhow::bail!("reconciler stopped")},
                result=workers.join_next(),if !workers.is_empty()=>{result.unwrap().context("shard panicked")??;anyhow::bail!("shard stopped unexpectedly")},
                changed=updates.changed()=>{changed.context("discovery stopped")?;snapshot=Some(updates.borrow_and_update().clone());},
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
            Ok(Err(e)) => {
                drained = Err(e);
            }
            Err(e) => {
                drained = Err(e.into());
            }
        }
    }
    result?;
    drained
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn drains_raw_queue_and_persists_loss_markers() -> anyhow::Result<()> {
        let pool = SqlitePool::connect("sqlite::memory:").await?;
        markets::initialize_schema(&pool).await?;
        initialize_schema(&pool).await?;
        let token = Arc::new(Token {
            asset: "btc".into(),
            timeframe: markets::Timeframe::M5,
            market_id: "test".into(),
            condition_id: polymarket_client_sdk_v2::types::B256::ZERO.to_string(),
            slug: "btc-updown-5m-1800000000".into(),
            question: "test".into(),
            start_ms: 1800000000000,
            end_ms: 1800000300000,
            token_id: U256::from(1),
            outcome: "up".into(),
        });
        let tokens = Arc::new([(token.token_id, token.clone())].into_iter().collect());
        let stats = Arc::new(Stats::default());
        stats.lag.store(7, Ordering::Relaxed);
        let (raw_tx, raw_rx) = mpsc::channel(2);
        for _ in 0..3 {
            let message = WsMessage::LastTradePrice(
                polymarket_client_sdk_v2::clob::ws::types::response::LastTradePrice::builder()
                    .market(polymarket_client_sdk_v2::types::B256::ZERO)
                    .asset_id(token.token_id)
                    .timestamp(token.start_ms + 1)
                    .price("0.5".parse()?)
                    .build(),
            );
            offer(
                &raw_tx,
                &stats,
                Envelope {
                    message,
                    tokens: Arc::clone(&tokens),
                    received_ms: token.start_ms + 2,
                },
            )?;
        }
        drop(raw_tx);
        let (tx, rx) = mpsc::channel(16);
        let processor = tokio::spawn(process(
            raw_rx,
            tx,
            stats,
            "session-test".into(),
            "btc".into(),
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
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count FROM collection_events WHERE kind='sdk_lag'"
            )
            .fetch_one(&pool)
            .await?,
            7
        );
        Ok(())
    }

    #[test]
    fn overflow_is_counted_without_blocking_ingestion() {
        let (tx, _rx) = mpsc::channel(1);
        let stats = Stats::default();
        let message = || {
            WsMessage::LastTradePrice(
                polymarket_client_sdk_v2::clob::ws::types::response::LastTradePrice::builder()
                    .market(polymarket_client_sdk_v2::types::B256::ZERO)
                    .asset_id(U256::from(1))
                    .timestamp(1)
                    .price("0.5".parse().unwrap())
                    .build(),
            )
        };
        for _ in 0..3 {
            offer(
                &tx,
                &stats,
                Envelope {
                    message: message(),
                    tokens: Arc::new(Tokens::new()),
                    received_ms: 1,
                },
            )
            .unwrap();
        }
        assert_eq!(stats.overflow.load(Ordering::Relaxed), 2);
    }
}
