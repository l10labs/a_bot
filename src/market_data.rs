//! Five-minute public execution ticks and final resolutions. No depth collection.
use crate::markets::{Token, in_active_window};
use polymarket_client_sdk_v2::clob::ws::types::response::LastTradePrice;
use sqlx::{Sqlite, Transaction};
use std::sync::Arc;

pub struct Trade {
    pub token: Arc<Token>,
    pub received_ms: i64,
    pub trade: LastTradePrice,
}
impl Trade {
    pub fn eligible(&self) -> bool {
        in_active_window(&self.token, self.trade.timestamp, self.received_ms)
    }
}
pub async fn store_trades(
    tx: &mut Transaction<'_, Sqlite>,
    asset: &str,
    records: &[&Trade],
) -> anyhow::Result<()> {
    for chunk in records.chunks(100) {
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(format!(
            "INSERT INTO \"{asset}_5m_trades\"(token_id,timestamp_ms,received_ms,price,size,side,fee_rate_bps) "
        ));
        query.push_values(chunk.iter(), |mut row, record| {
            let trade = &record.trade;
            row.push_bind(record.token.token_id.to_string())
                .push_bind(trade.timestamp)
                .push_bind(record.received_ms)
                .push_bind(trade.price.to_string())
                .push_bind(trade.size.map(|x| x.to_string()))
                .push_bind(trade.side.map(|x| x.to_string()))
                .push_bind(trade.fee_rate_bps.map(|x| x.to_string()));
        });
        query.build().execute(&mut **tx).await?;
    }
    Ok(())
}
pub struct Resolution {
    pub token: Arc<Token>,
    pub timestamp_ms: i64,
    pub received_ms: i64,
    pub source: String,
}
impl Resolution {
    pub fn eligible(&self) -> bool {
        self.received_ms >= self.token.end_ms
    }
}

pub async fn pending(pool: &sqlx::SqlitePool) -> anyhow::Result<Vec<Arc<Token>>> {
    type Row = (String, String, String, String, i64, i64, String, String);
    let rows:Vec<Row>=sqlx::query_as("SELECT m.asset,m.market_id,m.condition_id,m.slug,m.start_ms,m.end_ms,t.token_id,t.outcome
        FROM markets m JOIN tokens t USING(market_id) LEFT JOIN resolutions r USING(market_id) WHERE r.market_id IS NULL")
        .fetch_all(pool).await?;
    rows.into_iter()
        .map(
            |(asset, market_id, condition_id, slug, start_ms, end_ms, id, outcome)| {
                Ok(Arc::new(Token {
                    asset,
                    market_id,
                    condition_id,
                    slug,
                    start_ms,
                    end_ms,
                    token_id: id.parse()?,
                    outcome,
                }))
            },
        )
        .collect()
}

fn finalized_winner(
    market: &polymarket_client_sdk_v2::gamma::types::response::Market,
) -> Option<(polymarket_client_sdk_v2::types::U256, String)> {
    use polymarket_client_sdk_v2::types::Decimal;
    if market.closed != Some(true) || market.uma_resolution_status.as_deref() != Some("resolved") {
        return None;
    }
    let (Some(ids), Some(outcomes), Some(prices)) = (
        &market.clob_token_ids,
        &market.outcomes,
        &market.outcome_prices,
    ) else {
        return None;
    };
    if ids.len() != 2 || outcomes.len() != 2 || prices.len() != 2 {
        return None;
    }
    let index = prices.iter().position(|p| *p == Decimal::ONE)?;
    if prices[1 - index] != Decimal::ZERO {
        return None;
    }
    let outcome = outcomes[index].to_ascii_lowercase();
    ["up", "down"]
        .contains(&outcome.as_str())
        .then_some((ids[index], outcome))
}

pub async fn reconcile(
    pool: sqlx::SqlitePool,
    tx: tokio::sync::mpsc::Sender<crate::Write>,
) -> anyhow::Result<()> {
    let gamma = polymarket_client_sdk_v2::gamma::Client::default();
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let mut seen = std::collections::BTreeSet::new();
        for token in pending(&pool).await? {
            if token.end_ms > crate::now_ms() || !seen.insert(token.market_id.clone()) {
                continue;
            }
            let request =
                polymarket_client_sdk_v2::gamma::types::request::MarketByIdRequest::builder()
                    .id(token.market_id.clone())
                    .build();
            let market = match gamma.market_by_id(&request).await {
                Ok(m) => m,
                Err(error) => {
                    crate::console::warn(
                        "RESOLVE",
                        format!("market={} lookup failed: {error}", token.market_id),
                    );
                    continue;
                }
            };
            let Some((id, outcome)) = finalized_winner(&market) else {
                continue;
            };
            let mut winner = (*token).clone();
            winner.token_id = id;
            winner.outcome = outcome;
            let observed = crate::now_ms();
            tx.send(crate::Write::Resolution(Resolution {
                token: Arc::new(winner),
                timestamp_ms: observed,
                received_ms: observed,
                source: "gamma_finalized".into(),
            }))
            .await?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Write, markets};
    #[tokio::test]
    async fn stores_postexpiry_results_once_and_restores_pending() -> anyhow::Result<()> {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await?;
        markets::initialize_schema(&pool).await?;
        let t = markets::tests::token();
        markets::store_batch(
            &pool,
            &[Write::Market {
                token: t.clone(),
                received_ms: t.start_ms + 1,
            }],
        )
        .await?;
        assert_eq!(pending(&pool).await?.len(), 1);
        let early = Resolution {
            token: t.clone(),
            timestamp_ms: t.end_ms - 1,
            received_ms: t.end_ms - 1,
            source: "clob_ws".into(),
        };
        markets::store_batch(&pool, &[Write::Resolution(early)]).await?;
        assert_eq!(pending(&pool).await?.len(), 1);
        for _ in 0..2 {
            markets::store_batch(
                &pool,
                &[Write::Resolution(Resolution {
                    token: t.clone(),
                    timestamp_ms: t.end_ms + 100,
                    received_ms: t.end_ms + 101,
                    source: "clob_ws".into(),
                })],
            )
            .await?;
        }
        assert!(pending(&pool).await?.is_empty());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM resolutions")
                .fetch_one(&pool)
                .await?,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT winning_outcome FROM resolutions")
                .fetch_one(&pool)
                .await?,
            "up"
        );
        Ok(())
    }
    #[test]
    fn gamma_requires_explicit_finality_and_exact_payout() {
        use polymarket_client_sdk_v2::{
            gamma::types::response::Market,
            types::{Decimal, U256},
        };
        let mut market = Market::builder()
            .id("test".into())
            .closed(true)
            .uma_resolution_status("resolved".into())
            .outcomes(vec!["Down".into(), "Up".into()])
            .clob_token_ids(vec![U256::from(1), U256::from(2)])
            .outcome_prices(vec![Decimal::ZERO, Decimal::ONE])
            .build();
        assert_eq!(
            finalized_winner(&market),
            Some((U256::from(2), "up".into()))
        );
        market.uma_resolution_status = None;
        assert!(finalized_winner(&market).is_none());
        market.uma_resolution_status = Some("resolved".into());
        market.outcome_prices = Some(vec!["0.001".parse().unwrap(), "0.999".parse().unwrap()]);
        assert!(finalized_winner(&market).is_none());
    }

    #[tokio::test]
    async fn bulk_trades_preserve_precision_and_repeated_ticks() -> anyhow::Result<()> {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await?;
        markets::initialize_schema(&pool).await?;
        let t = markets::tests::token();
        let mut batch = Vec::new();
        for _ in 0..225 {
            let trade = LastTradePrice::builder()
                .asset_id(t.token_id)
                .market(polymarket_client_sdk_v2::types::B256::ZERO)
                .timestamp(t.start_ms + 1)
                .price("0.123456789".parse()?)
                .size("3.5".parse()?)
                .build();
            batch.push(Write::Trade(Trade {
                token: t.clone(),
                received_ms: t.start_ms + 2,
                trade,
            }));
        }
        markets::store_batch(&pool, &batch).await?;
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM btc_5m_trades WHERE price='0.123456789'"
            )
            .fetch_one(&pool)
            .await?,
            225
        );
        assert!(
            sqlx::query("PRAGMA foreign_key_check")
                .fetch_all(&pool)
                .await?
                .is_empty()
        );
        Ok(())
    }
}
