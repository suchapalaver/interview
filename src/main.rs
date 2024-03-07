use std::io;

pub mod server;

fn main() -> anyhow::Result<()> {
    let mut processor = Processor::new();
    for query in io::stdin().lines() {
        processor.process_query(query?);
    }
    Ok(())
}

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~ YOUR CODE HERE ~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
};

use anyhow::anyhow;
use chrono::DateTime;
use rust_decimal::prelude::ToPrimitive;
use tracing::{error, instrument, subscriber::set_global_default};
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Registry};

fn telemetry() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("error"));
    set_global_default(
        Registry::default()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().pretty()),
    )
    .ok();
}

#[derive(Clone, Copy)]
pub enum Count {
    Trades(usize),
    Volume(f64),
}

impl Count {
    fn add(&mut self, other: Count) {
        match (self, other) {
            (Count::Trades(a), Count::Trades(b)) => *a += b,
            (Count::Volume(a), Count::Volume(b)) => *a += b,
            _ => unreachable!("Cannot add different types of counts"),
        }
    }
}

impl From<usize> for Count {
    fn from(value: usize) -> Self {
        Count::Trades(value)
    }
}

impl From<f64> for Count {
    fn from(value: f64) -> Self {
        Count::Volume(value)
    }
}

impl std::fmt::Display for Count {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Count::Trades(value) => write!(f, "{}", value),
            Count::Volume(value) => write!(f, "{:.6}", value),
        }
    }
}

struct TimeRange {
    start_timestamp_in_seconds: i64,
    end_timestamp_in_seconds: i64,
}

enum QueryType {
    TakerTrades,
    MarketBuys,
    MarketSells,
    TradingVolume,
}

struct Query {
    query_type: QueryType,
    range: TimeRange,
}

impl FromStr for Query {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split_whitespace();

        let count = parts.next().ok_or(anyhow!("Missing count"))?;

        let start_timestamp_in_seconds = parts
            .next()
            .ok_or(anyhow::anyhow!("Missing start timestamp"))?
            .parse()
            .map_err(|e| anyhow!("Failed to parse start timestamp: {e}"))?;

        let end_timestamp_in_seconds = parts
            .next()
            .ok_or(anyhow::anyhow!("Missing end timestamp"))?
            .parse()
            .map_err(|e| anyhow!("Failed to parse end timestamp: {e}"))?;

        let range = TimeRange {
            start_timestamp_in_seconds,
            end_timestamp_in_seconds,
        };

        let query_type = match count {
            "C" => QueryType::TakerTrades,
            "B" => QueryType::MarketBuys,
            "S" => QueryType::MarketSells,
            "V" => QueryType::TradingVolume,
            _ => return Err(anyhow!("Invalid count request: {s}")),
        };

        Ok(Query { query_type, range })
    }
}

impl Query {
    pub fn get_count(
        &self,
        cache: &QueryCache,
    ) -> anyhow::Result<Option<Count>> {
        let mut cache_lock = cache.lock().unwrap();

        let (mut start, mut end) = (
            self.range.start_timestamp_in_seconds,
            self.range.end_timestamp_in_seconds,
        );

        let mut count: Option<Count> = None;

        let mut to_update = Vec::new();

        let mut done = false;

        for (cached_start, cached_end) in cache_lock.keys() {
            if start == *cached_start && end == *cached_end {
                if let Some(fills) = cache_lock.get(&(*cached_start, *cached_end)) {
                    let cached_count = match self.query_type {
                        QueryType::TradingVolume => {
                            self.count_trading_volume(fills, start, end).into()
                        }
                        QueryType::MarketBuys => self.count_market_buys(fills, start, end).into(),
                        QueryType::MarketSells => self.count_market_sells(fills, start, end).into(),
                        QueryType::TakerTrades => self.count_taker_trades(fills, start, end).into(),
                    };

                    match count.as_mut() {
                        Some(c) => {
                            c.add(cached_count);
                        }
                        None => {
                            count = Some(cached_count);
                        }
                    }

                    // We've covered the entire range we need, so we can return the count.
                    done = true;
                    break;
                }
            } else if start >= *cached_start && end <= *cached_end {
                // If the start is after and the end is before the range we have cached:
                if let Some(fills) = cache_lock.get(&(*cached_start, *cached_end)) {
                    let cached_count = match self.query_type {
                        QueryType::TradingVolume => {
                            self.count_trading_volume(fills, start, end).into()
                        }
                        QueryType::MarketBuys => self.count_market_buys(fills, start, end).into(),
                        QueryType::MarketSells => self.count_market_sells(fills, start, end).into(),
                        QueryType::TakerTrades => self.count_taker_trades(fills, start, end).into(),
                    };

                    match count.as_mut() {
                        Some(c) => {
                            c.add(cached_count);
                        }
                        None => {
                            count = Some(cached_count);
                        }
                    }

                    // We've covered the entire range we need, so we can return the count.
                    done = true;
                    break;
                }
            } else if start <= *cached_start && end >= *cached_start && end <= *cached_end {
                // If the start is before and the end is within the range we have cached:
                if let Some(fills) = cache_lock.get(&(*cached_start, *cached_end)) {
                    let cached_count = match self.query_type {
                        QueryType::TradingVolume => {
                            self.count_trading_volume(fills, start, end).into()
                        }
                        QueryType::MarketBuys => self.count_market_buys(fills, start, end).into(),
                        QueryType::MarketSells => self.count_market_sells(fills, start, end).into(),
                        QueryType::TakerTrades => self.count_taker_trades(fills, start, end).into(),
                    };

                    match count.as_mut() {
                        Some(c) => {
                            c.add(cached_count);
                        }
                        None => {
                            count = Some(cached_count);
                        }
                    }

                    end = *cached_start;

                    // We know there's still more to cover, so we can continue.
                    continue;
                }
            } else if start >= *cached_start && start <= *cached_end && end >= *cached_end {
                // If the start is within the range we have cached, but the end is after:
                if let Some(fills) = cache_lock.get(&(*cached_start, *cached_end)) {
                    let cached_count = match self.query_type {
                        QueryType::TradingVolume => {
                            self.count_trading_volume(fills, start, end).into()
                        }
                        QueryType::MarketBuys => self.count_market_buys(fills, start, end).into(),
                        QueryType::MarketSells => self.count_market_sells(fills, start, end).into(),
                        QueryType::TakerTrades => self.count_taker_trades(fills, start, end).into(),
                    };

                    match count.as_mut() {
                        Some(c) => {
                            c.add(cached_count);
                        }
                        None => {
                            count = Some(cached_count);
                        }
                    }

                    start = *cached_end;

                    // We know there's still more to cover, so we can continue.
                    continue;
                }
            } else if start <= *cached_start && end >= *cached_end {
                // If the start is before and the end is after the range we have cached:
                let cached_count = if let Some(fills) =
                    cache_lock.get(&(*cached_start, *cached_end))
                {
                    match self.query_type {
                        QueryType::TradingVolume => {
                            self.count_trading_volume(fills, start, end).into()
                        }
                        QueryType::MarketBuys => self.count_market_buys(fills, start, end).into(),
                        QueryType::MarketSells => self.count_market_sells(fills, start, end).into(),
                        QueryType::TakerTrades => self.count_taker_trades(fills, start, end).into(),
                    }
                } else {
                    continue;
                };

                match count.as_mut() {
                    Some(c) => {
                        c.add(cached_count);
                    }
                    None => {
                        count = Some(cached_count);
                    }
                }

                let before_fills = server::get_fills_api(start, *cached_start)?;
                let before_count: Count = match self.query_type {
                    QueryType::TradingVolume => {
                        self.count_trading_volume(&before_fills, start, end).into()
                    }
                    QueryType::MarketBuys => {
                        self.count_market_buys(&before_fills, start, end).into()
                    }
                    QueryType::MarketSells => {
                        self.count_market_sells(&before_fills, start, end).into()
                    }
                    QueryType::TakerTrades => {
                        self.count_taker_trades(&before_fills, start, end).into()
                    }
                };
                to_update.push(((start, *cached_start), before_fills));
                let after_fills = server::get_fills_api(*cached_end, end)?;
                
                let after_count: Count = match self.query_type {
                    QueryType::TradingVolume => {
                        self.count_trading_volume(&after_fills, start, end).into()
                    }
                    QueryType::MarketBuys => {
                        self.count_market_buys(&after_fills, start, end).into()
                    }
                    QueryType::MarketSells => {
                        self.count_market_sells(&after_fills, start, end).into()
                    }
                    QueryType::TakerTrades => {
                        self.count_taker_trades(&after_fills, start, end).into()
                    }
                };
                to_update.push(((*cached_end, end), after_fills));
                match count.as_mut() {
                    Some(c) => {
                        c.add(before_count);
                        c.add(after_count);
                    }
                    None => {
                        count = Some(before_count);
                        count.unwrap().add(after_count);
                    }
                }

                done = true;
                break;
            } else {
                continue;
            }
        }

        if !done {
            let fills = server::get_fills_api(start, end)?;

            let additional_count = match self.query_type {
                QueryType::TradingVolume => self.count_trading_volume(&fills, start, end).into(),
                QueryType::MarketBuys => self.count_market_buys(&fills, start, end).into(),
                QueryType::MarketSells => self.count_market_sells(&fills, start, end).into(),
                QueryType::TakerTrades => self.count_taker_trades(&fills, start, end).into(),
            };

            cache_lock.insert((start, end), fills);

            match count.as_mut() {
                Some(c) => {
                    c.add(additional_count);
                }
                None => {
                    count = Some(additional_count);
                }
            }
        }

        for (range, fills) in to_update {
            cache_lock.insert(range, fills);
        }

        Ok(count)
    }

    fn count_taker_trades(&self, fills: &[server::Fill], start: i64, end: i64) -> usize {
        fills
            .iter()
            .filter(|fill| {
                fill.time > DateTime::from_timestamp(start, 0).unwrap()
                    && fill.time <= DateTime::from_timestamp(end, 0).unwrap()
            })
            .map(|v| v.sequence_number)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    fn count_market_buys(&self, fills: &[server::Fill], start: i64, end: i64) -> usize {
        fills
            .iter()
            .filter(|fill| {
                fill.time > DateTime::from_timestamp(start, 0).unwrap()
                    && fill.time <= DateTime::from_timestamp(end, 0).unwrap()
            })
            .filter(|fill| fill.direction == 1)
            .map(|v| v.sequence_number)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    fn count_market_sells(&self, fills: &[server::Fill], start: i64, end: i64) -> usize {
        fills
            .iter()
            .filter(|fill| {
                fill.time > DateTime::from_timestamp(start, 0).unwrap()
                    && fill.time <= DateTime::from_timestamp(end, 0).unwrap()
            })
            .filter(|fill| fill.direction == -1)
            .map(|v| v.sequence_number)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    fn count_trading_volume(&self, fills: &[server::Fill], start: i64, end: i64) -> f64 {
        fills
            .iter()
            .filter(|fill| {
                fill.time > DateTime::from_timestamp(start, 0).unwrap()
                    && fill.time <= DateTime::from_timestamp(end, 0).unwrap()
            })
            .filter_map(|fill| (fill.price * fill.quantity).to_f64())
            .sum()
    }
}

type QueryCache = Arc<Mutex<HashMap<(i64, i64), Vec<server::Fill>>>>;

#[derive(Debug, Default)]
pub struct Processor {
    handles: Vec<JoinHandle<anyhow::Result<Option<Count>>>>,
    cache: QueryCache,
}

impl Drop for Processor {
    #[instrument]
    fn drop(&mut self) {
        for handle in self.handles.drain(..) {
            match handle.join() {
                Ok(Ok(Some(count))) => println!("{count}"),
                Ok(Ok(None)) => println!("0"),
                Ok(Err(e)) => error!("Failed to process query: {e:?}"),
                Err(e) => error!("Failed to join thread when dropping 'Processor': {e:?}"),
            }
        }
    }
}

impl Processor {
    pub fn new() -> Self {
        telemetry();
        Processor::default()
    }

    pub fn process_query(&mut self, query: String) {
        let cache = Arc::clone(&self.cache);

        let handle = thread::spawn(move || -> anyhow::Result<Option<Count>> {
            let query = match Query::from_str(&query) {
                Ok(query) => query,
                Err(e) => {
                    error!("Failed to parse query: {e}");
                    return Ok(None);
                }
            };
            let count = query.get_count(&cache)?;
            Ok(count)
        });
        self.handles.push(handle);
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    /// We print to stdout, so we run the tests with a `test` Makefile directive from the repository root.
    /// You can also run this test from the terminal using `$ make test`.
    #[test]
    fn test_test_input() {
        let output = Command::new("make").arg("test").output().unwrap();

        assert!(output.status.success())
    }

    #[test]
    fn test_input() {
        let output = Command::new("make").arg("test-input").output().unwrap();

        assert!(output.status.success())
    }
}
