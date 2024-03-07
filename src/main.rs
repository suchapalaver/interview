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

use std::num::NonZeroUsize;
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
};

use anyhow::anyhow;
use chrono::DateTime;
use lru::LruCache;
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
    fn count_from_range(&self, fills: &[server::Fill], start: i64, end: i64) -> Count {
        match self.query_type {
            QueryType::TradingVolume => fills.trading_volume(start, end).into(),
            QueryType::MarketBuys => fills.market_buys(start, end).into(),
            QueryType::MarketSells => fills.market_sells(start, end).into(),
            QueryType::TakerTrades => fills.taker_trades(start, end).into(),
        }
    }

    pub fn get_count(&self, cache: &QueryCache) -> anyhow::Result<Option<Count>> {
        let mut cache_lock = cache.lock().unwrap();
        let (mut start, mut end) = (
            self.range.start_timestamp_in_seconds,
            self.range.end_timestamp_in_seconds,
        );
        let mut count: Option<Count> = None;
        let mut to_update = Vec::new();
        let mut done = false;

        for ((cached_start, cached_end), fills) in cache_lock.iter() {
            if start <= *cached_end && end >= *cached_start {
                let cached_count = self.count_from_range(fills, start, end);

                if let Some(c) = count.as_mut() {
                    c.add(cached_count);
                } else {
                    count = Some(cached_count);
                }

                if start >= *cached_start && end <= *cached_end {
                    done = true;
                    break;
                } else if start <= *cached_start && end >= *cached_end {
                    let before_fills = server::get_fills_api(start, *cached_start)?;
                    let after_fills = server::get_fills_api(*cached_end, end)?;

                    let before_count = self.count_from_range(&before_fills, start, end);
                    to_update.push(((start, *cached_start), before_fills));

                    let after_count = self.count_from_range(&after_fills, start, end);
                    to_update.push(((*cached_end, end), after_fills));

                    if let Some(c) = count.as_mut() {
                        c.add(before_count);
                        c.add(after_count);
                    } else {
                        count = Some(before_count);
                        count.unwrap().add(after_count);
                    }

                    done = true;
                    break;
                } else if start <= *cached_start && end <= *cached_end {
                    end = *cached_start;
                    continue;
                } else if start >= *cached_start && end >= *cached_end {
                    start = *cached_end;
                    continue;
                }
            }
        }

        if !done {
            let fills = server::get_fills_api(start, end)?;

            let additional_count = self.count_from_range(&fills, start, end);

            cache_lock.put((start, end), fills);

            if let Some(c) = count.as_mut() {
                c.add(additional_count);
            } else {
                count = Some(additional_count);
            }
        }

        for (range, fills) in to_update {
            cache_lock.put(range, fills);
        }

        Ok(count)
    }
}

trait CountFilter {
    fn filter_fills<F>(&self, start: i64, end: i64, filter_func: F) -> usize
    where
        F: Fn(&server::Fill) -> bool;

    fn taker_trades(&self, start: i64, end: i64) -> usize;
    fn market_buys(&self, start: i64, end: i64) -> usize;
    fn market_sells(&self, start: i64, end: i64) -> usize;
    fn trading_volume(&self, start: i64, end: i64) -> f64;
}

impl CountFilter for &[server::Fill] {
    fn filter_fills<F>(&self, start: i64, end: i64, filter_func: F) -> usize
    where
        F: Fn(&server::Fill) -> bool,
    {
        self.iter()
            .filter(|fill| {
                fill.time > DateTime::from_timestamp(start, 0).unwrap()
                    && fill.time <= DateTime::from_timestamp(end, 0).unwrap()
            })
            .filter(|fill| filter_func(fill))
            .map(|v| v.sequence_number)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    fn taker_trades(&self, start: i64, end: i64) -> usize {
        self.filter_fills(start, end, |_| true)
    }

    fn market_buys(&self, start: i64, end: i64) -> usize {
        self.filter_fills(start, end, |fill| fill.direction == 1)
    }

    fn market_sells(&self, start: i64, end: i64) -> usize {
        self.filter_fills(start, end, |fill| fill.direction == -1)
    }

    fn trading_volume(&self, start: i64, end: i64) -> f64 {
        self.iter()
            .filter(|fill| {
                fill.time > DateTime::from_timestamp(start, 0).unwrap()
                    && fill.time <= DateTime::from_timestamp(end, 0).unwrap()
            })
            .filter_map(|fill| (fill.price * fill.quantity).to_f64())
            .sum()
    }
}

type CountHandles = Vec<JoinHandle<anyhow::Result<Option<Count>>>>;

type QueryCache = Arc<Mutex<LruCache<(i64, i64), Vec<server::Fill>>>>;

const CACHE_SIZE: usize = 10_000;

#[derive(Debug)]
pub struct Processor {
    handles: CountHandles,
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

impl Default for Processor {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor {
    pub fn new() -> Self {
        telemetry();
        Processor {
            handles: Vec::new(),
            cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(CACHE_SIZE).unwrap(),
            ))),
        }
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
