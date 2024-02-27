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
use rust_decimal::prelude::ToPrimitive;

#[derive(Debug, Clone, Copy)]
pub enum Count {
    Trades(usize),
    Volume(f64),
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

#[derive(Debug)]
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
            .map_err(|e| anyhow!("Failed to parse start timestamp: {}", e))?;

        let end_timestamp_in_seconds = parts
            .next()
            .ok_or(anyhow::anyhow!("Missing end timestamp"))?
            .parse()
            .map_err(|e| anyhow!("Failed to parse end timestamp: {}", e))?;

        let range = TimeRange {
            start_timestamp_in_seconds,
            end_timestamp_in_seconds,
        };

        let query_type = match count {
            "C" => QueryType::TakerTrades,
            "B" => QueryType::MarketBuys,
            "S" => QueryType::MarketSells,
            "V" => QueryType::TradingVolume,
            _ => return Err(anyhow!("Invalid count request: {}", s)),
        };

        Ok(Query { query_type, range })
    }
}

impl Query {
    pub fn get_count(
        &self,
        cache: &Arc<Mutex<HashMap<(i64, i64), Count>>>,
    ) -> anyhow::Result<Count> {
        let cache = cache.lock().unwrap();

        match self.query_type {
            QueryType::TradingVolume => {
                for (cached_start, cached_end) in cache.keys() {
                    if *cached_start >= self.range.start_timestamp_in_seconds
                        && *cached_end <= self.range.end_timestamp_in_seconds
                    {
                        if let Some(existing_vol) = cache.get(&(*cached_start, *cached_end)) {
                            let before_fills = server::get_fills_api(
                                self.range.start_timestamp_in_seconds,
                                *cached_start,
                            )?;
                            let after_fills = server::get_fills_api(
                                *cached_end,
                                self.range.end_timestamp_in_seconds,
                            )?;

                            let before_count = self.count_trading_volume(&before_fills);
                            let after_count = self.count_trading_volume(&after_fills);

                            match existing_vol {
                                Count::Volume(vol) => {
                                    return Ok((vol + before_count + after_count).into())
                                }
                                Count::Trades(_) => unreachable!(),
                            }
                        }
                    }
                }
                drop(cache);
                let fills = self.get_fills_api()?;
                Ok(self.count_trading_volume(&fills).into())
            }
            QueryType::MarketBuys => {
                for (cached_start, cached_end) in cache.keys() {
                    if *cached_start >= self.range.start_timestamp_in_seconds
                        && *cached_end <= self.range.end_timestamp_in_seconds
                    {
                        if let Some(existing_vol) = cache.get(&(*cached_start, *cached_end)) {
                            let before_fills = server::get_fills_api(
                                self.range.start_timestamp_in_seconds,
                                *cached_start,
                            )?;
                            let after_fills = server::get_fills_api(
                                *cached_end,
                                self.range.end_timestamp_in_seconds,
                            )?;

                            let before_count = self.count_market_buys(&before_fills);
                            let after_count = self.count_market_buys(&after_fills);

                            match existing_vol {
                                Count::Trades(vol) => {
                                    return Ok((vol + before_count + after_count).into())
                                }
                                Count::Volume(_) => unreachable!(),
                            }
                        }
                    }
                }
                drop(cache);
                let fills = self.get_fills_api()?;
                Ok(self.count_market_buys(&fills).into())
            }
            QueryType::MarketSells => {
                for (cached_start, cached_end) in cache.keys() {
                    if *cached_start >= self.range.start_timestamp_in_seconds
                        && *cached_end <= self.range.end_timestamp_in_seconds
                    {
                        if let Some(existing_vol) = cache.get(&(*cached_start, *cached_end)) {
                            let before_fills = server::get_fills_api(
                                self.range.start_timestamp_in_seconds,
                                *cached_start,
                            )?;
                            let after_fills = server::get_fills_api(
                                *cached_end,
                                self.range.end_timestamp_in_seconds,
                            )?;

                            let before_count = self.count_market_sells(&before_fills);
                            let after_count = self.count_market_sells(&after_fills);

                            match existing_vol {
                                Count::Trades(vol) => {
                                    return Ok((vol + before_count + after_count).into())
                                }
                                Count::Volume(_) => unreachable!(),
                            }
                        }
                    }
                }
                drop(cache);
                let fills = self.get_fills_api()?;
                Ok(self.count_market_sells(&fills).into())
            }
            QueryType::TakerTrades => {
                for (cached_start, cached_end) in cache.keys() {
                    if *cached_start >= self.range.start_timestamp_in_seconds
                        && *cached_end <= self.range.end_timestamp_in_seconds
                    {
                        if let Some(existing_vol) = cache.get(&(*cached_start, *cached_end)) {
                            let before_fills = server::get_fills_api(
                                self.range.start_timestamp_in_seconds,
                                *cached_start,
                            )?;
                            let after_fills = server::get_fills_api(
                                *cached_end,
                                self.range.end_timestamp_in_seconds,
                            )?;

                            let before_count = self.count_taker_trades(&before_fills);
                            let after_count = self.count_taker_trades(&after_fills);

                            match existing_vol {
                                Count::Trades(vol) => {
                                    return Ok((vol + before_count + after_count).into())
                                }
                                Count::Volume(_) => unreachable!(),
                            }
                        }
                    }
                }
                drop(cache);
                let fills = self.get_fills_api()?;
                Ok(self.count_taker_trades(&fills).into())
            }
        }
    }

    fn get_fills_api(&self) -> anyhow::Result<Vec<server::Fill>> {
        let (start, end) = (
            self.range.start_timestamp_in_seconds,
            self.range.end_timestamp_in_seconds,
        );
        server::get_fills_api(start, end)
    }

    fn count_taker_trades(&self, fills: &[server::Fill]) -> usize {
        fills
            .iter()
            .map(|v| v.sequence_number)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    fn count_market_buys(&self, fills: &[server::Fill]) -> usize {
        fills
            .iter()
            .filter(|fill| fill.direction == 1)
            .map(|v| v.sequence_number)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    fn count_market_sells(&self, fills: &[server::Fill]) -> usize {
        fills
            .iter()
            .filter(|fill| fill.direction == -1)
            .map(|v| v.sequence_number)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    fn count_trading_volume(&self, fills: &[server::Fill]) -> f64 {
        fills
            .iter()
            .filter_map(|fill| (fill.price * fill.quantity).to_f64())
            .sum()
    }
}

#[derive(Default)]
pub struct Processor {
    handles: Vec<JoinHandle<Count>>,
    cache: QueryCache,
}

impl Drop for Processor {
    fn drop(&mut self) {
        for handle in self.handles.drain(..) {
            match handle.join() {
                Ok(count) => println!("{}", count),
                Err(e) => eprintln!("Failed to join thread when dropping 'Processor': {:?}", e),
            }
        }
    }
}

impl Processor {
    pub fn new() -> Self {
        Processor::default()
    }

    pub fn process_query(&mut self, query: String) {
        let query = Query::from_str(&query).unwrap();

        let cache = match query.query_type {
            QueryType::MarketBuys => Arc::clone(&self.cache.buys),
            QueryType::MarketSells => Arc::clone(&self.cache.sells),
            QueryType::TakerTrades => Arc::clone(&self.cache.trades),
            QueryType::TradingVolume => Arc::clone(&self.cache.volume),
        };

        let handle = thread::spawn(move || {
            let count = query.get_count(&cache).unwrap();
            let mut cache = cache.lock().unwrap();
            cache.insert(
                (
                    query.range.start_timestamp_in_seconds,
                    query.range.end_timestamp_in_seconds,
                ),
                count,
            );
            count
        });
        self.handles.push(handle);
    }
}

// Define a tolerance value for comparing time ranges
// const TIME_RANGE_TOLERANCE: i64 = 3600;

#[derive(Default)]
struct QueryCache {
    buys: Arc<Mutex<HashMap<(i64, i64), Count>>>,
    sells: Arc<Mutex<HashMap<(i64, i64), Count>>>,
    trades: Arc<Mutex<HashMap<(i64, i64), Count>>>,
    volume: Arc<Mutex<HashMap<(i64, i64), Count>>>,
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
