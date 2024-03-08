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
    str::FromStr,
    thread::{self, JoinHandle},
};

use anyhow::anyhow;
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

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct TimeRange {
    start_timestamp_in_seconds: i64,
    end_timestamp_in_seconds: i64,
}

impl TimeRange {
    fn new(start_timestamp_in_seconds: i64, end_timestamp_in_seconds: i64) -> Self {
        Self {
            start_timestamp_in_seconds,
            end_timestamp_in_seconds,
        }
    }
}

impl From<(i64, i64)> for TimeRange {
    fn from(range: (i64, i64)) -> Self {
        TimeRange::new(range.0, range.1)
    }
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
    fn count_from_range(&self, fills: &[server::Fill]) -> Count {
        match self.query_type {
            QueryType::TradingVolume => fills.trading_volume().into(),
            QueryType::MarketBuys => fills.market_buys().into(),
            QueryType::MarketSells => fills.market_sells().into(),
            QueryType::TakerTrades => fills.taker_trades().into(),
        }
    }

    pub fn get_count(&self) -> anyhow::Result<Count> {
        let fills = server::get_fills_api(
            self.range.start_timestamp_in_seconds,
            self.range.end_timestamp_in_seconds,
        )?;
        let count = self.count_from_range(&fills);

        Ok(count)
    }
}

trait CountFilter {
    fn filter_fills<F>(&self, filter_func: F) -> usize
    where
        F: Fn(&server::Fill) -> bool;

    fn taker_trades(&self) -> usize;
    fn market_buys(&self) -> usize;
    fn market_sells(&self) -> usize;
    fn trading_volume(&self) -> f64;
}

impl CountFilter for &[server::Fill] {
    fn filter_fills<F>(&self, filter_func: F) -> usize
    where
        F: Fn(&server::Fill) -> bool,
    {
        self.iter()
            .filter(|fill| filter_func(fill))
            .map(|v| v.sequence_number)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    fn taker_trades(&self) -> usize {
        self.filter_fills(|_| true)
    }

    fn market_buys(&self) -> usize {
        self.filter_fills(|fill| fill.direction == 1)
    }

    fn market_sells(&self) -> usize {
        self.filter_fills(|fill| fill.direction == -1)
    }

    fn trading_volume(&self) -> f64 {
        self.iter()
            .filter_map(|fill| (fill.price * fill.quantity).to_f64())
            .sum()
    }
}

type CountHandles = Vec<JoinHandle<anyhow::Result<Option<Count>>>>;

pub struct Processor {
    handles: CountHandles,
}

impl Drop for Processor {
    #[instrument(skip(self))]
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
        }
    }

    pub fn process_query(&mut self, query: String) {
        let handle = thread::spawn(move || -> anyhow::Result<Option<Count>> {
            let query = match Query::from_str(&query) {
                Ok(query) => query,
                Err(e) => {
                    error!("Failed to parse query: {e}");
                    return Ok(None);
                }
            };
            let count = query.get_count()?;
            Ok(Some(count))
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
