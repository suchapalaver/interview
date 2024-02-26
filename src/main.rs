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

enum Count {
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
    fn get_count(&self) -> anyhow::Result<Count> {
        let fills = self.get_fills_api()?;
        let count = match self.query_type {
            QueryType::TakerTrades => self.count_taker_trades(&fills).into(),
            QueryType::MarketBuys => self.count_market_buys(&fills).into(),
            QueryType::MarketSells => self.count_market_sells(&fills).into(),
            QueryType::TradingVolume => self.count_trading_volume(&fills).into(),
        };
        Ok(count)
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
    handles: Vec<JoinHandle<anyhow::Result<Count>>>,
}

impl Drop for Processor {
    fn drop(&mut self) {
        for handle in self.handles.drain(..) {
            match handle.join() {
                Ok(Ok(count)) => println!("{count}"),
                Ok(Err(e)) => eprintln!("Failed to process query: {e:?}"),
                Err(e) => eprintln!("Failed to join thread when dropping 'Processor': {e:?}"),
            }
        }
    }
}

impl Processor {
    pub fn new() -> Self {
        Processor::default()
    }

    pub fn process_query(&mut self, query: String) {
        let handle = thread::spawn(move || -> anyhow::Result<Count> {
            let query = Query::from_str(&query)?;
            query.get_count()
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
