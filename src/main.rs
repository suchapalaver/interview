pub mod server;

fn main() -> anyhow::Result<()> {
    let processor = Processor::new();
    let start_time = Instant::now();

    for (i, query) in io::stdin().lines().enumerate() {
        processor.process_query(query?, i)?;
    }

    let end_time = Instant::now();
    let duration = end_time - start_time;
    println!("Time taken: {:?}", duration);
    Ok(())
}

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~ YOUR CODE HERE ~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

use std::{
    io,
    str::FromStr,
    sync::mpsc::{Receiver, Sender},
    thread::{self},
    time::Instant,
};

use rust_decimal::prelude::ToPrimitive;

pub struct Processor {
    sender: Option<Sender<(String, usize)>>, // Tuple containing query and sequence number
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for Processor {
    fn drop(&mut self) {
        let _ = self.sender.take();
        if let Some(handle) = self.handle.take() {
            handle.join().unwrap();
        }
    }
}

impl Processor {
    pub fn new() -> Self {
        let (sender, receiver) = std::sync::mpsc::channel::<(String, usize)>();

        let handle = thread::spawn(move || {
            Processor::receiver_loop(receiver);
        });

        Processor {
            sender: Some(sender),
            handle: Some(handle),
        }
    }

    fn receiver_loop(receiver: Receiver<(String, usize)>) {
        let mut results = Vec::new();

        while let Ok((query, seq)) = receiver.recv() {
            let handle = thread::spawn(move || {
                let query = Query::from_str(&query).unwrap();
                query.get_count().unwrap()
            });
            results.push((handle, seq));
        }

        results.sort_by_key(|&(_, seq)| seq);
        for (result, _) in results {
            eprintln!("{}", result.join().unwrap());
        }
    }

    pub fn process_query(&self, query: String, seq: usize) -> anyhow::Result<()> {
        match self.sender {
            Some(ref sender) => {
                sender.send((query, seq))?;
            }
            None => return Err(anyhow::anyhow!("Processor has been dropped")),
        }
        Ok(())
    }
}

impl Default for Processor {
    fn default() -> Self {
        Self::new()
    }
}

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
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split_whitespace();

        let count = parts.next().ok_or("Missing count")?;

        let start_timestamp_in_seconds = parts
            .next()
            .ok_or("Missing start timestamp")?
            .parse()
            .map_err(|e| format!("Failed to parse start timestamp: {}", e))?;

        let end_timestamp_in_seconds = parts
            .next()
            .ok_or("Missing end timestamp")?
            .parse()
            .map_err(|e| format!("Failed to parse end timestamp: {}", e))?;

        let time_range = TimeRange {
            start_timestamp_in_seconds,
            end_timestamp_in_seconds,
        };

        let query_type = match count {
            "C" => QueryType::TakerTrades,
            "B" => QueryType::MarketBuys,
            "S" => QueryType::MarketSells,
            "V" => QueryType::TradingVolume,
            _ => return Err("Invalid count request: {s}".to_string()),
        };

        Ok(Query {
            query_type,
            range: time_range,
        })
    }
}

impl Query {
    pub fn get_count(self) -> anyhow::Result<Count> {
        let fills = self.get_fills_api()?;
        match self.query_type {
            QueryType::TakerTrades => Ok(self.count_taker_trades(fills).into()),
            QueryType::MarketBuys => Ok(self.count_market_buys(fills).into()),
            QueryType::MarketSells => Ok(self.count_market_sells(fills).into()),
            QueryType::TradingVolume => Ok(self.count_trading_volume(fills).into()),
        }
    }

    fn get_fills_api(&self) -> anyhow::Result<Vec<server::Fill>> {
        let (start, end) = (
            self.range.start_timestamp_in_seconds,
            self.range.end_timestamp_in_seconds,
        );
        server::get_fills_api(start, end)
    }

    fn count_taker_trades(&self, fills: Vec<server::Fill>) -> usize {
        fills
            .iter()
            .map(|v| v.sequence_number)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    fn count_market_buys(&self, fills: Vec<server::Fill>) -> usize {
        fills
            .iter()
            .filter(|fill| fill.direction == 1)
            .map(|v| v.sequence_number)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    fn count_market_sells(&self, fills: Vec<server::Fill>) -> usize {
        fills
            .iter()
            .filter(|fill| fill.direction == -1)
            .map(|v| v.sequence_number)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    fn count_trading_volume(&self, fills: Vec<server::Fill>) -> f64 {
        fills
            .iter()
            .filter_map(|fill| (fill.price * fill.quantity).to_f64())
            .sum()
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
