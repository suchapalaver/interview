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
    num::NonZeroUsize,
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

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
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
    fn count_from_range(&self, fills: &[server::Fill], range: TimeRange) -> Count {
        match self.query_type {
            QueryType::TradingVolume => fills.trading_volume(range).into(),
            QueryType::MarketBuys => fills.market_buys(range).into(),
            QueryType::MarketSells => fills.market_sells(range).into(),
            QueryType::TakerTrades => fills.taker_trades(range).into(),
        }
    }

    fn time_slots_map(&self) -> HashMap<i64, (i64, i64)> {
        let mut time_slots_map = HashMap::new();
        let mut current_time = self.range.start_timestamp_in_seconds;
        let slot_size = SLOT_SIZE;

        while current_time < self.range.end_timestamp_in_seconds {
            let half_hour_slot = current_time / slot_size * slot_size;
            let mut next_time = half_hour_slot + slot_size;
            if next_time > self.range.end_timestamp_in_seconds {
                next_time = self.range.end_timestamp_in_seconds;
            }
            time_slots_map.insert(half_hour_slot, (current_time, next_time));
            current_time = next_time;
        }
        time_slots_map
    }

    pub fn get_count(&self, cache: &QueryCache) -> anyhow::Result<Option<Count>> {
        let mut count: Option<Count> = None;

        let time_slots_map = self.time_slots_map();

        for (time_slot, (query_start, query_end)) in &time_slots_map {
            let mut query_start = *query_start;
            let mut query_end = *query_end;
            let mut to_update: Vec<(TimeRange, Vec<server::Fill>)> = Vec::new();
            let mut done = false;
            {
                let mut cache_lock = cache.0.lock().unwrap();

                if let Some(cached_range_fill_map) = cache_lock.get(time_slot) {
                    for (cached_range, fills) in cached_range_fill_map.iter() {
                        if query_start <= cached_range.end_timestamp_in_seconds
                            && query_end >= cached_range.start_timestamp_in_seconds
                        {
                            if query_start >= cached_range.start_timestamp_in_seconds
                                && query_end <= cached_range.end_timestamp_in_seconds
                            {
                                let cached_count =
                                    self.count_from_range(fills, (query_start, query_end).into());

                                if let Some(c) = count.as_mut() {
                                    c.add(cached_count);
                                } else {
                                    count = Some(cached_count);
                                }

                                // Break out if the query range is fully covered by the cached range.
                                done = true;
                                break;
                            } else if query_start <= cached_range.start_timestamp_in_seconds
                                && query_end >= cached_range.end_timestamp_in_seconds
                            {
                                let cached_count = self.count_from_range(
                                    fills,
                                    (
                                        cached_range.start_timestamp_in_seconds,
                                        cached_range.end_timestamp_in_seconds,
                                    )
                                        .into(),
                                );

                                if let Some(c) = count.as_mut() {
                                    c.add(cached_count);
                                } else {
                                    count = Some(cached_count);
                                }

                                // Split the query range into before and after the cached range.
                                let before_fills = server::get_fills_api(
                                    query_start,
                                    cached_range.start_timestamp_in_seconds,
                                )?;
                                let after_fills = server::get_fills_api(
                                    cached_range.end_timestamp_in_seconds,
                                    query_end,
                                )?;

                                let before_count = self.count_from_range(
                                    &before_fills,
                                    (query_start, cached_range.start_timestamp_in_seconds).into(),
                                );
                                let after_count = self.count_from_range(
                                    &after_fills,
                                    (cached_range.end_timestamp_in_seconds, query_end).into(),
                                );

                                to_update.push((
                                    (query_start, cached_range.start_timestamp_in_seconds).into(),
                                    before_fills,
                                ));
                                to_update.push((
                                    (cached_range.end_timestamp_in_seconds, query_end).into(),
                                    after_fills,
                                ));

                                if let Some(c) = count.as_mut() {
                                    c.add(before_count);
                                    c.add(after_count);
                                } else {
                                    count = Some(before_count);
                                    count.unwrap().add(after_count);
                                }

                                // Break out after processing the split ranges.
                                done = true;
                                break;
                            } else if query_start <= cached_range.start_timestamp_in_seconds
                                && query_end <= cached_range.end_timestamp_in_seconds
                            {
                                let cached_count = self.count_from_range(
                                    fills,
                                    (cached_range.start_timestamp_in_seconds, query_end).into(),
                                );

                                if let Some(c) = count.as_mut() {
                                    c.add(cached_count);
                                } else {
                                    count = Some(cached_count);
                                }

                                // Update query range to exclude the part that is already cached.
                                query_end = cached_range.start_timestamp_in_seconds;
                                continue;
                            } else if query_start >= cached_range.start_timestamp_in_seconds
                                && query_end >= cached_range.end_timestamp_in_seconds
                            {
                                let cached_count = self.count_from_range(
                                    fills,
                                    (query_start, cached_range.end_timestamp_in_seconds).into(),
                                );

                                if let Some(c) = count.as_mut() {
                                    c.add(cached_count);
                                } else {
                                    count = Some(cached_count);
                                }

                                // Update query range to exclude the part that is already cached.
                                query_start = cached_range.end_timestamp_in_seconds;
                                continue;
                            }
                        }
                    }
                }
            }

            if !done {
                let remaining_fills = server::get_fills_api(query_start, query_end)?;

                let range = (query_start, query_end).into();

                let remaining_count = self.count_from_range(&remaining_fills, range);

                if let Some(c) = count.as_mut() {
                    c.add(remaining_count);
                } else {
                    count = Some(remaining_count);
                }

                {
                    let mut cache_lock = cache.0.lock().unwrap();

                    if let Some(cache_entry) = cache_lock.get_mut(time_slot) {
                        cache_entry.insert(range, remaining_fills);
                    } else {
                        let mut new_cache_entry = HashMap::new();
                        new_cache_entry.insert(range, remaining_fills);
                        cache_lock.put(*time_slot, new_cache_entry);
                    }
                }
            }

            if !to_update.is_empty() {
                let mut cache_lock = cache.0.lock().unwrap();

                if let Some(cache_entry) = cache_lock.get_mut(time_slot) {
                    for (range, fills) in to_update {
                        cache_entry.insert(range, fills);
                    }
                }
            }
        }

        Ok(count)
    }
}

trait CountFilter {
    fn filter_fills<F>(&self, range: TimeRange, filter_func: F) -> usize
    where
        F: Fn(&server::Fill) -> bool;

    fn taker_trades(&self, range: TimeRange) -> usize;
    fn market_buys(&self, range: TimeRange) -> usize;
    fn market_sells(&self, range: TimeRange) -> usize;
    fn trading_volume(&self, range: TimeRange) -> f64;
}

impl CountFilter for &[server::Fill] {
    fn filter_fills<F>(&self, range: TimeRange, filter_func: F) -> usize
    where
        F: Fn(&server::Fill) -> bool,
    {
        self.iter()
            .filter(|fill| {
                fill.time > DateTime::from_timestamp(range.start_timestamp_in_seconds, 0).unwrap()
                    && fill.time
                        <= DateTime::from_timestamp(range.end_timestamp_in_seconds, 0).unwrap()
            })
            .filter(|fill| filter_func(fill))
            .map(|v| v.sequence_number)
            .collect::<std::collections::HashSet<_>>()
            .len()
    }

    fn taker_trades(&self, range: TimeRange) -> usize {
        self.filter_fills(range, |_| true)
    }

    fn market_buys(&self, range: TimeRange) -> usize {
        self.filter_fills(range, |fill| fill.direction == 1)
    }

    fn market_sells(&self, range: TimeRange) -> usize {
        self.filter_fills(range, |fill| fill.direction == -1)
    }

    fn trading_volume(&self, range: TimeRange) -> f64 {
        self.iter()
            .filter(|fill| {
                fill.time > DateTime::from_timestamp(range.start_timestamp_in_seconds, 0).unwrap()
                    && fill.time
                        <= DateTime::from_timestamp(range.end_timestamp_in_seconds, 0).unwrap()
            })
            .filter_map(|fill| (fill.price * fill.quantity).to_f64())
            .sum()
    }
}

type CountHandles = Vec<JoinHandle<anyhow::Result<Option<Count>>>>;

type Cache = LruCache<Slot, HashMap<TimeRange, Vec<server::Fill>>>;
struct QueryCache(Arc<Mutex<Cache>>);
const CACHE_SIZE: usize = 10_000;

type Slot = i64;
const SLOT_SIZE: i64 = 4500;

pub struct Processor {
    handles: CountHandles,
    cache: QueryCache,
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
            cache: QueryCache(Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(CACHE_SIZE).unwrap(),
            )))),
        }
    }

    pub fn process_query(&mut self, query: String) {
        let cache = QueryCache(Arc::clone(&self.cache.0));

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
