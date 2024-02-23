pub mod server;

fn main() -> anyhow::Result<()> {
    let mut processor = Processor::new();
    for query in io::stdin().lines() {
        processor.process_query(query?)?;
    }
    Ok(())
}

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~ YOUR CODE HERE ~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

use std::{io, str::FromStr};

use rust_decimal::prelude::ToPrimitive;

#[derive(Default)]
pub struct Processor;

impl Processor {
    pub fn new() -> Self {
        Processor
    }

    pub fn process_query(&mut self, query: String) -> anyhow::Result<Count> {
        let query = Query::from_str(&query).map_err(|err| anyhow::anyhow!(err))?;
        let count = query.get_count().map_err(|err| anyhow::anyhow!(err))?;
        println!("{}", count);
        Ok(count)
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

#[derive(Debug)]
enum Query {
    TakerTrades {
        start_timestamp_in_seconds: i64,
        end_timestamp_in_seconds: i64,
    },
    MarketBuys {
        start_timestamp_in_seconds: i64,
        end_timestamp_in_seconds: i64,
    },
    MarketSells {
        start_timestamp_in_seconds: i64,
        end_timestamp_in_seconds: i64,
    },
    TradingVolume {
        start_timestamp_in_seconds: i64,
        end_timestamp_in_seconds: i64,
    },
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

        match count {
            "C" => Ok(Query::TakerTrades {
                start_timestamp_in_seconds,
                end_timestamp_in_seconds,
            }),
            "B" => Ok(Query::MarketBuys {
                start_timestamp_in_seconds,
                end_timestamp_in_seconds,
            }),
            "S" => Ok(Query::MarketSells {
                start_timestamp_in_seconds,
                end_timestamp_in_seconds,
            }),
            "V" => Ok(Query::TradingVolume {
                start_timestamp_in_seconds,
                end_timestamp_in_seconds,
            }),
            _ => Err("Invalid count request: {s}".to_string()),
        }
    }
}

impl Query {
    pub fn get_count(self) -> Result<Count, String> {
        let fills = self.get_fills_api()?;
        match self {
            Query::TakerTrades { .. } => Ok(self.count_taker_trades(fills).into()),
            Query::MarketBuys { .. } => Ok(self.count_market_buys(fills).into()),
            Query::MarketSells { .. } => Ok(self.count_market_sells(fills).into()),
            Query::TradingVolume { .. } => Ok(self.count_trading_volume(fills).into()),
        }
    }

    fn get_fills_api(&self) -> Result<Vec<server::Fill>, String> {
        let (start, end) = match self {
            Query::TakerTrades {
                start_timestamp_in_seconds,
                end_timestamp_in_seconds,
            }
            | Query::MarketBuys {
                start_timestamp_in_seconds,
                end_timestamp_in_seconds,
            }
            | Query::MarketSells {
                start_timestamp_in_seconds,
                end_timestamp_in_seconds,
            }
            | Query::TradingVolume {
                start_timestamp_in_seconds,
                end_timestamp_in_seconds,
            } => (*start_timestamp_in_seconds, *end_timestamp_in_seconds),
        };
        server::get_fills_api(start, end).map_err(|err| err.to_string())
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
    use std::{
        fs::File,
        io::{BufRead, BufReader, Write},
    };

    use tempfile::NamedTempFile;

    use crate::Processor;

    fn create_temporary_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("Failed to create temporary file");
        file.write_all(content.as_bytes())
            .expect("Failed to write to temporary file");
        file
    }

    fn test_read_file_contents() -> BufReader<File> {
        let file_content = "C 1701386660 1701388446\n\
                            V 1701255344 1701257371\n\
                            B 1701100629 1701103957\n\
                            V 1700849561 1700851499\n\
                            B 1701308094 1701308363\n\
                            B 1701194626 1701195835\n\
                            C 1701082476 1701085013\n\
                            B 1700854470 1700855525\n\
                            S 1700890509 1700892422\n\
                            B 1700845439 1700848389\n";

        let temp_file = create_temporary_file(file_content);
        let file = temp_file.reopen().unwrap();
        BufReader::new(file)
    }

    fn process_input(processor: &mut Processor, input: BufReader<File>) -> Vec<String> {
        let mut output = Vec::new();
        for query in input.lines().map(|l| l.unwrap()) {
            output.push(processor.process_query(query).unwrap().to_string());
        }
        output
    }

    #[test]
    fn test_processor_with_test_input() {
        let mut processor = Processor::new();
        let input = test_read_file_contents();
        let output = process_input(&mut processor, input);
        insta::assert_display_snapshot!(output.join("\n"), @r###"
        249
        473024.288315
        414
        192122.227366
        3
        345
        714
        115
        141
        482
        "###);
    }
}
