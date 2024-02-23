use std::io;

use crate::server::get_fills_api;

pub mod request;
pub mod server;

fn main() -> anyhow::Result<()> {
    let mut processor = Processor::new();
    for query in io::stdin().lines() {
        processor.process_query(query?);
    }
    Ok(())
}

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~ YOUR CODE HERE ~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

pub struct Processor {
    // TODO
}

impl Processor {
    pub fn new() -> Self {
        Processor {}
    }

    pub fn process_query(&mut self, query: String) {
        // TODO
        // parse query
        println!("{}", query);
    }
}
