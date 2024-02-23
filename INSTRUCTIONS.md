### Ellipsis Labs Technical Challenge

The goal of this task is to implement a "proxy" API server for Phoenix trades. Please read over the instructions carefully.

Your final deliverable should contain the following:

- Clear source code that compiles, runs out of the box, and produces the correct output.
- A detailed README.md file that describes your assumptions, your design choices, and the tradeoffs you considered when solving the challenge.
- The README should also contain instructions for how to run the code.

**Your writeup is an important part of the submission!**


#### Program Input
The program is given a list of input queries with the following format:

```
QUERY_TYPE START_TIME END_TIME
```

`QUERY_TYPE` will either be `C`, `B`, `S`, or `V`. The server should print the following for each query type respectively:

`C`: Prints the count of all of the taker trades in the given time range (> start, <= end)

`B`: Prints the count of all of the market buys in the given time range (> start, <= end)

`S`: Prints the count of all of the market sells in the given time range (> start, <= end)

`V`: Prints the total trading volume in USD in given time range (> start, <= end)

`START_TIME` is a unix timestamp in seconds. You should only look at trades that occurred after this time

`END_TIME` is a unix timestamp in seconds. You should only look at trades that occurred before and at this time


#### Constraints:
- END_TIME - START_TIME <= 3600
- All time inputs will always be in the time range of the available trade data
- A _taker trade_ is uniquely identified by a sequence number. If 2 fills have the same sequence number, it means that they are associated with the same taker trade. (Note: market buys and market sells are the 2 types of taker trades)

It's useful to consider the distribution and frequency of trades on a Phoenix market before designing a solution.

Your proxy server is given access to a black box function with the following interface:

```rust
pub fn get_fills_api(
    start_timestamp_in_seconds: i64,
    end_timestamp_in_seconds: i64,
) -> anyhow::Result<Vec<Fill>>
```

The `Fill` struct is defined as follows:

```rust
pub struct Fill {
    pub time: DateTime<Utc>,
    /// -1 for sell and 1 for buy
    pub direction: i32,
    pub price: Decimal,
    pub quantity: Decimal,
    pub sequence_number: u64,
}
```

**Calls to the black box function can potentially be expensive.**

#### To run the program:

```
cat input.txt | cargo run
```

#### Test input

Input:

```
C 1701386660 1701388446
V 1701255344 1701257371
B 1701100629 1701103957
V 1700849561 1700851499
B 1701308094 1701308363
B 1701194626 1701195835
C 1701082476 1701085013
B 1700854470 1700855525
S 1700890509 1700892422
B 1700845439 1700848389
```

Expected Output:

```
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
```

To test:

```
cat test_input.txt | cargo run
```
