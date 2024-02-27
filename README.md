# Interview

## Assumptions about Limitations

### Synchronous `main`

There many points at which I was tempted to work above the line we see below
the `main` function here.

```rust
fn main() -> anyhow::Result<()> {
    let mut processor = Processor::new();
    for query in io::stdin().lines() {
        processor.process_query(query?);
    }
    Ok(())
}

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~ YOUR CODE HERE ~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
```

The current iteration of this application uses the Rust standard library's
threading model, but I found myself wanting some of the functionality of `tokio`'s
channels, especially for exploring ways of alleviating the throttling effects of
printing output to stdout.

It was also tempting to allow errors potentially occuring in the code that I wrote
to "bubble up" to `main` by adding a single `?` sigis, but I decided the rules were
the rules in this case.

### Printing output to `stdout`

Since the [instructions](INSTRUCTIONS.md#to-run-the-program) specify running
the program from the command line, I took it as a restriction that we needed to
somehow work around the trade-offs brought about by needing to make blocking
calls to `println!`. 

I am aware of ways of implementing a writer with access to a handle to stdout,
however a further restriction I took to be implied by the [testing instructions](./INSTRUCTIONS.md#test-input)
was that results needed to be output in the order in which they were received.

The main outcome of these considerations were the somewhat unorthodox - you
read differing opinions - `Drop` implementation on our `Processor` struct, which
calls the `join` method on the `JoinHandles` to the query-processing threads. If those
threads had already somehow gone out of scope we might expect some confusing issues.
This was a trade-off accepted in an attempt to allow asynchronous threads to progress
as long as they could in the background before calling `join` on each of them in sequence
as the `Processor` went out of scope.  

### Time

With more time available I would have liked to use Rust's [`criterion`](https://docs.rs/criterion/latest/criterion/)
library for more robust benchmarking than simply running the application using the `time` Linux tool:

```terminal
time cat input.txt | cargo run
```

In retrospect this might have been worth pursuing from the get-go since the rough
benchmarking I've been using only gave a vague and sometimes confusing sense of the benefits
or lack thereof that different tweaks were making.

The two greatest impacts on performance were processing the queries asynchronously,
although this presented challenges related to [printing to stdout](#printing-output-to-stdout),
and introducing a rudimentary caching approach.

## Asynchronous Threads

Again, partly because of the assumed restriction of working with a [synchronous runtime](#synchronous-main),
for now I stuck with using the Rust standard library's [threading model](https://doc.rust-lang.org/std/thread/).
I also made this decision after trying various solutions with `tokio` and `crossbeam` channels -
I was mainly interested in exploring the use of `oneshot` channels for returning query results along with
an enumerated sequence index, but what I had time to try out did not move the needle much at all
as far as performance was concerned.

## Query Caching

I fully acknowledge a gaping hole where I would like there to be a whole lot more knowledge and
experience in approaching problems such as those presented by the implementation of our `server`,
the main one consisting of this line:

```rust
let sleep_time = time::Duration::from_secs_f64(interval_length * 0.00001);
```

To be clear, reducing the time range of fill requests has potentially significant performance
benefits. It's perhaps worth noting that the [test input](./INSTRUCTIONS.md#test-input) contains
zero cache hits, at least for my current implementation, whereas there are a significant number
produced in the larger `input.txt` queries. 

This effectively ruled out attempts to fetch a batch of fills after analyzing
the possible ranges involved, which neutralized any of the benefits of processing
individual queries on asynchronous threads.

Given that the count of taker trades for a given range is the total of the buys and sells that
take place over the range, there appears to be an opportunity to refine the cache in this
direction, but this is something I didn't get to within the time I had available.
