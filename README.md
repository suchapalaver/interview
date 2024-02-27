# Interview

## Assumptions about Limitations

### Synchronous `main`

The code currently operates within the synchronous main function, adhering to the
provided structure:

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

While tempted to extend functionalities beyond this boundary, such as integrating
tokio's channels for mitigating stdout throttling effects, I adhered to the specified
constraints.

### Printing output to `stdout`

Given the [instruction](INSTRUCTIONS.md#to-run-the-program) to run the program
from the command line, blocking println! calls were inevitable. Although alternatives
like implementing a writer with access to stdout were considered, maintaining the output
order as per [testing instructions](./INSTRUCTIONS.md#test-input) took precedence. This
led to a unique `Drop` implementation within the `Processor` struct, ensuring sequential
join calls on query-processing threads upon scope exit.

### Time Constraints

Given more time, adopting Rust's [`criterion`](https://docs.rs/criterion/latest/criterion/)
library for comprehensive benchmarking, rather than relying solely on running the application
using the `time` Linux tool (`time cat input.txt | cargo run`), would have been beneficial.
This oversight potentially obscured the true impact of various optimizations attempted during
development.

## Asynchronous Threads

Despite exploring alternatives like `tokio` and `crossbeam` channels, the synchronous runtime
constraint led to utilizing Rust's standard threading model. Attempts with `oneshot` channels to
return query results, though promising, failed to significantly enhance performance.

## Query Caching

I fully acknowledge a gaping hole where I would like there to be a whole lot more knowledge and
experience in approaching problems such as those presented by the implementation of our `server`,
the main one consisting of this line:

```rust
let sleep_time = time::Duration::from_secs_f64(interval_length * 0.00001);
```

The decision to forego a [caching approach](https://github.com/suchapalaver/interview/pull/8)
in the provided version stemmed from its adverse effects on application performance. However,
recognizing the potential performance benefits, especially in scenarios with cache hits,
suggests a promising avenue for future exploration.

## Branches-Experiments on GitHub

I haven't shared it with anyone and worked on it privately until the point of sharing my
answer with Ellipsis, but I have been using `git` and GitHub as I worked on the problem. My
attempts to implement a caching approach can be seen on [this branch](https://github.com/suchapalaver/interview/pull/8),
and some of the experiments with using tracing spans to observe how long individual queries
were taking can be seen [here](https://github.com/suchapalaver/interview/pull/10).

## Instructions for Running the Code

### Prerequisites

#### Rust

Ensure Rust is installed on your system by following the instructions [here](https://www.rust-lang.org/tools/install).

### Running the program

You can run the program with the `input.txt` data from the command line:

```terminal
cat input.txt | cargo run
```

Similarly, you can run the program with the smaller test `test_input.txt` data like this:

```terminal
cat test_input.txt | cargo run
```

To use a different dataset, substitute the file name accordingly.

### Testing the program

Run the Rust test suite using `cargo`:

```terminal 
cargo test
```

Alternatively, execute tests against `test_input.txt` via Make:

```terminal
make test
```

For larger data (`input.txt`), run:

```terminal
make test-input
```

Ensure Make is installed on your system for executing Makefile directives, which can be
installed on macOS with `brew install make`.
