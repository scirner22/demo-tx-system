# Simple Payment Engine

## Assumptions

- It only makes sense for a client to dispute a deposit.
- When a dispute is active on a tx, issuing another dispute is ignored. If the tx is resolved (no
long actively disputed), that same tx can be disputed again. This is allowed infinitely many times.
I did this because I believe you can dispute transactions in the real world more than once.
- Using the `Decimal` crate for better floating point math. As the benchmark numbers at the bottom
of `main.rs` show, using this adds a substantial amount of time, but it's done for correctness.
- A locked account cannot transact with deposits and withdrawals, but disputes, resolves, and
chargebacks are still allowed.

## Surprises

In commit ef1f6c98f777b006e5d2ee0a1ac7384e25a3750a I attempted to move from an "owned" serde
struct to one with a lifetime. This was done to produce no allocations due to serde deserialization.
This made the code less readable, because sum types had to be moved to `'static str` types instead.
After completing this commit, I was surprised when the bench tests produced no noticable benefit.
After generating a flame graph (present at the root at `pretty-graph.svg`), it showed that the
majority of the time was actually being spent in `csv::trim` and `csv::StringRecord`. Since this
was the case, and the code was less readable, I reverted back to the previous commit and continued.

## Solution

This solution is single threaded so no locking has to come into play. Since clients are completely
independent of eachother, one nice way that this could be parallelized would be to modulo the
`client id` with the number of threads desired and produce a multi-threaded, lockless, solution.
This was not attempted due to the flamegraph results that are mentioned below.

## Next Optimization

I made a mistake by attempting the above optimization without first analyzing a flame graph. If I
was to continue with another optimization, I would instead attempt to write a custom deserializer.
The default `f32` and `Decimal` deserializer wants a trimmed string containing the number. This
results in having to use the `csv::trim` setting which causes multiple allocations based on the graph.
It seems possible to have a `f32` or `Decimal` custom deserializer that's able to seek to the first
character, parse the number from that position and stop at the end of that token or at the first
non-number character. The rest of the string can be seeked to verify that no illegal characters
are present.

#### Examples

- `"1.2" => dec!(1.2)`
- `"    1.2" => dec!(1.2)`
- `"    1.2    " => dec!(1.2)`
- `"    1.2    1" => Err(..)`
- `"    1.2asdf" => Err(..)`

## Links

- [flamegraph](./pretty-graph.svg)
