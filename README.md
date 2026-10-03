# udf-bench

Benchmarks for scalar, array, and aggregate function execution across
[Apache DataFusion](https://datafusion.apache.org/),
[DuckDB](https://duckdb.org/), and
[ClickHouse](https://clickhouse.com/).

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (Python package manager)
- At least one of the following CLI tools on your `PATH`:
  - `datafusion-cli` (from [Apache DataFusion](https://datafusion.apache.org/user-guide/cli/installation.html))
  - `duckdb` (from [DuckDB](https://duckdb.org/docs/installation/))
  - `clickhouse` (from [ClickHouse](https://clickhouse.com/docs/en/install))

## Quick Start

```bash
# Generate test data (~5M rows, ~300-500MB Parquet)
uv run generate_data.py

# Run all benchmarks
uv run bench.py

# Run specific UDFs or categories
uv run bench.py --udf upper --udf lower
uv run bench.py --category string
uv run bench.py --category agg_grouped

# Run against a single system
uv run bench.py --system datafusion
```

## Configuration

Edit `config.toml` to customize:

```toml
[general]
row_count = 5_000_000    # Number of rows in test data
warmup_runs = 1          # Warmup iterations (not timed)
bench_runs = 3           # Timed iterations

[systems.datafusion]
binary = "datafusion-cli"           # Or absolute path to custom build
# binary = "/path/to/my/datafusion-cli"

[systems.duckdb]
binary = "duckdb"

[systems.clickhouse]
binary = "clickhouse"

[udfs]
# categories = ["string", "math"]   # Restrict to categories
# include = ["upper", "lower"]      # Restrict to specific UDFs
# exclude = ["sin", "cos"]          # Skip specific UDFs
```

### Using a custom DataFusion build

To test a feature branch or local build of DataFusion:

```bash
# Build datafusion-cli from your branch
cd /path/to/datafusion && cargo build --release -p datafusion-cli

# Point the benchmark at it
# In config.toml:
# [systems.datafusion]
# binary = "/path/to/datafusion/target/release/datafusion-cli"

uv run bench.py
```

## UDF Categories

| Category | Count | Examples |
|---|---|---|
| `string` | 27 | upper, lower, initcap, substr, replace, levenshtein, overlay |
| `math` | 18 | abs, ceil, floor, round, sqrt, power, factorial, gcd, degrees |
| `trig` | 8 | sin, cos, tan, asin, acos, atan, atan2, cot |
| `datetime` | 8 | date_trunc, date_part, to_unixtime, make_date, to_char, date_bin |
| `conditional` | 4 | coalesce, nullif, greatest, least |
| `hash` | 3 | md5, sha256, to_hex |
| `regex` | 3 | regexp_replace, regexp_like, regexp_count |
| `array` | 16 | array_has, array_sort, array_distinct, array_intersect, array_slice |
| `agg_ungrouped` | 14 | sum, avg, count_distinct, stddev, stddev_pop, bit_and |
| `agg_grouped` | 25 | sum, avg, string_agg, corr, median, regr_slope (with GROUP BY) |
| `window` | 6 | row_number, rank, dense_rank, lag, lead, running_sum |

## Methodology

The queries are designed so that every system actually evaluates each
function on every row (see the docstring in `udfs.py`):

- **Scalar functions** are reduced with `SUM` of a cheap function of the
  result (e.g. `SUM(octet_length(upper(s)))`, `SUM(cardinality(array_sort(a)))`).
  `COUNT(f(x))` is *not* used: when an engine can prove `f(x)` is never NULL it
  rewrites this to `COUNT(*)` and answers it from Parquet metadata without
  calling `f` (ClickHouse does this for non-Nullable results such as arrays and
  booleans; DuckDB uses Parquet statistics).
- **Window functions** are reduced with `SUM` over the window output; with
  `COUNT(*)` the planner drops the window expression entirely.
- **Ungrouped `min`/`max`/`count(*)`** disable the metadata shortcuts that
  answer them from Parquet footer statistics.
- **Timing** uses the query time reported by each engine (`Elapsed` in
  datafusion-cli, `.timer` in DuckDB, `--time` in ClickHouse), which excludes
  process startup (~6ms for DataFusion, ~10ms for DuckDB, ~165ms for
  `clickhouse local`). Wall-clock time is recorded in the CSV as well.
- **Baselines**: each scalar UDF has a baseline query that applies the same
  reduction to its raw input columns. `net = query - baseline` approximates
  the cost of the function itself, separate from Parquet scan/decode cost
  (which differs a lot between systems, e.g. for list columns). Use
  `--no-baseline` to skip these.
- **Result check**: each single-row result is compared across systems. A
  mismatch means the systems aren't computing the same thing (or one has a
  bug), so their timings aren't directly comparable.

## Output

Results are printed as a table with per-system times and two ratio columns.
Values > 1.0x mean DataFusion is slower.

- `DF/best`: DataFusion time / fastest other system's time.
- `DF/best net`: the same ratio for net function time (scalar UDFs only).
  `<2ms` means a net time is below timing resolution, so no meaningful ratio.

A summary section shows the average median time per system, followed by any
cross-system result mismatches.

Detailed per-run timings, baselines, net times and results are saved to
`results/bench_YYYYMMDD_HHMMSS.csv`. Results from before this methodology
change are not comparable with newer ones.
