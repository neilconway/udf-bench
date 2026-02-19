#!/usr/bin/env python3
"""Generate Parquet test data for UDF benchmarks."""

import string
import tomllib
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def random_strings(rng, n, min_len, max_len, charset=string.ascii_lowercase):
    """Generate n random strings with lengths uniform in [min_len, max_len]."""
    char_arr = np.array(list(charset))
    max_possible = max_len
    indices = rng.integers(0, len(char_arr), size=(n, max_possible))
    all_chars = char_arr[indices]
    lengths = rng.integers(min_len, max_len + 1, size=n)
    return ["".join(row[:length]) for row, length in zip(all_chars, lengths)]


def random_pattern_strings(rng, n):
    """Generate structured strings like 'alpha-1234-beta' for regex/split tests."""
    prefixes = ["alpha", "beta", "gamma", "delta", "epsilon"]
    suffixes = ["one", "two", "three", "four", "five"]
    nums = rng.integers(1000, 9999, size=n)
    p_idx = rng.integers(0, len(prefixes), size=n)
    s_idx = rng.integers(0, len(suffixes), size=n)
    return [
        f"{prefixes[p]}-{num}-{suffixes[s]}"
        for p, num, s in zip(p_idx, nums, s_idx)
    ]


def apply_nulls(values, rng, null_fraction=0.1):
    """Replace ~null_fraction of values with None."""
    mask = rng.random(len(values)) < null_fraction
    return [None if m else v for m, v in zip(mask, values)]


def random_int_arrays(rng, n, min_len, max_len, min_val, max_val):
    """Generate n arrays of random ints with variable lengths."""
    lengths = rng.integers(min_len, max_len + 1, size=n)
    result = []
    for length in lengths:
        arr = rng.integers(min_val, max_val + 1, size=int(length)).tolist()
        result.append(arr)
    return result


def random_str_arrays(rng, n, min_len, max_len, str_min, str_max):
    """Generate n arrays of random strings with variable lengths."""
    lengths = rng.integers(min_len, max_len + 1, size=n)
    char_arr = np.array(list(string.ascii_lowercase))
    result = []
    for length in lengths:
        strs = []
        for _ in range(int(length)):
            slen = rng.integers(str_min, str_max + 1)
            indices = rng.integers(0, len(char_arr), size=slen)
            strs.append("".join(char_arr[indices]))
        result.append(strs)
    return result


def generate(config_path: Path = Path("config.toml")):
    with open(config_path, "rb") as f:
        raw = tomllib.load(f)

    n = raw["general"]["row_count"]
    seed = raw["general"]["seed"]
    data_dir = Path(raw["general"]["data_dir"])
    data_dir.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(seed)
    print(f"Generating {n:,} rows...")

    # --- Scalar columns ---
    id_col = pa.array(range(n), type=pa.int64())

    print("  strings...")
    str_short = random_strings(rng, n, 5, 10)
    str_medium = random_strings(
        rng, n, 20, 50, charset=string.ascii_letters + string.digits + "   "
    )
    str_long = random_strings(
        rng, n, 100, 200, charset=string.ascii_letters + string.digits + " .-_@"
    )
    str_nullable = apply_nulls(random_strings(rng, n, 20, 50), rng)
    str_pattern = random_pattern_strings(rng, n)
    str_second = random_strings(rng, n, 5, 15)

    print("  integers...")
    int_small = rng.integers(1, 1001, size=n)
    int_large = rng.integers(1, 1_000_001, size=n)
    int_nullable = apply_nulls(rng.integers(1, 1001, size=n).tolist(), rng)
    int_second = rng.integers(1, 1001, size=n)

    print("  floats...")
    float_pos = rng.uniform(0.001, 1000.0, size=n)
    float_angle = rng.uniform(-np.pi, np.pi, size=n)
    float_signed = rng.uniform(-1000.0, 1000.0, size=n)
    float_nullable = apply_nulls(rng.uniform(-100.0, 100.0, size=n).tolist(), rng)

    print("  timestamps...")
    ts_start = np.datetime64("2020-01-01T00:00:00", "us")
    ts_end = np.datetime64("2025-12-31T23:59:59", "us")
    ts_range = (ts_end - ts_start).astype(np.int64)
    ts_col = ts_start + rng.integers(0, ts_range, size=n).astype("timedelta64[us]")
    ts_second = ts_start + rng.integers(0, ts_range, size=n).astype("timedelta64[us]")

    print("  booleans...")
    bool_vals = rng.random(n) < 0.5
    bool_nulls = rng.random(n) < 0.05
    bool_col = [None if is_null else bool(v) for v, is_null in zip(bool_vals, bool_nulls)]

    # --- Array columns ---
    print("  arrays...")
    arr_int = random_int_arrays(rng, n, 5, 20, 1, 1000)
    arr_int_second = random_int_arrays(rng, n, 5, 20, 1, 1000)
    arr_str = random_str_arrays(rng, n, 3, 10, 3, 8)
    search_int = rng.integers(1, 1001, size=n)

    print("  building table...")
    table = pa.table(
        {
            "id": id_col,
            "str_short": pa.array(str_short, type=pa.utf8()),
            "str_medium": pa.array(str_medium, type=pa.utf8()),
            "str_long": pa.array(str_long, type=pa.utf8()),
            "str_nullable": pa.array(str_nullable, type=pa.utf8()),
            "str_pattern": pa.array(str_pattern, type=pa.utf8()),
            "str_second": pa.array(str_second, type=pa.utf8()),
            "int_small": pa.array(int_small, type=pa.int64()),
            "int_large": pa.array(int_large, type=pa.int64()),
            "int_nullable": pa.array(int_nullable, type=pa.int64()),
            "int_second": pa.array(int_second, type=pa.int64()),
            "float_pos": pa.array(float_pos, type=pa.float64()),
            "float_angle": pa.array(float_angle, type=pa.float64()),
            "float_signed": pa.array(float_signed, type=pa.float64()),
            "float_nullable": pa.array(float_nullable, type=pa.float64()),
            "ts": pa.array(ts_col, type=pa.timestamp("us")),
            "ts_second": pa.array(ts_second, type=pa.timestamp("us")),
            "bool_col": pa.array(bool_col, type=pa.bool_()),
            "arr_int": pa.array(arr_int, type=pa.list_(pa.int64())),
            "arr_int_second": pa.array(arr_int_second, type=pa.list_(pa.int64())),
            "arr_str": pa.array(arr_str, type=pa.list_(pa.utf8())),
            "search_int": pa.array(search_int, type=pa.int64()),
        }
    )

    out_path = data_dir / "bench_data.parquet"
    print(f"  writing {out_path}...")
    pq.write_table(table, out_path, compression="snappy")
    size_gb = out_path.stat().st_size / 1e9
    print(f"Done: {n:,} rows, {size_gb:.2f} GB -> {out_path}")


if __name__ == "__main__":
    generate()
