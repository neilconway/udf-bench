#!/usr/bin/env python3
"""Generate Parquet test data for UDF benchmarks."""

import string
import tomllib
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

POOL_SIZE = 100_000  # Pre-generate this many unique strings, then sample


def _make_string_pool(rng, pool_size, min_len, max_len, charset=string.ascii_lowercase):
    """Pre-generate a pool of random strings. Returns a Python list."""
    char_arr = np.array(list(charset))
    indices = rng.integers(0, len(char_arr), size=(pool_size, max_len))
    all_chars = char_arr[indices]
    lengths = rng.integers(min_len, max_len + 1, size=pool_size)
    return ["".join(row[:l]) for row, l in zip(all_chars, lengths)]


def random_strings_pooled(rng, n, min_len, max_len, charset=string.ascii_lowercase):
    """Generate n random strings by sampling from a pre-built pool."""
    pool = _make_string_pool(rng, POOL_SIZE, min_len, max_len, charset)
    indices = rng.integers(0, POOL_SIZE, size=n)
    return pa.array(pool, type=pa.utf8()).take(pa.array(indices, type=pa.int32()))


def random_pattern_strings(rng, n):
    """Generate structured strings like 'alpha-1234-beta' for regex/split tests."""
    prefixes = np.array(["alpha", "beta", "gamma", "delta", "epsilon"])
    suffixes = np.array(["one", "two", "three", "four", "five"])
    p = prefixes[rng.integers(0, 5, size=n)]
    s = suffixes[rng.integers(0, 5, size=n)]
    nums = rng.integers(1000, 9999, size=n)
    # Build with numpy string ops
    return pa.array(
        np.char.add(np.char.add(np.char.add(p, "-"), nums.astype(str)), np.char.add("-", s)),
        type=pa.utf8(),
    )


def make_list_array_int(rng, n, min_len, max_len, min_val, max_val):
    """Build a pa.ListArray of int64 using flat values + offsets (no Python loop)."""
    lengths = rng.integers(min_len, max_len + 1, size=n)
    offsets = np.zeros(n + 1, dtype=np.int64)
    np.cumsum(lengths, out=offsets[1:])
    total = int(offsets[-1])
    flat_values = rng.integers(min_val, max_val + 1, size=total)
    return pa.ListArray.from_arrays(
        pa.array(offsets, type=pa.int64()),
        pa.array(flat_values, type=pa.int64()),
    )


def make_list_array_str(rng, n, min_arr_len, max_arr_len, str_min, str_max):
    """Build a pa.ListArray of utf8 using flat pool + offsets (no Python loop)."""
    lengths = rng.integers(min_arr_len, max_arr_len + 1, size=n)
    offsets = np.zeros(n + 1, dtype=np.int64)
    np.cumsum(lengths, out=offsets[1:])
    total = int(offsets[-1])
    # Sample from a string pool for the flat values
    pool = _make_string_pool(rng, POOL_SIZE, str_min, str_max)
    indices = rng.integers(0, POOL_SIZE, size=total)
    flat_values = pa.array(pool, type=pa.utf8()).take(pa.array(indices, type=pa.int32()))
    return pa.ListArray.from_arrays(
        pa.array(offsets, type=pa.int64()),
        flat_values,
    )


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
    id_col = pa.array(np.arange(n, dtype=np.int64))

    print("  strings...")
    str_short = random_strings_pooled(rng, n, 5, 10)
    str_medium = random_strings_pooled(
        rng, n, 20, 50, charset=string.ascii_letters + string.digits + "   "
    )
    str_long = random_strings_pooled(
        rng, n, 100, 200, charset=string.ascii_letters + string.digits + " .-_@"
    )
    str_nullable_base = random_strings_pooled(rng, n, 20, 50)
    str_pattern = random_pattern_strings(rng, n)
    str_second = random_strings_pooled(rng, n, 5, 15)

    # Apply nulls via pa.compute
    null_mask = pa.array(rng.random(n) < 0.1)
    str_nullable = pa.compute.if_else(null_mask, None, str_nullable_base)

    print("  integers...")
    int_small = pa.array(rng.integers(1, 1001, size=n), type=pa.int64())
    int_large = pa.array(rng.integers(1, 1_000_001, size=n), type=pa.int64())
    int_second = pa.array(rng.integers(1, 1001, size=n), type=pa.int64())

    int_nullable_vals = pa.array(rng.integers(1, 1001, size=n), type=pa.int64())
    int_null_mask = pa.array(rng.random(n) < 0.1)
    int_nullable = pa.compute.if_else(int_null_mask, None, int_nullable_vals)

    print("  floats...")
    float_pos = pa.array(rng.uniform(0.001, 1000.0, size=n))
    float_angle = pa.array(rng.uniform(-np.pi, np.pi, size=n))
    float_signed = pa.array(rng.uniform(-1000.0, 1000.0, size=n))

    float_nullable_vals = pa.array(rng.uniform(-100.0, 100.0, size=n))
    float_null_mask = pa.array(rng.random(n) < 0.1)
    float_nullable = pa.compute.if_else(float_null_mask, None, float_nullable_vals)

    print("  timestamps...")
    ts_start = np.datetime64("2020-01-01T00:00:00", "us")
    ts_end = np.datetime64("2025-12-31T23:59:59", "us")
    ts_range = (ts_end - ts_start).astype(np.int64)
    ts_col = pa.array(
        ts_start + rng.integers(0, ts_range, size=n).astype("timedelta64[us]"),
        type=pa.timestamp("us"),
    )
    ts_second = pa.array(
        ts_start + rng.integers(0, ts_range, size=n).astype("timedelta64[us]"),
        type=pa.timestamp("us"),
    )

    print("  booleans...")
    bool_vals = pa.array(rng.random(n) < 0.5)
    bool_null_mask = pa.array(rng.random(n) < 0.05)
    bool_col = pa.compute.if_else(bool_null_mask, None, bool_vals)

    # --- Array columns ---
    print("  arrays...")
    arr_int = make_list_array_int(rng, n, 5, 20, 1, 1000)
    arr_int_second = make_list_array_int(rng, n, 5, 20, 1, 1000)
    arr_str = make_list_array_str(rng, n, 3, 10, 3, 8)
    search_int = pa.array(rng.integers(1, 1001, size=n), type=pa.int64())

    print("  building table...")
    table = pa.table(
        {
            "id": id_col,
            "str_short": str_short,
            "str_medium": str_medium,
            "str_long": str_long,
            "str_nullable": str_nullable,
            "str_pattern": str_pattern,
            "str_second": str_second,
            "int_small": int_small,
            "int_large": int_large,
            "int_nullable": int_nullable,
            "int_second": int_second,
            "float_pos": float_pos,
            "float_angle": float_angle,
            "float_signed": float_signed,
            "float_nullable": float_nullable,
            "ts": ts_col,
            "ts_second": ts_second,
            "bool_col": bool_col,
            "arr_int": arr_int,
            "arr_int_second": arr_int_second,
            "arr_str": arr_str,
            "search_int": search_int,
        }
    )

    out_path = data_dir / "bench_data.parquet"
    print(f"  writing {out_path}...")
    pq.write_table(table, out_path, compression="snappy")
    size_gb = out_path.stat().st_size / 1e9
    print(f"Done: {n:,} rows, {size_gb:.2f} GB -> {out_path}")


if __name__ == "__main__":
    generate()
