#!/usr/bin/env python3
"""
UDF Benchmark Suite: Compare scalar and aggregate function performance
across DataFusion, DuckDB, and ClickHouse.
"""

import argparse
import csv
import sys
import tomllib
from datetime import datetime
from pathlib import Path

from runners import (
    BenchResult,
    ClickHouseRunner,
    DataFusionRunner,
    DuckDBRunner,
    SystemRunner,
)
from udfs import UDFS, UDFBenchmark


def load_config(path: Path) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


def create_runners(
    config: dict, data_path: Path, only: list[str] | None = None
) -> dict[str, SystemRunner]:
    """Instantiate runners for enabled systems."""
    runner_classes: dict[str, type[SystemRunner]] = {
        "datafusion": DataFusionRunner,
        "duckdb": DuckDBRunner,
        "clickhouse": ClickHouseRunner,
    }
    runners = {}
    for name, cls in runner_classes.items():
        if only and name not in only:
            continue
        sys_cfg = config["systems"].get(name, {})
        if not sys_cfg.get("enabled", True):
            continue
        try:
            runner = cls(binary=sys_cfg["binary"], data_path=data_path)
            runners[name] = runner
        except RuntimeError as e:
            print(f"WARNING: Skipping {name}: {e}", file=sys.stderr)
    return runners


def filter_udfs(
    udfs: list[UDFBenchmark],
    config: dict,
    cli_udfs: list[str] | None,
    cli_categories: list[str] | None,
) -> list[UDFBenchmark]:
    """Apply include/exclude filters from config and CLI args."""
    udfs_cfg = config.get("udfs", {})
    include = udfs_cfg.get("include")
    exclude = set(udfs_cfg.get("exclude", []))
    categories = udfs_cfg.get("categories")

    all_names = {u.name for u in udfs}

    if include:
        include_set = set(include)
        unknown = include_set - all_names
        if unknown:
            print(f"WARNING: include contains unknown UDF names: {sorted(unknown)}", file=sys.stderr)
        udfs = [u for u in udfs if u.name in include_set]
    if exclude:
        exclude_set = set(exclude)
        unknown = exclude_set - all_names
        if unknown:
            print(f"WARNING: exclude contains unknown UDF names: {sorted(unknown)}", file=sys.stderr)
        udfs = [u for u in udfs if u.name not in exclude_set]
    if categories:
        cat_set = set(categories)
        udfs = [u for u in udfs if u.category in cat_set]

    # CLI overrides
    if cli_udfs:
        udf_set = set(cli_udfs)
        unknown = udf_set - all_names
        if unknown:
            print(f"WARNING: --udf contains unknown UDF names: {sorted(unknown)}", file=sys.stderr)
        udfs = [u for u in udfs if u.name in udf_set]
    if cli_categories:
        cat_set = set(cli_categories)
        udfs = [u for u in udfs if u.category in cat_set]

    return udfs


def format_time(seconds: float) -> str:
    if seconds == float("inf"):
        return "ERROR"
    if seconds < 0.01:
        return f"{seconds * 1000:.1f}ms"
    return f"{seconds:.3f}s"


def format_ratio(target: float, baseline: float) -> str:
    if baseline <= 0 or baseline == float("inf") or target == float("inf"):
        return "n/a"
    ratio = target / baseline
    return f"{ratio:.2f}x"


# Net (function-only) times below this are within timing noise (engines report
# query time with millisecond resolution), so ratios between them are meaningless.
NET_MIN_SECONDS = 0.002


def net_time(result: BenchResult | None, baseline: BenchResult | None) -> float | None:
    """Query time minus the baseline (same reduction over the raw input columns)."""
    if not result or result.error or not baseline or baseline.error:
        return None
    return max(result.median_time - baseline.median_time, 0.0)


def format_net_ratio(df_net: float | None, best_other_net: float | None) -> str:
    if df_net is None or best_other_net is None:
        return "n/a"
    if df_net < NET_MIN_SECONDS or best_other_net < NET_MIN_SECONDS:
        return "<2ms"
    return format_ratio(df_net, best_other_net)


def print_results_table(
    results: dict[str, dict[str, BenchResult]],
    baselines: dict[str, dict[str, BenchResult]],
    systems: list[str],
    udfs: list[UDFBenchmark],
):
    """Print results as a formatted table to stdout."""
    # Build header: time columns, then ratio columns (DF/each other system)
    other_systems = [s for s in systems if s != "datafusion"]
    show_ratios = "datafusion" in systems and len(other_systems) > 0
    show_net = show_ratios and bool(baselines)

    header = ["UDF", "Category"]
    for s in systems:
        header.append(s)
    if show_ratios:
        header.append("DF/best")
    if show_net:
        header.append("DF/best net")

    udf_lookup = {u.name: u for u in udfs}
    rows = []
    for udf_name, sys_results in results.items():
        udf_def = udf_lookup.get(udf_name)
        cat = udf_def.category if udf_def else "?"

        row = [udf_name, cat]
        medians: dict[str, float] = {}

        for s in systems:
            r = sys_results.get(s)
            if r and not r.error:
                row.append(format_time(r.median_time))
                medians[s] = r.median_time
            elif r and r.error:
                row.append("ERROR")
            else:
                row.append("n/a")

        if show_ratios:
            df_median = medians.get("datafusion", float("inf"))
            best_other = min(
                (medians.get(s, float("inf")) for s in other_systems),
                default=float("inf"),
            )
            row.append(format_ratio(df_median, best_other))

        if show_net:
            nets = {
                s: net_time(sys_results.get(s), baselines.get(udf_name, {}).get(s))
                for s in systems
            }
            other_nets = [nets[s] for s in other_systems if nets[s] is not None]
            row.append(format_net_ratio(
                nets.get("datafusion"), min(other_nets) if other_nets else None
            ))

        rows.append(row)

    # Print
    widths = [max(len(header[i]), *(len(r[i]) for r in rows)) for i in range(len(header))]
    fmt = " | ".join(f"{{:<{w}}}" for w in widths)
    sep = "-+-".join("-" * w for w in widths)

    print(fmt.format(*header))
    print(sep)
    for row in rows:
        print(fmt.format(*row))


def print_summary(
    results: dict[str, dict[str, BenchResult]],
    systems: list[str],
):
    """Print per-system averages: mean of medians, count of successes/errors."""
    print("-" * 60)
    print("SUMMARY (average of median times)")
    print("-" * 60)
    for s in systems:
        total = 0.0
        ok = 0
        errors = 0
        skipped = 0
        for sys_results in results.values():
            r = sys_results.get(s)
            if r and not r.error:
                total += r.median_time
                ok += 1
            elif r and r.error:
                errors += 1
            else:
                skipped += 1
        avg = total / ok if ok > 0 else float("inf")
        parts = [f"{s}: {format_time(avg)} avg ({ok} UDFs)"]
        if errors:
            parts.append(f"{errors} errors")
        if skipped:
            parts.append(f"{skipped} skipped")
        print("  " + ", ".join(parts))
    print()


def _same_result(a: str, b: str) -> bool:
    """Compare single-row results, allowing float rounding differences."""
    try:
        x, y = float(a), float(b)
    except ValueError:
        return a == b
    return abs(x - y) <= 1e-6 * max(abs(x), abs(y), 1.0)


def print_result_mismatches(
    results: dict[str, dict[str, BenchResult]],
    udfs: list[UDFBenchmark],
):
    """Flag UDFs whose systems computed different answers.

    A mismatch means the systems are not evaluating the same function (or one
    of them has a bug), so their timings are not directly comparable.
    """
    udf_lookup = {u.name: u for u in udfs}
    mismatches = []
    for udf_name, sys_results in results.items():
        udf_def = udf_lookup.get(udf_name)
        if udf_def and not udf_def.check_result:
            continue
        values = {
            s: r.result for s, r in sys_results.items()
            if not r.error and r.result is not None
        }
        if len(values) < 2:
            continue
        first = next(iter(values.values()))
        if not all(_same_result(first, v) for v in values.values()):
            mismatches.append((udf_name, values))

    if not mismatches:
        return
    print("-" * 60)
    print("RESULT MISMATCHES (systems computed different answers)")
    print("-" * 60)
    for udf_name, values in mismatches:
        print(f"  {udf_name}: " + ", ".join(f"{s}={v}" for s, v in values.items()))
    print()


def save_csv(
    results: dict[str, dict[str, BenchResult]],
    baselines: dict[str, dict[str, BenchResult]],
    systems: list[str],
    udfs: list[UDFBenchmark],
    output_path: Path,
):
    """Save detailed results to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    udf_lookup = {u.name: u for u in udfs}
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "udf", "category", "system", "median_s", "min_s", "all_times",
            "median_wall_s", "baseline_median_s", "net_s", "result", "error",
        ])
        for udf_name, sys_results in results.items():
            udf_def = udf_lookup.get(udf_name)
            category = udf_def.category if udf_def else "?"
            for s in systems:
                r = sys_results.get(s)
                if r:
                    b = baselines.get(udf_name, {}).get(s)
                    net = net_time(r, b)
                    writer.writerow([
                        udf_name,
                        category,
                        s,
                        f"{r.median_time:.6f}",
                        f"{r.min_time:.6f}",
                        ";".join(f"{t:.6f}" for t in r.times),
                        f"{r.median_wall_time:.6f}",
                        f"{b.median_time:.6f}" if b and not b.error else "",
                        f"{net:.6f}" if net is not None else "",
                        r.result if r.result is not None else "",
                        r.error or "",
                    ])
    print(f"\nResults saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="UDF Benchmark Suite")
    parser.add_argument("--config", default="config.toml", help="Path to config file")
    parser.add_argument("--udf", action="append", help="Run only specific UDF(s)")
    parser.add_argument("--category", action="append", help="Run only specific category(ies)")
    parser.add_argument("--system", action="append", help="Run only specific system(s)")
    parser.add_argument("--output", default=None, help="CSV output path")
    parser.add_argument("--unicode", action="store_true",
                        help="Use Unicode string columns (ustr_*) instead of ASCII (str_*)")
    parser.add_argument("--no-baseline", action="store_true",
                        help="Skip baseline (raw input scan) queries used to compute "
                             "net function time for scalar UDFs")
    args = parser.parse_args()

    config = load_config(Path(args.config))
    g = config["general"]

    data_path = Path(g["data_dir"]) / "bench_data.parquet"
    if not data_path.exists():
        print(f"ERROR: Data file not found: {data_path}", file=sys.stderr)
        print("Run: uv run generate_data.py", file=sys.stderr)
        sys.exit(1)

    data_path = data_path.resolve()

    # Create runners
    runners = create_runners(config, data_path, only=args.system)
    if not runners:
        print("ERROR: No systems available", file=sys.stderr)
        sys.exit(1)

    system_names = list(runners.keys())

    # Filter UDFs
    udfs = filter_udfs(UDFS, config, args.udf, args.category)
    if not udfs:
        print("ERROR: No UDFs selected", file=sys.stderr)
        sys.exit(1)

    # Unicode column remapping: str_short -> ustr_short, etc.
    # str_pattern is excluded (fixed ASCII format for regex/split benchmarks).
    _UNICODE_REMAP = {
        "str_short": "ustr_short",
        "str_medium": "ustr_medium",
        "str_long": "ustr_long",
        "str_nullable": "ustr_nullable",
        "str_second": "ustr_second",
    }

    use_unicode = args.unicode

    def render(query: str, runner: SystemRunner) -> str:
        sql = query.format(table=runner.table_ref())
        if use_unicode:
            for ascii_col, unicode_col in _UNICODE_REMAP.items():
                sql = sql.replace(ascii_col, unicode_col)
        return sql

    print(f"Systems: {', '.join(system_names)}")
    print(f"UDFs: {len(udfs)}")
    if use_unicode:
        print("Strings: Unicode")
    print(f"Warmup: {g['warmup_runs']}, Runs: {g['bench_runs']}")
    print()

    # Run benchmarks
    all_results: dict[str, dict[str, BenchResult]] = {}
    all_baselines: dict[str, dict[str, BenchResult]] = {}
    # Many UDFs share a baseline (e.g. a scan of str_medium); run each once.
    baseline_cache: dict[tuple[str, str], BenchResult] = {}

    for i, udf in enumerate(udfs, 1):
        print(f"[{i}/{len(udfs)}] {udf.name} ({udf.category})")
        all_results[udf.name] = {}

        for sys_name, runner in runners.items():
            query = udf.query_for(sys_name)
            if query is None:
                print(f"  {sys_name}: n/a")
                continue
            result = runner.benchmark(
                udf_name=udf.name,
                sql=render(query, runner),
                warmup=g["warmup_runs"],
                runs=g["bench_runs"],
            )
            all_results[udf.name][sys_name] = result

            if result.error:
                print(f"  {sys_name}: ERROR: {result.error[:60]}")
                continue

            line = f"  {sys_name}: {format_time(result.median_time)}"
            baseline_query = udf.baseline_for(sys_name)
            if baseline_query and not args.no_baseline:
                baseline_sql = render(baseline_query, runner)
                key = (sys_name, baseline_sql)
                if key not in baseline_cache:
                    baseline_cache[key] = runner.benchmark(
                        udf_name=f"{udf.name} (baseline)",
                        sql=baseline_sql,
                        warmup=g["warmup_runs"],
                        runs=g["bench_runs"],
                    )
                baseline = baseline_cache[key]
                all_baselines.setdefault(udf.name, {})[sys_name] = baseline
                if baseline.error:
                    line += f" (baseline ERROR: {baseline.error[:60]})"
                else:
                    line += (f" (baseline {format_time(baseline.median_time)},"
                             f" net {format_time(net_time(result, baseline))})")
            print(line)

    # Output
    print()
    print("=" * 80)
    print("RESULTS")
    print("=" * 80)
    print()
    print_results_table(all_results, all_baselines, system_names, udfs)
    print()
    print_summary(all_results, system_names)
    print_result_mismatches(all_results, udfs)

    # Save CSV
    output_path = Path(args.output) if args.output else (
        Path(g["results_dir"])
        / f"bench_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    save_csv(all_results, all_baselines, system_names, udfs, output_path)


if __name__ == "__main__":
    main()
