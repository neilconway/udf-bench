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

    if include:
        include_set = set(include)
        udfs = [u for u in udfs if u.name in include_set]
    if exclude:
        udfs = [u for u in udfs if u.name not in exclude]
    if categories:
        cat_set = set(categories)
        udfs = [u for u in udfs if u.category in cat_set]

    # CLI overrides
    if cli_udfs:
        udf_set = set(cli_udfs)
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
        return "N/A"
    ratio = target / baseline
    return f"{ratio:.2f}x"


def print_results_table(
    results: dict[str, dict[str, BenchResult]],
    systems: list[str],
    udfs: list[UDFBenchmark],
):
    """Print results as a formatted table to stdout."""
    # Build header: time columns, then ratio columns (DF/each other system)
    other_systems = [s for s in systems if s != "datafusion"]
    show_ratios = "datafusion" in systems and len(other_systems) > 0

    header = ["UDF", "Category"]
    for s in systems:
        header.append(s)
    if show_ratios:
        for s in other_systems:
            header.append(f"DF/{s}")

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
            for s in other_systems:
                other_median = medians.get(s, float("inf"))
                row.append(format_ratio(df_median, other_median))

        rows.append(row)

    # Print
    widths = [max(len(header[i]), *(len(r[i]) for r in rows)) for i in range(len(header))]
    fmt = " | ".join(f"{{:<{w}}}" for w in widths)
    sep = "-+-".join("-" * w for w in widths)

    print(fmt.format(*header))
    print(sep)
    for row in rows:
        print(fmt.format(*row))


def save_csv(
    results: dict[str, dict[str, BenchResult]],
    systems: list[str],
    udfs: list[UDFBenchmark],
    output_path: Path,
):
    """Save detailed results to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    udf_lookup = {u.name: u for u in udfs}
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["udf", "category", "system", "median_s", "min_s", "all_times", "error"])
        for udf_name, sys_results in results.items():
            udf_def = udf_lookup.get(udf_name)
            category = udf_def.category if udf_def else "?"
            for s in systems:
                r = sys_results.get(s)
                if r:
                    writer.writerow([
                        udf_name,
                        category,
                        s,
                        f"{r.median_time:.6f}",
                        f"{r.min_time:.6f}",
                        ";".join(f"{t:.6f}" for t in r.times),
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

    print(f"Systems: {', '.join(system_names)}")
    print(f"UDFs: {len(udfs)}")
    print(f"Warmup: {g['warmup_runs']}, Runs: {g['bench_runs']}")
    print()

    # Run benchmarks
    all_results: dict[str, dict[str, BenchResult]] = {}

    for i, udf in enumerate(udfs, 1):
        print(f"[{i}/{len(udfs)}] {udf.name} ({udf.category})")
        all_results[udf.name] = {}

        for sys_name, runner in runners.items():
            query = udf.query_for(sys_name)
            if query is None:
                print(f"  {sys_name}: n/a")
                continue
            sql = query.format(table=runner.table_ref())
            result = runner.benchmark(
                udf_name=udf.name,
                sql=sql,
                warmup=g["warmup_runs"],
                runs=g["bench_runs"],
            )
            all_results[udf.name][sys_name] = result

            if result.error:
                print(f"  {sys_name}: ERROR: {result.error[:60]}")
            else:
                print(f"  {sys_name}: {format_time(result.median_time)}")

    # Output
    print()
    print("=" * 80)
    print("RESULTS")
    print("=" * 80)
    print()
    print_results_table(all_results, system_names, udfs)

    # Save CSV
    output_path = Path(args.output) if args.output else (
        Path(g["results_dir"])
        / f"bench_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    save_csv(all_results, system_names, udfs, output_path)


if __name__ == "__main__":
    main()
