"""Abstract base class for database system benchmark runners."""

import subprocess
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class BenchResult:
    udf_name: str
    system: str
    # Query time as reported by the engine itself (excludes process startup).
    times: list[float] = field(default_factory=list)
    median_time: float = float("inf")
    min_time: float = float("inf")
    # Wall-clock time of the whole CLI invocation (includes process startup).
    wall_times: list[float] = field(default_factory=list)
    median_wall_time: float = float("inf")
    # Query result, if it was a single row; used for cross-system checks.
    result: str | None = None
    error: str | None = None


def _median(values: list[float]) -> float:
    values = sorted(values)
    n = len(values)
    if n % 2 == 1:
        return values[n // 2]
    return (values[n // 2 - 1] + values[n // 2]) / 2


def _error_message(output: str) -> str:
    """Error text from CLI output, skipping banners and timing lines before it."""
    lines = [line for line in output.strip().splitlines() if line.strip()]
    for i, line in enumerate(lines):
        if any(k in line for k in ("Error", "Exception", "error:")):
            lines = lines[i:]
            break
    return " ".join(lines)[:200]


class SystemRunner(ABC):
    """Base class for running benchmarks against a specific database system."""

    def __init__(self, binary: str, data_path: Path):
        self.binary = binary
        self.data_path = data_path
        self._verify_binary()

    def _verify_binary(self):
        """Check that the binary exists and is executable."""
        try:
            result = subprocess.run(
                self._version_cmd(),
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"{self.name} binary '{self.binary}' failed: {result.stderr.strip()}"
                )
        except FileNotFoundError:
            raise RuntimeError(
                f"{self.name} binary '{self.binary}' not found"
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"{self.name} binary '{self.binary}' timed out on version check"
            )

    @property
    @abstractmethod
    def name(self) -> str:
        """System name for display."""
        ...

    @abstractmethod
    def _version_cmd(self) -> list[str]:
        """Command to check version / verify the binary works."""
        ...

    @abstractmethod
    def _build_command(self, sql: str) -> list[str]:
        """Build the shell command to execute a SQL query."""
        ...

    @abstractmethod
    def _parse_output(self, stdout: str, stderr: str) -> tuple[float | None, list[str]]:
        """Extract (engine-reported seconds of the last statement, result rows)."""
        ...

    def table_ref(self) -> str:
        """Return the SQL table reference for the parquet file."""
        return f"'{self.data_path}'"

    def run_query(
        self, sql: str, timeout: float = 300.0
    ) -> tuple[float, float, str | None, str | None]:
        """Run a SQL query.

        Returns (engine_seconds, wall_clock_seconds, result, error_or_none).
        ``result`` is the single result row, or None if there wasn't exactly one.
        """
        cmd = self._build_command(sql)
        start = time.perf_counter()
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            return timeout, timeout, None, "TIMEOUT"
        wall = time.perf_counter() - start
        if result.returncode != 0:
            return wall, wall, None, _error_message(result.stderr or result.stdout)
        engine, rows = self._parse_output(result.stdout, result.stderr)
        if engine is None:
            return wall, wall, None, "could not parse engine-reported query time"
        return engine, wall, rows[0] if len(rows) == 1 else None, None

    def benchmark(
        self, udf_name: str, sql: str, warmup: int = 1, runs: int = 3
    ) -> BenchResult:
        """Run warmup + timed runs, return BenchResult."""
        # Warmup
        for _ in range(warmup):
            _, _, _, err = self.run_query(sql)
            if err:
                return BenchResult(
                    udf_name=udf_name,
                    system=self.name,
                    error=f"warmup failed: {err}",
                )

        # Timed runs
        times = []
        wall_times = []
        value = None
        for _ in range(runs):
            t, wall, value, err = self.run_query(sql)
            if err:
                return BenchResult(
                    udf_name=udf_name,
                    system=self.name,
                    times=times,
                    wall_times=wall_times,
                    error=err,
                )
            times.append(t)
            wall_times.append(wall)

        times.sort()
        wall_times.sort()
        return BenchResult(
            udf_name=udf_name,
            system=self.name,
            times=times,
            median_time=_median(times),
            min_time=times[0],
            wall_times=wall_times,
            median_wall_time=_median(wall_times),
            result=value,
        )
