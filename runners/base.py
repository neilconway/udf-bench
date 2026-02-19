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
    times: list[float] = field(default_factory=list)
    median_time: float = float("inf")
    min_time: float = float("inf")
    error: str | None = None


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

    def table_ref(self) -> str:
        """Return the SQL table reference for the parquet file."""
        return f"'{self.data_path}'"

    def run_query(self, sql: str, timeout: float = 300.0) -> tuple[float, str | None]:
        """Run a SQL query. Returns (wall_clock_seconds, error_or_none)."""
        cmd = self._build_command(sql)
        start = time.perf_counter()
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            elapsed = time.perf_counter() - start
            if result.returncode != 0:
                return elapsed, result.stderr.strip()[:200]
            return elapsed, None
        except subprocess.TimeoutExpired:
            return timeout, "TIMEOUT"

    def benchmark(
        self, udf_name: str, sql: str, warmup: int = 1, runs: int = 3
    ) -> BenchResult:
        """Run warmup + timed runs, return BenchResult."""
        # Warmup
        for _ in range(warmup):
            _, err = self.run_query(sql)
            if err:
                return BenchResult(
                    udf_name=udf_name,
                    system=self.name,
                    error=f"warmup failed: {err}",
                )

        # Timed runs
        times = []
        for _ in range(runs):
            t, err = self.run_query(sql)
            if err:
                return BenchResult(
                    udf_name=udf_name,
                    system=self.name,
                    times=times,
                    error=err,
                )
            times.append(t)

        times.sort()
        median = times[len(times) // 2]
        return BenchResult(
            udf_name=udf_name,
            system=self.name,
            times=times,
            median_time=median,
            min_time=times[0],
        )
