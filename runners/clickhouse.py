"""ClickHouse local benchmark runner."""

import re

from .base import SystemRunner

_TIME_RE = re.compile(r"^\d+(?:\.\d+)?$")


class ClickHouseRunner(SystemRunner):
    @property
    def name(self) -> str:
        return "clickhouse"

    def _version_cmd(self) -> list[str]:
        return [self.binary, "local", "--version"]

    def _build_command(self, sql: str) -> list[str]:
        return [
            self.binary,
            "local",
            "--max_threads=1",
            "--time",
            "--query",
            sql,
        ]

    def _parse_output(self, stdout: str, stderr: str) -> tuple[float | None, list[str]]:
        # --time prints the elapsed seconds of each query to stderr.
        timings = [line.strip() for line in stderr.splitlines() if _TIME_RE.match(line.strip())]
        if not timings:
            return None, []
        rows = [line for line in stdout.splitlines() if line.strip()]
        return float(timings[-1]), rows

    def table_ref(self) -> str:
        return f"file('{self.data_path}', Parquet)"
