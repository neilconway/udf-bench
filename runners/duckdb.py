"""DuckDB CLI benchmark runner."""

import re

from .base import SystemRunner

_TIMER_RE = re.compile(r"^Run Time \(s\): real (\d+(?:\.\d+)?)")


class DuckDBRunner(SystemRunner):
    @property
    def name(self) -> str:
        return "duckdb"

    def _version_cmd(self) -> list[str]:
        return [self.binary, "--version"]

    def _build_command(self, sql: str) -> list[str]:
        full_sql = f"SET threads = 1; {sql}"
        return [self.binary, "-noheader", "-csv", "-cmd", ".timer on", "-c", full_sql]

    def _parse_output(self, stdout: str, stderr: str) -> tuple[float | None, list[str]]:
        lines = stdout.splitlines()
        timings = [m.group(1) for line in lines if (m := _TIMER_RE.match(line))]
        if not timings:
            return None, []
        rows = [line for line in lines if line.strip() and not _TIMER_RE.match(line)]
        return float(timings[-1]), rows
