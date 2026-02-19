"""DuckDB CLI benchmark runner."""

from .base import SystemRunner


class DuckDBRunner(SystemRunner):
    @property
    def name(self) -> str:
        return "duckdb"

    def _version_cmd(self) -> list[str]:
        return [self.binary, "--version"]

    def _build_command(self, sql: str) -> list[str]:
        full_sql = f"SET threads = 1; {sql}"
        return [self.binary, "-noheader", "-csv", "-c", full_sql]
