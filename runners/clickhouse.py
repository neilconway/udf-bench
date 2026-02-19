"""ClickHouse local benchmark runner."""

from .base import SystemRunner


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
            "--query",
            sql,
        ]

    def table_ref(self) -> str:
        return f"file('{self.data_path}', Parquet)"
