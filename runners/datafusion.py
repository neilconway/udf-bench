"""DataFusion CLI benchmark runner."""

from .base import SystemRunner


class DataFusionRunner(SystemRunner):
    @property
    def name(self) -> str:
        return "datafusion"

    def _version_cmd(self) -> list[str]:
        return [self.binary, "--version"]

    def _build_command(self, sql: str) -> list[str]:
        full_sql = (
            "SET datafusion.execution.target_partitions = 1; "
            f"{sql}"
        )
        return [self.binary, "-c", full_sql]
