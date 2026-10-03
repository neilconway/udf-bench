"""DataFusion CLI benchmark runner."""

import json
import re

from .base import SystemRunner

_ELAPSED_RE = re.compile(r"^Elapsed (\d+(?:\.\d+)?) seconds\.")
_FETCHED_RE = re.compile(r"^\d+ row\(s\) fetched\.")


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
        # nd-json (unlike csv) can print nested types, e.g. array_agg results.
        return [self.binary, "--format", "nd-json", "-c", full_sql]

    def _parse_output(self, stdout: str, stderr: str) -> tuple[float | None, list[str]]:
        # Each statement prints one JSON object per row, then
        # "N row(s) fetched." and "Elapsed X seconds."; we want the last one.
        lines = stdout.splitlines()
        fetched = [i for i, line in enumerate(lines) if _FETCHED_RE.match(line)]
        elapsed = [m.group(1) for line in lines if (m := _ELAPSED_RE.match(line))]
        if not fetched or not elapsed:
            return None, []
        start = fetched[-2] + 1 if len(fetched) > 1 else 0
        block = [
            line for line in lines[start:fetched[-1]]
            if line.strip()
            and not _ELAPSED_RE.match(line)
            and not line.startswith("DataFusion CLI")
        ]
        rows = [",".join(str(v) for v in json.loads(line).values()) for line in block]
        return float(elapsed[-1]), rows
