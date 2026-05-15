import logging
from datetime import date
from typing import Optional

import httpx2

from ..models import DayConsumption
from .base import BaseStorage

_logger = logging.getLogger(__name__)


class VictoriaMetricsStorage(BaseStorage):
    def __init__(self, url: str):
        self.url = url
        self._client = httpx2.AsyncClient()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def save(self, consumption: DayConsumption) -> None:
        lines = []
        for i, record in enumerate(consumption.records):
            # Format: measurement,tag=val field=val timestamp_ns
            timestamp_ns = int(record.timestamp.timestamp() * 1e9)

            # Build fields dynamically to handle optional/None values
            fields = []
            if record.metered is not None:
                fields.append(f"metered={record.metered}")
            if record.grid_usage_leftover is not None:
                fields.append(f"grid_usage_leftover={record.grid_usage_leftover}")
            if record.self_coverage is not None:
                fields.append(f"self_coverage={record.self_coverage}")
            if record.joint_tenancy is not None:
                fields.append(f"joint_tenancy={record.joint_tenancy}")

            # Add mean_profile value for this specific 15-minute interval
            if consumption.mean_profile and i < len(consumption.mean_profile):
                val = consumption.mean_profile[i]
                if val is not None:
                    fields.append(f"mean_profile={val}")

            if fields:
                line = f"consumption,metering_point={consumption.metering_point} {','.join(fields)} {timestamp_ns}"
                lines.append(line)

        if not lines:
            return

        payload = "\n".join(lines) + "\n"
        _logger.debug("Sending %d records to VictoriaMetrics for %s", len(lines), consumption.day)
        response = await self._client.post(self.url, content=payload)
        response.raise_for_status()

    async def load(self, metering_point: str, day: date) -> Optional[DayConsumption]:
        raise NotImplementedError("VictoriaMetrics is a write-only sink for this app.")

    async def exists(self, metering_point: str, day: date) -> bool:
        # VictoriaMetrics is typically used as a write-only sink in this context.
        # Existence checks would require querying the database, which is not implemented.
        # Deduplication is usually handled by VictoriaMetrics itself based on timestamp and labels.
        return False

    async def close(self) -> None:
        await self._client.aclose()
