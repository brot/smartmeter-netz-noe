#!/usr/bin/env python3
"""Utility script to record manual meter readings in VictoriaMetrics."""

import argparse
import asyncio
import datetime
import logging
from enum import Enum
from zoneinfo import ZoneInfo

from .settings import Settings
from .storage.filesystem import FilesystemStorage
from .storage.victoriametrics import VictoriaMetricsStorage

# Hard-coded metric name as requested
METRIC_NAME = "manual_meter_reading"
_VIENNA_TZ = ZoneInfo("Europe/Vienna")
_logger = logging.getLogger(__name__)


class ReadingType(str, Enum):
    ANNUAL_BILL = "annual_bill"
    INTERMEDIATE = "intermediate"


class ReadingSource(str, Enum):
    NETZ_NOE = "netz_noe"
    ME = "me"


async def record_reading(
    metering_point: str,
    reading_type: ReadingType,
    source: ReadingSource,
    value: float,
    timestamp: datetime.datetime,
):
    """Records the manual reading to VictoriaMetrics and Filesystem."""
    settings = Settings()
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=_VIENNA_TZ)

    backends = []
    if settings.use_filesystem_backup:
        backends.append(FilesystemStorage(settings.storage_path, settings.manual_readings_folder))
    if settings.use_victoriametrics:
        backends.append(VictoriaMetricsStorage(settings.victoriametrics_url))

    for backend in backends:
        try:
            _logger.info("Saving to %s...", backend.__class__.__name__)
            await backend.save_manual_reading(metering_point, reading_type.value, source.value, value, timestamp)
        except Exception as e:
            _logger.error("Failed to save to %s: %s", backend.__class__.__name__, e)

        try:
            await backend.close()
        except Exception as e:
            _logger.error("Failed to close %s: %s", backend.__class__.__name__, e)


async def main():
    parser = argparse.ArgumentParser(description="Record a manual meter reading (e.g. from an annual bill)")

    parser.add_argument("--metering-point", required=True, help="The metering point ID (e.g. AT...)")
    parser.add_argument(
        "--type",
        required=True,
        choices=[t.value for t in ReadingType],
        help="Type of reading: annual_bill or intermediate",
    )
    parser.add_argument(
        "--source", required=True, choices=[s.value for s in ReadingSource], help="Source of the data: netz_noe or me"
    )
    parser.add_argument("--value", required=True, type=float, help="The meter reading value (Zählerstand)")
    parser.add_argument(
        "--timestamp",
        help="ISO 8601 timestamp (e.g. 2024-03-31T23:59:00). Defaults to now.",
        type=lambda s: datetime.datetime.fromisoformat(s),
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    timestamp = args.timestamp or datetime.datetime.now(_VIENNA_TZ)

    await record_reading(
        metering_point=args.metering_point,
        reading_type=ReadingType(args.type),
        source=ReadingSource(args.source),
        value=args.value,
        timestamp=timestamp,
    )


if __name__ == "__main__":
    asyncio.run(main())
