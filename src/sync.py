#!/usr/bin/env python3
"""Utility to sync data from filesystem backup to VictoriaMetrics."""

import asyncio
import datetime
import logging
import pathlib

from . import settings
from .models import DayConsumption
from .storage.victoriametrics import VictoriaMetricsStorage

_logger = logging.getLogger(__name__)
_settings = settings.Settings()

# VictoriaMetrics handles bulk imports well, but opening 1000s of sockets at once is bad.
_semaphore = asyncio.Semaphore(10)


async def _process_single_file(
    file_path: pathlib.Path,
    vm_storage_instance: VictoriaMetricsStorage,
    meter_point_id: str,
) -> bool:
    """Processes a single JSON file, checking existence and saving to VM."""
    try:
        day_str = file_path.stem
        day = datetime.date.fromisoformat(day_str)

        if await vm_storage_instance.exists(meter_point_id, day):
            _logger.debug("Data for %s on %s already in VM, skipping", meter_point_id, day)
            return False

        # Load the file as a DayConsumption model directly
        async with _semaphore:
            # Standard file reading is blocking; for large syncs we use to_thread
            data = await asyncio.to_thread(file_path.read_text, encoding="utf-8")
            consumption = DayConsumption.model_validate_json(data)

            if not consumption.records:
                _logger.warning("File %s contains no records. Skipping sync.", file_path.name)
                return False

            _logger.info("Syncing %s for %s (%d records)", day, meter_point_id, len(consumption.records))
            await vm_storage_instance.save(consumption)

        _logger.info("Successfully synced %s for %s", day, meter_point_id)
        return True

    except Exception as e:
        _logger.error("Failed to sync file %s: %s", file_path, e, exc_info=True)
        return False


async def sync_filesystem_to_vm():
    """Iterates through the local storage and pushes missing data to VictoriaMetrics."""
    if not _settings.use_victoriametrics:
        _logger.error("VictoriaMetrics is disabled in settings.")
        return

    async with VictoriaMetricsStorage(url=_settings.victoriametrics_url) as vm_storage:
        base_path = pathlib.Path(_settings.storage_path)
        if not base_path.exists():
            _logger.error("Storage path %s does not exist", base_path)
            return

        _logger.info("Starting sync from %s to VictoriaMetrics", base_path)
        total_synced = 0

        for meter_dir in sorted(d for d in base_path.iterdir() if d.is_dir()):
            metering_point = meter_dir.name
            _logger.info("Processing metering point: %s", metering_point)

            # Collect periodic consumption data from year-month subdirectories,
            # skipping the manual readings folder and zip archives.
            json_files = []
            for sub_dir in meter_dir.iterdir():
                if sub_dir.is_dir() and sub_dir.name != _settings.manual_readings_folder:
                    json_files.extend(sub_dir.rglob("*.json"))

            json_files.sort()

            tasks = [_process_single_file(f, vm_storage, metering_point) for f in json_files]
            results = await asyncio.gather(*tasks)
            total_synced += sum(1 for r in results if r)

        _logger.info("Sync completed. Total days synced: %s", total_synced)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    asyncio.run(sync_filesystem_to_vm())
