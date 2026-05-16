#!/usr/bin/env python3
"""Main orchestration script for downloading and storing consumption data."""

import asyncio
import datetime
import logging
from typing import Optional

from . import settings, smartmeter
from .storage.base import BaseStorage
from .storage.filesystem import FilesystemStorage

_logger = logging.getLogger(__name__)
_settings = settings.Settings()


class ConsumptionDownloader:
    """Orchestrates downloading consumption data and storing it in configured backends."""

    def __init__(self):
        self.smartmeter = smartmeter.SmartMeter(_settings.username, _settings.password)
        self.backends: list[BaseStorage] = []

        if _settings.use_victoriametrics:
            try:
                from .storage.victoriametrics import VictoriaMetricsStorage

                self.backends.append(
                    VictoriaMetricsStorage(
                        url=_settings.victoriametrics_url,
                    )
                )
                _logger.info("VictoriaMetrics backend enabled")
            except Exception as e:  # ImportError or runtime failure
                _logger.error("VictoriaMetrics backend requested but could not be initialized: %s", e)

        if _settings.use_filesystem_backup:
            self.backends.append(FilesystemStorage(_settings.storage_path, _settings.manual_readings_folder))
            _logger.info("Filesystem backup enabled")

        if not self.backends:
            raise ValueError("At least one storage backend must be enabled!")

    async def __aenter__(self):
        """Enter the async context, performing login and initializing backends."""
        await self.smartmeter.login()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the async context, ensuring resources are closed."""
        await self.smartmeter.close()
        for backend in self.backends:
            await backend.close()

    async def download_for_meter(
        self,
        metering_point: str,
        start_date: datetime.date,
        end_date: Optional[datetime.date] = None,
    ) -> int:
        """
        Download consumption data for a metering point.

        Args:
            metering_point: Metering point ID
            start_date: Start date for download
            end_date: End date for download (defaults to today)

        Returns:
            Number of successfully stored records
        """
        if end_date is None:
            end_date = datetime.date.today()

        days_to_process = (end_date - start_date).days + 1
        stored_count = 0

        for i in range(days_to_process):
            current_date = start_date + datetime.timedelta(days=i)

            # Partition backends based on data existence
            misses = []
            local_source = None

            for backend in self.backends:
                try:
                    if await backend.exists(metering_point, current_date):
                        if local_source is None:
                            local_source = backend
                    else:
                        misses.append(backend)
                except Exception as e:
                    _logger.warning(
                        "Existence check failed for %s on %s: %s", backend.__class__.__name__, current_date, e
                    )
                    misses.append(backend)

            if not misses:
                _logger.debug(
                    "Data for %s on %s already exists in all backends, skipping", metering_point, current_date
                )
                continue

            try:
                consumption = None

                # 1. Try local restoration
                if local_source:
                    try:
                        consumption = await local_source.load(metering_point, current_date)
                        if consumption:
                            _logger.info("Restoring %s from local %s", current_date, local_source.__class__.__name__)
                    except NotImplementedError:
                        consumption = None

                # 2. Web fallback
                if consumption is None:
                    _logger.info("Downloading data for %s on %s from web portal", metering_point, current_date)
                    consumption = await self.smartmeter.get_consumption_records_for_day(metering_point, current_date)

                # Validate data
                if not consumption.records:
                    _logger.warning("No consumption records for %s on %s", metering_point, current_date)
                    continue

                # 3. Save to missing backends
                for backend in misses:
                    try:
                        await backend.save(consumption)
                    except Exception:
                        _logger.error("Failed to store data in %s", backend.__class__.__name__)

                stored_count += 1
                _logger.info(
                    "Successfully stored %s records for %s on %s",
                    len(consumption.records),
                    metering_point,
                    current_date,
                )

            except Exception as e:
                _logger.error(
                    "Failed to download data for %s on %s: %s", metering_point, current_date, e, exc_info=True
                )

        return stored_count

    async def run(self) -> None:
        """Main execution loop for downloading all consumption data."""
        consumption_infos = await self.smartmeter.get_consumption_info()
        _logger.info("Found %s metering points", len(consumption_infos))

        total_stored = 0
        for info in consumption_infos:
            _logger.info("Processing account %s, metering point %s", info.account_id, info.metering_point_id)
            stored = await self.download_for_meter(info.metering_point_id, _settings.measure_start_date)
            total_stored += stored

            _logger.info("Download completed. Total records stored: %s", total_stored)


async def main():
    """Entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    async with ConsumptionDownloader() as downloader:
        await downloader.run()


if __name__ == "__main__":
    asyncio.run(main())
