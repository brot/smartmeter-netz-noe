import asyncio
import datetime
import json
import pathlib
from datetime import date
from typing import Optional
from zoneinfo import ZoneInfo

from ..models import DayConsumption
from .base import BaseStorage

_VIENNA_TZ = ZoneInfo("Europe/Vienna")


class FilesystemStorage(BaseStorage):
    def __init__(self, base_path: pathlib.Path, manual_readings_folder: str = "manual_readings"):
        self.base_path = base_path
        self.manual_readings_folder = manual_readings_folder

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()  # No-op for FilesystemStorage, but good for consistency

    def _get_path(self, metering_point: str, day: date) -> pathlib.Path:
        """Returns path like: base/AT123/2024/05/2024-05-12.json"""
        return self.base_path / metering_point / str(day.year) / f"{day.month:02d}" / f"{day.isoformat()}.json"

    async def save(self, consumption: DayConsumption) -> None:
        path = self._get_path(consumption.metering_point, consumption.day)
        await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(path.write_text, consumption.model_dump_json(indent=2), encoding="utf-8")

    async def load(self, metering_point: str, day: date) -> Optional[DayConsumption]:
        path = self._get_path(metering_point, day)
        exists = await asyncio.to_thread(path.exists)
        if not exists:
            return None

        data = await asyncio.to_thread(path.read_text, encoding="utf-8")
        consumption = DayConsumption.model_validate_json(data)
        # Ensure timestamps are localized correctly upon loading
        for record in consumption.records:
            if record.timestamp.tzinfo is None:  # If naive, assume it's Vienna time and localize
                record.timestamp = record.timestamp.replace(tzinfo=_VIENNA_TZ)
            elif not isinstance(record.timestamp.tzinfo, ZoneInfo):  # If it's another tzinfo, convert to Vienna
                record.timestamp = record.timestamp.astimezone(_VIENNA_TZ)
        return consumption

    async def exists(self, metering_point: str, day: date) -> bool:
        return await asyncio.to_thread(self._get_path(metering_point, day).exists)

    async def close(self) -> None:
        """FilesystemStorage does not have open connections to close."""
        pass

    async def save_manual_reading(
        self,
        metering_point: str,
        reading_type: str,
        source: str,
        value: float,
        timestamp: datetime.datetime,
    ) -> None:
        reading_dir = self.base_path / metering_point / self.manual_readings_folder
        await asyncio.to_thread(reading_dir.mkdir, parents=True, exist_ok=True)

        # Use a filesystem-safe ISO format for the filename
        file_timestamp = timestamp.strftime("%Y-%m-%dT%H-%M-%S")
        file_path = reading_dir / f"{file_timestamp}.json"

        reading_data = {
            "metering_point": metering_point,
            "timestamp": timestamp.isoformat(),
            "type": reading_type,
            "source": source,
            "value": value,
            "metric_name": "manual_meter_reading",
        }

        await asyncio.to_thread(file_path.write_text, json.dumps(reading_data, indent=2), encoding="utf-8")
