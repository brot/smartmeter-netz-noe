import datetime
from abc import ABC, abstractmethod
from datetime import date
from typing import Optional

from ..models import DayConsumption


class BaseStorage(ABC):
    """Abstract base class for all storage backends."""

    @abstractmethod
    async def save(self, consumption: DayConsumption) -> None:
        """Save a DayConsumption record to the storage."""
        pass

    @abstractmethod
    async def load(self, metering_point: str, day: date) -> Optional[DayConsumption]:
        """Load a DayConsumption record from storage if it exists."""
        pass

    @abstractmethod
    async def exists(self, metering_point: str, day: date) -> bool:
        """Check if a record already exists in storage."""
        pass

    @abstractmethod
    async def save_manual_reading(
        self,
        metering_point: str,
        reading_type: str,
        source: str,
        value: float,
        timestamp: datetime.datetime,
    ) -> None:
        """Save a manual meter reading (e.g. annual bill) to storage."""
        pass

    async def close(self) -> None:
        """Close connections and cleanup resources."""
        pass
