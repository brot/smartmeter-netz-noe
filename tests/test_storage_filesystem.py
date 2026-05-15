import datetime

import pytest
from src.models import ConsumptionRecord, DayConsumption
from src.storage.filesystem import FilesystemStorage


@pytest.mark.asyncio
async def test_filesystem_storage_save_and_load(tmp_path):
    """Test that saving and loading a record produces the same data."""
    storage = FilesystemStorage(base_path=tmp_path)
    day = datetime.date(2024, 5, 12)
    metering_point = "AT001"

    # Create dummy data
    record = ConsumptionRecord(
        timestamp=datetime.datetime(2024, 5, 12, 12, 0, tzinfo=datetime.timezone.utc), metered=1.5, metered_peak=2.0
    )
    consumption = DayConsumption(
        metering_point=metering_point, day=day, records=[record], mean_profile=[1.2], raw_data={"test": "data"}
    )

    # Test existence before
    assert not await storage.exists(metering_point, day)

    # Save
    await storage.save(consumption)

    # Check if file exists at correct structured path
    expected_file = tmp_path / metering_point / "2024" / "05" / "2024-05-12.json"
    assert expected_file.exists()

    # Test existence after
    assert await storage.exists(metering_point, day)

    # Load and verify
    loaded = await storage.load(metering_point, day)
    assert loaded.metering_point == consumption.metering_point
    assert loaded.day == consumption.day
    assert len(loaded.records) == 1
    assert loaded.records[0].metered == 1.5
    assert loaded.mean_profile == [1.2]


@pytest.mark.asyncio
async def test_filesystem_storage_load_nonexistent(tmp_path):
    """Test loading a file that doesn't exist returns None."""
    storage = FilesystemStorage(base_path=tmp_path)
    result = await storage.load("NONEXISTENT", datetime.date(1999, 1, 1))
    assert result is None
