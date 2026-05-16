import argparse
import datetime
import logging
import pathlib
from unittest.mock import AsyncMock, patch

import pytest
from src.record_reading import (
    _VIENNA_TZ,
    ReadingSource,
    ReadingType,
    main,
    record_reading,
)
from src.settings import Settings
from src.storage.filesystem import FilesystemStorage
from src.storage.victoriametrics import VictoriaMetricsStorage


@pytest.fixture
def mock_settings(mocker):
    """Mocks the Settings object."""
    mock_settings_instance = mocker.Mock(spec=Settings)
    mock_settings_instance.use_filesystem_backup = True
    mock_settings_instance.use_victoriametrics = True
    mock_settings_instance.storage_path = pathlib.Path("/mock/storage")  # Use a real-ish path for pathlib ops
    mock_settings_instance.manual_readings_folder = "manual_readings"
    mock_settings_instance.victoriametrics_url = "http://mock-vm:8428/write"
    mocker.patch("src.record_reading.Settings", return_value=mock_settings_instance)
    return mock_settings_instance


@pytest.fixture
def mock_filesystem_storage_class(mocker):
    """Mocks the FilesystemStorage class."""
    mock_instance = mocker.AsyncMock(spec=FilesystemStorage)
    # Ensure the logged class name matches expectations for backend.__class__.__name__
    mock_instance.configure_mock(**{"__class__.__name__": "FilesystemStorage"})
    return mocker.patch("src.record_reading.FilesystemStorage", return_value=mock_instance)


@pytest.fixture
def mock_victoriametrics_storage_class(mocker):
    """Mocks the VictoriaMetricsStorage class."""
    mock_instance = mocker.AsyncMock(spec=VictoriaMetricsStorage)
    # Ensure the logged class name matches expectations for backend.__class__.__name__
    mock_instance.configure_mock(**{"__class__.__name__": "VictoriaMetricsStorage"})
    return mocker.patch("src.record_reading.VictoriaMetricsStorage", return_value=mock_instance)


@pytest.mark.asyncio
async def test_record_reading_both_backends_enabled(
    mock_settings, mock_filesystem_storage_class, mock_victoriametrics_storage_class
):
    metering_point = "AT123"
    reading_type = ReadingType.ANNUAL_BILL
    source = ReadingSource.NETZ_NOE
    value = 100.5
    timestamp = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=_VIENNA_TZ)

    await record_reading(metering_point, reading_type, source, value, timestamp)

    # Assert FilesystemStorage was initialized and called
    mock_filesystem_storage_class.assert_called_once_with(
        mock_settings.storage_path, mock_settings.manual_readings_folder
    )
    mock_filesystem_storage_class.return_value.save_manual_reading.assert_called_once_with(
        metering_point, reading_type.value, source.value, value, timestamp
    )
    mock_filesystem_storage_class.return_value.close.assert_called_once()

    # Assert VictoriaMetricsStorage was initialized and called
    mock_victoriametrics_storage_class.assert_called_once_with(mock_settings.victoriametrics_url)
    mock_victoriametrics_storage_class.return_value.save_manual_reading.assert_called_once_with(
        metering_point, reading_type.value, source.value, value, timestamp
    )
    mock_victoriametrics_storage_class.return_value.close.assert_called_once()


@pytest.mark.asyncio
async def test_record_reading_filesystem_only(
    mock_settings, mock_filesystem_storage_class, mock_victoriametrics_storage_class
):
    mock_settings.use_victoriametrics = False
    metering_point = "AT123"
    reading_type = ReadingType.ANNUAL_BILL
    source = ReadingSource.NETZ_NOE
    value = 100.5
    timestamp = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=_VIENNA_TZ)

    await record_reading(metering_point, reading_type, source, value, timestamp)

    mock_filesystem_storage_class.return_value.save_manual_reading.assert_called_once()
    mock_filesystem_storage_class.return_value.close.assert_called_once()
    mock_victoriametrics_storage_class.assert_not_called()  # Class itself should not be instantiated


@pytest.mark.asyncio
async def test_record_reading_victoriametrics_only(
    mock_settings, mock_filesystem_storage_class, mock_victoriametrics_storage_class
):
    mock_settings.use_filesystem_backup = False
    metering_point = "AT123"
    reading_type = ReadingType.ANNUAL_BILL
    source = ReadingSource.NETZ_NOE
    value = 100.5
    timestamp = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=_VIENNA_TZ)

    await record_reading(metering_point, reading_type, source, value, timestamp)

    mock_filesystem_storage_class.assert_not_called()  # Class itself should not be instantiated
    mock_victoriametrics_storage_class.return_value.save_manual_reading.assert_called_once()
    mock_victoriametrics_storage_class.return_value.close.assert_called_once()


@pytest.mark.asyncio
async def test_record_reading_filesystem_error_continues_to_vm(
    mock_settings, mock_filesystem_storage_class, mock_victoriametrics_storage_class, caplog
):
    mock_filesystem_storage_class.return_value.save_manual_reading.side_effect = Exception("FS Error")
    metering_point = "AT123"
    reading_type = ReadingType.ANNUAL_BILL
    source = ReadingSource.NETZ_NOE
    value = 100.5
    timestamp = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=_VIENNA_TZ)

    with caplog.at_level(logging.ERROR):
        await record_reading(metering_point, reading_type, source, value, timestamp)

    assert "Failed to save to FilesystemStorage: FS Error" in caplog.text
    mock_filesystem_storage_class.return_value.save_manual_reading.assert_called_once()
    mock_filesystem_storage_class.return_value.close.assert_called_once()  # Close should still be called
    mock_victoriametrics_storage_class.return_value.save_manual_reading.assert_called_once()
    mock_victoriametrics_storage_class.return_value.close.assert_called_once()


@pytest.mark.asyncio
async def test_record_reading_timestamp_localization_naive(
    mock_settings, mock_filesystem_storage_class, mock_victoriametrics_storage_class
):
    metering_point = "AT123"
    reading_type = ReadingType.ANNUAL_BILL
    source = ReadingSource.NETZ_NOE
    value = 100.5
    naive_timestamp = datetime.datetime(2024, 1, 1, 12, 0, 0)
    expected_timestamp = naive_timestamp.replace(tzinfo=_VIENNA_TZ)

    await record_reading(metering_point, reading_type, source, value, naive_timestamp)

    mock_filesystem_storage_class.return_value.save_manual_reading.assert_called_once_with(
        metering_point, reading_type.value, source.value, value, expected_timestamp
    )
    mock_victoriametrics_storage_class.return_value.save_manual_reading.assert_called_once_with(
        metering_point, reading_type.value, source.value, value, expected_timestamp
    )


@pytest.mark.asyncio
async def test_record_reading_timestamp_localization_aware(
    mock_settings, mock_filesystem_storage_class, mock_victoriametrics_storage_class
):
    metering_point = "AT123"
    reading_type = ReadingType.ANNUAL_BILL
    source = ReadingSource.NETZ_NOE
    value = 100.5
    aware_timestamp = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=_VIENNA_TZ)

    await record_reading(metering_point, reading_type, source, value, aware_timestamp)

    mock_filesystem_storage_class.return_value.save_manual_reading.assert_called_once_with(
        metering_point, reading_type.value, source.value, value, aware_timestamp
    )
    mock_victoriametrics_storage_class.return_value.save_manual_reading.assert_called_once_with(
        metering_point, reading_type.value, source.value, value, aware_timestamp
    )


# Test for the main function (CLI entry point)
@patch("src.record_reading.record_reading", new_callable=AsyncMock)
@patch("src.record_reading.argparse.ArgumentParser.parse_args")
@pytest.mark.asyncio
async def test_main_function_with_explicit_timestamp(mock_parse_args, mock_record_reading, mocker):
    explicit_timestamp = datetime.datetime(2023, 12, 31, 23, 59, 59)
    mock_parse_args.return_value = argparse.Namespace(
        metering_point="AT456",
        type="annual_bill",
        source="netz_noe",
        value=200.0,
        timestamp=explicit_timestamp,
    )
    await main()
    mock_record_reading.assert_called_once_with(
        metering_point="AT456",
        reading_type=ReadingType.ANNUAL_BILL,
        source=ReadingSource.NETZ_NOE,
        value=200.0,
        timestamp=explicit_timestamp,
    )


@patch("src.record_reading.record_reading", new_callable=AsyncMock)
@patch("src.record_reading.argparse.ArgumentParser.parse_args")
@pytest.mark.asyncio
async def test_main_function_with_default_timestamp(mock_parse_args, mock_record_reading, mocker):
    # Mock datetime.datetime.now to control the default timestamp
    mock_now = mocker.patch("src.record_reading.datetime.datetime")
    mock_now.now.return_value = datetime.datetime(2024, 5, 16, 10, 0, 0, tzinfo=_VIENNA_TZ)
    # Ensure fromisoformat still works for argparse type conversion
    mock_now.fromisoformat = datetime.datetime.fromisoformat

    mock_parse_args.return_value = argparse.Namespace(
        metering_point="AT789",
        type="intermediate",
        source="me",
        value=50.0,
        timestamp=None,  # No timestamp provided, should use now()
    )
    await main()
    mock_record_reading.assert_called_once_with(
        metering_point="AT789",
        reading_type=ReadingType.INTERMEDIATE,
        source=ReadingSource.ME,
        value=50.0,
        timestamp=datetime.datetime(2024, 5, 16, 10, 0, 0, tzinfo=_VIENNA_TZ),
    )
