import datetime
from zoneinfo import ZoneInfo

import pytest
from src import smartmeter
from src.models import DayConsumption

# Define a fixed timezone for testing consistency
_VIENNA_TZ = ZoneInfo("Europe/Vienna")


@pytest.fixture
def mock_settings(mocker):
    """Fixture to mock settings for SmartMeter client."""
    mock_settings_instance = mocker.Mock()
    mock_settings_instance.username = "testuser"
    mock_settings_instance.password = "testpass"
    mock_settings_instance.user_agent = "test-agent"
    mocker.patch("src.smartmeter._settings", new=mock_settings_instance)
    return mock_settings_instance


@pytest.fixture
def mock_httpx2_client(mocker):
    """Fixture to mock httpx2.AsyncClient."""
    mock_client_class = mocker.patch("src.smartmeter.httpx2.AsyncClient")
    return mock_client_class.return_value


@pytest.mark.asyncio
async def test_smartmeter_login_success(mock_settings, mock_httpx2_client, mocker):
    """Test successful login."""
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_httpx2_client.post = mocker.AsyncMock(return_value=mock_response)
    mock_httpx2_client.request = mocker.AsyncMock(return_value=mock_response)  # Added this line
    mock_httpx2_client.get = mocker.AsyncMock(return_value=mock_response)

    sm = smartmeter.SmartMeter(mock_settings.username, mock_settings.password)
    await sm.login()

    mock_httpx2_client.post.assert_called_once_with(
        f"{smartmeter.BASE_URL}/Authentication/Login",
        json={"user": mock_settings.username, "pwd": mock_settings.password},
    )
    mock_httpx2_client.request.assert_called_once_with(
        "GET", f"{smartmeter.BASE_URL}/Authentication/ExtendSessionLifetime"
    )
    assert sm.username == mock_settings.username
    assert sm.password == mock_settings.password


@pytest.mark.asyncio
async def test_smartmeter_login_maintenance_mode(mock_settings, mock_httpx2_client, mocker):
    """Test login during maintenance mode (status code 999)."""
    mock_response = mocker.Mock()
    mock_response.status_code = 999
    mock_httpx2_client.post = mocker.AsyncMock(return_value=mock_response)

    sm = smartmeter.SmartMeter(mock_settings.username, mock_settings.password)

    with pytest.raises(RuntimeError, match="Platform maintenance in progress"):
        await sm.login()


def test_parse_day_response_single_record():
    """Test parsing a single raw response record."""
    metering_point = "AT12345"
    day = datetime.date(2023, 1, 1)
    raw_response = {
        "peakDemandTimes": ["2023-01-01T00:00:00", "2023-01-01T00:15:00"],
        "meteredValues": [0.1, 0.2],
        "estimatedValues": [0.0, 0.0],
        "meteredPeakDemands": [0.5, 0.6],
        "estimatedPeakDemands": [0.0, 0.0],
        "gridUsageLeftoverValues": [0.1, 0.2],
        "selfCoverageValues": [0.0, 0.0],
        "jointTenancyProportionValues": [0.0, 0.0],
        "blindConsumptionValue": [0.0, 0.0],
        "blindPowerFeedValue": [0.0, 0.0],
        "ec_id": None,
    }
    mean_profile = [0.15, 0.18]

    consumption = smartmeter.SmartMeter.parse_day_response(metering_point, day, raw_response, mean_profile)

    assert isinstance(consumption, DayConsumption)
    assert consumption.metering_point == metering_point
    assert consumption.day == day
    assert consumption.ec_id is None
    assert consumption.mean_profile == mean_profile
    assert consumption.raw_data == raw_response
    assert len(consumption.records) == 2

    expected_timestamp_0 = datetime.datetime(2023, 1, 1, 0, 0, tzinfo=_VIENNA_TZ)
    assert consumption.records[0].timestamp == expected_timestamp_0
    assert consumption.records[0].metered == 0.1
    assert consumption.records[0].metered_peak == 0.5


def test_parse_day_response_multiple_records_with_standard():
    """Test parsing multiple records, preferring the one with ec_id=None."""
    metering_point = "AT12345"
    day = datetime.date(2023, 1, 1)
    raw_response = [
        {
            "peakDemandTimes": ["2023-01-01T00:00:00"],
            "meteredValues": [0.3],
            "ec_id": "some_ec_id",
        },
        {
            "peakDemandTimes": ["2023-01-01T00:00:00"],
            "meteredValues": [0.1],
            "ec_id": None,  # This should be preferred
        },
    ]

    consumption = smartmeter.SmartMeter.parse_day_response(metering_point, day, raw_response)

    assert consumption.ec_id is None
    assert consumption.records[0].metered == 0.1
    assert consumption.raw_data == raw_response  # Ensure full raw data is stored
