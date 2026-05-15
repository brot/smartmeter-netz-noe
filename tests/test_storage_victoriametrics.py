import datetime

import pytest
from src.models import ConsumptionRecord, DayConsumption
from src.storage.victoriametrics import VictoriaMetricsStorage


@pytest.mark.asyncio
async def test_victoriametrics_line_protocol_generation(mocker):
    """Verify the line protocol format including mean_profile alignment."""
    # Mock httpx2 AsyncClient
    mock_client_class = mocker.patch("src.storage.victoriametrics.httpx2.AsyncClient")  # Mocks the class
    mock_client_instance = mock_client_class.return_value  # This is the instance returned by httpx2.AsyncClient()

    mock_response = mocker.Mock()
    mock_response.status_code = 204
    mock_client_instance.post = mocker.AsyncMock(return_value=mock_response)

    vm_url = "http://localhost:8428/write"
    storage = VictoriaMetricsStorage(url=vm_url)

    ts = datetime.datetime(2024, 5, 12, 10, 0, tzinfo=datetime.timezone.utc)
    ts_ns = int(ts.timestamp() * 1e9)

    consumption = DayConsumption(
        metering_point="AT_TEST",
        day=datetime.date(2024, 5, 12),
        records=[
            ConsumptionRecord(
                timestamp=ts,
                metered=0.5,
                metered_peak=None,
                grid_usage_leftover=0.4,
                self_coverage=0.1,
                joint_tenancy=0.1,
            )
        ],
        mean_profile=[0.75],
        raw_data={},
    )

    await storage.save(consumption)

    # Expected Line:
    # measurement,tag field1,field2 timestamp
    expected_line = f"consumption,metering_point=AT_TEST metered=0.5,grid_usage_leftover=0.4,self_coverage=0.1,joint_tenancy=0.1,mean_profile=0.75 {ts_ns}\n"

    mock_client_instance.post.assert_called_once()
    args, kwargs = mock_client_instance.post.call_args
    assert args[0] == vm_url
    assert kwargs["content"] == expected_line


@pytest.mark.asyncio
async def test_victoriametrics_http_error(mocker):
    """Test that storage raises an exception if the HTTP request fails."""
    mock_client_class = mocker.patch("src.storage.victoriametrics.httpx2.AsyncClient")
    mock_client_instance = mock_client_class.return_value  # This is the instance returned by httpx2.AsyncClient()

    mock_response = mocker.Mock()
    mock_response.raise_for_status.side_effect = Exception("HTTP Error")
    mock_client_instance.post = mocker.AsyncMock(return_value=mock_response)

    storage = VictoriaMetricsStorage(url="http://fail")
    consumption = DayConsumption(
        metering_point="AT_FAIL",
        day=datetime.date(2024, 5, 12),
        records=[ConsumptionRecord(timestamp=datetime.datetime(2024, 5, 12, 10, 0), metered=1.0)],
    )

    with pytest.raises(Exception, match="HTTP Error"):
        await storage.save(consumption)
