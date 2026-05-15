#!/usr/bin/env python3
"""SmartMeter API client for downloading consumption data."""

import datetime
import json
import logging
from enum import Enum
from typing import Optional, Union
from zoneinfo import ZoneInfo

import httpx2
from tenacity import retry, stop_after_attempt, wait_exponential

from . import settings
from .models import ConsumptionInfo, ConsumptionRecord, DayConsumption

BASE_URL = "https://smartmeter.netz-noe.at/orchestration"

_logger = logging.getLogger(__name__)
_settings = settings.Settings()
_VIENNA_TZ = ZoneInfo("Europe/Vienna")


class Context(Enum):
    CONSUMPTION_INFO = 2
    DOWNLOAD_INFO = 5


class SmartMeter:
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.password = password
        # Add a default timeout to prevent hanging requests
        self.client = httpx2.AsyncClient(
            headers={"User-Agent": _settings.user_agent},
            timeout=30.0,  # Default timeout of 30 seconds
        )

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self.client.aclose()

    async def _make_authenticated_request(self, method: str, url: str, **kwargs) -> httpx2.Response:
        """
        Helper to make authenticated HTTP requests, handling 401 re-login.
        """
        try:
            response = await self.client.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except httpx2.HTTPStatusError as e:
            if e.response.status_code == 401:
                _logger.warning("Session expired (401), re-logging in...")
                await self.login()  # Re-login
                # Retry the original request after successful re-login
                response = await self.client.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            raise  # Re-raise other HTTP errors

    async def login(self) -> None:
        """Log into the smartmeter portal from Netz NÖ."""
        url = f"{BASE_URL}/Authentication/Login"
        response = await self.client.post(url, json={"user": self.username, "pwd": self.password})

        # Maintenance mode handling
        if response.status_code == 999:
            _logger.error("Smartmeter Platform führt Wartungsarbeiten durch!")
            raise RuntimeError("Platform maintenance in progress")

        response.raise_for_status()
        _logger.info("Successfully logged in")
        await self._extend_session_lifetime()

    async def _extend_session_lifetime(self) -> None:
        """Extend the current session lifetime."""
        url = f"{BASE_URL}/Authentication/ExtendSessionLifetime"
        # Use the new helper method
        await self._make_authenticated_request("GET", url)

    async def get_consumption_info(self) -> list[ConsumptionInfo]:
        """
        Retrieve all metering points and account IDs linked to the login.

        Returns:
            List of ConsumptionInfo objects with account and metering point IDs
        """
        url = f"{BASE_URL}/User/GetMeteringPointsByBusinesspartnerId"
        # Use the new helper method
        response = await self._make_authenticated_request(
            "GET", url, params={"context": Context.CONSUMPTION_INFO.value}
        )

        json_response = response.json()
        _logger.debug("Response: %s", json.dumps(json_response))

        return [
            ConsumptionInfo(
                account_id=consumption_info["accountId"], metering_point_id=consumption_info["meteringPointId"]
            )
            for consumption_info in json_response
        ]

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    async def _get_mean_profile_for_day(self, metering_point: str, day: datetime.date) -> list[Union[None, float]]:
        """Retrieve mean profile for the given day with retry logic."""
        url = f"{BASE_URL}/ConsumptionRecord/MeanProfileDay"
        # Use the new helper method
        response = await self._make_authenticated_request(
            "GET", url, params={"meterId": metering_point, "day": day.isoformat()}
        )

        result = response.json()
        _logger.debug("Mean profile for %s: %s", day, json.dumps(result))
        return result

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    async def get_consumption_records_for_day(
        self,
        metering_point: str,
        day: datetime.date,
    ) -> DayConsumption:
        """
        Retrieve the consumption records for the given day and metering point.

        Args:
            metering_point: Metering point ID
            day: Date to retrieve data for

        Returns:
            DayConsumption object with parsed records
        """
        url = f"{BASE_URL}/ConsumptionRecord/Day"
        # Use the new helper method
        response = await self._make_authenticated_request(
            "GET", url, params={"meterId": metering_point, "day": day.isoformat()}
        )
        raw_response = response.json()
        mean_profile = await self._get_mean_profile_for_day(metering_point, day)

        return self.parse_day_response(metering_point, day, raw_response, mean_profile=mean_profile)

    @staticmethod
    def parse_day_response(
        metering_point: str,
        day: datetime.date,
        raw_response: Union[dict, list[dict]],
        mean_profile: Optional[list[float]] = None,
    ) -> DayConsumption:
        """
        Transform a raw JSON response from the portal into a DayConsumption model.
        This method is static to allow parsing data from local backups without a session.
        """
        _logger.debug("Parsing raw response for %s on %s", metering_point, day)

        # Determine the primary consumption data for parsing records and ec_id
        if isinstance(raw_response, list) and raw_response:
            # Prefer the record without ec_id (Standard-Bezug) for immediate parsing
            primary_consumption_data = next((r for r in raw_response if r.get("ec_id") is None), raw_response[0])
        elif isinstance(raw_response, dict):
            primary_consumption_data = raw_response
        else:
            primary_consumption_data = {}

        # Parse records
        records = []

        # Ensure all data series have the same length as the timestamps.
        # zip() stops at the shortest list, so missing keys (common in tests or sparse responses)
        # must be padded with None to ensure all records are parsed.
        peak_times = primary_consumption_data.get("peakDemandTimes", [])

        def get_series(key: str) -> list:
            val = primary_consumption_data.get(key)
            return val if isinstance(val, list) and len(val) > 0 else [None] * len(peak_times)

        data_iter = zip(
            peak_times,
            get_series("meteredValues"),
            get_series("estimatedValues"),
            get_series("meteredPeakDemands"),
            get_series("estimatedPeakDemands"),
            get_series("gridUsageLeftoverValues"),
            get_series("selfCoverageValues"),
            get_series("jointTenancyProportionValues"),
            get_series("blindConsumptionValue"),
            get_series("blindPowerFeedValue"),
        )

        for pt, met, est, m_peak, e_peak, grid, self_c, joint, b_cons, b_feed in data_iter:
            if not pt:  # Handle cases where peak_times might be empty, preventing IndexError
                continue
            timestamp = datetime.datetime.fromisoformat(pt).replace(tzinfo=_VIENNA_TZ)
            record = ConsumptionRecord(
                timestamp=timestamp,
                metered=met,
                estimated=est,
                metered_peak=m_peak,
                estimated_peak=e_peak,
                grid_usage_leftover=grid,
                self_coverage=self_c,
                joint_tenancy=joint,
                blind_consumption=b_cons,
                blind_power_feed=b_feed,
            )
            records.append(record)

        return DayConsumption(
            metering_point=metering_point,
            day=day,
            ec_id=primary_consumption_data.get("ec_id"),
            records=records,
            mean_profile=mean_profile,
            raw_data=raw_response,  # Store the entire raw API response here
        )
