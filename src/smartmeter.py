#!/usr/bin/env python3
import datetime
import json
import logging
import pathlib
import sys
from enum import Enum
from pydantic import BaseModel
from typing import Union

import requests

import settings

BASE_URL = "https://smartmeter.netz-noe.at/orchestration"

_logger = logging.getLogger(__name__)
_settings = settings.Settings()


class Context(Enum):
    CONSUMPTION_INFO = 2
    DOWNLOAD_INFO = 5


# These are only the needed fields of the consumption info response
class ConsumptionInfo(BaseModel):
    account_id: str
    metering_point_id: str


class SmartMeter:
    def __init__(self, username: str, password: str) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": _settings.user_agent})

        self._login(username, password)
        self._extend_session_lifetime()

    def _login(self, username: str, password: str) -> None:
        """Log into the smartmeter portal from Netz NÖ."""
        url = f"{BASE_URL}/Authentication/Login"
        response = self.session.post(url, json={"user": username, "pwd": password})
        response.raise_for_status()
        # raise_for_status does not deal with 999 -> smartmeter wartungsarbeiten
        if response.status_code == 999:
            print("Smartmeter Platform dürfte wieder Wartungsarbeiten durchführen!")
            sys.exit(1)

    def _extend_session_lifetime(self) -> None:
        url = f"{BASE_URL}/Authentication/ExtendSessionLifetime"
        response = self.session.get(url)
        response.raise_for_status()

    def get_consumption_info(self) -> list[ConsumptionInfo]:
        """
        Retrieves all metering points and account-ids linked to the login.

        :return: Dictionary with
            key: account-id
            value: list of metering points for the account
        """
        url = f"{BASE_URL}/User/GetMeteringPointsByBusinesspartnerId"
        response = self.session.get(url, params={"context": Context.CONSUMPTION_INFO.value})
        response.raise_for_status()
        json_response = response.json()
        _logger.debug("Response for '%s' was: %s", url, json.dumps(json_response))

        return [
            ConsumptionInfo(
                account_id=consumption_info["accountId"], metering_point_id=consumption_info["meteringPointId"]
            )
            for consumption_info in json_response
        ]

    def _get_mean_profile_for_day(
        self, metering_point: str, day: datetime.date
    ) -> dict[str, list[Union[None, str, float]]]:
        """Retrieve mean profile for the given day."""
        url = f"{BASE_URL}/ConsumptionRecord/MeanProfileDay"

        response = self.session.get(url, params={"meterId": metering_point, "day": day.isoformat()})
        response.raise_for_status()
        _logger.debug("Response for '%s' was: %s", url, json.dumps(response.json()))

        return response.json()

    def get_consumption_records_for_day(
        self,
        metering_point: str,
        day: datetime.date,
        *,
        include_mean_profile: bool = True,
    ) -> dict[str, list[Union[None, str, float]]]:
        """
        Retrieve the consumption records for the given day and the given metering point.

        :param metering_point: metering point
        :param day: day
        :param include_mean_profile: indicates if mean profile of the day should be included
        """
        url = f"{BASE_URL}/ConsumptionRecord/Day"

        response = self.session.get(url, params={"meterId": metering_point, "day": day.isoformat()})
        response.raise_for_status()
        _logger.debug("Response for '%s' was: %s", url, json.dumps(response.json()))

        consumption_per_day = response.json()
        if isinstance(consumption_per_day, list):
            if len(consumption_per_day) > 1:
                _logger.error(
                    "Consumption records for '%s' and '%s' returns more than one entry!",
                    metering_point,
                    day.isoformat(),
                )
                sys.exit(1)

        if not include_mean_profile:
            return consumption_per_day[0]

        mean_profile = self._get_mean_profile_for_day(metering_point, day)
        return consumption_per_day[0] | {"meanProfile": mean_profile}


def download_consumptions_for_meter(
    smartmeter: SmartMeter,
    storage_path: pathlib.Path,
    energy_meter: str,
    start_date: datetime.date,
):
    """
    Download consumption records starting from start-date for given engery-meter.
    """
    number_since_measure_start = (datetime.date.today() - start_date).days
    for day in [start_date + datetime.timedelta(days=i) for i in range(number_since_measure_start)]:
        json_file = storage_path / f"{day.isoformat()}.json"
        if json_file.exists():
            _logger.debug(
                "Consumption records for '%s' and '%s' already exists.",
                energy_meter,
                day.isoformat(),
            )
            continue

        consumption_records = smartmeter.get_consumption_records_for_day(energy_meter, day)
        if not consumption_records["meteredValues"]:
            _logger.error(
                "Consumption records for '%s' and '%s' missing data.",
                energy_meter,
                day.isoformat(),
            )
            continue

        json_file.write_text(json.dumps(consumption_records, indent=4))


def main():
    smartmeter = SmartMeter(_settings.username, _settings.password)
    for consumption_info in smartmeter.get_consumption_info():
        account_dir = _settings.storage_path / consumption_info.account_id
        if not account_dir.exists():
            account_dir.mkdir()

        energy_meter_dir = account_dir / consumption_info.metering_point_id
        if not energy_meter_dir.exists():
            energy_meter_dir.mkdir()

        download_consumptions_for_meter(
            smartmeter, energy_meter_dir, consumption_info.metering_point_id, _settings.measure_start_date
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    main()
