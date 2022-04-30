#!/usr/bin/env python3
import datetime
import json
import logging
import pathlib
from typing import Union

import requests

import settings

BASE_URL = "https://smartmeter.netz-noe.at/orchestration"

_logger = logging.getLogger(__name__)
_settings = settings.Settings()


class SmartMeter:
    def __init__(self, username: str, password: str) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": _settings.user_agent})

        self._login(username, password)

    def _login(self, username: str, password: str) -> None:
        """Log into the smartmeter portal from Netz NÖ."""
        url = f"{BASE_URL}/Authentication/Login"
        response = self.session.post(url, json={"user": username, "pwd": password})
        response.raise_for_status()

    def _get_account_ids(self) -> list[str]:
        """
        Retrieve account ids linked to the login.

        :return: list of account ids
        """
        url = f"{BASE_URL}/User/GetAccountIdByBussinespartnerId"

        response = self.session.get(url)
        response.raise_for_status()
        _logger.debug("Response for '%s' was: %s", url, json.dumps(response.json()))

        return [account["accountId"] for account in response.json()]

    def _get_meters(self, account_id: str) -> list[str]:
        """
        Retrieve metering points for the given account id.

        :param account_id: account id
        :return: list of metering points
        """
        url = f"{BASE_URL}/User/GetMeteringPointByAccountId"

        response = self.session.get(url, params={"accountId": account_id})
        response.raise_for_status()
        _logger.debug("Response for '%s' was: %s", url, json.dumps(response.json()))

        return [entry["meteringPointId"] for entry in response.json()]

    def get_account_meters(self) -> dict[str, list[str]]:
        """
        Retrieves all metering points and account-ids linked to the login.

        :return: Dictionary with
            key: account-id
            value: list of metering points for the account
        """
        return {
            account_id: self._get_meters(account_id)
            for account_id in self._get_account_ids()
        }

    def _get_mean_profile_for_day(
        self, metering_point: str, day: datetime.date
    ) -> dict[str, list[Union[None, str, float]]]:
        """Retrieve mean profile for the given day."""
        url = f"{BASE_URL}/ConsumptionRecord/MeanProfileDay"

        response = self.session.get(
            url, params={"meterId": metering_point, "day": day.isoformat()}
        )
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

        response = self.session.get(
            url, params={"meterId": metering_point, "day": day.isoformat()}
        )
        response.raise_for_status()
        _logger.debug("Response for '%s' was: %s", url, json.dumps(response.json()))

        if not include_mean_profile:
            return response.json()

        mean_profile = self._get_mean_profile_for_day(metering_point, day)
        return response.json() | {"meanProfile": mean_profile}


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
    for day in [
        start_date + datetime.timedelta(days=i)
        for i in range(number_since_measure_start)
    ]:
        json_file = storage_path / f"{day.isoformat()}.json"
        if json_file.exists():
            _logger.debug(
                "Consumption records for '%s' and '%s' already exists.",
                energy_meter,
                day.isoformat(),
            )
            continue

        consumption_records = smartmeter.get_consumption_records_for_day(
            energy_meter, day
        )
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
    acount_meters = smartmeter.get_account_meters()
    for account_id, energy_meters in acount_meters.items():
        account_dir = _settings.storage_path / account_id
        if not account_dir.exists():
            account_dir.mkdir()

        for energy_meter in energy_meters:
            energy_meter_dir = account_dir / energy_meter
            if not energy_meter_dir.exists():
                energy_meter_dir.mkdir()

            download_consumptions_for_meter(
                smartmeter, energy_meter_dir, energy_meter, _settings.measure_start_date
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    main()
