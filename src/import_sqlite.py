#!/usr/bin/env python3
import datetime
import json
import sqlite3

import settings

_settings = settings.Settings()

DB_FILENAME = "consumption_data.db"

CREATE_TABLE_SQL = """
CREATE TABLE consumption_data
(id INTEGER PRIMARY KEY AUTOINCREMENT,
timestamp DATETIME,
metered REAL,
estimated REAL,
metered_peak REAL,
estimated_peak REAL,
mean_profile REAL)
"""
CREATE_UNIQUE_INDEX_SQL = """
CREATE UNIQUE INDEX consumption_timestamp_idx
ON consumption_data (timestamp)
"""
INSERT_SQL = """
INSERT INTO consumption_data
(timestamp, metered, estimated, metered_peak, estimated_peak, mean_profile)
VALUES
(?, ?, ?, ?, ?, ?)
ON CONFLICT(timestamp) DO NOTHING
"""

IGNORED_PATH = [".DS_Store"]


def adapt_datetime_iso(val):
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return val.isoformat()


def convert_datetime(val):
    """Convert ISO 8601 datetime to datetime.datetime object."""
    return datetime.datetime.fromisoformat(val.decode())


sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
sqlite3.register_converter("datetime", convert_datetime)


def _check_database(con):
    try:
        con.execute("SELECT 1 FROM consumption_data WHERE false")
    except sqlite3.OperationalError as except_inst:
        print(except_inst)
        con.execute(CREATE_TABLE_SQL)
        con.execute(CREATE_UNIQUE_INDEX_SQL)


def import_data():
    for account_path in _settings.storage_path.iterdir():
        # account_id = account_path.name
        if not account_path.is_dir():
            continue

        for energy_meter_path in account_path.iterdir():
            if energy_meter_path.name in IGNORED_PATH:
                continue

            db_file = energy_meter_path / DB_FILENAME
            con = sqlite3.connect(str(db_file))
            _check_database(con)

            for day_path in sorted(energy_meter_path.glob("*.json")):
                # day = datetime.datetime.strptime(day_path.stem, "%Y-%m-%d").date()
                with day_path.open() as fobj:
                    json_data = json.loads(fobj.read())

                data = list(
                    zip(
                        [
                            datetime.datetime.fromisoformat(d)
                            for d in json_data["peakDemandTimes"]
                        ],
                        json_data["meteredValues"],
                        json_data["estimatedValues"],
                        json_data["meteredPeakDemands"],
                        json_data["estimatedPeakDemands"],
                        json_data["meanProfile"],
                    )
                )

                with con:
                    con.executemany(INSERT_SQL, data)

            con.close()


if __name__ == "__main__":
    import_data()
