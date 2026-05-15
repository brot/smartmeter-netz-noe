import argparse
import asyncio
import datetime
import json
import logging
import math
import pathlib

from .models import DayConsumption
from .settings import Settings
from .smartmeter import SmartMeter
from .storage.filesystem import FilesystemStorage
from .storage.victoriametrics import VictoriaMetricsStorage

_logger = logging.getLogger(__name__)


def compare_consumption(old: DayConsumption, new: DayConsumption) -> bool:
    """
    Compares two DayConsumption objects.
    We ignore 'raw_data' as legacy raw format might differ slightly from API raw format.
    """
    if len(old.records) != len(new.records):
        _logger.error(
            "Record count mismatch: old has %d records, new has %d records", len(old.records), len(new.records)
        )
        return False

    # Compare records (timestamp and metered value)
    for i, (r1, r2) in enumerate(zip(old.records, new.records)):
        # 1. Check Timestamps (compare UTC points via timestamp())
        if r1.timestamp.timestamp() != r2.timestamp.timestamp():
            _logger.error(
                "Timestamp mismatch at index %d: old=%s (%s), new=%s (%s)",
                i,
                r1.timestamp.isoformat(),
                r1.timestamp.timestamp(),
                r2.timestamp.isoformat(),
                r2.timestamp.timestamp(),
            )
            return False

        # 2. Check Metered values using float tolerance
        v1 = r1.metered if r1.metered is not None else 0.0
        v2 = r2.metered if r2.metered is not None else 0.0
        if not math.isclose(v1, v2, rel_tol=1e-9):
            _logger.error("Metered value mismatch at %s: old=%r, new=%r", r1.timestamp, r1.metered, r2.metered)
            return False

    # 3. Compare Mean Profile
    if (old.mean_profile is None) != (new.mean_profile is None):
        _logger.error(
            "Mean profile presence mismatch: old=%s, new=%s", old.mean_profile is None, new.mean_profile is None
        )
        return False

    if old.mean_profile and new.mean_profile:
        if len(old.mean_profile) != len(new.mean_profile):
            _logger.error("Mean profile length mismatch")
            return False
        for i, (m1, m2) in enumerate(zip(old.mean_profile, new.mean_profile)):
            mv1 = m1 if m1 is not None else 0.0
            mv2 = m2 if m2 is not None else 0.0
            if not math.isclose(mv1, mv2, rel_tol=1e-9):
                _logger.error("Mean profile value mismatch at index %d: old=%r, new=%r", i, m1, m2)
                return False

    return True


async def run_migration(legacy_path: pathlib.Path):
    settings = Settings()

    # Initialize modern Sinks
    new_fs = FilesystemStorage(settings.storage_path)
    vm_storage = VictoriaMetricsStorage(settings.victoriametrics_url)

    _logger.info("Starting migration from %s to %s", legacy_path, settings.storage_path)

    # Legacy structure: {path}/{bp_id}/{metering_point}/{YYYY-MM-DD}.json
    for old_file in legacy_path.rglob("*.json"):
        try:
            # 1. Extract context from path
            # stem is YYYY-MM-DD
            day_str = old_file.stem
            try:
                day = datetime.date.fromisoformat(day_str)
            except ValueError:
                continue  # Skip files not named like YYYY-MM-DD.json

            metering_point = old_file.parent.name

            # 2. READ & TRANSFORM
            with open(old_file, "r", encoding="utf-8") as f:
                raw_data = json.load(f)

                # Extract mean profile if it exists in the legacy JSON structure
                legacy_mean_profile = None
                if isinstance(raw_data, dict):
                    legacy_mean_profile = raw_data.get("meanProfile")
                elif isinstance(raw_data, list) and len(raw_data) > 0:
                    # Some legacy files might be lists, check first element
                    legacy_mean_profile = raw_data[0].get("meanProfile")

                consumption = SmartMeter.parse_day_response(
                    metering_point=metering_point, day=day, raw_response=raw_data, mean_profile=legacy_mean_profile
                )

            # 3. Check for existing data and compare or save
            if await new_fs.exists(metering_point, day):
                existing_data = await new_fs.load(metering_point, day)
                if existing_data and compare_consumption(consumption, existing_data):
                    _logger.debug("Data for %s on %s is identical in both, skipping.", metering_point, day)
                else:
                    _logger.error(
                        "DATA MISMATCH: %s on %s exists in new format but differs from legacy!", metering_point, day
                    )
            elif not consumption.records:
                _logger.warning("Legacy file %s resulted in 0 records. Skipping migration.", old_file.name)
            else:
                # Data doesn't exist in new format, migrate it
                _logger.info("Migrating %s for %s", day, metering_point)

                if settings.use_filesystem_backup:
                    await new_fs.save(consumption)

                if settings.use_victoriametrics:
                    await vm_storage.save(consumption)

        except Exception as e:
            _logger.error("Failed to migrate %s: %s", old_file.name, e, exc_info=True)


async def main():
    parser = argparse.ArgumentParser(description="Migrate legacy SmartMeter JSON files to new structure")
    parser.add_argument(
        "--path",
        type=pathlib.Path,
        required=True,
        help="Path to the legacy storage root (where bp_id folders are located)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    await run_migration(args.path)


if __name__ == "__main__":
    asyncio.run(main())
