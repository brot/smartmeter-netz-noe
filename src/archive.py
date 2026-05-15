#!/usr/bin/env python3
"""Archive raw consumption data into monthly ZIP files."""

import argparse
import datetime
import logging
import pathlib
import zipfile
from typing import Optional

from . import settings

_logger = logging.getLogger(__name__)
_settings = settings.Settings()


class ConsumptionArchiver:
    """Archives raw consumption JSON files into monthly ZIP files."""

    def __init__(self, base_path: pathlib.Path):
        """
        Initialize the archiver.

        Args:
            base_path: Base directory containing year-month subdirectories
        """
        self.base_path = pathlib.Path(base_path)

    def _get_archive_filename(self, year: int, month: int) -> str:
        """Get the ZIP filename for a given year and month."""
        return f"consumption_{year:04d}_{month:02d}.zip"

    def _get_year_month_dir(self, year: int, month: int) -> pathlib.Path:
        """Get the directory containing data for a given year and month."""
        # Not used for meter-rooted layout; kept for compatibility
        return self.base_path / f"{year:04d}" / f"{month:02d}"

    def archive_month(
        self,
        year: int,
        month: int,
        delete_after: bool = False,
        keep_current_month: bool = True,
    ) -> bool:
        """
        Archive all JSON files for a given month into a ZIP file.

        Args:
            year: Year
            month: Month (1-12)
            delete_after: Delete original files after archiving
            keep_current_month: Never archive the current month

        Returns:
            bool: True if successful, False otherwise
        """
        # Don't archive current month by default
        if keep_current_month:
            today = datetime.date.today()
            if year == today.year and month == today.month:
                _logger.info("Skipping current month %s-%s", year, month)
                return False

        # Collect JSON files across meter/YYYY/MM layout
        json_files = []
        for meter_dir in sorted(self.base_path.iterdir()):
            if not meter_dir.is_dir():
                continue
            candidate = meter_dir / f"{year:04d}" / f"{month:02d}"
            if not candidate.exists() or not candidate.is_dir():
                continue
            json_files.extend(sorted(candidate.glob("*.json")))

        if not json_files:
            _logger.info("No JSON files found for %04d-%02d", year, month)
            return False

        archive_name = self._get_archive_filename(year, month)
        archive_path = self.base_path / archive_name

        if archive_path.exists():
            _logger.info("Archive %s already exists", archive_path)
            return True

        try:
            _logger.info("Creating archive %s with %s files", archive_path, len(json_files))
            with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
                for json_file in json_files:
                    try:
                        arcname = str(json_file.relative_to(self.base_path))
                    except Exception:
                        arcname = json_file.name
                    zip_file.write(json_file, arcname=arcname)
                    _logger.debug("Added %s to archive", arcname)

            _logger.info("Archive created successfully: %s", archive_path)

            if delete_after:
                # Delete files and collect parent directories for cleanup
                parents = set()
                for jf in json_files:
                    try:
                        jf.unlink()
                        _logger.debug("Deleted original file %s", jf)
                    except Exception as e:
                        _logger.warning("Failed to delete %s: %s", jf, e)
                    month_dir = jf.parent
                    year_dir = month_dir.parent
                    meter_dir = year_dir.parent
                    parents.add(month_dir)
                    parents.add(year_dir)
                    parents.add(meter_dir)

                # Attempt to remove empty directories (deepest first)
                for d in sorted(parents, key=lambda p: len(p.parts), reverse=True):
                    try:
                        d.rmdir()
                        _logger.info("Removed empty directory %s", d)
                    except OSError:
                        pass

            return True
        except Exception as e:
            _logger.error("Failed to create archive for %s-%s: %s", year, month, e)
            return False

    def archive_all_months(
        self,
        start_date: Optional[datetime.date] = None,
        end_date: Optional[datetime.date] = None,
        delete_after: bool = False,
        keep_current_month: bool = True,
    ) -> int:
        """
        Archive all months within the specified date range.

        Args:
            start_date: Start month (defaults to oldest available)
            end_date: End month (defaults to previous month)
            delete_after: Delete original files after archiving
            keep_current_month: Never archive the current month

        Returns:
            Number of successfully archived months
        """
        if end_date is None:
            today = datetime.date.today()
            # Default to previous month
            end_date = today.replace(day=1) - datetime.timedelta(days=1)

        if start_date is None:
            # Discover the oldest year/month across all meter directories
            candidates = []
            for meter_dir in sorted(self.base_path.iterdir()):
                if not meter_dir.is_dir():
                    continue
                for year_dir in sorted(meter_dir.iterdir()):
                    if not year_dir.is_dir() or not year_dir.name.isdigit():
                        continue
                    for month_dir in sorted(year_dir.iterdir()):
                        if not month_dir.is_dir() or not month_dir.name.isdigit():
                            continue
                        candidates.append((int(year_dir.name), int(month_dir.name)))

            if not candidates:
                _logger.info("No data directories found")
                return 0

            year, month = min(candidates)
            start_date = datetime.date(year, month, 1)

        archived_count = 0
        current_date = start_date.replace(day=1)
        end_date = end_date.replace(day=1)

        while current_date <= end_date:
            if self.archive_month(
                current_date.year,
                current_date.month,
                delete_after=delete_after,
                keep_current_month=keep_current_month,
            ):
                archived_count += 1

            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)

        _logger.info("Archived %s months", archived_count)
        return archived_count


def main():
    """Entry point for archiving script."""
    parser = argparse.ArgumentParser(description="Archive raw consumption data into monthly ZIP files")
    parser.add_argument(
        "--path",
        type=pathlib.Path,
        default=_settings.storage_path,
        help="Base path for storage (default: from settings)",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Specific year to archive (requires --month)",
    )
    parser.add_argument(
        "--month",
        type=int,
        help="Specific month to archive (requires --year)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Archive all available months",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete original JSON files after archiving",
    )
    parser.add_argument(
        "--allow-current",
        action="store_true",
        help="Also archive current month (default: skip current month)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    archiver = ConsumptionArchiver(args.path)

    if args.year and args.month:
        # Archive specific month
        if args.month < 1 or args.month > 12:
            _logger.error("Month must be between 1 and 12")
            return 1

        success = archiver.archive_month(
            args.year,
            args.month,
            delete_after=args.delete,
            keep_current_month=not args.allow_current,
        )
        return 0 if success else 1

    elif args.all:
        # Archive all months
        archived = archiver.archive_all_months(
            delete_after=args.delete,
            keep_current_month=not args.allow_current,
        )
        return 0 if archived > 0 else 1

    else:
        # Default: archive all months except current
        archived = archiver.archive_all_months(
            delete_after=args.delete,
            keep_current_month=not args.allow_current,
        )
        return 0 if archived > 0 else 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
