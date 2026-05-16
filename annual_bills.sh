#!/bin/bash

METERING_POINT=$1
TYPE=annual_bill
SOURCE=netz_noe

uv run python -m src.record_reading --metering-point ${METERING_POINT} --type ${TYPE} --source ${SOURCE} --value 4239.802 --timestamp 2025-03-12T00:00:00
uv run python -m src.record_reading --metering-point ${METERING_POINT} --type ${TYPE} --source ${SOURCE} --value 5960.992 --timestamp 2026-03-11T23:59:45