#!/usr/bin/env python3
"""Data models for smartmeter consumption records."""

import datetime
from typing import Optional, Union

from pydantic import BaseModel, Field


class ConsumptionInfo(BaseModel):
    """Information about a metering point and account."""

    account_id: str
    metering_point_id: str


class ConsumptionRecord(BaseModel):
    """A single consumption record at a specific point in time."""

    timestamp: datetime.datetime = Field(description="Zeitstempel des 15-Minuten-Intervalls")
    metered: Optional[float] = Field(
        None, description="Wirkenergie Bezug (physisch) in kWh. Was tatsächlich über die Leitung floss."
    )
    estimated: Optional[float] = Field(
        None, description="Ersatzwerte in kWh. Berechnet vom Netzbetreiber bei fehlender Funkverbindung."
    )
    metered_peak: Optional[float] = Field(None, description="Gemessene Viertelstundenleistung in kW.")
    estimated_peak: Optional[float] = Field(None, description="Geschätzte Viertelstundenleistung in kW.")
    grid_usage_leftover: Optional[float] = Field(
        None, description="Netzrestbezug in kWh. Tatsächlicher Bezug nach Abzug von Gemeinschaftsstrom."
    )
    self_coverage: Optional[float] = Field(
        None, description="Eigenabdeckung in kWh. Anteil der durch die Energiegemeinschaft gedeckt wurde."
    )
    joint_tenancy: Optional[float] = Field(
        None, description="Anteil Gemeinschaftsanlage in kWh (z.B. PV-Anlage in Mehrparteienhäusern)."
    )
    blind_consumption: Optional[float] = Field(None, description="Blindenergie Bezug in kvarh (reaktive Energie).")
    blind_power_feed: Optional[float] = Field(None, description="Blindenergie Einspeisung in kvarh.")


class DayConsumption(BaseModel):
    """Complete consumption data for a single day."""

    metering_point: str
    day: datetime.date
    ec_id: Optional[str] = Field(
        None, description="Energy Community ID. Falls vorhanden, gehört dieser Datensatz zu einer Energiegemeinschaft."
    )
    records: list[ConsumptionRecord]
    mean_profile: Optional[list[float]] = Field(None, description="Vergleichsprofil (Durchschnittswerte) in kWh.")
    raw_data: Union[dict, list[dict]] = Field(
        default_factory=dict, description="Original JSON response from the API for archiving."
    )

    model_config = {"arbitrary_types_allowed": True}
