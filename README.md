# SmartMeter Netz NÖ Downloader

A Python-based utility to automate the downloading and storage of electricity consumption data from the Netz NÖ Smart Meter portal.

## 🏗️ Architecture & Features

The project is designed with modularity and robustness in mind, utilizing a modern asynchronous stack:

1.  **High-Performance Async**: Built on the latest **`httpx2`** library for efficient, non-blocking network operations.

2.  **Modular Storage Backends**
    *   Uses an abstract `BaseStorage` interface.
    *   **VictoriaMetrics**: High-performance time-series storage (Prometheus/InfluxDB line protocol compatible).
    *   **Filesystem**: Local JSON backup and monthly archiving utility.
    *   Easily extensible for other databases (e.g., PostgreSQL).

3.  **Reliability**
    *   Automatic retry logic with exponential backoff using the `tenacity` library.
    *   Session management that automatically re-authenticates on expired sessions (401 errors).
    *   Structured logging for easy debugging.

4.  **Data Integrity**
    *   Strongly typed data models using **Pydantic**.
    *   Validation of API responses and storage formats.
    *   Handles Energy Community (`ec_id`) data sets by prioritizing primary consumption data.

5.  **Archiving**
    *   Includes an archiving script to compress daily JSON files into monthly ZIP archives, keeping the storage clean and efficient.

## 📁 Project Structure

```
src/
├── smartmeter.py          # SmartMeter API Client
├── models.py              # Pydantic Datenmodelle
├── settings.py            # Konfigurationsverwaltung
├── main.py                # Hauptskript für Download & Storage
├── archive.py             # Archivierungsskript
├── migrate.py             # Migration helper for old folder layouts
├── sync.py                # Utility to backfill VictoriaMetrics from local files
└── storage/
   ├── base.py            # Abstract BaseStorage Klasse
   ├── victoriametrics.py # VictoriaMetrics Backend Implementation
   └── filesystem.py      # Filesystem Backend Implementation
```

## 🚀 Getting Started

### 1. Installation
Install project and its dependencies using uv:

      uv sync

### 2. Configuration
Copy the template and edit the .env file with your credentials.

      cp .env.example .env

Required Variables:
- WEB_PORTAL_USERNAME: Username for smartmeter.netz-noe.at
- WEB_PORTAL_PASSWORD: Password
- STORAGE_PATH: Path for local JSON backup storage

Optional Variables:
- VICTORIAMETRICS_URL: Write endpoint for VictoriaMetrics
- USE_VICTORIAMETRICS: Enable storage in VictoriaMetrics (default: true)

### 3. Downloading Data
Execute the main script:

      uv run python -m src.main

With debug logging:

      LOG_LEVEL=DEBUG uv run python -m src.main

The script connects to the portal, fetches available metering points, downloads missing data, and stores it in the configured backends.

### 4. Testing
To run the test suite, ensure development dependencies are installed and use `uv`:

      uv add --dev pytest  # If not already present
      uv run python -m pytest

Tests are located in the `tests/` directory at the project root.

### 5. Syncing Data
If you have local JSON files but VictoriaMetrics is empty, use the sync utility to backfill data without hitting the API:

      uv run python -m src.sync

### 6. Archiving
Archive all months except the current one:

      uv run python -m src.archive --all

Archive and delete original JSON files:

      uv run python -m src.archive --all --delete

Archives are stored as ZIP files in the root of your storage path.

## 🔁 Migration
If you have data from older versions in a flat folder structure, use the migration utility:

      uv run python -m src.migrate --path /path/to/storage

## 🏛️ Technical Details

### VictoriaMetrics Backend
Uses InfluxDB Line Protocol. Data is stored with labels for metering_point and day. Metrics include metered consumption, peak power, and self-coverage values.

### Filesystem Backend
Structure: {STORAGE_PATH}/{meter}/{YYYY}/{MM}/{YYYY-MM-DD}.json
Format: Indented JSON containing raw API response data for long-term backup and local processing.

### Query Examples (PromQL)
Total consumption per day: sum by (day) (increase(consumption_metered[1d]))
