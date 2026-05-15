# Project Guidelines & Architectural Standards

This document outlines the architectural decisions and coding standards established for the `smartmeter-netz-noe` project.

## 1. Project Structure
We follow the standard Python project layout to ensure tool compatibility (e.g., `pytest`, `uv`) and clear separation of concerns.

- **`src/`**: Contains all production code. It is treated as a top-level package.
- **`tests/`**: Located at the project root (not inside `src/`). This keeps the production package lean and simplifies test discovery.

## 2. Package Initialization
- Every directory within `src/` intended to be a Python package or sub-package must contain an `__init__.py` file.
- This allows tools like `pytest` to correctly resolve the module hierarchy when running from the project root.

## 3. Import Conventions
To avoid `ModuleNotFoundError` and maintain consistency:

- **Inside `src/`**: Use **relative imports** (e.g., `from . import settings`) when referencing sibling modules.
- **Inside `tests/`**: Use **absolute imports** starting from the root package (e.g., `from src import smartmeter`).

## 4. Configuration (Pydantic V2)
We use Pydantic V2 for settings management via `pydantic-settings`.

- **Deprecation**: Do not use the V1-style `class Config`.
- **`model_config`**: Use the `model_config` attribute with `SettingsConfigDict`.
  ```python
  model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
  ```

## 5. Development Workflow
We use `uv` for dependency management and task execution.

### Running Tests
```bash
uv run pytest
```

### Running Modules
Run scripts as modules to ensure the `src` package is correctly added to the `PYTHONPATH`:
```bash
uv run python -m src.main
```

## 6. Architectural Principles

### Loose Coupling & Testability
- **Dependency Injection**: Storage backends should be injected into service classes (like `ConsumptionDownloader`).
- **Interfaces**: Use Abstract Base Classes (ABCs) to define storage interfaces. This allows switching between VictoriaMetrics, Filesystem, or Mocks for testing without changing core logic.

### Strict Typing
- Every function and method must have complete type hints (PEP 484).
- Use Pydantic models for data transfer objects (DTOs) to ensure data integrity across the system.

### Timezone Handling
- The source API provides naive local times.
- Always explicitly localize timestamps to `Europe/Vienna` using `zoneinfo.ZoneInfo` before processing or storage.

## 7. Storage Backend Strategy
The system is designed to support multiple "Sinks":

- **VictoriaMetrics**: High-performance time-series storage using InfluxDB Line Protocol.
- **Filesystem**: Structured JSON storage (`{path}/{meter}/{YYYY}/{MM}/{YYYY-MM-DD}.json`).

## 8. Migration & Interoperability
The architecture supports complex data migrations (e.g., moving from an old flat file structure to the new nested structure or a database).

### Migration Pattern: Read -> Transform -> Write
1. **Read**: Use a specialized `LegacyReader` or the existing `FilesystemBackend` to load raw data.
2. **Transform**: Map raw dictionary data into the unified `DayConsumption` Pydantic model.
3. **Write**: Pass the validated `DayConsumption` object to any modern `BaseStorage` implementation (New Filesystem layout or VictoriaMetrics).

This approach ensures that migration logic is decoupled from the actual storage implementation, making it possible to re-run migrations or change target backends with ease.

## 9. Error Handling
- Use `tenacity` for retrying network operations (API calls, DB writes).
- Log errors with `exc_info=True` in exception blocks to capture stack traces.
- Distinguish between transient errors (network) and permanent errors (validation/logic).

## 10. Dependency Management
- **Maintained Libraries Only**: We only use libraries that are currently and actively maintained. Avoid using "dead" or abandoned projects.
- **Explicit Prohibitions**: Do not use `aiopath` (unmaintained). Use `asyncio.to_thread` with standard `pathlib` for filesystem operations to keep dependencies lean and reliable.
- **Async First**: Prefer modern asynchronous libraries (like httpx2) over legacy synchronous ones (like requests).