import datetime
import pathlib

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Web portal credentials
    username: str = Field(..., validation_alias="WEB_PORTAL_USERNAME")
    password: str = Field(..., validation_alias="WEB_PORTAL_PASSWORD")
    user_agent: str

    # Data collection
    measure_start_date: datetime.date

    # Storage backends
    storage_path: pathlib.Path = Field(description="Path for filesystem backup storage")

    # VictoriaMetrics configuration
    victoriametrics_url: str = Field(default="http://localhost:8428/write", validation_alias="VICTORIAMETRICS_URL")
    victoriametrics_query_url: str = Field(
        default="http://localhost:8428/api/v1/query", validation_alias="VICTORIAMETRICS_QUERY_URL"
    )

    # Feature flags
    use_victoriametrics: bool = Field(default=True, description="Enable VictoriaMetrics storage")
    use_filesystem_backup: bool = Field(default=True, description="Enable filesystem backup")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
