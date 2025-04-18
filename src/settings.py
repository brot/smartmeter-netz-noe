import datetime
import pathlib

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    username: str = Field(..., validation_alias="WEB_PORTAL_USERNAME")
    password: str = Field(..., validation_alias="WEB_PORTAL_PASSWORD")
    measure_start_date: datetime.date
    storage_path: pathlib.Path
    user_agent: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
