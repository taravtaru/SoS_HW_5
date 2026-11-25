from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Self
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, field_validator, model_validator


class EventType(StrEnum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(StrEnum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


Identifier = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class MovieEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID = Field(default_factory=uuid4)
    user_id: Identifier
    movie_id: Identifier
    event_type: EventType
    timestamp: AwareDatetime
    device_type: DeviceType
    session_id: Identifier
    progress_seconds: Annotated[int, Field(strict=True, ge=0, le=2**31 - 1)] | None = None

    @field_validator("timestamp", mode="before")
    @classmethod
    def timestamp_format(cls, value: object) -> object:
        if not isinstance(value, (str, datetime)):
            raise ValueError("timestamp must be a datetime with a UTC offset")
        return value

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        value = value.astimezone(UTC)
        if not 1970 <= value.year < 2100:
            raise ValueError("timestamp must be between 1970 and 2099 UTC")
        return value.replace(microsecond=value.microsecond // 1000 * 1000)

    @model_validator(mode="after")
    def require_view_progress(self) -> Self:
        if self.event_type.startswith("VIEW_") and self.progress_seconds is None:
            raise ValueError("progress_seconds is required for playback events")
        return self
