"""Request models and input normalisation.

Identifiers are non-empty strings; integers are accepted for convenience
(exposure numbers are often sent as JSON numbers) and normalised to their
decimal spelling.  Timestamps accept ISO 8601 with ``Z``, an explicit offset
or no offset (assumed UTC), as well as epoch seconds/milliseconds, and are
normalised to a UTC ``...Z`` string.
"""

from __future__ import annotations

import calendar
from datetime import datetime, timezone
from typing import Any

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    field_validator,
    model_validator,
)


def _clean_text(value: Any, *, field: str, max_length: int) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"{field} must be a string")
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field} must not be empty")
    if len(text) > max_length:
        raise ValueError(f"{field} must be at most {max_length} characters")
    return text


_FALLBACK_TIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
)


def normalise_utc(value: Any) -> tuple[str, int]:
    """Return ``(canonical UTC string, microseconds since epoch)``."""

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, bool):
        raise ValueError("occurred_at must be an ISO 8601 string")
    elif isinstance(value, (int, float)):
        seconds = float(value)
        if abs(seconds) >= 1e12:  # epoch milliseconds
            seconds /= 1000.0
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("occurred_at must not be empty")
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            for fmt in _FALLBACK_TIME_FORMATS:
                try:
                    dt = datetime.strptime(text, fmt)
                except ValueError:
                    continue
                break
            else:
                raise ValueError(
                    "occurred_at must be an ISO 8601 timestamp, "
                    "e.g. 2026-01-01T00:00:00Z"
                )
    else:
        raise ValueError("occurred_at must be an ISO 8601 string")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    timespec = "microseconds" if dt.microsecond else "seconds"
    canonical = dt.isoformat(timespec=timespec).replace("+00:00", "Z")
    micros = calendar.timegm(dt.utctimetuple()) * 1_000_000 + dt.microsecond
    return canonical, micros


_EXPOSURE_FIELD_ALIASES = {
    "exposure_id": (
        "exposure_id",
        "exposureId",
        "exposure_no",
        "exposureNo",
        "exposure_number",
        "exposureNumber",
        "id",
        "number",
    ),
    "session_id": ("session_id", "sessionId", "session_no", "sessionNo", "session", "sid"),
    "occurred_at": (
        "occurred_at",
        "occurredAt",
        "utc_time",
        "utcTime",
        "timestamp",
        "time",
        "ts",
        "viewed_at",
        "viewedAt",
    ),
    "variant": (
        "variant",
        "variant_id",
        "variantId",
        "group",
        "group_name",
        "groupName",
        "bucket",
        "arm",
        "version",
        "experiment_group",
        "experimentGroup",
        "guide_variant",
        "guideVariant",
    ),
}

_BINDING_FIELD_ALIASES = {
    "session_id": ("session_id", "sessionId", "session_no", "sessionNo", "session", "sid"),
    "account_id": (
        "account_id",
        "accountId",
        "account",
        "user_id",
        "userId",
        "user",
        "uid",
    ),
}


class ExposureIn(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    exposure_id: str = Field(
        validation_alias=AliasChoices(*_EXPOSURE_FIELD_ALIASES["exposure_id"])
    )
    session_id: str = Field(
        validation_alias=AliasChoices(*_EXPOSURE_FIELD_ALIASES["session_id"])
    )
    occurred_at: str = Field(
        validation_alias=AliasChoices(*_EXPOSURE_FIELD_ALIASES["occurred_at"])
    )
    variant: str = Field(
        validation_alias=AliasChoices(*_EXPOSURE_FIELD_ALIASES["variant"])
    )

    # Microsecond epoch derived from the canonical timestamp; ordering key.
    _occurred_us: int = PrivateAttr(default=0)

    @property
    def occurred_us(self) -> int:
        return self._occurred_us

    @field_validator("exposure_id", mode="before")
    @classmethod
    def _exposure_id(cls, value: Any) -> str:
        return _clean_text(value, field="exposure_id", max_length=256)

    @field_validator("session_id", mode="before")
    @classmethod
    def _session_id(cls, value: Any) -> str:
        return _clean_text(value, field="session_id", max_length=256)

    @field_validator("variant", mode="before")
    @classmethod
    def _variant(cls, value: Any) -> str:
        # Variant labels are opaque; ``1`` and ``"1"`` intentionally coincide.
        return _clean_text(value, field="variant", max_length=64)

    @field_validator("occurred_at", mode="before")
    @classmethod
    def _occurred_at(cls, value: Any) -> str:
        canonical, _ = normalise_utc(value)
        return canonical

    @model_validator(mode="after")
    def _derive_epoch(self) -> "ExposureIn":
        _, self._occurred_us = normalise_utc(self.occurred_at)
        return self


class BindingIn(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    session_id: str = Field(
        validation_alias=AliasChoices(*_BINDING_FIELD_ALIASES["session_id"])
    )
    account_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices(*_BINDING_FIELD_ALIASES["account_id"]),
    )

    @field_validator("session_id", mode="before")
    @classmethod
    def _session_id(cls, value: Any) -> str:
        return _clean_text(value, field="session_id", max_length=256)

    @field_validator("account_id", mode="before")
    @classmethod
    def _account_id(cls, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        return _clean_text(value, field="account_id", max_length=256)
