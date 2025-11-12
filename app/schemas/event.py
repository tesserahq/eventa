from typing import Any, Optional
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel

from app.schemas.user import User


class EventBase(BaseModel):
    """Base event model containing common event attributes."""

    source: str
    """Source of the event. Required field."""

    spec_version: str
    """Spec version of the event. Required field."""

    event_type: str
    """Type of the event. Required field."""

    event_data: dict[str, Any]
    """Data of the event. Required field."""

    data_content_type: str
    """Content type of the event data. Required field."""

    subject: str
    """Subject of the event. Required field."""

    time: datetime
    """Time of the event. Required field."""

    tags: Optional[list[str]] = None
    """Tags associated with the event."""

    labels: Optional[dict[str, Any]] = None
    """Labels applied to the event."""

    privy: bool = False
    """Whether the event is private. Defaults to False."""

    user_id: Optional[UUID] = None
    """User ID associated with the event."""


class EventCreate(EventBase):
    """Schema for creating a new event."""

    pass


class EventUpdate(BaseModel):
    """Schema for updating an existing event."""

    source: Optional[str] = None
    spec_version: Optional[str] = None
    event_type: Optional[str] = None
    event_data: Optional[dict[str, Any]] = None
    data_content_type: Optional[str] = None
    subject: Optional[str] = None
    time: Optional[datetime] = None
    tags: Optional[list[str]] = None
    labels: Optional[dict[str, Any]] = None
    privy: Optional[bool] = None


class EventInDB(EventBase):
    """Schema representing an event stored in the database."""

    id: UUID
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class Event(EventInDB):
    """Schema for event data returned in API responses."""

    user: Optional[User] = None
    """User associated with the event. None if user_id doesn't exist in users table."""
