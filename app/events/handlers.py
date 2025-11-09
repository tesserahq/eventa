import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Optional

from app.schemas.event import Event, EventCreate
from app.services.event_service import EventService
from app.db import SessionLocal

logger = logging.getLogger(__name__)


async def handle_event(event: Event) -> None:
    """Handle account-related events published on JetStream."""
    db = SessionLocal()
    try:
        # Extract specific fields from the event for model columns
        event_create = EventCreate(
            source=event.source,
            spec_version=event.spec_version,
            event_type=event.event_type,
            event_data=event,  # Store entire event here
            data_content_type=event.data_content_type,
            subject=event.subject,
            time=event.time,
            tags=event.tags,
            labels=event.labels,
        )
        
        # Create event using EventService
        event_service = EventService(db)
        created_event = event_service.create_event(event_create)
        logger.info(f"Event created successfully: {created_event.id}")
    except Exception as e:
        logger.error(f"Error creating event: {e}", exc_info=True)
        db.rollback()
        raise
    finally:
        db.close()


EventHandler = Callable[[Event], Awaitable[None]]


@dataclass(frozen=True)
class EventSubscription:
    subject: str
    handler: EventHandler
    queue: Optional[str] = None
    auto_ack: bool = True
    stream: Optional[str] = None


EVENT_SUBSCRIPTIONS: list[EventSubscription] = [
    EventSubscription(
        subject="linden.*.*",
        handler=handle_event,
        queue="linden-account-workers",
    ),
]
