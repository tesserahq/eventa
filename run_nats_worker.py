import asyncio
import sys
from datetime import datetime, timezone
from app.config import get_settings
from app.core.logging_config import LoggingConfig, get_logger
from app.services.event_service import EventService
from app.schemas.event import EventCreate
from app.db import SessionLocal
from faststream import FastStream
from faststream.nats import NatsBroker

# Initialize logging configuration
LoggingConfig()
logger = get_logger("nats_worker")


async def _run_async() -> None:
    """Async function that runs the FastStream application."""
    settings = get_settings()
    logger.info("Starting NATS worker...")
    logger.info(f"NATS URL: {settings.nats_url}")
    logger.info(f"NATS Enabled: {settings.nats_enabled}")

    if not settings.nats_enabled:
        logger.error("NATS is not enabled in settings!")
        sys.exit(1)

    broker = NatsBroker(settings.nats_url)
    app = FastStream(broker)

    @app.on_startup
    async def on_startup():
        logger.info("NATS worker started and connected!")
        logger.info(f"Subscribed to: com.mylinden.> (JetStream stream)")
        if settings.nats_queue:
            logger.info(f"Using queue: {settings.nats_queue}")

    # Subscribe with queue for load balancing (if configured)
    # Note: For JetStream streams, messages are stored in the stream and need to be consumed
    # FastStream should handle this automatically, but we need to ensure the subject pattern matches

    # Subscribe to all subjects under com.mylinden using the '>' wildcard (matches all levels)
    # Only pass queue parameter if it's configured (FastStream expects a string, not None)
    subscriber_kwargs = {"queue": settings.nats_queue} if settings.nats_queue else {}

    @broker.subscriber("com.mylinden.>", **subscriber_kwargs)
    async def handler(msg: dict) -> None:
        """Handle incoming NATS events and store them in the database."""
        logger.info(f"Received message: {msg}")

        db = SessionLocal()
        try:
            # Parse time if it's a string
            time_value = msg.get("time")
            if isinstance(time_value, str):
                time_value = datetime.fromisoformat(time_value.replace("Z", "+00:00"))
            elif time_value is None:
                time_value = datetime.now(timezone.utc)

            # Extract specific fields from the event for model columns
            event_create = EventCreate(
                source=msg.get("source", ""),
                spec_version=msg.get("spec_version", "1.0"),
                event_type=msg.get("event_type", ""),
                event_data=msg.get("event_data"),  # Store entire event here
                data_content_type=msg.get("data_content_type", "application/json"),
                subject=msg.get("subject", ""),
                time=time_value,
                tags=msg.get("tags"),
                labels=msg.get("labels"),
                privy=msg.get("privy", False),  # Default to False if not provided
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

    logger.info("Running FastStream app...")
    await app.run()


def main() -> None:
    """Synchronous entry point for process managers."""
    asyncio.run(_run_async())


if __name__ == "__main__":
    main()
