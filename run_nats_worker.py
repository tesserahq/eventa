import asyncio
import sys
from datetime import datetime, timezone
from uuid import UUID
from app.config import get_settings
from app.core.logging_config import LoggingConfig, get_logger
from app.services.event_service import EventService
from app.schemas.event import EventCreate
from app.db import SessionLocal
from faststream import FastStream
from faststream.nats import NatsBroker, JStream
from nats.js.api import DeliverPolicy
from app.schemas.user import UserOnboard
from app.services.user_service import UserService
from tessera_sdk import IdentiesClient
from tessera_sdk.utils.m2m_token import M2MTokenClient

# Initialize logging configuration
LoggingConfig()
logger = get_logger("nats_worker")


def fetch_user(
    user_id: str,
):

    db = SessionLocal()
    user_service = UserService(db)

    # If the user doesn't exist, we need to fetch it from Identies
    if user_service.get_user(user_id):
        return

    m2m_token = _get_m2m_token()

    identies_client = IdentiesClient(
        base_url=get_settings().identies_base_url,
        # TODO: This is a temporary solution, we need to move this into jobs
        timeout=320,  # Shorter timeout for middleware
        max_retries=1,  # Fewer retries for middleware
        api_token=m2m_token,
    )

    logger.info(f"Fetching user from Identies: {user_id}")
    identies_user = identies_client.get_user(user_id)
    logger.info(f"Identies user: {identies_user}")
    user = UserOnboard(
        id=identies_user.id,
        email=identies_user.email,
        username=identies_user.username,
        first_name=identies_user.first_name,
        last_name=identies_user.last_name,
        avatar_url=identies_user.avatar_url,
        provider=identies_user.provider,
        verified=identies_user.verified,
        verified_at=identies_user.verified_at,
        confirmed_at=identies_user.confirmed_at,
        external_id=identies_user.external_id,
    )

    user_service.onboard_user(user)


async def _run_async() -> None:
    """Async function that runs the FastStream application."""
    settings = get_settings()
    logger.info("Starting NATS worker...")
    logger.info(f"NATS URL: {settings.nats_url}")
    logger.info(f"NATS Enabled: {settings.nats_enabled}")
    subjects = settings.nats_subjects.split(",")

    if not settings.nats_enabled:
        logger.error("NATS is not enabled in settings!")
        sys.exit(1)

    broker = NatsBroker(settings.nats_url)
    app = FastStream(broker)

    @app.on_startup
    async def on_startup():
        logger.info("NATS worker started and connected!")
        logger.info(f"Subscribed to: {subjects} (JetStream stream)")
        if settings.nats_queue:
            logger.info(f"Using queue: {settings.nats_queue}")

    # Subscribe with queue for load balancing (if configured)
    # Note: For JetStream streams, messages are stored in the stream and need to be consumed
    # FastStream should handle this automatically, but we need to ensure the subject pattern matches

    # Subscribe to all subjects under com.mylinden using the '>' wildcard (matches all levels)
    # Only pass queue parameter if it's configured (FastStream expects a string, not None)
    # JetStream stream definition (adjust name/subjects to match your server config)
    js_stream = JStream(
        name=settings.nats_stream_name,  # must match the JetStream stream name
        # subjects="com.identies.>",
        declare=False,  # set True if you want FastStream to create/update it
    )

    subscriber_kwargs = {"queue": settings.nats_queue} if settings.nats_queue else {}

    @broker.subscriber(
        "com.>",
        stream=js_stream,  # THIS makes it JetStream
        durable=settings.nats_queue,  # durable consumer name
        deliver_policy=DeliverPolicy.LAST,  # or DeliverPolicy.LAST, etc.
        **subscriber_kwargs,
    )
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
                user_id=msg.get("user_id"),
            )

            # Create event using EventService
            event_service = EventService(db)
            created_event = event_service.create_event(event_create)

            # TODO: We need to move this into a task
            fetch_user(msg.get("user_id"))

            logger.info(f"Event created successfully: {created_event.id}")
        except Exception as e:
            logger.error(f"Error creating event: {e}", exc_info=True)
            db.rollback()
            raise
        finally:
            db.close()

    logger.info("Running FastStream app...")
    await app.run()


def _get_m2m_token() -> str:
    """
    Get an M2M token for Quore.
    """
    return M2MTokenClient().get_token_sync().access_token


def main() -> None:
    """Synchronous entry point for process managers."""
    asyncio.run(_run_async())


if __name__ == "__main__":
    main()
