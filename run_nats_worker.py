import asyncio
import sys
from app.config import get_settings
from app.core.logging_config import LoggingConfig, get_logger
from app.tasks.process_nats_event import process_nats_event_task
from faststream import FastStream
from faststream.nats import NatsBroker, JStream
from nats.js.api import DeliverPolicy

# Initialize logging configuration
LoggingConfig()
logger = get_logger("nats_worker")
# A newly created durable (for example after a migration or cutover) must start
# after the current stream tail. LAST would replay the newest stored event, which
# the previous durable may already have processed.
NATS_DELIVER_POLICY = DeliverPolicy.NEW


async def _run_async() -> None:
    """Async function that runs the FastStream application."""
    settings = get_settings()
    logger.debug("Starting NATS worker...")
    # The URL contains the application account password in production. Never
    # include it in logs, even during connection troubleshooting.
    logger.debug("NATS connection endpoint configured")
    logger.debug(f"NATS Enabled: {settings.nats_enabled}")
    subjects = settings.nats_subjects.split(",")

    if not settings.nats_enabled:
        logger.error("NATS is not enabled in settings!")
        sys.exit(1)

    broker = NatsBroker(settings.nats_url)
    app = FastStream(broker)

    @app.on_startup
    async def on_startup():
        logger.debug("NATS worker started and connected!")
        logger.debug(f"Subscribed to: {subjects} (JetStream stream)")
        if settings.nats_queue:
            logger.debug(f"Using queue: {settings.nats_queue}")

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
        deliver_policy=NATS_DELIVER_POLICY,
        **subscriber_kwargs,
    )
    async def handler(msg: dict) -> None:
        """Handle incoming NATS events and dispatch them to a Celery task."""
        logger.debug(f"Received message: {msg}")
        # Dispatch to Celery task for async processing
        process_nats_event_task.delay(msg)

    logger.debug("Running FastStream app...")
    await app.run()


def main() -> None:
    """Synchronous entry point for process managers."""
    asyncio.run(_run_async())


if __name__ == "__main__":
    main()
