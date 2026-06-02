from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy.orm import Session

from app.core.celery_app import celery_app
from app.core.logging_config import get_logger
from app.schemas.event import EventCreate
from app.schemas.user import UserOnboard
from app.services.event_service import EventService
from app.services.user_service import UserService
from tessera_sdk.clients.identies import IdentiesClient
from app.utils.db.db_session_helper import db_session
from tessera_sdk.infra.events import Event
from pydantic import ValidationError
from tessera_sdk.infra import AuthTokenProvider

logger = get_logger("process_nats_event_task")


@celery_app.task
def process_nats_event_task(msg: dict) -> None:
    """Handle incoming NATS events and store them in the database."""
    logger.info(f"Processing NATS event: {msg}")

    try:
        event = Event.model_validate(msg)
    except ValidationError as e:
        logger.warning("Invalid NATS event payload: %s", e)
        return None

    with db_session() as db:
        # Parse time if it's a string
        time_value = msg.get("time")
        if isinstance(time_value, str):
            time_value = datetime.fromisoformat(time_value.replace("Z", "+00:00"))
        elif time_value is None:
            time_value = datetime.now(timezone.utc)

        # Extract specific fields from the event for model columns
        event_create = EventCreate(
            source=event.source,
            spec_version=event.spec_version,
            event_type=event.event_type,
            event_data=event.event_data,
            data_content_type=event.data_content_type,
            subject=event.subject,
            time=event.time,
            tags=event.tags,
            labels=event.labels,
            privy=event.privy,
            user_id=event.user_id,
            project_id=event.project_id,
        )

        # Create event using EventService
        event_service = EventService(db)
        created_event = event_service.create_event(event_create)

        # Ensure user is onboarded if user_id is provided
        user_id = event.user_id
        if user_id:
            _ensure_user_onboarded(db, user_id)

        logger.info(f"Event created successfully: {created_event.id}")


def _ensure_user_onboarded(db: Session, user_id: UUID) -> None:
    """
    Ensure a user is onboarded by checking if they exist locally,
    and if not, fetching from Identies and onboarding them.
    """
    try:
        user_service = UserService(db)
        logger.info(f"Ensuring user is onboarded: {user_id}")
        # Check if user is already onboarded
        existing_user = user_service.get_user(user_id)
        if existing_user:
            logger.debug(f"User already onboarded: {user_id}")
            return

        # User doesn't exist, fetch from Identies and onboard
        m2m_token = _get_auth_token()
        identies_client = IdentiesClient(
            # TODO: This is a temporary solution, we need to move this into jobs
            timeout=320,  # Shorter timeout for middleware
            api_token=m2m_token,
        )

        identies_user = identies_client.get_user(user_id)
        user = UserOnboard(
            id=UUID(identies_user.id),
            email=identies_user.email,
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
        logger.info(f"User onboarded successfully: {user.id}")
    except Exception as e:
        # Log error but don't fail the event processing
        logger.error(f"Error fetching/onboarding user: {e}", exc_info=True)


def _get_auth_token() -> str:
    """
    Get an M2M token for Quore.
    """
    return AuthTokenProvider().get_token()
