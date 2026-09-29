from datetime import datetime, timezone
from uuid import UUID


from app.core.celery_app import celery_app
from app.core.logging_config import get_logger
from app.schemas.event import EventCreate
from app.schemas.user import UserOnboard
from app.services.event_service import EventService
from app.services.user_service import UserService
from tessera_sdk.clients.identies import IdentiesClient
from app.db import session_scope
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

    # Phase 1: store the event. It commits on its own, so a failure to
    # onboard the user below can never lose it.
    with session_scope() as db:
        # Extract specific fields from the event for model columns
        event_create = EventCreate(
            source=event.source,
            spec_version=event.spec_version,
            event_type=event.event_type,
            event_data=event.event_data,
            data_content_type=event.data_content_type,
            subject=event.subject,
            # A missing time defaults to when the event was received.
            time=event.time or datetime.now(timezone.utc),
            tags=event.tags,
            labels=event.labels,
            privy=event.privy,
            user_id=event.user_id,
            project_id=event.project_id,
        )

        created_event = EventService(db).create_event(event_create)
        event_id = created_event.id
        needs_onboarding = bool(event.user_id) and (
            UserService(db).get_user(event.user_id) is None
        )

    logger.info(f"Event created successfully: {event_id}")

    # Phase 2: onboard an unknown user. Identies is called with no database
    # transaction open.
    if needs_onboarding:
        _onboard_user(event.user_id)


def _onboard_user(user_id: UUID) -> None:
    """
    Fetch a user from Identies and onboard them locally. Best effort: errors
    are logged, and the next event for this user tries again.
    """
    try:
        logger.info(f"Onboarding user from Identies: {user_id}")
        identies_user = _fetch_identies_user(user_id)
        with session_scope() as db:
            user_service = UserService(db)
            # Another event for the same user may have onboarded them while
            # Identies was being called.
            if user_service.get_user(user_id) is not None:
                logger.debug(f"User already onboarded: {user_id}")
                return
            user_service.onboard_user(identies_user)
        logger.info(f"User onboarded successfully: {user_id}")
    except Exception as e:
        # Log error but don't fail the event processing
        logger.error(f"Error fetching/onboarding user: {e}", exc_info=True)


def _fetch_identies_user(user_id: UUID) -> UserOnboard:
    """Read a user from Identies. No database access."""
    identies_client = IdentiesClient(
        # TODO: This is a temporary solution, we need to move this into jobs
        timeout=320,  # Shorter timeout for middleware
        api_token=_get_auth_token(),
    )

    identies_user = identies_client.get_user(user_id)
    return UserOnboard(
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


def _get_auth_token() -> str:
    """
    Get an M2M token for Quore.
    """
    return AuthTokenProvider().get_token()
