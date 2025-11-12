from uuid import UUID
from datetime import datetime
from typing import Optional

from app.config import get_settings
from app.db import SessionLocal
from app.core.celery_app import celery_app

# Import heavy dependencies only when task executes (lazy loading)
from app.core.logging_config import get_logger
from app.schemas.user import UserOnboard
from app.services.user_service import UserService
from tessera_sdk import IdentiesClient
from tessera_sdk.utils.m2m_token import M2MTokenClient


@celery_app.task
def fetch_user(
    user_id: str,
):

    db = SessionLocal()
    m2m_token = _get_m2m_token()

    identies_client = IdentiesClient(
        base_url=get_settings().identies_base_url,
        # TODO: This is a temporary solution, we need to move this into jobs
        timeout=320,  # Shorter timeout for middleware
        max_retries=1,  # Fewer retries for middleware
        api_token=m2m_token,
    )

    identies_user = identies_client.get_user(user_id)
    user = UserOnboard(
        id=UUID(identies_user.id),
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
    user_service = UserService(db)
    user_service.onboard_user(user)


def _get_m2m_token(self) -> str:
    """
    Get an M2M token for Quore.
    """
    return M2MTokenClient().get_token_sync().access_token
