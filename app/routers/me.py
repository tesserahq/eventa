from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session
from fastapi_pagination import Page, Params  # type: ignore[import-not-found]
from fastapi_pagination.ext.sqlalchemy import paginate  # type: ignore[import-not-found]

from app.db import get_db
from app.schemas.event import Event
from app.services.event_service import EventService
from tessera_sdk.utils.auth import get_current_user
from app.models.user import User

router = APIRouter(
    prefix="",
    tags=["me"],
    responses={404: {"description": "Not found"}},
)


@router.get("/me", response_model=Page[Event], status_code=status.HTTP_200_OK)
def list_my_events(
    params: Params = Depends(),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return events for the current authenticated user."""
    query = EventService(db).get_events_by_user_id_query(user_id=current_user.id)  # type: ignore[arg-type]
    return paginate(db, query, params)
