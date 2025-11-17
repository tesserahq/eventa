import json
from typing import Annotated, Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from fastapi_pagination import Page, Params  # type: ignore[import-not-found]
from fastapi_pagination.ext.sqlalchemy import paginate  # type: ignore[import-not-found]

from app.db import get_db
from app.schemas.event import Event
from app.services.event_service import EventService

router = APIRouter(
    prefix="/events",
    tags=["events"],
    responses={404: {"description": "Not found"}},
)


@router.get("", response_model=Page[Event], status_code=status.HTTP_200_OK)
def list_events(
    user_id: Annotated[
        Optional[UUID],
        Query(description="User ID to filter events by"),
    ] = None,
    tags: Annotated[
        Optional[List[str]],
        Query(
            description="Event tags to match (requires at least one tag if provided)"
        ),
    ] = None,
    labels: Annotated[
        Optional[str],
        Query(
            description="Optional JSON object containing label key/value pairs to match"
        ),
    ] = None,
    params: Params = Depends(),
    db: Session = Depends(get_db),
):
    """Return events filtered by user_id OR by tags/labels (not both)."""
    # Validate that either user_id OR tags is provided, but not both
    if user_id and tags:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot filter by both user_id and tags. Please use either user_id or tags/labels.",
        )

    if not user_id and not tags:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Either user_id or tags must be provided.",
        )

    # Filter by user_id
    if user_id:
        query = EventService(db).get_events_by_user_id_query(user_id=user_id)
        return paginate(db, query, params)

    # Filter by tags and optionally labels
    if not tags or len(tags) == 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="At least one tag is required when filtering by tags.",
        )

    labels_payload: Optional[Dict[str, Any]] = None
    if labels:
        try:
            parsed_labels = json.loads(labels)
            if not isinstance(parsed_labels, dict):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="labels must be a JSON object",
                )
            labels_payload = parsed_labels
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="labels parameter must be valid JSON",
            )

    query = EventService(db).get_events_by_tags_and_labels_query(
        tags=tags, labels=labels_payload, privy=False
    )
    return paginate(db, query, params)
