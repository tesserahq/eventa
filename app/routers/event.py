import json
from typing import Annotated, Any, Dict, List, Optional

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
def list_events_by_tags(
    tags: Annotated[
        List[str], Query(..., min_items=1, description="Event tags to match")
    ],
    labels: Annotated[
        Optional[str],
        Query(
            description="Optional JSON object containing label key/value pairs to match"
        ),
    ] = None,
    params: Params = Depends(),
    db: Session = Depends(get_db),
):
    """Return events filtered by tags and optionally by labels."""
    if not tags:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="At least one tag is required.",
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
    print(str(query))
    return paginate(db, query, params)
