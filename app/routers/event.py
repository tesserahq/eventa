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
from app.auth.rbac import build_rbac_dependencies
from fastapi import Request


router = APIRouter(
    prefix="/events",
    tags=["events"],
    responses={404: {"description": "Not found"}},
)


async def infer_project(request: Request) -> Optional[str]:
    """
    Infer the project from the query parameter 'tags' by extracting the value for 'project_id'.
    The 'tags' parameter is expected to be an array of strings in the format 'key:value'.
    Returns the value of 'project_id' if present, otherwise returns '*'.
    """
    # First, check for explicit project parameter
    project_id = request.query_params.get("project_id")
    if project_id:
        return project_id

    return "*"


RESOURCE = "event"
rbac = build_rbac_dependencies(
    resource=RESOURCE,
    project_resolver=infer_project,
)


@router.get("", response_model=Page[Event], status_code=status.HTTP_200_OK)
def list_events(
    project_id: Annotated[
        Optional[UUID],
        Query(description="Project ID to filter events by"),
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
    _authorized: bool = Depends(rbac["read"]),
):
    """Return events filtered by user_id OR by tags/labels (not both)."""

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
        tags=tags, labels=labels_payload, privy=False, project_id=project_id
    )
    return paginate(db, query, params)
