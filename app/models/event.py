from sqlalchemy.dialects.postgresql import JSONB, UUID, ARRAY
from sqlalchemy.orm import Mapped, mapped_column
from app.models.mixins import TimestampMixin, SoftDeleteMixin
from sqlalchemy import Column, DateTime, ForeignKey, String
import uuid

from app.db import Base


class Event(Base, TimestampMixin, SoftDeleteMixin):
    """Event model for the application.
    This model represents an event in the system.
    """

    __tablename__ = "events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source = Column(String, nullable=False)
    spec_version = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    event_data = Column(JSONB, nullable=False)
    data_content_type = Column(String, nullable=False)
    subject = Column(String, nullable=False)
    time = Column(DateTime, nullable=False)
    tags = Column(ARRAY(String), nullable=True)
    labels = Column(JSONB, default=dict, nullable=False)  # Dictionary of labels

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
