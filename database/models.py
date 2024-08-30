import uuid

from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import UUID

from database import db


class DeforestationRequest(db.Base):
    __tablename__ = 'deforestation_request'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status = Column(String(30), nullable=False)
    total = Column(Integer, nullable=False)
    completed = Column(Integer, nullable=False, default=0)
    is_synced = Column(Boolean, nullable=False, default=False)
    error = Column(String, nullable=True, default=None)
    timestamp = Column(DateTime, nullable=False)
