import uuid

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.shared_modules.db import Base
from app.shared_modules.models.user import User # noqa: F401

class Session(Base):
    """Session model - stores session details"""
    __tablename__ = "sessions"
    __table_args__ = {"schema": "user"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("user.users.id", ondelete="CASCADE"), nullable=False)
    tenant_id = Column(UUID(as_uuid=True), nullable=True)
    access_token = Column(String, nullable=False)
    refresh_token = Column(String, nullable=False)
    access_token_expiry = Column(DateTime(timezone=True), nullable=False)
    refresh_token_expiry = Column(DateTime(timezone=True), nullable=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.current_timestamp())
    updated_at = Column(DateTime(timezone=True), server_default=func.current_timestamp(), onupdate=func.current_timestamp())

    user = relationship("User", backref="sessions")