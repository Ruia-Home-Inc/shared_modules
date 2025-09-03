import uuid

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from ..db import Base


class TenantUser(Base):
    """Tenant User model - stores tenant and user mappings"""
    __tablename__ = "tenant_users"
    __table_args__ = {"schema": "user"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, unique=True, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("user.users.id", ondelete="CASCADE"), nullable=False, index=True)
    tenant_id = Column(UUID(as_uuid=True), nullable=True)
    is_admin = Column(Boolean, default=False)
    status = Column(String(100), nullable=False)
    first_login = Column(Boolean, default=False)         
    created_at = Column(DateTime(timezone=True), server_default=func.current_timestamp())
    updated_at = Column(DateTime(timezone=True), server_default=func.current_timestamp(), onupdate=func.current_timestamp())
