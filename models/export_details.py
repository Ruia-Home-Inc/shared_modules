from datetime import datetime
from uuid import uuid4
from enum import Enum

from sqlalchemy import Column, String, DateTime, Integer
from sqlalchemy.dialects.postgresql import UUID, JSONB

from ..db import Base


class ExportStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class ExportDetail(Base):
    __tablename__ = "export_details"
    __table_args__ = {"schema": "export"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4, index=True)
    module_name = Column(String, nullable=False)
    feature_name = Column(String, nullable=False)
    file_type = Column(String, nullable=False)
    parameters = Column(JSONB, nullable=True)     # stores export configuration/filters
    status = Column(String, nullable=False, default=ExportStatus.PENDING.value)
    created_by = Column(JSONB, nullable=True)     # user/system that initiated the export
    file_url = Column(String, nullable=True)      # path or URL of the exported file
    file_object_key = Column(String, nullable=True)  # S3 object key if using S3 for storage
    total_records = Column(Integer, nullable=True)  # number of records included in the export
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
