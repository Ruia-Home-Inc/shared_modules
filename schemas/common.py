import re
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field, validator


class HealthCheck(BaseModel):
    service: str
    version: str
    timestamp: str
    status: Optional[str] = "healthy"


class ErrorResponse(BaseModel):
    detail: str
    error_code: Optional[str] = None 


class UserData(BaseModel):
    """Schema representing individual user data with privileges"""
    email: str = Field(..., description="User's email address")
    user_id: str = Field(..., description="Unique identifier for the user")
    tenant_id: str = Field(..., description="Tenant associated with the user")
    name: Optional[str] = Field(None, description="Name of the user")
    status: str = Field(..., description="Current status of the user")
    user_details: Dict[str, Any] = Field(..., description="Additional user details")
    privileges: List[str] = Field(..., description="List of user privileges")

    class Config:
        json_schema_extra = {
            "example": {
                "email": "user@example.com",
                "user_id": "u123",
                "tenant_id": "t001",
                "name": "John Doe",
                "status": "active",
                "user_details": {"department": "IT", "location": "Remote"},
                "privileges": ["read", "write"]
            }
        }

class UserResponse(BaseModel):
    """Response schema for a list of users with an optional message"""
    users: List[UserData] = Field(..., description="List of user data entries")
    message: Optional[str] = Field(None, description="Optional status or info message")

    class Config:
        json_schema_extra = {
            "example": {
                "users": [
                    {
                        "email": "user@example.com",
                        "user_id": "u123",
                        "tenant_id": "t001",
                        "name": "John Doe",
                        "status": "active",
                        "user_details": {"department": "IT"},
                        "privileges": ["read"]
                    }
                ],
                "message": "1 user found"
            }
        }

class UserTenantStatusResponse(BaseModel):
    active_tenant_id: Optional[UUID] = Field(None, description="Active tenent ID of the user")
    
    class Config:
        json_schema_extra = {
            "example": {
                "active_tenant_id": "123e4567-e89b-12d3-a456-426614174000"
            }
        }