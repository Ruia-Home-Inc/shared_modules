from typing import Dict, Any
from pydantic import BaseModel, EmailStr, Field, ValidationError

import httpx
from app.core.config import settings

EMAIL_SERVICE_URL = settings.email_service_url

# Define Pydantic model for email payload
class EmailPayload(BaseModel):
    type: str
    to_email: EmailStr  # validates proper email format
    payload: Dict[str, Any]
    action_triggered_user: Dict[str, Any]
    module: str
    module_identifier: str


async def send_email(
    email_type: str,
    to_email: str,
    payload: Dict[str, Any],
    user: Dict[str, Any],
    module: str,
    module_identifier: str,
) -> Dict[str, Any]:
    """
    Sends an email via the email-service after validating the payload.
    """
    data = {
        "type": email_type,
        "to_email": to_email,
        "payload": payload,
        "action_triggered_user": user,
        "module": module,
        "module_identifier": module_identifier,
    }

    # Validate the data
    try:
        validated_data = EmailPayload(**data)
    except ValidationError as e:
        print("Email payload validation failed:", e.json())
        raise ValueError(f"Invalid email payload: {e}")

    async with httpx.AsyncClient() as client:
        response = await client.post(EMAIL_SERVICE_URL, json=validated_data.dict())
        response.raise_for_status()
        return response.json()
