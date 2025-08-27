import httpx
from typing import Dict, Any

from app.core.config import settings

EMAIL_SERVICE_URL= settings.email_service_url

async def send_email(
    email_type: str,
    to_email: str,
    payload: Dict[str, Any],
    user: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Sends an email via the email-service.

    Args:
        user_id (str): ID of the user triggering the email.
        email_type (str): Type of the email (e.g., signup_otp, reset_password).
        to_email (str): Recipient email address.
        payload (dict): Dynamic payload for the email template.
        user (dict): User metadata.

    Returns:
        dict: JSON response from the email service.
    """
    data = {
        "type": email_type,
        "to_email": to_email,
        "payload": payload,
        "user": user,
    }
    print("Sending email with data:", data)
    print("emai url", EMAIL_SERVICE_URL)
    async with httpx.AsyncClient() as client:
        response = await client.post(EMAIL_SERVICE_URL, json=data)
        response.raise_for_status()  # Raise exception if status code >=400
        return response.json()
