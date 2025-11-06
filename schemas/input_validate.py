import re
from datetime import datetime
from pydantic_core import PydanticCustomError
from typing import Optional

def format_field_name(field_name: str) -> str:
    """Convert snake_case field names to readable format."""
    return field_name.replace('_', ' ').title() 
 
def check_str(value: str, field_name: str) -> str:
    """Validate string contains only alphanumeric characters and spaces."""
    pattern = r"^[a-zA-Z0-9\s,._\-–—']+$"
    if not re.match(pattern, value):
        formatted_name = format_field_name(field_name)
        raise PydanticCustomError(
            'string_pattern',
            f"{formatted_name} should contain only alphanumeric characters and spaces"
        )
    return value
   
# def check_date_range(valid_from: datetime, valid_to: datetime) -> None:
#     if valid_to < valid_from:
#         raise ValueError(f"given date {valid_to} should not be earlier than {valid_from}")
   
def check_comment(value: str, field_name: str) -> str:
    """Validate comment field is non-empty and meets length requirements."""
    formatted_name = format_field_name(field_name)
    
    if not value or not value.strip():
        raise PydanticCustomError(
            'comment_empty',
            f"{formatted_name} must not be empty"
        )
    
    value = value.strip()
    
    if len(value) < 5:
        raise PydanticCustomError(
            'comment_too_short',
            f"{formatted_name} must be at least 5 characters long"
        )
    
    if not re.match(r'^[a-zA-Z0-9_\s,]+$', value):
        raise PydanticCustomError(
            'comment_pattern',
            f"{formatted_name} must contain only alphanumeric characters, spaces, commas, or underscores"
        )
    
    return value
 
def check_otp(value: str, field_name: str) -> str:
    """Validate OTP is a 6-digit code."""
    if not re.match(r'^\d{6}$', value):
        formatted_name = format_field_name(field_name)
        raise PydanticCustomError(
            'otp_pattern',
            f"{formatted_name} should contain a 6-digit code"
        )
    return value
   
def check_name(value: str, field_name: str) -> str:
    """Validate name contains only alphabetic characters and spaces."""
    if not re.match(r'^[a-zA-Z\s]+$', value):
        formatted_name = format_field_name(field_name)
        raise PydanticCustomError(
            'name_pattern',
            f"{formatted_name} should contain only alphabetic characters and spaces"
        )
    return value
 
def check_password(value: str, field_name: str) -> str:
    """Validate password meets complexity requirements."""
    if not re.match(r'^(?=.*[A-Z])(?=.*[a-z])(?=.*\d)(?=.*[!@#$%^&*()]).+$', value):
        formatted_name = format_field_name(field_name)
        raise PydanticCustomError(
            'password_pattern',
            f"{formatted_name} should contain at least 1 uppercase letter, "
            f"1 lowercase letter, 1 digit, and 1 special character"
        )
    return value

def validate_comments_required(
    action_key: str, 
    comments: str | None, 
    required_action_key: str, 
    error_message: str
) -> None:
    """Validate comments are provided when required for specific actions."""
    if action_key == required_action_key:
        if not comments or len(comments) == 0:
            raise PydanticCustomError(
                'comments_required',
                error_message
            )
        
def check_hs_code(value: str, field_name: str) -> str:
    """Validate HS code is numeric and remove all decimal points."""
    cleaned_value = value.replace('.', '')
    if not re.match(r'^\d+$', cleaned_value):
        formatted_name = format_field_name(field_name)
        raise PydanticCustomError(
            'hs_code_pattern',
            f"{formatted_name} must be a number"
        )
    return cleaned_value
 
def check_date_range(valid_from: datetime | None, valid_to: datetime | None) -> None:
    """Validate date range is logical (valid_to >= valid_from)."""
    today = datetime.now().date()
    
    if valid_from and valid_to:
        if valid_to < valid_from:
            raise PydanticCustomError(
                'date_range_invalid',
                f"End date {valid_to.date()} should not be earlier than start date {valid_from.date()}"
            )
    elif valid_to:
        if valid_to.date() < today:
            raise PydanticCustomError(
                'date_past',
                f"End date {valid_to.date()} should not be earlier than today ({today})"
            )
            
def check_alpha_str(value: Optional[str], field_name: str) -> Optional[str]:
    """Validate string contains only alphabets, spaces, and commas (nullable-safe)."""
    if value is None or value.strip() == "":
        return value

    pattern = r"^[a-zA-Z\s,]+$"
    if not re.match(pattern, value):
        raise PydanticCustomError(
            'string_pattern',
            f"{field_name} should contain only alphabets, spaces, or commas"
        )
    return value