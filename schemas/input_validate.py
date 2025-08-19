import re
from datetime import datetime
 
 
def check_str(value: str, field_name: str) -> str:
    if not re.match(r'^[a-zA-Z0-9_\s,]+$', value):
        raise ValueError(f"{field_name} should contain only alphanumeric chars and space")
    return value
   
def check_date_range(valid_from: datetime, valid_to: datetime) -> None:
    if valid_to < valid_from:
        raise ValueError(f"given date {valid_to} should not be earlier than {valid_from}")
   
def check_comment(value: str, field_name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} must not be empty")  
    value = value.strip()
    if len(value) < 5:
        raise ValueError(f"{field_name} must be at least 5 characters long")    
    if not re.match(r'^[a-zA-Z0-9_\s,]+$', value):
        raise ValueError(f"{field_name} must contain only alphanumeric characters, spaces, or underscores")
    return value
 
def check_otp(value: str, field_name: str) -> str:
    if not re.match(r'^\d{6}$', value):
        raise ValueError(f"{field_name} should contain 6 digit code")
   
def check_name(value: str, field_name: str) -> str:
    if not re.match(r'^[a-zA-Z\s]+$', value):
        raise ValueError(f"{field_name} should contain only alphabet and space")
    return value
 
def check_password(value: str, field_name: str) -> str:
    if not re.match(r'^(?=.*[A-Z])(?=.*[a-z])(?=.*\d)(?=.*[!@#$%^&*()]).+$', value):
        raise ValueError(f"{field_name} should contain at least 1 uppercase letter,smallcase letter, digit and a special character")
    return value