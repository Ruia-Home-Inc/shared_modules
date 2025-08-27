from enum import Enum

class UserInviteStatus:
    ACTIVE = "active"
    EXPIRED = "expired"
    VERIFIED = "verified"
    
    @classmethod
    def all(cls):
        return [
            cls.ACTIVE,
            cls.EXPIRED,
            cls.VERIFIED
        ]

class TenantUserStatus:
    INVITED = "invited"
    ACTIVE = "active"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    SUSPENDED = "suspended"
    DELETED = "deleted"

    @classmethod
    def all(cls):
        return [
            cls.INVITED,
            cls.ACTIVE,
            cls.ACCEPTED,
            cls.REJECTED,
            cls.SUSPENDED,
            cls.DELETED
        ]

class OTPTypes:
    LOGIN = "login"
    RESET_PASSWORD = "reset_password"
    TRANSACTION = "transaction"
    ACTIVATION = "activation"

    @classmethod
    def all(cls):
        return [
            cls.LOGIN,
            cls.RESET_PASSWORD,
            cls.TRANSACTION,
            cls.ACTIVATION
        ]
class LoginType:
    EMAIL_PASSWORD = "email_password"
    MICROSOFT = "microsoft"
    GOOGLE = "google"
    LINKEDIN = "linkedin"

    @classmethod
    def all(cls):
        return [
            cls.EMAIL_PASSWORD,
            cls.MICROSOFT,
            cls.GOOGLE,
            cls.LINKEDIN
        ]

class RoleTypes:
    TENANT = "tenant"
    GLOBAL = "super_admin"

    @classmethod
    def all(cls):
        return [
            cls.TENANT,
            cls.GLOBAL
        ]

API_PERMISSIONS = {
    "user_invite": ["user_management:create"],
    "get_users": ["user_management:view"],
    "assign_privilege":["privilege:assign"],
    "check_templates_exist": ["user_management:view"],
    "create_role": ["user_management:create"],
    "get_privilege": ["user_management:view"],
    "list_resource_privilege": ["user_management:view"],
    "remove_privilege":["privilege:revoke"],
    "list_user_privilege": ["user_management:view"],
    "bulk_status_update": ["user_management:edit"],
    "list_user_details": ["user_management:view"],
    "update_user": ["user_management:edit"],
    "user_statuses": ["user_management:view"],
    "check_email_exist":["user_management:create"],
    "user_delete": ["user_management:delete"],
    "bulk_delete":["user_management:delete"],
    "check_template_name":["user_management:view"],
    "bulk_status_update":["user_management:edit"]
}



class EmailType(str, Enum):
    """Email types supported by the notification service"""
    WELCOME = "welcome"
    SIGNUP_OTP = "signup_otp"
    INVITE = "invite"