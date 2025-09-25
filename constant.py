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
    "bulk_status_update":["user_management:edit"],
 
    "add_freight_rate":["freight_rate:create"],
    "update_freight_rate":["freight_rate:edit"],
    "list_freight_rates":["freight_rate:view"],
    "get_freight_rate_by_id":["freight_rate:view"],
    "bulk_freight_rate_status_change":["freight_rate:enable_disable"],
    "add_container_type":["freight_rate:create"],
    "delete_container_type":["freight_rate:delete"],
    "list_container_types":["freight_rate:view"],
    "add_freight_rate_comment":["freight_rate:create"],
    "list_freight_rate_comments":["freight_rate:view"],
    "freight_change_history":["freight_rate:view"],
    "freight_rate_change":["freight_rate:view"],
 
    "add_tariff_rate":["tariff_rate:create"],
    "update_tariff_rate":["tariff_rate:edit"],
    "list_tariff_rates":["tariff_rate:view"],
    "get_tariff_rate_by_id":["tariff_rate:view"],
    "bulk_tariff_rate_status_change":["tariff_rate:enable_disable"],
    "add_tariff_rate_comment":["tariff_rate:create"],
    "list_tariff_rate_comments":["tariff_rate:view"],
    "tariff_change_history":["tariff_rate:view"],
    "tariff_rate_change":["tariff_rate:view"],
 
    "upload_to_s3": {
        "freight_rate": ["freight_rate:import"],
        "tariff_rate": ["tariff_rate:import"],
    },
   
    "upload_summary_counts":{
        "freight_rate": ["freight_rate:view"],
        "tariff_rate": ["tariff_rate:view"],
    },
 
    "get_feature_template":{
        "freight_rate": ["freight_rate:view"],
        "tariff_rate": ["tariff_rate:view"],
    },
    "list_uploads":{
        "freight_rate": ["freight_rate:view"],
        "tariff_rate": ["tariff_rate:view"],
    },

    "raise_request": ["request:create"],
    "review_request": ["request:review"],
    "get_request_by_id": ["request:detailed_view"],
    "list_requests": ["request:view"]
 
}
 




class EmailType(str, Enum):
    """Email types supported by the notification service"""
    WELCOME = "welcome-email"
    SIGNUP_OTP = "signup_otp-email"
    INVITE = "invite-email"
    PASSWORD_RESET = "password_reset-email"



class Modules(str, Enum):
    """Supported modules in the system"""
    ITEMS_MASTER = "items_master"
    USER_MANAGEMENT = "user_management"
    FREIGHT_RATE = "freight_rate"
    FX_RATE = "fx_rate"
    TARIFF_RATE = "tariff_rate"

    # Add more modules here if needed

class Features(str, Enum):
    """Supported modules in the system"""
    SCENARIO_BUILDER = "scenario_builder"