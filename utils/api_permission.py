from typing import Any, Dict

from fastapi import HTTPException, status
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.shared_modules.constant import API_PERMISSIONS, TenantUserStatus
from app.shared_modules.db import get_postgres_db
from app.shared_modules.models import TenantUser


async def check_api_permissions(db: AsyncSession, api_name: str, current_user: Dict[str, Any]) -> None:
    """
    Check if the current user has all required permissions for the specified API.
    """
    if api_name not in API_PERMISSIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"API '{api_name}' is not defined in permission mapping"
        )

    user_data = current_user.get('users')[0]
    if not user_data:
        raise HTTPException(status_code=404, detail="No User Found")

    tenant_id = user_data.get('tenant_id')
    user_id = user_data.get('user_id')

    if tenant_id and tenant_id != "None":
        try:
            user_status_query = await db.execute(
                select(TenantUser).where(
                    and_(
                        TenantUser.user_id == user_id,
                        TenantUser.tenant_id == tenant_id
                    )
                )
            )
            user_status = user_status_query.scalars().first()
            print("check_api_permissions - user_status:", user_status)

            if not user_status or user_status.status != TenantUserStatus.ACTIVE:
                raise HTTPException(status_code=403, detail="User is not Active in this tenant")
        except Exception as e:
            print("Error while checking user status:", str(e))
            raise HTTPException(status_code=500, detail="Internal server error during user status check")

    required_permissions = API_PERMISSIONS[api_name]
    user_privileges = user_data.get('privileges', [])
        
    missing_permissions = [perm for perm in required_permissions if perm not in user_privileges]
    if missing_permissions:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied - You don't have permission for:{api_name}"
        )