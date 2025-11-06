import json
import logging
from datetime import datetime, timezone
from uuid import UUID

from app.shared_modules.open_search.manager import OpenSearchManager
from fastapi import Request
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, ExpiredSignatureError, jwt
from redis.asyncio import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from .cacheManager import cache_manager
from .db import get_postgres_db
from .models import Session, TenantUser
from .schemas.common import UserData, UserResponse

logger = logging.getLogger(__name__)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/v1/user-login")

async def get_current_user_session_token(token: str = Depends(oauth2_scheme)) -> str:
    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    return token

async def get_current_user(token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_postgres_db)) -> dict:
    """Validates JWT token and retrieves user privileges from Redis cache, handling super_admin case."""
    try:
        try:
            payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
            required_fields = {"sub": str, "user_id": str, "tenant_id": str, "exp": (int, float)}
            
            missing_fields = [field for field, type_ in required_fields.items() 
                            if not isinstance(payload.get(field), type_)]
            if missing_fields:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                                  detail=f"Invalid token: missing or invalid {', '.join(missing_fields)}",
                                  headers={"WWW-Authenticate": "Bearer"})

            if payload["exp"] < datetime.now(timezone.utc).timestamp():
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                                  detail="Token has expired",
                                  headers={"WWW-Authenticate": "Bearer"})

            for field in ("user_id", "tenant_id"):
                if payload[field] != 'None':
                    try:
                        payload[field] = str(UUID(payload[field]))
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                                         detail=f"Invalid {field} format",
                                         headers={"WWW-Authenticate": "Bearer"})
        except ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token has expired")
        except JWTError as e:
            logger.error(f"JWT decode error: {e}")
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                              detail="Invalid token",
                              headers={"WWW-Authenticate": "Bearer"})
        
        result = await db.execute(
            select(Session)
            .where(Session.access_token == token, Session.deleted_at.is_(None))
        )
        session = result.scalar_one_or_none()
        if not session:
            raise HTTPException(status_code=401, detail="Session not found or logged out")

        #super admin (tenant_id is 'None')
        cache_key = f"userprivilege:{'None' if payload['tenant_id'] == 'None' else payload['tenant_id']}:{payload['user_id']}"
        is_super_admin = payload["tenant_id"] == 'None'

        if is_super_admin:
            result = await db.execute(select(TenantUser).where(TenantUser.user_id == UUID(payload["user_id"]), TenantUser.is_admin == True))
            if not result.scalars().first():
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                                  detail="User is not a super admin",
                                  headers={"WWW-Authenticate": "Bearer"})

        cached_data = await cache_manager.get(cache_key)
        if not cached_data:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                              detail="User not found in cache",
                              headers={"WWW-Authenticate": "Bearer"})

        if isinstance(cached_data, str):
            try:
                cached_data = json.loads(cached_data)
            except json.JSONDecodeError:
                logger.error(f"Invalid cached data format for key: {cache_key}")
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                  detail="Invalid cached data format",
                                  headers={"WWW-Authenticate": "Bearer"})

        if not isinstance(cached_data, dict) or "user" not in cached_data:
            logger.error(f"Invalid cached data structure for key: {cache_key}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                              detail="Invalid cached data structure",
                              headers={"WWW-Authenticate": "Bearer"})

        user_details = cached_data["user"]
        if user_details.get("status") != "active":
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                              detail="User account is not active",
                              headers={"WWW-Authenticate": "Bearer"})

        privileges = cached_data.get("privileges", {})
        privilege_list = [f"{resource}:{privilege}" for resource, privs in privileges.items()
                         for privilege in (privs if isinstance(privs, list) else [privs])]

        user_data = {
            "email": user_details.get("email", payload["sub"]),
            "user_id": user_details.get("user_id", payload["user_id"]),
            "tenant_id": "None" if is_super_admin else user_details.get("tenant_id", payload["tenant_id"]),
            "name": user_details.get("name"),
            "status": user_details.get("status", "active"),
            "user_details": user_details,
            "privileges": privilege_list
        }

        try:
            user_data_model = UserData(**user_data)
            return UserResponse(
                users=[user_data_model],
                message="No privileges assigned to user" if not privilege_list else None
            ).dict()
        except ValueError as e:
            logger.error(f"Failed to validate user data: {str(e)}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                              detail=f"Failed to validate user data: {str(e)}",
                              headers={"WWW-Authenticate": "Bearer"})

    except RedisError:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                          detail="Cache service unavailable",
                          headers={"WWW-Authenticate": "Bearer"})
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Internal server error: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                          detail=f"Internal server error: {str(e)}",
                          headers={"WWW-Authenticate": "Bearer"})
        
async def get_opensearch_manager(request: Request) -> OpenSearchManager:
    """
    Dependency to retrieve the OpenSearchManager from FastAPI app state.
    """
    return request.app.state.opensearch_manager

async def get_opensearch_manager_direct() -> OpenSearchManager:
    return OpenSearchManager()