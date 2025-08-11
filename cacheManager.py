import asyncio
import json
import logging
import pickle
from datetime import timedelta
from typing import Any, Dict, List, Optional, Union

import redis.asyncio as redis
from redis.exceptions import RedisError

from app.core.config import settings

logger = logging.getLogger(__name__)

class CacheManager:
    def __init__(self):
        self._redis_client: Optional[redis.Redis] = None
        self._connection_pool = None
        
    async def initialize(self) -> None:
        try:
            self._connection_pool = redis.ConnectionPool.from_url(
                settings.redis_url,
                max_connections=20,
                retry_on_timeout=True,
                socket_keepalive=True,
                socket_keepalive_options={},
                health_check_interval=30
            )
            self._redis_client = redis.Redis(connection_pool=self._connection_pool)
            await self._redis_client.ping()
            logger.info("Redis connection established successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Redis connection: {e}")
            raise
    
    async def close(self) -> None:
        if self._redis_client:
            await self._redis_client.close()
        if self._connection_pool:
            await self._connection_pool.disconnect()
        logger.info("Redis connection closed")
    
    def _serialize_value(self, value: Any) -> str:
        try:
            return json.dumps(value, default=str)
        except (TypeError, ValueError):
            return pickle.dumps(value).hex()
    
    def _deserialize_value(self, value: str) -> Any:
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            try:
                return pickle.loads(bytes.fromhex(value))
            except (ValueError, pickle.UnpicklingError):
                return value
    
    async def set(self, key: str, value: Any, expire: Optional[Union[int, timedelta]] = None) -> bool:
        try:
            if not self._redis_client:
                await self.initialize()
            
            serialized_value = self._serialize_value(value)
            
            if isinstance(expire, timedelta):
                expire = int(expire.total_seconds())
            
            result = await self._redis_client.set(key, serialized_value, ex=expire)
            logger.debug(f"SET: {key} = {result}")
            return bool(result)
        except RedisError as e:
            logger.error(f"Redis SET error for key {key}: {e}")
            return False
    
    async def get(self, key: str) -> Optional[Any]:
        try:
            if not self._redis_client:
                await self.initialize()
            
            value = await self._redis_client.get(key)
            if value is None:
                return None
            
            if isinstance(value, bytes):
                value = value.decode('utf-8')
            
            return self._deserialize_value(value)
        except RedisError as e:
            logger.error(f"Redis GET error for key {key}: {e}")
            return None
    
    async def delete(self, key: str) -> bool:
        try:
            if not self._redis_client:
                await self.initialize()
            
            result = await self._redis_client.delete(key)
            logger.debug(f"DELETE: {key} = {result}")
            return bool(result)
        except RedisError as e:
            logger.error(f"Redis DELETE error for key {key}: {e}")
            return False
    
    async def exists(self, key: str) -> bool:
        try:
            if not self._redis_client:
                await self.initialize()
            
            result = await self._redis_client.exists(key)
            return bool(result)
        except RedisError as e:
            logger.error(f"Redis EXISTS error for key {key}: {e}")
            return False
    
    async def expire(self, key: str, expire: Union[int, timedelta]) -> bool:
        try:
            if not self._redis_client:
                await self.initialize()
            
            if isinstance(expire, timedelta):
                expire = int(expire.total_seconds())
            
            result = await self._redis_client.expire(key, expire)
            return bool(result)
        except RedisError as e:
            logger.error(f"Redis EXPIRE error for key {key}: {e}")
            return False
    
    async def ttl(self, key: str) -> int:
        try:
            if not self._redis_client:
                await self.initialize()
            
            return await self._redis_client.ttl(key)
        except RedisError as e:
            logger.error(f"Redis TTL error for key {key}: {e}")
            return -1
    
    async def incr(self, key: str, amount: int = 1) -> Optional[int]:
        try:
            if not self._redis_client:
                await self.initialize()
            
            return await self._redis_client.incr(key, amount)
        except RedisError as e:
            logger.error(f"Redis INCR error for key {key}: {e}")
            return None
    
    async def hset(self, name: str, key: str, value: Any) -> bool:
        try:
            if not self._redis_client:
                await self.initialize()
            
            serialized_value = self._serialize_value(value)
            result = await self._redis_client.hset(name, key, serialized_value)
            return True
        except RedisError as e:
            logger.error(f"Redis HSET error for hash {name}, key {key}: {e}")
            return False
    
    async def hget(self, name: str, key: str) -> Optional[Any]:
        try:
            if not self._redis_client:
                await self.initialize()
            
            value = await self._redis_client.hget(name, key)
            if value is None:
                return None
            
            if isinstance(value, bytes):
                value = value.decode('utf-8')
            
            return self._deserialize_value(value)
        except RedisError as e:
            logger.error(f"Redis HGET error for hash {name}, key {key}: {e}")
            return None
    
    async def hgetall(self, name: str) -> Dict[str, Any]:
        try:
            if not self._redis_client:
                await self.initialize()
            
            hash_data = await self._redis_client.hgetall(name)
            result = {}
            
            for key, value in hash_data.items():
                if isinstance(key, bytes):
                    key = key.decode('utf-8')
                if isinstance(value, bytes):
                    value = value.decode('utf-8')
                result[key] = self._deserialize_value(value)
            
            return result
        except RedisError as e:
            logger.error(f"Redis HGETALL error for hash {name}: {e}")
            return {}
    
    async def sadd(self, name: str, *values: Any) -> int:
        try:
            if not self._redis_client:
                await self.initialize()
            
            serialized_values = [self._serialize_value(value) for value in values]
            return await self._redis_client.sadd(name, *serialized_values)
        except RedisError as e:
            logger.error(f"Redis SADD error for set {name}: {e}")
            return 0
    
    async def smembers(self, name: str) -> List[Any]:
        try:
            if not self._redis_client:
                await self.initialize()
            
            members = await self._redis_client.smembers(name)
            result = []
            
            for member in members:
                if isinstance(member, bytes):
                    member = member.decode('utf-8')
                result.append(self._deserialize_value(member))
            
            return result
        except RedisError as e:
            logger.error(f"Redis SMEMBERS error for set {name}: {e}")
            return []
    
    async def flushdb(self) -> bool:
        try:
            if not self._redis_client:
                await self.initialize()
            
            await self._redis_client.flushdb()
            logger.info("Redis database flushed successfully")
            return True
        except RedisError as e:
            logger.error(f"Redis FLUSHDB error: {e}")
            return False
    
    def create_tenant_key(self, prefix: str, user_id: Union[int, str], tenant_id: Union[int, str]) -> str:
        return f"{prefix}:{user_id}:{tenant_id}"
    
    def create_privilege_key(self, tenant_id: str, user_id: str) -> str:
        return f"userprivilege:{tenant_id}:{user_id}"
    
    async def set_tenant(self, prefix: str, user_id: Union[int, str], tenant_id: Union[int, str], 
                        value: Any, expire: Optional[Union[int, timedelta]] = None) -> bool:
        key = self.create_tenant_key(prefix, user_id, tenant_id)
        return await self.set(key, value, expire)
    
    async def get_tenant(self, prefix: str, user_id: Union[int, str], tenant_id: Union[int, str]) -> Optional[Any]:
        key = self.create_tenant_key(prefix, user_id, tenant_id)
        return await self.get(key)
    
    async def delete_tenant(self, prefix: str, user_id: Union[int, str], tenant_id: Union[int, str]) -> bool:
        key = self.create_tenant_key(prefix, user_id, tenant_id)
        return await self.delete(key)
    
    async def exists_tenant(self, prefix: str, user_id: Union[int, str], tenant_id: Union[int, str]) -> bool:
        key = self.create_tenant_key(prefix, user_id, tenant_id)
        return await self.exists(key)
    
    async def invalidate_tenant_pattern(self, prefix: str, user_id: Optional[Union[int, str]] = None, 
                                      tenant_id: Optional[Union[int, str]] = None) -> bool:
        try:
            if not self._redis_client:
                await self.initialize()
            
            if user_id and tenant_id:
                pattern = f"{prefix}:{user_id}:{tenant_id}"
            elif user_id:
                pattern = f"{prefix}:{user_id}:*"
            elif tenant_id:
                pattern = f"{prefix}:*:{tenant_id}"
            else:
                pattern = f"{prefix}:*"
            
            keys = await self._redis_client.keys(pattern)
            if keys:
                await self._redis_client.delete(*keys)
                logger.debug(f"Invalidated tenant cache keys: {keys}")
                return True
            return False
        except RedisError as e:
            logger.error(f"Redis invalidate tenant pattern error: {e}")
            return False
    
    def cache(self, expire: Union[int, timedelta] = 3600, key_prefix: str = ""):
        def decorator(func):
            async def async_wrapper(*args, **kwargs):
                cache_key = f"{key_prefix}:{func.__name__}:{hash(str(args) + str(sorted(kwargs.items())))}"
                
                cached_result = await self.get(cache_key)
                if cached_result is not None:
                    logger.debug(f"Cache HIT for {cache_key}")
                    return cached_result
                
                result = await func(*args, **kwargs)
                await self.set(cache_key, result, expire)
                logger.debug(f"Cache MISS for {cache_key}, stored result")
                return result
            
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            else:
                def sync_wrapper(*args, **kwargs):
                    cache_key = f"{key_prefix}:{func.__name__}:{hash(str(args) + str(sorted(kwargs.items())))}"
                    
                    cached_result = self.get_sync(cache_key)
                    if cached_result is not None:
                        logger.debug(f"Cache HIT for {cache_key}")
                        return cached_result
                    
                    result = func(*args, **kwargs)
                    self.set_sync(cache_key, result, expire)
                    logger.debug(f"Cache MISS for {cache_key}, stored result")
                    return result
                return sync_wrapper
        
        return decorator
    
    def cache_with_tenant(self, expire: Union[int, timedelta] = 3600, key_prefix: str = ""):
        def decorator(func):
            async def async_wrapper(*args, **kwargs):
                user_id = kwargs.get('user_id') or (args[0] if args else None)
                tenant_id = kwargs.get('tenant_id') or (args[1] if len(args) > 1 else None)
                
                if not user_id or not tenant_id:
                    logger.warning(f"Missing user_id or tenant_id for cache key generation in {func.__name__}")
                    cache_key = f"{key_prefix}:{func.__name__}:{hash(str(args) + str(sorted(kwargs.items())))}"
                else:
                    cache_key = f"{key_prefix}:{user_id}:{tenant_id}"
                
                cached_result = await self.get(cache_key)
                if cached_result is not None:
                    logger.debug(f"Cache HIT for {cache_key}")
                    return cached_result
                
                result = await func(*args, **kwargs)
                await self.set(cache_key, result, expire)
                logger.debug(f"Cache MISS for {cache_key}, stored result")
                return result
            

            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            else:
                def sync_wrapper(*args, **kwargs):
                    user_id = kwargs.get('user_id') or (args[0] if args else None)
                    tenant_id = kwargs.get('tenant_id') or (args[1] if len(args) > 1 else None)
                    
                    if not user_id or not tenant_id:
                        logger.warning(f"Missing user_id or tenant_id for cache key generation in {func.__name__}")
                        cache_key = f"{key_prefix}:{func.__name__}:{hash(str(args) + str(sorted(kwargs.items())))}"
                    else:
                        cache_key = f"{key_prefix}:{user_id}:{tenant_id}"
                    
                    cached_result = self.get_sync(cache_key)
                    if cached_result is not None:
                        logger.debug(f"Cache HIT for {cache_key}")
                        return cached_result
                    
                    result = func(*args, **kwargs)
                    self.set_sync(cache_key, result, expire)
                    logger.debug(f"Cache MISS for {cache_key}, stored result")
                    return result
                return sync_wrapper
        
        return decorator
    
    def invalidate_cache(self, key_pattern: str):
        def decorator(func):
            async def async_wrapper(*args, **kwargs):
                result = await func(*args, **kwargs)
                if self._redis_client:
                    keys = await self._redis_client.keys(key_pattern)
                    if keys:
                        await self._redis_client.delete(*keys)
                        logger.debug(f"Invalidated cache keys: {keys}")
                return result
            
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            else:
                def sync_wrapper(*args, **kwargs):
                    result = func(*args, **kwargs)
                    if self._redis_client:
                        keys = self._redis_client.keys(key_pattern)
                        if keys:
                            self._redis_client.delete(*keys)
                            logger.debug(f"Invalidated cache keys: {keys}")
                    return result
                return sync_wrapper
        
        return decorator


cache_manager = CacheManager()
