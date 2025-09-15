from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from app.core.config import settings


# Async SQLAlchemy setup with psycopg
engine = create_async_engine(
    settings.postgres_url.replace("postgresql://", "postgresql+psycopg://"),
    pool_pre_ping=True,
    pool_recycle=300,
    echo=settings.debug
)

AsyncSessionLocal = async_sessionmaker(
    engine, 
    class_=AsyncSession, 
    expire_on_commit=False
)

Base = declarative_base()


async def get_postgres_db() -> AsyncGenerator[AsyncSession, None]:
    """Get PostgreSQL async database session"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def create_tables():
    """Create all PostgreSQL tables"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def close_db_connections():
    """Close all database connections"""
    await engine.dispose()











# from typing import AsyncGenerator
# from sqlalchemy import event, select
# from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
# from sqlalchemy.exc import OperationalError
# from sqlalchemy.sql import Select, Insert, Update, Delete
# from sqlalchemy.ext.declarative import declarative_base
# from contextlib import asynccontextmanager
# from tenacity import retry, stop_after_attempt, wait_fixed  # pip install tenacity

# from app.core.config import settings


# # Primary engine (writes)
# primary_url = settings.postgres_url.replace("postgresql://", "postgresql+psycopg://")
# primary_engine = create_async_engine(
#     primary_url,
#     pool_pre_ping=True,
#     pool_recycle=300,
#     pool_size=5,  # Tune for writes
#     max_overflow=10,
#     echo=settings.debug
# )

# # Replica engine (reads)
# replica_url = settings.postgres_replica_url.replace("postgresql://", "postgresql+psycopg://")
# replica_engine = create_async_engine(
#     replica_url,
#     pool_pre_ping=True,
#     pool_recycle=300,
#     pool_size=10,  # Higher for reads
#     max_overflow=20,
#     echo=settings.debug
# )

# # Session makers
# PrimarySessionLocal = async_sessionmaker(primary_engine, class_=AsyncSession, expire_on_commit=False)
# ReplicaSessionLocal = async_sessionmaker(replica_engine, class_=AsyncSession, expire_on_commit=False)

# Base = declarative_base()


# class RoutingAsyncSession(AsyncSession):
#     """Custom session for dynamic routing: Reads to replica, writes to primary."""
#     def __init__(self, primary_session: AsyncSession, replica_session: AsyncSession):
#         self.primary_session = primary_session
#         self.replica_session = replica_session
#         self._use_primary_next = False  # Flag for read-after-write consistency
#         super().__init__(bind=primary_engine)  # Default bind to primary

#     @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))  # Retry on replica errors
#     async def execute(self, statement, *multiparams, **params):
#         try:
#             if isinstance(statement, Select) and not self._use_primary_next:
#                 # Route reads to replica
#                 return await self.replica_session.execute(statement, *multiparams, **params)
#             else:
#                 # Route writes/modifies or flagged reads to primary
#                 result = await self.primary_session.execute(statement, *multiparams, **params)
#                 if isinstance(statement, (Insert, Update, Delete)):
#                     self._use_primary_next = True  # Flag next read to primary for consistency
#                 return result
#         except OperationalError as e:
#             # Fallback to primary on replica failure
#             print(f"Replica error, falling back to primary: {e}")
#             return await self.primary_session.execute(statement, *multiparams, **params)

#     async def commit(self):
#         await self.primary_session.commit()
#         await self.replica_session.commit()  # Sync if needed
#         self._use_primary_next = False  # Reset flag

#     async def rollback(self):
#         await self.primary_session.rollback()
#         await self.replica_session.rollback()

#     async def close(self):
#         await self.primary_session.close()
#         await self.replica_session.close()


# @asynccontextmanager
# async def get_routing_session() -> AsyncGenerator[RoutingAsyncSession, None]:
#     """Get a routing database session."""
#     primary = PrimarySessionLocal()
#     replica = ReplicaSessionLocal()
#     session = RoutingAsyncSession(primary, replica)
#     try:
#         # Set read-only on replica session
#         await replica.execute("SET TRANSACTION READ ONLY")
#         yield session
#     finally:
#         await session.close()


# async def get_postgres_db() -> AsyncGenerator[AsyncSession, None]:
#     """Get PostgreSQL async database session (now routing)."""
#     async with get_routing_session() as session:
#         yield session


# async def create_tables():
#     """Create all PostgreSQL tables (on primary only)."""
#     async with primary_engine.begin() as conn:
#         await conn.run_sync(Base.metadata.create_all)


# async def close_db_connections():
#     """Close all database connections."""
#     await primary_engine.dispose()
#     await replica_engine.dispose()






