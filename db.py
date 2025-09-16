import logging
from typing import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
    async_sessionmaker,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import Select, Insert, Update, Delete

from tenacity import retry, stop_after_attempt, wait_fixed  # pip install tenacity

from app.core.config import settings



# Configure logger for this module
logger = logging.getLogger(__name__)


# -------------------------
# Engines
# -------------------------

# Primary engine (writes)
primary_url = settings.postgres_url.replace("postgresql://", "postgresql+psycopg://")
primary_engine = create_async_engine(
    primary_url,
    pool_pre_ping=True,
    pool_recycle=300,
    pool_size=5,   # tune for writes
    max_overflow=10,
    echo=settings.debug,
)

# Replica engine (reads)
replica_url = settings.postgres_replica_url.replace("postgresql://", "postgresql+psycopg://")
replica_engine = create_async_engine(
    replica_url,
    pool_pre_ping=True,
    pool_recycle=300,
    pool_size=10,  # higher for reads
    max_overflow=20,
    echo=settings.debug,
)


# -------------------------
# Session makers
# -------------------------
PrimarySessionLocal = async_sessionmaker(
    primary_engine, class_=AsyncSession, expire_on_commit=False
)
ReplicaSessionLocal = async_sessionmaker(
    replica_engine, class_=AsyncSession, expire_on_commit=False
)

# Direct session to primary (for explicit usage)
AsyncSessionLocal = async_sessionmaker(
    primary_engine, class_=AsyncSession, expire_on_commit=False
)

# Declarative base
Base = declarative_base()


# -------------------------
# Routing Session
# -------------------------
class RoutingAsyncSession(AsyncSession):
    """
    Custom session for dynamic routing:
    - Reads -> Replica
    - Writes -> Primary
    - Read-after-write consistency -> Next read goes to primary
    """

    def __init__(self, primary_session: AsyncSession, replica_session: AsyncSession):
        self.primary_session = primary_session
        self.replica_session = replica_session
        self._use_primary_next = False  # Ensures consistency after write
        super().__init__(bind=primary_engine)  # default bind (not used directly)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def execute(self, statement, *multiparams, **params):
        try:
            if isinstance(statement, Select) and not self._use_primary_next:
                # Route SELECTs to replica
                return await self.replica_session.execute(statement, *multiparams, **params)
            else:
                # Writes and flagged reads -> primary
                result = await self.primary_session.execute(statement, *multiparams, **params)
                if isinstance(statement, (Insert, Update, Delete)):
                    self._use_primary_next = True
                return result
        except OperationalError as e:
            # Fallback to primary if replica is down
            msg = f"[DB WARNING] Replica error, falling back to primary: {e}"
            print(msg)  # keeps console visibility
            logger.warning(msg, exc_info=True)
            return await self.primary_session.execute(statement, *multiparams, **params)

    async def commit(self):
        await self.primary_session.commit()
        # Do NOT commit replica (read-only connection)
        self._use_primary_next = False

    async def rollback(self):
        await self.primary_session.rollback()
        await self.replica_session.rollback()

    async def close(self):
        await self.primary_session.close()
        await self.replica_session.close()


# -------------------------
# Context Managers
# -------------------------

@asynccontextmanager
async def get_routing_session() -> AsyncGenerator[RoutingAsyncSession, None]:
    """Get a routing session (reads -> replica, writes -> primary)."""
    primary = PrimarySessionLocal()
    replica = ReplicaSessionLocal()
    session = RoutingAsyncSession(primary, replica)
    try:
        yield session
    finally:
        await session.close()


async def get_postgres_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency for FastAPI: Routing DB session."""
    async with get_routing_session() as session:
        yield session


async def get_primary_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency for FastAPI: Always use primary DB (direct)."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


# -------------------------
# Utilities
# -------------------------

async def create_tables():
    """Create all tables (on primary only)."""
    async with primary_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def close_db_connections():
    """Dispose connections cleanly (on app shutdown)."""
    await primary_engine.dispose()
    await replica_engine.dispose()






















# from typing import AsyncGenerator

# from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
# from sqlalchemy.ext.declarative import declarative_base

# from app.core.config import settings


# # Async SQLAlchemy setup with psycopg
# engine = create_async_engine(
#     settings.postgres_url.replace("postgresql://", "postgresql+psycopg://"),
#     pool_pre_ping=True,
#     pool_recycle=300,
#     echo=settings.debug
# )

# AsyncSessionLocal = async_sessionmaker(
#     engine, 
#     class_=AsyncSession, 
#     expire_on_commit=False
# )

# Base = declarative_base()


# async def get_postgres_db() -> AsyncGenerator[AsyncSession, None]:
#     """Get PostgreSQL async database session"""
#     async with AsyncSessionLocal() as session:
#         try:
#             yield session
#         finally:
#             await session.close()


# async def create_tables():
#     """Create all PostgreSQL tables"""
#     async with engine.begin() as conn:
#         await conn.run_sync(Base.metadata.create_all)


# async def close_db_connections():
#     """Close all database connections"""
#     await engine.dispose()


















