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