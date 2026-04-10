import asyncio
import os
import time
import asyncpg
from typing import Optional


class PostgresService:
    """
    Async PostgreSQL connection pool.
    Mirrors: query(), withTransaction(), testConnection(), close()
    from postgres.service.js
    """

    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None
        self._connect_lock = asyncio.Lock()

    async def connect(self):
        """Initialize connection pool. Called on app startup."""
        async with self._connect_lock:
            if self._pool and not self._pool.is_closing():
                return

            old_pool = self._pool
            self._pool = None

            dsn = os.getenv("PG_CONNECTION_STRING")
            if dsn:
                pool = await asyncpg.create_pool(
                    dsn=dsn,
                    min_size=1,
                    max_size=10,
                    command_timeout=60,
                )
            else:
                host = os.getenv("PG_HOST")
                if not host:
                    raise RuntimeError(
                        "PostgreSQL not configured. Set PG_CONNECTION_STRING or "
                        "PG_HOST/PG_PORT/PG_DATABASE/PG_USER/PG_PASSWORD in .env"
                    )
                pool = await asyncpg.create_pool(
                    host=host,
                    port=int(os.getenv("PG_PORT", "5432")),
                    database=os.getenv("PG_DATABASE", "ecommerce_scraper"),
                    user=os.getenv("PG_USER", "postgres"),
                    password=os.getenv("PG_PASSWORD", ""),
                    min_size=1,
                    max_size=10,
                    command_timeout=60,
                )

            self._pool = pool

            if old_pool and not old_pool.is_closing():
                await old_pool.close()

    @property
    def pool(self) -> asyncpg.Pool:
        if not self._pool:
            raise RuntimeError("PostgreSQL pool not initialized. Call connect() first.")
        return self._pool

    async def query(self, sql: str, *params) -> list[dict]:
        start = time.monotonic()
        rows = await self._run_with_reconnect("fetch", sql, *params)
        duration = (time.monotonic() - start) * 1000
        if duration > 1000:
            print(f"[PG] Slow query ({duration:.0f}ms): {sql[:80]}")
        return [dict(r) for r in rows]

    async def execute(self, sql: str, *params) -> str:
        return await self._run_with_reconnect("execute", sql, *params)

    async def executemany(self, sql: str, args: list) -> None:
        await self._run_with_reconnect("executemany", sql, args)

    async def with_transaction(self, fn):
        async def _run():
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    return await fn(conn)

        return await self._run_callable_with_reconnect(_run)

    async def test_connection(self) -> str:
        rows = await self.query("SELECT NOW() AS now")
        return str(rows[0]["now"])

    async def close(self):
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def _run_with_reconnect(self, method: str, sql: str, *params):
        async def _run():
            async with self.pool.acquire() as conn:
                if method == "fetch":
                    return await conn.fetch(sql, *params)
                if method == "execute":
                    return await conn.execute(sql, *params)
                if method == "executemany":
                    return await conn.executemany(sql, *params)
                raise ValueError(f"Unsupported PostgreSQL method: {method}")

        return await self._run_callable_with_reconnect(_run)

    async def _run_callable_with_reconnect(self, fn):
        await self.connect()
        try:
            return await fn()
        except (
            asyncpg.InterfaceError,
            asyncpg.ConnectionDoesNotExistError,
            ConnectionError,
            OSError,
        ):
            await self.close()
            await self.connect()
            return await fn()


# Singleton
pg_service = PostgresService()
