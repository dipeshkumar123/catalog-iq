"""Async SQLite database manager with WAL mode and serialized write locks."""

import asyncio
from typing import Any, Dict, List, Optional, Sequence, Union
import aiosqlite

from app.core.config import settings


class DatabaseManager:
    """Manages persistent asynchronous SQLite connections, WAL pragmas, and serialized writes."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or settings.DATABASE_PATH
        self._lock: Optional[asyncio.Lock] = None
        self._conn: Optional[aiosqlite.Connection] = None
        self._conn_loop: Optional[asyncio.AbstractEventLoop] = None

    @property
    def write_lock(self) -> asyncio.Lock:
        """Lazily initialize or refresh the write lock to guarantee binding to the active event loop."""
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if self._lock is None:
            self._lock = asyncio.Lock()
        elif current_loop is not None and getattr(self._lock, "_loop", None) not in (None, current_loop):
            self._lock = asyncio.Lock()

        return self._lock

    async def get_connection(self) -> aiosqlite.Connection:
        """Provides a persistent aiosqlite connection configured with production WAL PRAGMAs."""
        loop = asyncio.get_running_loop()
        if self._conn is None or self._conn_loop is not loop:
            if self._conn is not None:
                try:
                    await self._conn.close()
                except Exception:
                    pass
            self._conn = await aiosqlite.connect(self.db_path)
            self._conn.row_factory = aiosqlite.Row

            # Configure WAL mode and concurrency performance pragmas
            await self._conn.execute("PRAGMA journal_mode = WAL;")
            await self._conn.execute("PRAGMA synchronous = NORMAL;")
            await self._conn.execute("PRAGMA busy_timeout = 5000;")
            await self._conn.execute("PRAGMA foreign_keys = ON;")
            self._conn_loop = loop

        return self._conn

    async def close(self) -> None:
        """Closes the underlying database connection."""
        if self._conn is not None:
            try:
                await self._conn.close()
            except Exception:
                pass
            self._conn = None
            self._conn_loop = None

    async def execute_write(
        self,
        query: str,
        parameters: Union[Sequence[Any], Dict[str, Any]] = (),
    ) -> Optional[int]:
        """Executes a single write query guarded by the write lock and commits.

        Returns:
            The lastrowid inserted, or rowcount affected.
        """
        async with self.write_lock:
            db = await self.get_connection()
            cursor = await db.execute(query, parameters)
            await db.commit()
            return cursor.lastrowid

    async def execute_write_many(
        self,
        query: str,
        parameters_list: Sequence[Union[Sequence[Any], Dict[str, Any]]],
    ) -> int:
        """Executes a batch of writes under the write lock in a single transaction."""
        async with self.write_lock:
            db = await self.get_connection()
            cursor = await db.executemany(query, parameters_list)
            await db.commit()
            return cursor.rowcount

    async def execute_script(self, script: str) -> None:
        """Executes a multi-statement DDL/DML script guarded by the write lock."""
        async with self.write_lock:
            db = await self.get_connection()
            await db.executescript(script)
            await db.commit()

    async def fetch_one(
        self,
        query: str,
        parameters: Union[Sequence[Any], Dict[str, Any]] = (),
    ) -> Optional[Dict[str, Any]]:
        """Fetches a single row as a dictionary, or None if not found."""
        db = await self.get_connection()
        async with db.execute(query, parameters) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row is not None else None

    async def fetch_all(
        self,
        query: str,
        parameters: Union[Sequence[Any], Dict[str, Any]] = (),
    ) -> List[Dict[str, Any]]:
        """Fetches all matching rows as a list of dictionaries."""
        db = await self.get_connection()
        async with db.execute(query, parameters) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


# Global database manager instance
db_manager = DatabaseManager()
