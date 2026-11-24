"""In-flight and persistent deduplication manager for CatalogIQ."""

import asyncio
import hashlib
import json
import logging
import re
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from app.core.utils import now_iso
from app.db.session import DatabaseManager, db_manager

logger = logging.getLogger(__name__)


def normalize_listing_text(text: Optional[str]) -> str:
    """Normalizes listing text by lowercasing, collapsing whitespace runs, and stripping."""
    if not text:
        return ""
    # Lowercase and collapse all whitespace sequences (including tabs/newlines) to single space
    collapsed = re.sub(r"\s+", " ", text.lower()).strip()
    return collapsed


def content_hash(raw_title: str, raw_description: Optional[str] = None) -> str:
    """Computes deterministic SHA-256 hash of normalized raw title and description.

    Formula:
        sha256(normalize(raw_title) + " " + normalize(raw_description))
    """
    norm_title = normalize_listing_text(raw_title)
    norm_desc = normalize_listing_text(raw_description)
    combined = f"{norm_title} {norm_desc}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


class DedupManager:
    """Manages persistent cache queries and in-flight request coalescing (leader/follower)."""

    def __init__(
        self,
        manager: Optional[DatabaseManager] = None,
        metrics: Optional[Any] = None,
    ):
        self.db_manager = manager or db_manager
        self.metrics = metrics
        self._in_flight: Dict[str, asyncio.Future] = {}

    @property
    def in_flight(self) -> Dict[str, asyncio.Future]:
        """Provides read-only access to in-flight futures dictionary."""
        return self._in_flight

    async def get_or_compute(
        self,
        content_hash_key: str,
        compute_fn: Callable[[], Awaitable[Dict[str, Any]]],
        on_cache_hit: Optional[Callable[[], Any]] = None,
    ) -> Tuple[Dict[str, Any], bool]:
        """Retrieves result from persistent cache, joins an in-flight leader, or executes as leader.

        Args:
            content_hash_key: SHA-256 hash representing product content.
            compute_fn: Callable returning awaitable enrichment result (executed by leader only).
            on_cache_hit: Optional callback invoked whenever persistent cache or in-flight hit occurs.

        Returns:
            Tuple of (enrichment_result_dict, was_cache_hit_bool).
        """
        # =========================================================================
        # 1. PERSISTENT CACHE CHECK (SQLite dedup_cache)
        # =========================================================================
        cached = await self.db_manager.fetch_one(
            """
            SELECT clean_title, category, brand, tags
            FROM dedup_cache
            WHERE content_hash = :content_hash
            """,
            {"content_hash": content_hash_key},
        )
        if cached:
            # Parse tags JSON string into list if necessary
            tags_val = cached.get("tags")
            if isinstance(tags_val, str):
                try:
                    cached["tags"] = json.loads(tags_val)
                except Exception:
                    cached["tags"] = []

            if on_cache_hit:
                res = on_cache_hit()
                if asyncio.iscoroutine(res):
                    await res

            return (cached, True)

        # =========================================================================
        # 2. IN-FLIGHT CHECK (Follower Path)
        # Single-threaded event loop ensures atomic check without OS mutex.
        # =========================================================================
        if content_hash_key in self._in_flight:
            leader_future = self._in_flight[content_hash_key]
            # shield prevents cancellation of a follower task from cancelling the leader's future
            result = await asyncio.shield(leader_future)

            # on_cache_hit is invoked ONLY after leader succeeds; if leader fails,
            # follower inherits the failure and is NOT counted as a cache hit.
            if on_cache_hit:
                res = on_cache_hit()
                if asyncio.iscoroutine(res):
                    await res

            return (result, True)

        # =========================================================================
        # 3. LEADER PATH (First concurrent request registers Future & computes)
        # =========================================================================
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._in_flight[content_hash_key] = fut

        # ATOMICITY & RACE CONDITION GUARANTEE:
        #
        # Ordering is strictly:
        # 1. DB write to dedup_cache
        # 2. fut.set_result(result)
        # 3. self._in_flight.pop(content_hash_key, None)
        #
        # Why this sequence?
        # - Cache Write First: Writing to SQLite before unblocking waiting followers
        #   guarantees that when followers resume (or subsequent callers arrive), the
        #   persistent cache is already durable. No follower or subsequent job can observe
        #   a cache miss.
        # - set_result Before pop: Keeping the future in `_in_flight` until after result
        #   resolution prevents an interleaved coroutine from finding neither an in-flight
        #   leader nor a committed cache row.
        # - try / except (BaseException) / finally: Catching BaseException (including
        #   asyncio.CancelledError) ensures that if the leader task is abruptly cancelled
        #   or crashes, all followers waiting on `fut` receive the exception immediately
        #   rather than hanging indefinitely. The `finally` block guarantees `_in_flight`
        #   is cleaned up unconditionally.
        try:
            result = await compute_fn()

            # 1. Persist result to SQLite dedup_cache
            tags_json = json.dumps(result.get("tags", []))
            await self.db_manager.execute_write(
                """
                INSERT OR REPLACE INTO dedup_cache (content_hash, clean_title, category, brand, tags, created_at)
                VALUES (:content_hash, :clean_title, :category, :brand, :tags, :created_at)
                """,
                {
                    "content_hash": content_hash_key,
                    "clean_title": result["clean_title"],
                    "category": result["category"],
                    "brand": result.get("brand"),
                    "tags": tags_json,
                    "created_at": now_iso(),
                },
            )

            # 2. Unblock all follower futures
            if not fut.done():
                fut.set_result(result)

            return (result, False)

        except BaseException as exc:
            # Propagate error / cancellation to all waiting followers
            if not fut.done():
                fut.set_exception(exc)
            raise

        finally:
            # 3. Unconditionally release in-flight lease
            self._in_flight.pop(content_hash_key, None)
            # Mark exception retrieved so Python GC does not warn if no followers joined
            if fut.done() and not fut.cancelled():
                fut.exception()
