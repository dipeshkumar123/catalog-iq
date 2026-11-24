"""Asynchronous job queue and background worker consumer task."""

import asyncio
import logging
from typing import Optional
from app.core.config import settings

logger = logging.getLogger(__name__)

_job_queue: Optional[asyncio.Queue[str]] = None
_consumer_task: Optional[asyncio.Task] = None
_redis_client = None


def get_job_queue() -> asyncio.Queue[str]:
    """Returns the shared asyncio job queue instance, lazily created in the active loop."""
    global _job_queue
    if _job_queue is None:
        _job_queue = asyncio.Queue()
    return _job_queue


async def enqueue_job(job_id: str) -> None:
    """Non-blocking enqueue of a job ID onto the job queue."""
    if settings.REDIS_URL:
        client = await _get_redis_client()
        await client.xadd(settings.REDIS_QUEUE_STREAM, {"job_id": job_id})
        logger.info("Enqueued job '%s' into Redis Streams.", job_id)
        return
    queue = get_job_queue()
    queue.put_nowait(job_id)
    logger.info("Enqueued job '%s' for background processing.", job_id)


async def queue_consumer() -> None:
    """Continuously consumes job IDs from the queue and delegates to process_job."""
    from app.pipeline.worker import process_job

    if settings.REDIS_URL:
        await _redis_queue_consumer(process_job)
        return

    queue = get_job_queue()
    logger.info("Job queue background consumer started.")

    while True:
        try:
            job_id = await queue.get()
            try:
                logger.info("Consumer picked up job '%s'.", job_id)
                await process_job(job_id)
                logger.info("Consumer completed job '%s'.", job_id)
            except Exception as e:
                logger.exception("Unexpected error while processing job '%s': %s", job_id, e)
            finally:
                queue.task_done()
        except asyncio.CancelledError:
            logger.info("Job queue background consumer received cancellation.")
            break


async def _get_redis_client():
    global _redis_client
    if _redis_client is None:
        try:
            from redis.asyncio import Redis
        except ImportError as exc:
            raise RuntimeError("REDIS_URL requires the redis package") from exc
        _redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis_client


async def initialize_redis_queue() -> None:
    """Fail fast on bad Redis configuration and ensure the stream group exists."""
    if not settings.REDIS_URL:
        return
    client = await _get_redis_client()
    await client.ping()
    try:
        await client.xgroup_create(settings.REDIS_QUEUE_STREAM, settings.REDIS_QUEUE_GROUP, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def _redis_queue_consumer(process_job) -> None:
    client = await _get_redis_client()
    stream, group = settings.REDIS_QUEUE_STREAM, settings.REDIS_QUEUE_GROUP
    try:
        await client.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise
    consumer = f"catalogiq-{__import__('os').getpid()}"
    while True:
        try:
            claimed = await client.xautoclaim(stream, group, consumer,
                min_idle_time=settings.REDIS_QUEUE_CLAIM_IDLE_MS, start_id="0-0", count=1)
            entries = claimed[1] if claimed and len(claimed) > 1 else []
            if not entries:
                batches = await client.xreadgroup(group, consumer, {stream: ">"}, count=1, block=1000)
                entries = [(message_id, fields) for _, messages in batches for message_id, fields in messages]
            for message_id, fields in entries:
                job_id = fields.get("job_id")
                try:
                    if job_id:
                        await process_job(job_id)
                    await client.xack(stream, group, message_id)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Redis Streams job processing failed; message remains pending")
        except asyncio.CancelledError:
            raise


async def close_redis_queue() -> None:
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None


def start_queue_consumer() -> asyncio.Task:
    """Starts the background consumer task."""
    global _consumer_task
    if _consumer_task is None or _consumer_task.done():
        _consumer_task = asyncio.create_task(queue_consumer())
    return _consumer_task


async def stop_queue_consumer() -> None:
    """Stops and cancels the background consumer task gracefully."""
    global _consumer_task
    if _consumer_task and not _consumer_task.done():
        _consumer_task.cancel()
        try:
            await _consumer_task
        except asyncio.CancelledError:
            pass
        _consumer_task = None
