"""Asynchronous job queue and background worker consumer task."""

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_job_queue: Optional[asyncio.Queue[str]] = None
_consumer_task: Optional[asyncio.Task] = None


def get_job_queue() -> asyncio.Queue[str]:
    """Returns the shared asyncio job queue instance, lazily created in the active loop."""
    global _job_queue
    if _job_queue is None:
        _job_queue = asyncio.Queue()
    return _job_queue


async def enqueue_job(job_id: str) -> None:
    """Non-blocking enqueue of a job ID onto the job queue."""
    queue = get_job_queue()
    queue.put_nowait(job_id)
    logger.info("Enqueued job '%s' for background processing.", job_id)


async def queue_consumer() -> None:
    """Continuously consumes job IDs from the queue and delegates to process_job."""
    from app.pipeline.worker import process_job

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
