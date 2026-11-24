"""CatalogIQ FastAPI Main Application Entrypoint."""

import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api.routes import router as api_router
from app.core.exceptions import (
    InvalidProductDataError,
    JobNotFoundError,
    ProductNotFoundError,
)
from app.core.config import settings
from app.core.singletons import get_llm_provider_instance, get_dedup_manager, init_singletons
from app.db.init_db import init_db, run_crash_recovery
from app.pipeline.queue import start_queue_consumer, stop_queue_consumer, close_redis_queue, initialize_redis_queue
from app.pipeline.rate_limit import close_rate_limiter

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager coordinating DB init, singletons, crash recovery, and queue consumer."""
    # 1. Initialize SQLite schema & WAL settings
    await init_db()
    await initialize_redis_queue()

    # 2. Initialize singletons (semaphore, provider, dedup) inside the active event loop
    init_singletons()
    logger.info("LLM provider: %s", settings.LLM_PROVIDER)
    logger.info("LLM concurrency: %d", settings.LLM_CONCURRENCY)

    # 3. Run crash recovery on startup to re-enqueue interrupted jobs
    await run_crash_recovery()

    # 4. Start the background queue consumer task
    start_queue_consumer()

    yield

    # Clean shutdown: stop queue consumer
    await stop_queue_consumer()
    await close_redis_queue()
    await close_rate_limiter()
    await get_dedup_manager().aclose()
    provider = get_llm_provider_instance()
    close_provider = getattr(provider, "aclose", None)
    if close_provider is not None:
        await close_provider()


app = FastAPI(
    title="CatalogIQ",
    description="Intelligent Catalogue Enrichment Pipeline Service",
    version="1.0.0",
    lifespan=lifespan,
)

# Mount API routes
app.include_router(api_router)

# Mount Frontend Static Files & Index
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse


@app.get("/")
async def serve_index():
    return FileResponse("frontend/index.html")


app.mount("/static", StaticFiles(directory="frontend"), name="static")


# =========================================================================
# Error Handlers: Guarantee every error response body is {"error": "message"}
# =========================================================================

@app.exception_handler(JobNotFoundError)
async def job_not_found_handler(request: Request, exc: JobNotFoundError):
    return JSONResponse(status_code=404, content={"error": exc.message})


@app.exception_handler(ProductNotFoundError)
async def product_not_found_handler(request: Request, exc: ProductNotFoundError):
    return JSONResponse(status_code=404, content={"error": exc.message})


@app.exception_handler(InvalidProductDataError)
async def invalid_product_data_handler(request: Request, exc: InvalidProductDataError):
    return JSONResponse(status_code=400, content={"error": exc.message})


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Formats FastAPI request validation errors into standard {"error": "message"} format with 400."""
    errors = exc.errors()
    if errors:
        first = errors[0]
        msg = first.get("msg", "Invalid parameter")
        loc = ".".join(str(l) for l in first.get("loc", []) if l not in ("body", "query", "path"))
        detail = f"Invalid value for '{loc}': {msg}" if loc else msg
    else:
        detail = "Validation error"
    return JSONResponse(status_code=400, content={"error": detail})


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    return JSONResponse(status_code=exc.status_code, content={"error": str(exc.detail)})


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled server exception: %s", exc)
    return JSONResponse(status_code=500, content={"error": str(exc)})
