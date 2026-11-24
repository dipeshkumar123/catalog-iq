"""Tests verifying FastAPI static file serving and frontend root endpoint."""

import httpx
from httpx import ASGITransport
import pytest

from app.main import app


@pytest.mark.asyncio
async def test_root_serves_frontend_html():
    """GET / must return 200 with text/html and contain id='app'."""
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.get("/")
        assert res.status_code == 200
        assert "text/html" in res.headers["content-type"]
        assert 'id="app"' in res.text
        assert 'role="tablist"' in res.text


@pytest.mark.asyncio
async def test_static_javascript_serving():
    """GET /static/app.js must return 200 and JavaScript content."""
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.get("/static/app.js")
        assert res.status_code == 200
        assert any(
            t in res.headers["content-type"]
            for t in ["javascript", "application/x-javascript", "text/plain"]
        )
        assert "parseCSV" in res.text


@pytest.mark.asyncio
async def test_static_css_serving():
    """GET /static/style.css must return 200 and text/css."""
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.get("/static/style.css")
        assert res.status_code == 200
        assert "text/css" in res.headers["content-type"]
        assert "--primary:" in res.text
