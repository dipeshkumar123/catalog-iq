"""Common utility helpers for CatalogIQ."""

from datetime import datetime, timezone


def now_iso() -> str:
    """Returns the current UTC timestamp formatted as ISO-8601 without timezone offset suffix.

    Format: YYYY-MM-DDTHH:MM:SS
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
