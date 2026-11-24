"""Tests for retry with exponential backoff and schema validation.

Verifies:
- Transient mock LLM errors trigger retry attempts up to 4 total tries.
- Exponential backoff wait intervals increase between attempts.
- Invalid JSON or invalid category counts as a failed attempt and triggers retry.
- After 4 failed attempts, product is marked 'failed' with error, and job continues.
"""
