"""Unit and stress tests for global LLM concurrency limiting.

Verifies:
- At no point during batch processing do concurrent LLM calls exceed LLM_CONCURRENCY.
- Global semaphore applies across multiple concurrently submitted jobs.
- High-water mark metric (max_concurrent_llm_calls) is accurately reported.
"""
