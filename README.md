# CatalogIQ

> Enriches messy product listings into a clean searchable catalogue using an LLM pipeline built for correctness under load.

## What it does

Upload a CSV, then CatalogIQ runs parallel LLM enrichment with persistent and in-flight deduplication, retries, and schema validation ([worker](app/pipeline/worker.py), [dedup manager](app/pipeline/dedup.py), [retry wrapper](app/pipeline/retry.py), [validator](app/llm/validator.py)). Results appear in a searchable catalogue for human review and approval ([API routes](app/api/routes.py), [frontend](frontend/index.html)).

## Quick start (mock mode, no API key needed)

1. Create and activate a virtual environment. On macOS/Linux:

   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

   On Windows PowerShell:

   ```powershell
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   ```

   You should see `(venv)` in the shell prompt.
2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

   You should see packages installed or already satisfied, with no errors ([requirements](requirements.txt)).
3. Copy the example configuration. On macOS/Linux:

   ```bash
   cp .env.example .env
   ```

   On Windows:

   ```powershell
   copy .env.example .env
   ```

   The copy command completes without output and creates `.env`. The example sets `LLM_PROVIDER=mock`; no API key is needed ([example config](.env.example), [settings](app/core/config.py)).
4. Start the server:

   ```bash
   uvicorn app.main:app --reload --host 0.0.0.0
   ```

   You should see `Uvicorn running on http://0.0.0.0:8000` ([application setup](app/main.py)).
5. Open <http://localhost:8000>. You should see the Upload, Catalogue, and Review tabs ([page](frontend/index.html)).

## Using a real LLM (Groq)

Create an account at [Groq Console](https://console.groq.com) and create an API key under **API Keys**. Set the key and provider in your shell:

```bash
export GROQ_API_KEY=your_groq_api_key_here
export LLM_PROVIDER=groq
```

In PowerShell:

```powershell
$env:GROQ_API_KEY="your_groq_api_key_here"
$env:LLM_PROVIDER="groq"
```

Alternatively, set these in `.env`, then restart with `uvicorn app.main:app --reload`. Verify `GET /api/health` reports `"llm_provider":"groq"`. Submit a small job and inspect the title, category, brand, and tags returned by the [Groq provider](app/llm/groq.py); output is checked by the [shared validator](app/llm/validator.py).

### Optional Gemini fallback

Set `GEMINI_API_KEY` in the shell or `.env`. With `LLM_PROVIDER=groq`, Groq remains primary and Gemini is used for transient rate-limit, overload, or timeout failures. `GEMINI_MODEL` defaults to `gemini-3.5-flash-lite`; set `LLM_FALLBACK_PROVIDER=none` to disable fallback. `LLM_COMPLEX_PROVIDER=gemini` routes long titles or description-heavy listings to Gemini. Keys stay in environment configuration and are never sent to the browser.

### Optional Redis services

Install dependencies with `pip install -r requirements.txt`, then set `REDIS_URL` to a reachable Redis endpoint before starting the app. Redis Streams supplies the cross-process job queue, and Redis caches deduplicated enrichment results. Set `LLM_RATE_LIMIT_RPM` and/or `LLM_RATE_LIMIT_TPM` to enable a shared token bucket; zero disables that limit. Startup checks Redis connectivity and creates the consumer group. Without `REDIS_URL`, the local asyncio queue and SQLite cache remain in use.

**Note:** the brief lists `llama-3.3-70b-versatile` as the recommended model, but Groq deprecated it for free and developer tiers on 2026-08-16 ([Groq deprecation notice](https://console.groq.com/docs/deprecations)). This project defaults to `openai/gpt-oss-120b`; override it with `GROQ_MODEL`.

Groq limits depend on the model and account tier; check [current limits](https://console.groq.com/docs/rate-limits) and the Console before larger runs. `LLM_CONCURRENCY=5` limits concurrent requests; reduce it to `3` if you see 429 responses.

## How the pipeline works

- `asyncio.Semaphore` bounds concurrent provider calls within this process ([singletons](app/core/singletons.py), [worker](app/pipeline/worker.py)).
- Normalized title/description hashes use persistent cache hits and in-flight futures to coalesce duplicate work ([dedup manager](app/pipeline/dedup.py)).
- Transient provider errors and invalid output receive up to four total attempts with exponential backoff and jitter ([retry wrapper](app/pipeline/retry.py)).
- The validator enforces exact fields, title bounds, allowed categories, brand type, single-word lowercase tags, and finite confidence values; quality gates route uncertain listings to review ([validator](app/llm/validator.py), [quality checks](app/llm/quality.py)).
- SQLite WAL stores jobs and product state; startup resets interrupted products and requeues their jobs ([database recovery](app/db/init_db.py), [recovery tests](tests/test_crash_recovery.py)).

See [DESIGN.md](DESIGN.md) for the trade-offs, limitations, and scaling plan.

## Running the tests

```bash
pytest -q
pytest -m integration -q
node frontend/test_csv.mjs
```

The Python suite covers unit, API, database, pipeline, and mocked-provider behavior without network access ([tests](tests/)). The integration test calls Groq only when `GROQ_API_KEY` is set; otherwise it is skipped ([live test](tests/test_groq_live.py)). The Node test covers CSV quoting, line endings, BOM, missing fields, and title whitespace ([CSV test](frontend/test_csv.mjs)).

## Project structure

| Path | Purpose |
|---|---|
| [app/api](app/api/routes.py) | FastAPI jobs, products, review, and health endpoints |
| [app/core](app/core/config.py) | Settings, exceptions, metrics, and shared provider state |
| [app/db](app/db/init_db.py) | SQLite schema, data access, and crash recovery |
| [app/llm](app/llm/provider.py) | Mock and Groq provider implementations plus output validation |
| [app/pipeline](app/pipeline/worker.py) | Queue workers, deduplication, and retries |
| [frontend](frontend/index.html) | Vanilla HTML, CSS, JavaScript, and CSV parser |
| [tests](tests/test_api.py) | Python unit and integration tests |
| [docs](docs/MANUAL_DEMO.md) | Manual interview demo checklist |
| [data/sample.csv](data/sample.csv) | Sample catalogue listings for the upload demo |
| [DESIGN.md](DESIGN.md) | Architecture decisions and scaling answers |

## The prompt used for the real LLM

```text
You are a product catalogue enricher. Given a raw product listing, return STRICT JSON with exactly these keys:
- clean_title: a clean human-readable product title (string, 5-100 chars)
- category: exactly one of: Groceries, Beverages, Personal Care, Household, Electronics, Fashion, Home & Kitchen, Other
- brand: the brand name as a string, or null if you cannot determine it
- tags: up to 5 lowercase single-word strings describing the product
Return ONLY the JSON object. No markdown fences. No explanation.
```

The user message appends `Title: {raw_title}` and `Description: {raw_description or '(none)'}`.

## Assumptions made

1. **Content normalization:** dedup uses `lowercase(raw_title + " " + raw_description)` with whitespace collapse; the delimiter is one space per the [assignment PDF](CatalogIQ_Internship_Assignment.pdf), and the prompt's inline `|` was treated as a typo ([implementation](app/pipeline/dedup.py), [tests](tests/test_dedup.py)).
2. **Global persistent cache:** a product enriched in Job A can be a cache hit in Job B because cache keys are content hashes, not job IDs ([schema](app/db/init_db.py), [dedup manager](app/pipeline/dedup.py)).
3. **Terminal catalogue results:** `GET /api/products` returns `enriched`, `pending_review`, `failed`, and `approved`; `pending` and `in_progress` are represented through job progress ([route](app/api/routes.py)).
4. **SKU resubmission:** upserting an existing SKU resets it to `pending` and clears enrichment and approval fields, including when raw content changes ([job route](app/api/routes.py)).
5. **Cache-hit recovery:** `done` and `failed` are recalculated after a crash, but `cache_hits` retains its last persisted value ([recovery](app/db/init_db.py)).
6. **Follower failures:** in-flight duplicates inherit the leader's terminal failure rather than independently retrying ([dedup manager](app/pipeline/dedup.py), [pipeline tests](tests/test_dedup.py)).
7. **Page size:** values above 100 are clamped to 100 instead of rejected ([products route](app/api/routes.py)).
8. **Job IDs:** IDs use `j_` plus eight hexadecimal characters via `secrets.token_hex(4)` ([job route](app/api/routes.py)).

## Remaining deployment limits

- **Storage scale:** FTS5, keyset pagination, and category/SKU indexes are in place. SQLite is still a single-node database; multi-host production should move product/job state to a shared database such as PostgreSQL.
- **Distributed workers:** Redis Streams distributes jobs and the Redis cache/rate limiter coordinates provider usage. Semaphore and in-flight request coalescing remain process-local; Redis queue delivery is at least once, with SQLite job claims preventing duplicate execution.
- **Observability:** metrics are in-memory; there is no structured logging pipeline or Prometheus export ([metrics](app/core/metrics.py)).
- **Retry:** backoff is exponential from 200ms with jitter, not provider-rate-aware ([retry wrapper](app/pipeline/retry.py)).
- **Frontend build:** there is no bundler or minification; the plain files are served directly by design ([static serving](app/main.py), [frontend](frontend/index.html)).

## AI tools used

Antigravity (Gemini) was used for architecture and the backend pipeline (Steps 1–6). Codex (OpenAI CLI) was used for frontend polish, real-LLM integration, and test hardening (Steps 7–9). I manually reviewed the concurrency, deduplication, and retry code, wrote the atomicity argument in [DESIGN.md §1](DESIGN.md#1-architecture-how-does-a-job-move-from-upload-to-finished-products), and can explain the trade-offs. AI assistance was especially useful for the Groq model deprecation, CSV parser edge cases, and test fixture hermeticity.

## License / Acknowledgements

For evaluation purposes; no separate license has been declared.
