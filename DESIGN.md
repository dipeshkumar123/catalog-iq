# CatalogIQ — System Design & Architectural Decisions (DESIGN.md)
This document details the architectural choices, trade-offs, scaling strategies, and design decisions behind CatalogIQ.
---
## 1. Architecture: Flow, Parallelism, and Concurrency Choice
### End-to-End Job Lifecycle
```mermaid
flowchart LR
    Client([Web UI / Client])
    DB[(SQLite Database)]
    
    subgraph API_Layer [API Layer]
        Endpoint[POST /api/jobs]
        Queue[[Async Job Queue]]
    end
    
    subgraph Pipeline [Background Pipeline]
        Worker[Job Worker]
        Dedup{Dedup Check}
        LLM[LLM Provider]
    end
    
    Client -->|1. Upload CSV| Endpoint
    Endpoint -->|2. Persist Job| DB
    Endpoint -.->|3. Return 202 under 1s| Client
    Endpoint -->|4. Enqueue job_id| Queue
    
    Queue -->|5. Dispatch| Worker
    Worker -->|6. Check Hash| Dedup
    
    Dedup -->|Hit / Follower| DB
    Dedup -->|Leader| LLM
    
    LLM -->|7. Retry + Semaphore| Worker
    LLM -->|8. Save Result| DB
```
### Flow Walkthrough
1. **Ingestion & Acknowledgment (< 1s):** The client sends a batch of products to `POST /api/jobs`. The API validates SKUs and raw titles, inserts the job record and individual product records with `status = 'pending'`, and pushes the `job_id` into an internal `asyncio.Queue`. The API returns HTTP `202 Accepted` with the job payload immediately within milliseconds.
2. **Worker Dispatch:** A background worker task pulls the `job_id` from the queue, marks the job `running`, and fetches pending product records. Products are dispatched through a controlled worker pool.
3. **Deduplication Check:** The content hash is checked against the persistent `dedup_cache` table. If cached, the item is enriched without calling the LLM and `cache_hits` increments. If in-flight, it joins the leader's `asyncio.Future`.
4. **Rate-Controlled LLM Execution:** The leader acquires an `asyncio.Semaphore` permit. It calls the LLM provider. If the call fails or validation fails (invalid JSON or non-whitelisted category), exponential backoff retries up to 3 times (4 attempts total). On terminal failure, product is marked `failed`.
5. **Persistence & Notification:** Results are written to SQLite, persistent cache is updated, and the leader's `asyncio.Future` is resolved, unblocking all follower tasks.
### Concurrency Choice: Why `asyncio` Wins
- **I/O-Bound Profile:** LLM interactions spend >99% of time awaiting HTTP socket I/O (or simulated `asyncio.sleep`). `asyncio` handles thousands of concurrent coroutines with minimal memory footprint compared to OS threads (which allocate MBs of stack space each).
- **Single-Threaded Atomicity:** Because Python `asyncio` is single-threaded and cooperatively scheduled, in-memory structures like `in_flight: dict[str, asyncio.Future]` and high-water mark counters can be checked and updated between `await` calls without OS mutexes or race conditions.
- **Why Threads Were Rejected:** Threading introduces preemptive context switching, requiring `threading.Lock` around cache lookups and metric increments. Multithreaded SQLite access is notoriously prone to `sqlite3.OperationalError: database is locked` without complex connection pooling.
- **Why Processes Were Rejected:** Multiprocessing is suited for CPU-bound tasks. Distributing I/O across processes requires IPC, shared memory managers, or external brokers (Redis) to synchronize the global semaphore and in-flight futures, adding immense unnecessary complexity.
---
## 2. Crash Recovery
### The Scenario
The server abruptly halts (e.g., kill signal, power interruption, kernel panic) halfway through a 10,000-item job where 4,000 items are enriched, 100 failed, 50 were mid-call, and 5,850 are pending.
### The Recovery Mechanism
1. **Durable Per-Item State in SQLite WAL:**
   - Every product's state transition is committed to SQLite with Write-Ahead Logging (`PRAGMA journal_mode=WAL;`).
   - The 4,000 completed items have `status = 'enriched'` and their values stored in `products` and `dedup_cache`.
   - The 100 failed items have `status = 'failed'` with error messages.
2. **Startup Initialization Hook (Lifespan):**
   When the application restarts, before accepting new traffic, it runs a recovery pass:
   ```sql
   -- Find interrupted jobs
   SELECT id FROM jobs WHERE status IN ('queued', 'running');
   -- Reset interrupted mid-flight products back to pending
   UPDATE products SET status = 'pending' WHERE job_id = :job_id AND status = 'in_progress';
   -- Resynchronize counts directly from truth
   UPDATE jobs SET 
       done = (SELECT COUNT(*) FROM products WHERE job_id = :job_id AND status = 'enriched'),
       failed = (SELECT COUNT(*) FROM products WHERE job_id = :job_id AND status = 'failed')
   WHERE id = :job_id;
   ```
3. **Resuming Without Duplicate Work:**
   The background pipeline selects only products where `job_id = :job_id AND status = 'pending'`. The 4,100 already-finished items are completely bypassed. Items that were interrupted mid-flight simply pick up from attempt 0, hitting `dedup_cache` if identical content was already committed by another item.

---

## 3. Scale and Cost: 1 Million Listings / Day with Rate-Limited LLM
Handling $1,000,000$ listings/day equates to an average of $\sim 11.6$ requests/second, with peak bursts likely hitting $50\text{–}100\text{ req/s}$. If the LLM provider has strict rate limits (e.g., 60 RPM or 5,000 RPM) and costs per token, we deploy a 5-layer scaling strategy:
### Strategy & Architecture
1. **Batching Multiple Products per Prompt:**
   - Instead of 1 LLM request per product, batch 10–25 products into a single structured prompt requesting a JSON array of results: `{"results": [{"sku": "...", "clean_title": "..."}, ...]}`.
   - **Benefit:** Reduces HTTP overhead, cuts input token repetition (system prompt shared across 25 items), and reduces required RPM by up to $25\times$ (transforming 1M calls into 40,000 calls).
2. **Persistent Multi-Tier Caching:**
   - E-commerce catalogues exhibit heavy Pareto distribution (20% of catalog items represent 80% of volume). By normalizing listings (`lower()`, whitespace collapse, token sorting) and caching in Redis/PostgreSQL, we intercept $30\%\text{--}50\%$ of queries before they ever reach the LLM.
3. **Leaky Bucket / Token Bucket Distributed Rate Limiter:**
   - Replace the in-process semaphore with a distributed rate limiter (e.g. Redis sliding-window token bucket) matching the LLM provider's tier limits (e.g., TPM and RPM ceilings).
4. **Multi-Provider Fallback & Tiering:**
   - Primary: High-speed, cost-effective provider (e.g., Groq / Llama 3.2 3B).
   - Secondary / Fallback: If primary returns 429 (Rate Limit) or 503 (Overloaded), automatically fall back to Gemini Flash or an alternative provider.
   - For simple listings (clear title, recognizable brand), use smaller/cheaper models; route complex listings to larger models.
5. **Horizontal Worker Pool:**
   - Decouple the ingestion API from workers using an external message queue (SQS, RabbitMQ, or Redis Streams). Workers pull batches based on rate-limit availability.

---

## 4. API Performance: 5 Million Products
At 5 million products, naive queries like `SELECT * FROM products WHERE raw_title LIKE '%amul%' OFFSET 100000 LIMIT 20;` will cause full table scans, high disk I/O, and seconds of latency.
### Bottlenecks and Solutions:
| Problem | Root Cause | Solution |
| :--- | :--- | :--- |
| **Substring Search `q=%...%`** | `LIKE '%term%'` cannot use standard B-Tree indexes, requiring a 5M row table scan. | **SQLite FTS5 (Full-Text Search):** Create an `fts5` virtual table indexing `clean_title`, `raw_title`, `brand`, and `tags`. FTS5 uses an inverted index delivering millisecond BM25 keyword searches. |
| **Deep Offset Pagination (`OFFSET 100000`)** | SQLite must scan and discard 100,000 rows before returning 20 rows. | **Cursor-Based / Keyset Pagination:** Replace `OFFSET` with `WHERE sku > :last_seen_sku ORDER BY sku ASC LIMIT 20`. This uses the primary key index directly, maintaining $O(\log N)$ performance regardless of depth. |
| **Category & Filtering Indexes** | Filtering by `category` on unindexed columns forces table scans. | Create composite indexes: `CREATE INDEX idx_products_cat_sku ON products(category, sku);`. |
| **Repeated Search Queries** | Popular searches (e.g., "iphone", "milk") repeatedly querying the DB. | **Response Caching:** Cache search results in an in-memory LRU or Redis cache with a short TTL (e.g. 60 seconds). |

---

## 5. Quality Control: Hallucination & Category Misclassification
LLMs can hallucinate non-existent brands or miscategorize items (e.g., placing "almond milk shampoo" under `Beverages` instead of `Personal Care`).
### Multi-Stage Validation & Human-in-the-Loop Workflow
```mermaid
flowchart TD
    Raw[Raw Listing] --> LLM[LLM Output]
    LLM --> V1{Schema & Strict Enum Check}
    V1 -->|Fail| R1[Retry Attempt]
    V1 -->|Pass| V2{Heuristic Quality Rules}
    
    subgraph Quality Guards
        V2 -->|Brand not in raw text & not known dictionary| FlagBrand[Flag: Suspicious Brand]
        V2 -->|Contradictory Keywords detected| FlagCat[Flag: Category Ambiguity]
        V2 -->|Logprob / Confidence Low| FlagConf[Flag: Low Confidence]
    end
    
    FlagBrand --> ReviewQueue[Mark status='pending_review']
    FlagCat --> ReviewQueue
    FlagConf --> ReviewQueue
    
    V2 -->|Pass all heuristics| AutoApprove[Mark status='enriched']
    ReviewQueue --> HumanDashboard[Human Review UI: Side-by-Side Diff]
    HumanDashboard -->|Approve / Edit| Approved[Mark status='approved']
```
### Specific Quality Verification Techniques:
1. **Brand Extraction Grounding Rule:**
   - Brand validation: If the LLM returns `brand = "Sony"`, check whether `"sony"` is a substring or token of `raw_title` or `raw_description`. If the LLM generates a brand that never appeared anywhere in the seller's input, flag it for human review.
2. **Negative Keyword Matrix for Categories:**
   - Maintain category conflict heuristics (e.g., if category is `Beverages` but `raw_title` contains `"shampoo"`, `"lotion"`, or `"conditioner"`, flag as conflict).
3. **Confidence Scoring:**
   - In real LLM providers supporting logprobs or structured output constraints (JSON schema enforcement), evaluate confidence scores. Products falling below 0.85 confidence are automatically routed to the human review UI (`status = 'pending_review'`).
4. **Human Review Dashboard (Side-by-Side):**
   - The UI surfaces raw listings next to LLM output, with highlighted diffs and one-click approvals or inline corrections (`PATCH /api/products/<sku>`), setting status to `approved`.