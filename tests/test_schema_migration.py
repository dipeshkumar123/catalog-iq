import sqlite3
import pytest
from app.db.init_db import init_db
from app.db.session import DatabaseManager


@pytest.mark.asyncio
async def test_legacy_database_migrates_review_columns_and_fts(tmp_path):
    path = tmp_path / "legacy.db"
    conn = sqlite3.connect(path)
    conn.executescript("""
    CREATE TABLE jobs(id TEXT PRIMARY KEY,status TEXT,total INTEGER,done INTEGER DEFAULT 0,failed INTEGER DEFAULT 0,cache_hits INTEGER DEFAULT 0,created_at TEXT DEFAULT CURRENT_TIMESTAMP,started_at TEXT,finished_at TEXT);
    INSERT INTO jobs(id,status,total) VALUES('old-job','completed',1);
    CREATE TABLE products(
      sku TEXT PRIMARY KEY,job_id TEXT NOT NULL,raw_title TEXT NOT NULL,raw_description TEXT,clean_title TEXT,
      category TEXT,brand TEXT,tags TEXT,status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','in_progress','enriched','failed','approved')),
      error TEXT,attempts INTEGER NOT NULL DEFAULT 0,content_hash TEXT NOT NULL,updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(job_id) REFERENCES jobs(id));
    INSERT INTO products(sku,job_id,raw_title,clean_title,category,brand,tags,status,content_hash)
      VALUES('OLD-1','old-job','Sony headphones','Sony headphones','Electronics','Sony','["wireless"]','enriched','hash');
    CREATE TABLE dedup_cache(content_hash TEXT PRIMARY KEY,clean_title TEXT NOT NULL,category TEXT NOT NULL,brand TEXT,tags TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP);
    """)
    conn.close()
    manager = DatabaseManager(str(path))
    await init_db(manager)
    row = await manager.fetch_one("SELECT status,confidence,review_reasons FROM products WHERE sku='OLD-1'")
    assert row == {"status":"enriched", "confidence":None, "review_reasons":"[]"}
    await manager.execute_write("UPDATE products SET status='pending_review' WHERE sku='OLD-1'")
    found = await manager.fetch_one("SELECT p.sku FROM products p JOIN products_fts f ON f.rowid=p.rowid WHERE products_fts MATCH 'Sony'")
    assert found["sku"] == "OLD-1"
    await manager.close()
