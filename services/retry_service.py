"""
Retry queue with exponential backoff for failed detail scrapes.

Failed scrapes are queued in MongoDB (collection: retry_queue) with
increasing delays between retries. Products are automatically retried
by a background worker.
"""

import asyncio
import logging
import math
from datetime import datetime, timedelta, UTC
from typing import Optional

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 5
BASE_DELAY_SECONDS = 60       # 1 minute for first retry
MAX_DELAY_SECONDS = 3600      # 1 hour max delay
BACKOFF_FACTOR = 2             # Exponential factor


def _next_retry_delay(attempt: int) -> int:
    """Calculate delay with exponential backoff + jitter."""
    import random
    delay = min(BASE_DELAY_SECONDS * (BACKOFF_FACTOR ** attempt), MAX_DELAY_SECONDS)
    jitter = random.uniform(0, delay * 0.2)
    return int(delay + jitter)


async def enqueue_retry(db, item_id: str, platform: str, language: str, error: str):
    """Add a failed scrape to the retry queue."""
    existing = await db.retry_queue.find_one({"itemId": item_id})

    if existing:
        attempt = (existing.get("attempt", 0) or 0) + 1
        if attempt >= MAX_RETRIES:
            # Mark as permanently failed
            await db.retry_queue.update_one(
                {"itemId": item_id},
                {"$set": {
                    "status": "exhausted",
                    "attempt": attempt,
                    "lastError": error,
                    "updatedAt": datetime.now(UTC),
                }}
            )
            logger.info(f"Retry exhausted for {item_id} after {attempt} attempts")
            return

        delay = _next_retry_delay(attempt)
        next_retry = datetime.now(UTC) + timedelta(seconds=delay)
        await db.retry_queue.update_one(
            {"itemId": item_id},
            {"$set": {
                "status": "pending",
                "attempt": attempt,
                "nextRetryAt": next_retry,
                "lastError": error,
                "updatedAt": datetime.now(UTC),
            }}
        )
    else:
        delay = _next_retry_delay(0)
        await db.retry_queue.insert_one({
            "itemId": item_id,
            "platform": platform,
            "language": language,
            "status": "pending",  # pending, processing, exhausted
            "attempt": 0,
            "nextRetryAt": datetime.now(UTC) + timedelta(seconds=delay),
            "lastError": error,
            "createdAt": datetime.now(UTC),
            "updatedAt": datetime.now(UTC),
        })


async def get_ready_retries(db, limit: int = 10) -> list:
    """Get items ready to be retried (past their nextRetryAt time)."""
    now = datetime.now(UTC)
    cursor = db.retry_queue.find(
        {"status": "pending", "nextRetryAt": {"$lte": now}},
        {"_id": 0},
    ).sort("nextRetryAt", 1).limit(limit)
    return await cursor.to_list(length=limit)


async def mark_processing(db, item_id: str):
    await db.retry_queue.update_one(
        {"itemId": item_id},
        {"$set": {"status": "processing", "updatedAt": datetime.now(UTC)}}
    )


async def mark_success(db, item_id: str):
    """Remove from retry queue on successful scrape."""
    await db.retry_queue.delete_one({"itemId": item_id})


async def mark_failed(db, item_id: str, error: str):
    """Re-enqueue with incremented attempt count."""
    doc = await db.retry_queue.find_one({"itemId": item_id})
    if doc:
        await enqueue_retry(db, item_id, doc.get("platform", ""), doc.get("language", "en"), error)


async def get_retry_stats(db) -> dict:
    """Get retry queue statistics."""
    pipeline = [
        {"$group": {
            "_id": "$status",
            "count": {"$sum": 1},
        }}
    ]
    status_counts = await db.retry_queue.aggregate(pipeline).to_list(length=10)
    stats = {s["_id"]: s["count"] for s in status_counts}
    total = sum(stats.values())

    return {
        "total": total,
        "pending": stats.get("pending", 0),
        "processing": stats.get("processing", 0),
        "exhausted": stats.get("exhausted", 0),
    }


async def get_retry_queue(db, status: Optional[str] = None, limit: int = 50) -> list:
    filt = {}
    if status:
        filt["status"] = status
    cursor = db.retry_queue.find(filt, {"_id": 0}).sort("nextRetryAt", 1).limit(limit)
    return await cursor.to_list(length=limit)


async def clear_exhausted(db) -> int:
    """Clear all exhausted items from the retry queue."""
    result = await db.retry_queue.delete_many({"status": "exhausted"})
    return result.deleted_count


async def ensure_retry_indexes(db):
    col = db.retry_queue
    await col.create_index([("status", 1), ("nextRetryAt", 1)])
    await col.create_index("itemId", unique=True)


class RetryWorker:
    """Background worker that processes the retry queue."""

    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self, db):
        if self._running:
            return
        # Recover items stuck in "processing" (from a previous crash)
        stale_cutoff = datetime.now(UTC) - timedelta(minutes=10)
        result = await db.retry_queue.update_many(
            {"status": "processing", "updatedAt": {"$lt": stale_cutoff}},
            {"$set": {"status": "pending", "updatedAt": datetime.now(UTC)}},
        )
        if result.modified_count:
            logger.info(f"Recovered {result.modified_count} stale retry items from 'processing' state")
        self._running = True
        self._task = asyncio.create_task(self._loop(db))
        logger.info("Retry worker started")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

    async def _loop(self, db):
        from services.scraper_service import scraper_service

        while self._running:
            try:
                items = await get_ready_retries(db, limit=5)
                for item in items:
                    item_id = item["itemId"]
                    platform = item.get("platform", "taobao")
                    language = item.get("language", "en")

                    product = await db.products.find_one({"itemId": item_id}, {"_id": 0})
                    if not product:
                        await mark_success(db, item_id)
                        continue

                    await mark_processing(db, item_id)
                    try:
                        details = await scraper_service._scrape_product_detail(product, platform, language)
                        if details and details.dataQuality.completeness >= 50:
                            import dataclasses
                            await db.products.update_one(
                                {"itemId": item_id},
                                {"$set": {
                                    "detailedInfo": dataclasses.asdict(details),
                                    "detailsScraped": True,
                                    "extractionQuality": details.dataQuality.completeness,
                                    "updatedAt": datetime.now(UTC),
                                }}
                            )
                            await mark_success(db, item_id)
                            logger.info(f"Retry success: {item_id} (quality={details.dataQuality.completeness}%)")
                        else:
                            await mark_failed(db, item_id, "Low quality or no data")
                    except Exception as e:
                        await mark_failed(db, item_id, str(e))
                        logger.warning(f"Retry failed: {item_id} -> {e}")

                    # Delay between retries
                    await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Retry worker error: {e}")

            await asyncio.sleep(30)  # Check queue every 30 seconds


retry_worker = RetryWorker()
