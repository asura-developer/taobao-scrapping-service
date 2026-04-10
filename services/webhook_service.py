"""
Webhook notification service.

Sends HTTP POST notifications when scrape jobs complete or fail.
Webhook URLs are configured via:
  - WEBHOOK_URL env var (single URL)
  - MongoDB webhooks collection (multiple URLs with filters)
"""

import os
import asyncio
import logging
from datetime import datetime, UTC
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
WEBHOOK_TIMEOUT = 10  # seconds


async def _send_webhook(url: str, payload: dict):
    """Send a POST request to a webhook URL."""
    try:
        async with httpx.AsyncClient(timeout=WEBHOOK_TIMEOUT) as client:
            response = await client.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            logger.info(f"Webhook sent to {url}: {response.status_code}")
            return response.status_code
    except Exception as e:
        logger.warning(f"Webhook failed for {url}: {e}")
        return None


async def notify_job_completed(db, job: dict):
    """Send notification when a scrape job completes."""
    payload = {
        "event": "job.completed",
        "timestamp": datetime.now(UTC).isoformat(),
        "job": {
            "jobId": job.get("jobId"),
            "platform": job.get("platform"),
            "searchType": job.get("searchType"),
            "status": job.get("status"),
            "results": job.get("results"),
            "searchParams": job.get("searchParams"),
            "startedAt": str(job.get("startedAt", "")),
            "completedAt": str(job.get("completedAt", "")),
        },
    }
    await _dispatch(db, payload, "job.completed")


async def notify_job_failed(db, job: dict, error: str):
    """Send notification when a scrape job fails."""
    payload = {
        "event": "job.failed",
        "timestamp": datetime.now(UTC).isoformat(),
        "job": {
            "jobId": job.get("jobId"),
            "platform": job.get("platform"),
            "status": "failed",
            "error": error,
        },
    }
    await _dispatch(db, payload, "job.failed")


async def notify_price_drop(db, item_id: str, old_price: float, new_price: float, platform: str):
    """Send notification for significant price drops."""
    change_pct = ((new_price - old_price) / old_price) * 100 if old_price else 0
    payload = {
        "event": "price.drop",
        "timestamp": datetime.now(UTC).isoformat(),
        "data": {
            "itemId": item_id,
            "platform": platform,
            "oldPrice": old_price,
            "newPrice": new_price,
            "changePct": round(change_pct, 2),
        },
    }
    await _dispatch(db, payload, "price.drop")


async def _dispatch(db, payload: dict, event_type: str):
    """Send to all configured webhook URLs."""
    urls = []

    # Global webhook from env
    if WEBHOOK_URL:
        urls.append(WEBHOOK_URL)

    # Per-webhook from DB
    try:
        webhooks = await db.webhooks.find(
            {"$or": [{"events": event_type}, {"events": "*"}], "enabled": True}
        ).to_list(length=50)
        for wh in webhooks:
            if wh.get("url"):
                urls.append(wh["url"])
    except Exception as e:
        logger.warning(f"Failed to load webhooks from DB: {e}")

    if not urls:
        return

    # Send all webhooks concurrently
    tasks = [_send_webhook(url, payload) for url in set(urls)]
    await asyncio.gather(*tasks, return_exceptions=True)


# ── Webhook CRUD ──────────────────────────────────────────────────────────

async def create_webhook(db, data: dict) -> dict:
    import uuid
    doc = {
        "webhookId": str(uuid.uuid4()),
        "name": data["name"],
        "url": data["url"],
        "events": data.get("events", ["*"]),  # ["job.completed", "job.failed", "price.drop"] or ["*"]
        "enabled": data.get("enabled", True),
        "createdAt": datetime.now(UTC),
    }
    await db.webhooks.insert_one(doc)
    return {k: v for k, v in doc.items() if k != "_id"}


async def list_webhooks(db) -> list:
    return await db.webhooks.find({}, {"_id": 0}).to_list(length=50)


async def delete_webhook(db, webhook_id: str) -> bool:
    result = await db.webhooks.delete_one({"webhookId": webhook_id})
    return result.deleted_count > 0


async def toggle_webhook(db, webhook_id: str) -> Optional[bool]:
    doc = await db.webhooks.find_one({"webhookId": webhook_id})
    if not doc:
        return None
    new_state = not doc.get("enabled", True)
    await db.webhooks.update_one(
        {"webhookId": webhook_id}, {"$set": {"enabled": new_state}}
    )
    return new_state
