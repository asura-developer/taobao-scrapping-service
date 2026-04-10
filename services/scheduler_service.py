"""
Cron-based job scheduler for recurring scrape jobs.

Stores scheduled tasks in MongoDB (collection: scheduled_jobs).
Uses asyncio background loop to check and trigger jobs on schedule.
"""

import asyncio
import uuid
import logging
from datetime import datetime, UTC
from typing import Optional

logger = logging.getLogger(__name__)


def _matches_cron_field(value: int, field: str) -> bool:
    """Check if a value matches a cron field expression (*, N, */N, N-M)."""
    if field == "*":
        return True
    if field.startswith("*/"):
        step = int(field[2:])
        return value % step == 0
    if "-" in field:
        low, high = field.split("-", 1)
        return int(low) <= value <= int(high)
    if "," in field:
        return value in [int(x) for x in field.split(",")]
    return value == int(field)


def matches_cron(now: datetime, cron: str) -> bool:
    """
    Check if current time matches a cron expression.
    Format: minute hour day_of_month month day_of_week
    Example: "0 */6 * * *" = every 6 hours
    """
    parts = cron.strip().split()
    if len(parts) != 5:
        return False

    minute, hour, dom, month, dow = parts
    return (
        _matches_cron_field(now.minute, minute)
        and _matches_cron_field(now.hour, hour)
        and _matches_cron_field(now.day, dom)
        and _matches_cron_field(now.month, month)
        and _matches_cron_field(now.weekday(), dow)  # 0=Monday
    )


class SchedulerService:
    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self, db):
        """Start the scheduler background loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(db))
        logger.info("Scheduler started")

    async def stop(self):
        """Stop the scheduler."""
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None
        logger.info("Scheduler stopped")

    async def _loop(self, db):
        """Check for scheduled jobs every 60 seconds."""
        from services.scraper_service import scraper_service

        while self._running:
            try:
                now = datetime.now(UTC)
                schedules = await db.scheduled_jobs.find(
                    {"enabled": True}
                ).to_list(length=100)

                for schedule in schedules:
                    if matches_cron(now, schedule.get("cron", "")):
                        # Check if already ran this minute
                        last_run = schedule.get("lastRunAt")
                        if last_run and (now - last_run).total_seconds() < 120:
                            continue

                        logger.info(f"Triggering scheduled job: {schedule['name']}")
                        try:
                            params = schedule.get("params", {})
                            result = await scraper_service.start_job(db, params)

                            await db.scheduled_jobs.update_one(
                                {"_id": schedule["_id"]},
                                {"$set": {
                                    "lastRunAt": now,
                                    "lastJobId": result.get("jobId"),
                                    "runCount": (schedule.get("runCount", 0) or 0) + 1,
                                    "lastError": None,
                                }}
                            )
                        except Exception as e:
                            logger.error(f"Scheduled job failed: {e}")
                            await db.scheduled_jobs.update_one(
                                {"_id": schedule["_id"]},
                                {"$set": {
                                    "lastRunAt": now,
                                    "lastError": str(e),
                                }}
                            )

            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")

            await asyncio.sleep(60)

    async def create_schedule(self, db, data: dict) -> dict:
        """Create a new scheduled job."""
        schedule_id = str(uuid.uuid4())
        doc = {
            "scheduleId": schedule_id,
            "name": data["name"],
            "cron": data["cron"],
            "params": data["params"],
            "enabled": data.get("enabled", True),
            "runCount": 0,
            "lastRunAt": None,
            "lastJobId": None,
            "lastError": None,
            "createdAt": datetime.now(UTC),
        }
        await db.scheduled_jobs.insert_one(doc)
        return {k: v for k, v in doc.items() if k != "_id"}

    async def list_schedules(self, db) -> list:
        cursor = db.scheduled_jobs.find({}, {"_id": 0}).sort("createdAt", -1)
        return await cursor.to_list(length=100)

    async def update_schedule(self, db, schedule_id: str, data: dict) -> bool:
        update_fields = {}
        for field in ("name", "cron", "params", "enabled"):
            if field in data:
                update_fields[field] = data[field]
        if not update_fields:
            return False
        result = await db.scheduled_jobs.update_one(
            {"scheduleId": schedule_id},
            {"$set": update_fields},
        )
        return result.modified_count > 0

    async def delete_schedule(self, db, schedule_id: str) -> bool:
        result = await db.scheduled_jobs.delete_one({"scheduleId": schedule_id})
        return result.deleted_count > 0

    async def toggle_schedule(self, db, schedule_id: str) -> Optional[bool]:
        doc = await db.scheduled_jobs.find_one({"scheduleId": schedule_id})
        if not doc:
            return None
        new_state = not doc.get("enabled", True)
        await db.scheduled_jobs.update_one(
            {"scheduleId": schedule_id},
            {"$set": {"enabled": new_state}},
        )
        return new_state


async def ensure_scheduler_indexes(db):
    """Create indexes for scheduled_jobs collection."""
    await db.scheduled_jobs.create_index("enabled")
    await db.scheduled_jobs.create_index("scheduleId", unique=True)


scheduler_service = SchedulerService()
