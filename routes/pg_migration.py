import json
from fastapi import APIRouter, Request, HTTPException
from sse_starlette.sse import EventSourceResponse
from pydantic import BaseModel

from services.mongo_to_postgres_service import mongo_to_pg_service
from services.postgres_service import pg_service

router = APIRouter()


class RunBody(BaseModel):
    force: bool = False


async def _ensure_migration_dependencies(db) -> None:
    try:
        await db.command("ping")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MongoDB unavailable: {exc}") from exc

    try:
        await pg_service.test_connection()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"PostgreSQL unavailable: {exc}") from exc


@router.post("/run")
async def run_migration(request: Request, body: RunBody):
    db = request.app.state.db
    await _ensure_migration_dependencies(db)

    async def event_generator():
        try:
            async for event in mongo_to_pg_service.run_stream(db, force=body.force):
                yield {"data": json.dumps(event)}
        except Exception as e:
            yield {"data": json.dumps({"type": "error", "error": str(e)})}

    return EventSourceResponse(event_generator())


@router.get("/verify")
async def verify(request: Request):
    db = request.app.state.db
    await _ensure_migration_dependencies(db)
    await mongo_to_pg_service.apply_schema()
    await mongo_to_pg_service.load_platform_cache()
    result = await mongo_to_pg_service.verify(db)
    return {"success": True, "data": result}


@router.get("/status")
async def migration_status():
    try:
        rows = await pg_service.query("""
            SELECT status, COUNT(*) AS count, MAX(migrated_at) AS last_migrated_at
            FROM mongo_migration_log
            GROUP BY status
        """)
        summary = {"success": 0, "failed": 0, "last_run": None}
        for row in rows:
            summary[row["status"]] = int(row["count"])
            if not summary["last_run"] or row["last_migrated_at"] > summary["last_run"]:
                summary["last_run"] = str(row["last_migrated_at"])
        return {"success": True, "data": summary}
    except Exception as e:
        if "does not exist" in str(e):
            return {"success": True, "data": {
                "success": 0, "failed": 0, "last_run": None,
                "note": "Migration has not been run yet"
            }}
        raise


@router.get("/failed")
async def failed_items(limit: int = 50):
    rows = await pg_service.query(
        """SELECT mongo_item_id, error_message, migrated_at
           FROM mongo_migration_log
           WHERE status = 'failed'
           ORDER BY migrated_at DESC
           LIMIT $1""",
        limit
    )
    return {"success": True, "data": rows}


@router.post("/retry-failed")
async def retry_failed(request: Request):
    db = request.app.state.db
    await _ensure_migration_dependencies(db)

    async def event_generator():
        try:
            await pg_service.execute("DELETE FROM mongo_migration_log WHERE status = 'failed'")
            yield {"data": json.dumps({"type": "progress", "message": "Cleared failed log entries — retrying..."})}
            async for event in mongo_to_pg_service.run_stream(db, force=False):
                yield {"data": json.dumps(event)}
        except Exception as e:
            yield {"data": json.dumps({"type": "error", "error": str(e)})}

    return EventSourceResponse(event_generator())
