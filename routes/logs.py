from typing import Optional
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from services.logging_service import get_recent_logs, get_log_stats

router = APIRouter()


@router.get("/")
async def recent_logs(limit: int = 100, level: Optional[str] = None):
    """Get recent log entries."""
    logs = get_recent_logs(limit, level)
    return JSONResponse({"success": True, "data": logs})


@router.get("/stats")
async def log_stats():
    """Get logging statistics."""
    return JSONResponse({"success": True, "data": get_log_stats()})
