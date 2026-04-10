from typing import Optional
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from services.retry_service import get_retry_stats, get_retry_queue, clear_exhausted
from utils.serializer import clean

router = APIRouter()


@router.get("/stats")
async def retry_stats(request: Request):
    db = request.app.state.db
    stats = await get_retry_stats(db)
    return JSONResponse({"success": True, "data": stats})


@router.get("/queue")
async def retry_queue(request: Request, status: Optional[str] = None, limit: int = 50):
    db = request.app.state.db
    items = await get_retry_queue(db, status, limit)
    return JSONResponse({"success": True, "data": clean(items)})


@router.delete("/exhausted")
async def clear_exhausted_items(request: Request):
    db = request.app.state.db
    count = await clear_exhausted(db)
    return JSONResponse({"success": True, "data": {"cleared": count}})
