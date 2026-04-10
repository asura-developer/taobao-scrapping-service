from typing import Optional
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from services.price_history_service import (
    get_price_history, get_price_changes, get_price_stats,
)
from utils.serializer import clean

router = APIRouter()


@router.get("/product/{item_id}")
async def product_price_history(request: Request, item_id: str, limit: int = 100):
    """Get price history for a specific product."""
    db = request.app.state.db
    history = await get_price_history(db, item_id, limit)
    if not history:
        raise HTTPException(404, "No price history found for this product")
    return JSONResponse({"success": True, "data": clean(history)})


@router.get("/changes")
async def price_changes(
    request: Request,
    platform: Optional[str] = None,
    minChangePct: float = 5.0,
    limit: int = 50,
):
    """Find products with significant price changes."""
    db = request.app.state.db
    changes = await get_price_changes(db, platform, minChangePct, limit)
    return JSONResponse({"success": True, "data": clean(changes)})


@router.get("/stats")
async def price_stats(request: Request):
    """Get price tracking statistics."""
    db = request.app.state.db
    stats = await get_price_stats(db)
    return JSONResponse({"success": True, "data": stats})
