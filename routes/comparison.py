from fastapi import APIRouter, Request, HTTPException, Query
from fastapi.responses import JSONResponse
from services.comparison_service import find_similar_products, compare_prices_across_platforms
from utils.serializer import clean

router = APIRouter()


@router.get("/similar/{item_id}")
async def similar_products(
    request: Request,
    item_id: str,
    minSimilarity: float = 0.3,
    limit: int = 20,
):
    """Find similar products across all platforms."""
    db = request.app.state.db
    results = await find_similar_products(db, item_id, minSimilarity, limit)
    return JSONResponse({"success": True, "data": clean(results)})


@router.get("/prices")
async def compare_prices(
    request: Request,
    keyword: str = Query(..., description="Product keyword to compare"),
    limit: int = 10,
):
    """Compare prices for a keyword across Taobao, Tmall, and 1688."""
    db = request.app.state.db
    result = await compare_prices_across_platforms(db, keyword, limit)
    return JSONResponse({"success": True, "data": clean(result)})
