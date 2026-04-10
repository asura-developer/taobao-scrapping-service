from typing import Optional
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from services.category_service import (
    discover_categories,
    get_categories,
    get_category_tree,
    get_groups,
    get_subs_for_group,
    seed_categories,
    seed_all_platforms,
    PLATFORMS,
)
from utils.serializer import clean

router = APIRouter()

VALID_PLATFORMS = set(PLATFORMS)


def _validate_platform(platform: str) -> Optional[JSONResponse]:
    if platform not in VALID_PLATFORMS:
        return JSONResponse(
            {"success": False, "error": f"Invalid platform. Must be one of: {', '.join(sorted(VALID_PLATFORMS))}"},
            status_code=400,
        )
    return None


# ── Existing endpoints (backward-compatible) ──────────────────────────────────

@router.get("/")
async def list_categories(request: Request, platform: Optional[str] = None):
    """Get all stored categories (flat list)."""
    db = request.app.state.db
    categories = await get_categories(db, platform)
    return JSONResponse({"success": True, "data": clean(categories)})


@router.post("/discover/{platform}")
async def discover(request: Request, platform: str):
    """Crawl a platform to discover new categories."""
    err = _validate_platform(platform)
    if err:
        return err
    db = request.app.state.db
    categories = await discover_categories(db, platform)
    return JSONResponse({
        "success": True,
        "data": clean(categories),
        "message": f"Discovered {len(categories)} categories for {platform}",
    })


@router.post("/discover-all")
async def discover_all(request: Request):
    """Discover categories for all platforms."""
    db = request.app.state.db
    results = {}
    for platform in PLATFORMS:
        cats = await discover_categories(db, platform)
        results[platform] = len(cats)
    return JSONResponse({
        "success": True,
        "data": results,
        "message": f"Discovery complete: {results}",
    })


# ── New hierarchy endpoints ────────────────────────────────────────────────────

@router.get("/tree")
async def category_tree(request: Request, platform: str = "taobao"):
    """
    Return the full two-level category hierarchy for a platform.

    Response shape:
      [{groupId, nameZh, nameEn, subCategories: [{subId, nameZh, nameEn, platformCatId}]}]
    """
    err = _validate_platform(platform)
    if err:
        return err
    db = request.app.state.db
    tree = await get_category_tree(db, platform)
    return JSONResponse({"success": True, "data": clean(tree), "platform": platform})


@router.get("/groups")
async def list_groups(request: Request, platform: str = "taobao"):
    """Return only group-level (parent) categories for a platform."""
    err = _validate_platform(platform)
    if err:
        return err
    db = request.app.state.db
    groups = await get_groups(db, platform)
    return JSONResponse({"success": True, "data": clean(groups), "platform": platform})


@router.get("/groups/{group_id}/subs")
async def list_subs(request: Request, group_id: str, platform: str = "taobao"):
    """Return sub-categories for a specific group on a platform."""
    err = _validate_platform(platform)
    if err:
        return err
    db = request.app.state.db
    subs = await get_subs_for_group(db, platform, group_id)
    if not subs:
        return JSONResponse(
            {"success": False, "error": f"Group '{group_id}' not found for platform '{platform}'"},
            status_code=404,
        )
    return JSONResponse({"success": True, "data": clean(subs), "groupId": group_id, "platform": platform})


@router.post("/seed")
async def seed_all(request: Request):
    """Seed the full Taobao/Tmall/1688 category tree for all platforms."""
    db = request.app.state.db
    results = await seed_all_platforms(db)
    total = sum(results.values())
    return JSONResponse({
        "success": True,
        "data": results,
        "message": f"Seeded {total} category documents across all platforms",
    })


@router.post("/seed/{platform}")
async def seed_platform(request: Request, platform: str):
    """Seed the category tree for a single platform."""
    err = _validate_platform(platform)
    if err:
        return err
    db = request.app.state.db
    count = await seed_categories(db, platform)
    return JSONResponse({
        "success": True,
        "data": {"platform": platform, "count": count},
        "message": f"Seeded {count} category documents for {platform}",
    })
