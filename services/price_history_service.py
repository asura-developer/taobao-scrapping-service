"""
Price history tracking service.

Records a price snapshot each time a product is scraped or updated.
Provides APIs to query price trends over time.
"""

from datetime import datetime, UTC
from typing import Optional


async def record_price_snapshot(db, product: dict):
    """
    Record a price snapshot for a product.
    Called during bulk save and detail scraping.
    """
    item_id = product.get("itemId")
    if not item_id:
        return

    # Extract price from listing or detail
    detail = product.get("detailedInfo") or {}
    price_raw = detail.get("price") or product.get("price")
    price_usd = detail.get("priceUsd")
    original_price = detail.get("originalPrice")

    # Try to parse numeric price
    price = None
    if price_raw is not None:
        try:
            price = float(str(price_raw).replace(",", ""))
        except (ValueError, TypeError):
            pass

    if price is None:
        return  # No price to record

    snapshot = {
        "itemId": item_id,
        "price": price,
        "priceUsd": price_usd,
        "originalPrice": original_price,
        "platform": product.get("platform"),
        "currency": _detect_currency(product),
        "source": "detail" if detail.get("price") else "listing",
        "recordedAt": datetime.now(UTC),
    }

    await db.price_history.insert_one(snapshot)


async def record_price_snapshots_bulk(db, products: list):
    """Record price snapshots for a batch of products."""
    docs = []
    now = datetime.now(UTC)

    for product in products:
        item_id = product.get("itemId")
        if not item_id:
            continue

        detail = product.get("detailedInfo") or {}
        price_raw = detail.get("price") or product.get("price")
        price_usd = detail.get("priceUsd")

        price = None
        if price_raw is not None:
            try:
                price = float(str(price_raw).replace(",", ""))
            except (ValueError, TypeError):
                pass

        if price is None:
            continue

        docs.append({
            "itemId": item_id,
            "price": price,
            "priceUsd": price_usd,
            "originalPrice": detail.get("originalPrice"),
            "platform": product.get("platform"),
            "currency": _detect_currency(product),
            "source": "detail" if detail.get("price") else "listing",
            "recordedAt": now,
        })

    if docs:
        await db.price_history.insert_many(docs, ordered=False)


async def get_price_history(
    db, item_id: str, limit: int = 100
) -> list[dict]:
    """Get price history for a single product, newest first."""
    cursor = db.price_history.find(
        {"itemId": item_id}, {"_id": 0}
    ).sort("recordedAt", -1).limit(limit)
    return await cursor.to_list(length=limit)


async def get_price_changes(
    db,
    platform: Optional[str] = None,
    min_change_pct: float = 5.0,
    limit: int = 50,
) -> list[dict]:
    """
    Find products with significant price changes.
    Compares latest price to previous price snapshot.
    """
    pipeline = []

    if platform:
        pipeline.append({"$match": {"platform": platform}})

    pipeline.extend([
        {"$sort": {"itemId": 1, "recordedAt": -1}},
        # Only keep the 2 most recent snapshots per product (avoids $push of all snapshots)
        {"$group": {
            "_id": "$itemId",
            "latestPrice": {"$first": "$price"},
            "latestPriceUsd": {"$first": "$priceUsd"},
            "latestRecordedAt": {"$first": "$recordedAt"},
            "latestPlatform": {"$first": "$platform"},
            "allPrices": {"$push": "$price"},
        }},
        # Only products with at least 2 snapshots
        {"$match": {"allPrices.1": {"$exists": True}}},
        {"$project": {
            "itemId": "$_id",
            "latest": {
                "price": "$latestPrice",
                "priceUsd": "$latestPriceUsd",
                "recordedAt": "$latestRecordedAt",
                "platform": "$latestPlatform",
            },
            "previous": {
                "price": {"$arrayElemAt": ["$allPrices", 1]},
            },
            "snapshotCount": {"$size": "$allPrices"},
        }},
        {"$addFields": {
            "priceChange": {"$subtract": ["$latest.price", "$previous.price"]},
            "changePct": {
                "$multiply": [
                    {"$divide": [
                        {"$subtract": ["$latest.price", "$previous.price"]},
                        {"$cond": [{"$eq": ["$previous.price", 0]}, 1, "$previous.price"]},
                    ]},
                    100,
                ]
            },
        }},
        {"$match": {
            "$or": [
                {"changePct": {"$gte": min_change_pct}},
                {"changePct": {"$lte": -min_change_pct}},
            ]
        }},
        {"$sort": {"changePct": -1}},
        {"$limit": limit},
    ])

    return await db.price_history.aggregate(pipeline).to_list(length=limit)


async def get_price_stats(db) -> dict:
    """Get overall price tracking statistics."""
    total_snapshots = await db.price_history.count_documents({})
    tracked_products = 0
    if total_snapshots > 0:
        pipeline = [{"$group": {"_id": "$itemId"}}, {"$count": "total"}]
        result = await db.price_history.aggregate(pipeline).to_list(1)
        tracked_products = result[0]["total"] if result else 0

    return {
        "totalSnapshots": total_snapshots,
        "trackedProducts": tracked_products,
    }


async def ensure_price_history_indexes(db):
    """Create indexes for price_history collection."""
    col = db.price_history
    await col.create_index([("itemId", 1), ("recordedAt", -1)])
    await col.create_index("recordedAt")
    await col.create_index("platform")


def _detect_currency(product: dict) -> str:
    """Detect currency from platform. Taobao/Tmall/1688 are always CNY."""
    platform = product.get("platform", "").lower()
    return {
        "taobao": "CNY", "tmall": "CNY", "1688": "CNY", "alibaba": "USD",
    }.get(platform, "CNY")
