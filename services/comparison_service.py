"""
Cross-platform price comparison service.

Finds similar products across Taobao, Tmall, and 1688 and compares prices.
Uses title text similarity to match products across platforms.
"""

import re
from typing import Optional


def _normalize_title(title: str) -> str:
    """Normalize title for comparison — remove noise, lowercase."""
    if not title:
        return ""
    # Remove common noise: special chars, platform-specific tags
    title = re.sub(r'[\[\]【】「」『』（）(){}《》<>]', ' ', title)
    title = re.sub(r'\s+', ' ', title).strip().lower()
    return title


def _title_similarity(a: str, b: str) -> float:
    """Simple Jaccard similarity on word sets."""
    if not a or not b:
        return 0.0
    words_a = set(_normalize_title(a).split())
    words_b = set(_normalize_title(b).split())
    if not words_a or not words_b:
        return 0.0
    intersection = words_a & words_b
    union = words_a | words_b
    return len(intersection) / len(union) if union else 0.0


async def find_similar_products(
    db, item_id: str, min_similarity: float = 0.3, limit: int = 20
) -> list[dict]:
    """
    Find products similar to the given one across all platforms.
    Returns list of matches with similarity scores.
    """
    source = await db.products.find_one({"itemId": item_id}, {"_id": 0})
    if not source:
        return []

    source_title = source.get("title") or ""
    source_platform = source.get("platform")

    # Search for candidates with overlapping keywords
    words = _normalize_title(source_title).split()
    if not words:
        return []

    # Use top 3 most meaningful words (longest) for initial filter
    key_words = sorted(words, key=len, reverse=True)[:3]
    regex_pattern = "|".join(re.escape(w) for w in key_words if len(w) > 1)
    if not regex_pattern:
        return []

    candidates = await db.products.find(
        {
            "itemId": {"$ne": item_id},
            "title": {"$regex": regex_pattern, "$options": "i"},
        },
        {"_id": 0, "itemId": 1, "title": 1, "price": 1, "platform": 1,
         "shopName": 1, "image": 1, "link": 1, "salesCount": 1,
         "detailedInfo.price": 1, "detailedInfo.priceUsd": 1},
    ).limit(200).to_list(length=200)

    # Score each candidate
    results = []
    for c in candidates:
        sim = _title_similarity(source_title, c.get("title", ""))
        if sim >= min_similarity:
            detail = c.get("detailedInfo") or {}
            results.append({
                "itemId": c["itemId"],
                "title": c.get("title"),
                "platform": c.get("platform"),
                "price": detail.get("price") or c.get("price"),
                "priceUsd": detail.get("priceUsd"),
                "shopName": c.get("shopName"),
                "image": c.get("image"),
                "link": c.get("link"),
                "salesCount": c.get("salesCount"),
                "similarity": round(sim, 3),
                "samePlatform": c.get("platform") == source_platform,
            })

    # Sort by similarity descending
    results.sort(key=lambda x: (-x["similarity"], x.get("price") or 999999))
    return results[:limit]


async def compare_prices_across_platforms(
    db, keyword: str, limit_per_platform: int = 10
) -> dict:
    """
    Compare prices for products matching a keyword across all platforms.
    Returns grouped results with platform statistics.
    """
    platforms = ["taobao", "tmall", "1688", "alibaba"]
    result = {}

    for platform in platforms:
        products = await db.products.find(
            {
                "platform": platform,
                "$or": [
                    {"title": {"$regex": keyword, "$options": "i"}},
                    {"searchKeyword": {"$regex": keyword, "$options": "i"}},
                ],
            },
            {"_id": 0, "itemId": 1, "title": 1, "price": 1, "platform": 1,
             "shopName": 1, "image": 1, "link": 1,
             "detailedInfo.price": 1, "detailedInfo.priceUsd": 1},
        ).sort("price", 1).limit(limit_per_platform).to_list(length=limit_per_platform)

        # Parse prices for stats
        prices = []
        items = []
        for p in products:
            detail = p.get("detailedInfo") or {}
            price_val = detail.get("price") or p.get("price")
            try:
                price_num = float(str(price_val).replace(",", ""))
                prices.append(price_num)
            except (ValueError, TypeError):
                price_num = None

            items.append({
                "itemId": p["itemId"],
                "title": p.get("title"),
                "price": price_val,
                "priceUsd": detail.get("priceUsd"),
                "shopName": p.get("shopName"),
                "image": p.get("image"),
                "link": p.get("link"),
            })

        result[platform] = {
            "count": len(items),
            "items": items,
            "stats": {
                "min": min(prices) if prices else None,
                "max": max(prices) if prices else None,
                "avg": round(sum(prices) / len(prices), 2) if prices else None,
            } if prices else None,
        }

    return {
        "keyword": keyword,
        "platforms": result,
    }
