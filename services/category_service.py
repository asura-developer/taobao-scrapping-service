"""
Category service.

Handles:
 - Auto-discovery of categories by crawling platform pages
 - Pre-seeding the full Taobao/Tmall/1688 category tree from the canonical
   data/category_tree.py definition (idempotent upserts on every startup)
 - Hierarchical queries (group → sub-categories)

MongoDB document shape (collection: categories):
  {
    categoryId:     str   — stable sub_id or platform-native ID
    name:           str   — display name (Chinese)
    nameEn:         str   — display name (English)
    platform:       str   — "taobao" | "tmall" | "1688"
    level:          int   — 1 = group, 2 = sub-category
    groupId:        str   — parent group_id (null for level-1 docs)
    groupName:      str   — parent Chinese name
    groupNameEn:    str   — parent English name
    url:            str | null
    source:         str   — "seed" | "discovered"
    discoveredAt:   datetime | null
    updatedAt:      datetime
  }
"""

import re
import logging
from datetime import datetime, UTC
from typing import Optional

from scrapling.fetchers import StealthyFetcher

from data.category_tree import (
    CATEGORY_TREE,
    get_all_groups,
    get_group_by_id,
)

logger = logging.getLogger(__name__)

PLATFORMS = ("taobao", "tmall", "1688")

# Known category page URLs
CATEGORY_URLS = {
    "taobao": "https://www.taobao.com/tbhome/page/market-list",
    "tmall":  "https://www.tmall.com/",
    "1688":   "https://www.1688.com/",
}


def _build_group_key(platform: str, group_id: str) -> str:
    return f"{platform}:group:{group_id}"


def _build_sub_key(platform: str, group_id: Optional[str], sub_id: str) -> str:
    parent = group_id or "ungrouped"
    return f"{platform}:sub:{parent}:{sub_id}"


def _serialize_group(
    *,
    platform: str,
    group_id: str,
    name_zh: str,
    name_en: str,
    sub_categories: Optional[list[dict]] = None,
) -> dict:
    return {
        "key": _build_group_key(platform, group_id),
        "groupId": group_id,
        "nameZh": name_zh,
        "nameEn": name_en,
        "subCategories": sub_categories or [],
    }


def _serialize_sub(
    *,
    platform: str,
    sub_id: str,
    name_zh: str,
    name_en: str,
    group_id: Optional[str] = None,
    platform_cat_id: Optional[str] = None,
    taobao_id: Optional[str] = None,
    tmall_id: Optional[str] = None,
    id_1688: Optional[str] = None,
    source: Optional[str] = None,
) -> dict:
    payload = {
        "key": _build_sub_key(platform, group_id, sub_id),
        "subId": sub_id,
        "groupId": group_id,
        "groupKey": _build_group_key(platform, group_id) if group_id else None,
        "nameZh": name_zh,
        "nameEn": name_en,
        "platformCatId": platform_cat_id,
        "taobaoId": taobao_id,
        "tmallId": tmall_id,
        "id1688": id_1688,
    }
    if source is not None:
        payload["source"] = source
    return payload


def _serialize_flat_category(doc: dict) -> dict:
    level = doc.get("level")
    category_id = doc.get("categoryId", "")
    platform = doc.get("platform", "")
    group_id = doc.get("groupId")

    return {
        **doc,
        "key": (
            _build_group_key(platform, category_id)
            if level == 1 else
            _build_sub_key(platform, group_id, category_id)
        ),
        "groupKey": _build_group_key(platform, group_id) if group_id else None,
    }


# ── Seeding ────────────────────────────────────────────────────────────────────

async def seed_categories(db, platform: str) -> int:
    """
    Upsert the canonical category tree into MongoDB for *platform*.

    Returns the number of documents upserted/updated.
    Idempotent — safe to call on every app startup.
    """
    from pymongo import UpdateOne

    ops: list[UpdateOne] = []
    now = datetime.now(UTC)

    for group in get_all_groups():
        # Upsert the group document (level 1)
        ops.append(UpdateOne(
            {"categoryId": group.group_id, "platform": platform},
            {"$set": {
                "categoryId":  group.group_id,
                "name":        group.name_zh,
                "nameEn":      group.name_en,
                "platform":    platform,
                "level":       1,
                "groupId":     None,
                "groupName":   None,
                "groupNameEn": None,
                "url":         None,
                "source":      "seed",
                "updatedAt":   now,
            }, "$setOnInsert": {"discoveredAt": now}},
            upsert=True,
        ))

        # Upsert each sub-category document (level 2)
        for sub in group.subs:
            # Prefer the platform-native ID if available, fall back to sub_id
            platform_id = (
                sub.taobao_id if platform == "taobao" else
                sub.tmall_id  if platform == "tmall"  else
                sub.id_1688   if platform == "1688"   else
                None
            ) or sub.sub_id

            ops.append(UpdateOne(
                {"categoryId": sub.sub_id, "platform": platform},
                {"$set": {
                    "categoryId":      sub.sub_id,
                    "platformCatId":   platform_id,
                    "name":            sub.name_zh,
                    "nameEn":          sub.name_en,
                    "platform":        platform,
                    "level":           2,
                    "groupId":         group.group_id,
                    "groupName":       group.name_zh,
                    "groupNameEn":     group.name_en,
                    "url":             None,
                    "source":          "seed",
                    "updatedAt":       now,
                }, "$setOnInsert": {"discoveredAt": now}},
                upsert=True,
            ))

    if not ops:
        return 0

    result = await db.categories.bulk_write(ops, ordered=False)
    total = result.upserted_count + result.modified_count
    logger.info(f"Seeded {total} category documents for {platform}")
    return total


async def seed_all_platforms(db) -> dict[str, int]:
    """Seed categories for all supported platforms."""
    results: dict[str, int] = {}
    for platform in PLATFORMS:
        try:
            results[platform] = await seed_categories(db, platform)
        except Exception as e:
            logger.error(f"Category seed error for {platform}: {e}")
            results[platform] = 0
    return results


# ── Queries ────────────────────────────────────────────────────────────────────

async def get_categories(db, platform: Optional[str] = None) -> list[dict]:
    """Get stored categories (flat list, backward-compatible)."""
    filt: dict = {}
    if platform:
        filt["platform"] = platform
    cursor = db.categories.find(filt, {"_id": 0}).sort("name", 1)
    docs = await cursor.to_list(length=1000)
    return [_serialize_flat_category(doc) for doc in docs]


async def get_category_tree(db, platform: str) -> list[dict]:
    """
    Return the full category hierarchy for *platform* from MongoDB.

    If the DB has no seeded data, falls back to the in-memory canonical tree.

    Returns a list of group dicts:
      [{groupId, nameZh, nameEn, subCategories: [...]}, ...]
    """
    # Fetch all level-1 and level-2 docs for this platform
    docs = await db.categories.find(
        {"platform": platform},
        {"_id": 0},
    ).to_list(length=2000)

    groups_map: dict[str, dict] = {}
    subs_map: dict[str, list[dict]] = {}

    for doc in docs:
        if doc.get("level") == 1:
            groups_map[doc["categoryId"]] = _serialize_group(
                platform=platform,
                group_id=doc["categoryId"],
                name_zh=doc.get("name", ""),
                name_en=doc.get("nameEn", ""),
            )
        elif doc.get("level") == 2:
            gid = doc.get("groupId")
            if gid:
                subs_map.setdefault(gid, []).append(_serialize_sub(
                    platform=platform,
                    group_id=gid,
                    sub_id=doc["categoryId"],
                    name_zh=doc.get("name", ""),
                    name_en=doc.get("nameEn", ""),
                    platform_cat_id=doc.get("platformCatId"),
                    taobao_id=doc.get("taobaoId"),
                    tmall_id=doc.get("tmallId"),
                    id_1688=doc.get("id1688"),
                    source=doc.get("source", "seed"),
                ))

    # Attach subs to groups
    for gid, subs in subs_map.items():
        if gid in groups_map:
            groups_map[gid]["subCategories"] = sorted(subs, key=lambda s: s["nameEn"])

    if groups_map:
        # Return in canonical CATEGORY_TREE order, then any extras
        ordered: list[dict] = []
        for gid in CATEGORY_TREE:
            if gid in groups_map:
                ordered.append(groups_map.pop(gid))
        ordered.extend(groups_map.values())
        return ordered

    # Fallback: return canonical in-memory tree (DB not yet seeded)
    return [
        _serialize_group(
            platform=platform,
            group_id=group.group_id,
            name_zh=group.name_zh,
            name_en=group.name_en,
            sub_categories=[
                _serialize_sub(
                    platform=platform,
                    group_id=group.group_id,
                    sub_id=sub.sub_id,
                    name_zh=sub.name_zh,
                    name_en=sub.name_en,
                    platform_cat_id=(
                        sub.taobao_id if platform == "taobao" else
                        sub.tmall_id if platform == "tmall" else
                        sub.id_1688 if platform == "1688" else
                        sub.sub_id
                    ),
                    taobao_id=sub.taobao_id,
                    tmall_id=sub.tmall_id,
                    id_1688=sub.id_1688,
                    source="seed",
                )
                for sub in group.subs
            ],
        )
        for group in get_all_groups()
    ]


async def get_groups(db, platform: str) -> list[dict]:
    """Return only group-level (level=1) categories for a platform."""
    docs = await db.categories.find(
        {"platform": platform, "level": 1},
        {"_id": 0, "categoryId": 1, "name": 1, "nameEn": 1},
    ).sort("name", 1).to_list(length=100)

    if docs:
        return [
            _serialize_group(
                platform=platform,
                group_id=d["categoryId"],
                name_zh=d["name"],
                name_en=d.get("nameEn", ""),
            )
            for d in docs
        ]

    # Fallback: canonical tree
    return [
        _serialize_group(
            platform=platform,
            group_id=g.group_id,
            name_zh=g.name_zh,
            name_en=g.name_en,
        )
        for g in get_all_groups()
    ]


async def get_subs_for_group(db, platform: str, group_id: str) -> list[dict]:
    """Return sub-categories for a specific group and platform."""
    docs = await db.categories.find(
        {"platform": platform, "level": 2, "groupId": group_id},
        {"_id": 0},
    ).sort("name", 1).to_list(length=200)

    if docs:
        return [
            _serialize_sub(
                platform=platform,
                group_id=group_id,
                sub_id=d["categoryId"],
                name_zh=d.get("name", ""),
                name_en=d.get("nameEn", ""),
                platform_cat_id=d.get("platformCatId"),
            )
            for d in docs
        ]

    # Fallback: canonical tree
    group = get_group_by_id(group_id)
    if not group:
        return []
    return [
        _serialize_sub(
            platform=platform,
            group_id=group_id,
            sub_id=sub.sub_id,
            name_zh=sub.name_zh,
            name_en=sub.name_en,
            platform_cat_id=(
                sub.taobao_id if platform == "taobao" else
                sub.tmall_id  if platform == "tmall"  else
                sub.id_1688   if platform == "1688"   else
                sub.sub_id
            ),
        )
        for sub in group.subs
    ]


# ── Auto-discovery (unchanged logic, adds hierarchy fields) ────────────────────

async def discover_categories(db, platform: str) -> list[dict]:
    """
    Crawl a platform's main page to discover categories.
    Discovered categories are stored with source="discovered" and
    level=2 (sub-category) with no groupId (hierarchy inferred later).
    """
    url = CATEGORY_URLS.get(platform)
    if not url:
        return []

    try:
        page = await StealthyFetcher.async_fetch(
            url=url, headless=True, network_idle=True,
        )
        if not page or page.status != 200:
            logger.warning(
                f"Category discovery failed for {platform}: status={getattr(page, 'status', None)}"
            )
            return []

        html = ""
        for attr in ("html_content", "content", "text_content"):
            val = getattr(page, attr, None)
            if val and isinstance(val, str) and len(val) > 100:
                html = val
                break

        if not html:
            return []

        categories = _extract_categories(html, platform)

        if categories:
            from pymongo import UpdateOne
            ops = [
                UpdateOne(
                    {"categoryId": cat["categoryId"], "platform": platform},
                    {"$set": {**cat, "updatedAt": datetime.now(UTC)}},
                    upsert=True,
                )
                for cat in categories
            ]
            await db.categories.bulk_write(ops, ordered=False)
            logger.info(f"Discovered {len(categories)} categories for {platform}")

        return categories

    except Exception as e:
        logger.error(f"Category discovery error for {platform}: {e}")
        return []


def _extract_categories(html: str, platform: str) -> list[dict]:
    """Extract category names and IDs from HTML (source="discovered")."""
    categories = []
    seen: set[str] = set()

    cat_patterns = [
        re.compile(r'href="([^"]*(?:catId|cat|categoryId)=(\d+)[^"]*)"[^>]*>([^<]+)<'),
        re.compile(r'href="(https?://[^"]*\.(?:taobao|tmall|1688)\.com/[^"]*)"[^>]*title="([^"]+)"'),
        re.compile(r'<a[^>]*href="([^"]*category[^"]*)"[^>]*>([^<]{2,30})</a>'),
    ]

    for pattern in cat_patterns:
        for m in pattern.finditer(html):
            groups = m.groups()
            if len(groups) == 3:
                href, cat_id, name = groups
            elif len(groups) == 2:
                href, name = groups
                id_match = re.search(r'(?:catId|cat|categoryId)=(\d+)', href)
                cat_id = id_match.group(1) if id_match else _generate_cat_id(name, platform)
            else:
                continue

            name = name.strip()
            if not name or len(name) < 2 or len(name) > 50:
                continue
            if cat_id in seen:
                continue
            seen.add(cat_id)

            categories.append({
                "categoryId": str(cat_id),
                "name":       name,
                "nameEn":     "",
                "platform":   platform,
                "level":      2,
                "groupId":    None,
                "groupName":  None,
                "groupNameEn":None,
                "url":        href if href.startswith("http") else None,
                "source":     "discovered",
                "discoveredAt": datetime.now(UTC),
            })

    nav_pattern = re.compile(
        r'class="[^"]*(?:cat-|category|nav-item|menu-item)[^"]*"[^>]*>'
        r'(?:<[^>]*>)*\s*([^<]{2,30})\s*(?:</[^>]*>)*',
        re.IGNORECASE,
    )
    for m in nav_pattern.finditer(html):
        name = m.group(1).strip()
        if not name or len(name) < 2 or name in seen:
            continue
        cat_id = _generate_cat_id(name, platform)
        if cat_id in seen:
            continue
        seen.add(cat_id)
        categories.append({
            "categoryId": cat_id,
            "name":       name,
            "nameEn":     "",
            "platform":   platform,
            "level":      2,
            "groupId":    None,
            "groupName":  None,
            "groupNameEn":None,
            "url":        None,
            "source":     "discovered",
            "discoveredAt": datetime.now(UTC),
        })

    return categories


def _generate_cat_id(name: str, platform: str) -> str:
    """Generate a deterministic category ID from name."""
    import hashlib
    return hashlib.md5(f"{platform}:{name}".encode()).hexdigest()[:12]


# ── Indexes ────────────────────────────────────────────────────────────────────

async def ensure_category_indexes(db) -> None:
    col = db.categories
    await col.create_index([("categoryId", 1), ("platform", 1)], unique=True)
    await col.create_index("platform")
    await col.create_index([("platform", 1), ("level", 1)])
    await col.create_index([("platform", 1), ("groupId", 1)])
    await col.create_index("groupId", sparse=True)
