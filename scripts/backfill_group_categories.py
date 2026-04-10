import argparse
import asyncio
import os
import sys
from datetime import datetime, UTC
from pathlib import Path

import motor.motor_asyncio
from dotenv import load_dotenv
from pymongo import UpdateOne

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data.category_tree import CATEGORY_TREE, get_group_by_id, find_group_for_platform_id, find_group_for_sub


GROUP_NAME_ALIASES = {
    "男装": "mens_clothing",
    "女装": "womens_clothing",
    "鞋靴": "shoes",
    "鞋": "shoes",
    "箱包": "bags_luggage",
    "美妆护肤": "beauty_skincare",
    "美妆": "beauty_skincare",
    "个人护理": "personal_care",
    "手机数码": "phones_digital",
    "数码": "phones_digital",
    "电脑办公": "computers_office",
    "家电": "home_appliances",
    "家用电器": "home_appliances",
    "家居家装": "home_living",
    "家居": "home_living",
    "母婴用品": "mother_baby",
    "母婴": "mother_baby",
    "食品生鲜": "food_fresh",
    "食品": "food_fresh",
    "运动户外": "sports_outdoors",
    "珠宝饰品": "jewelry_accessories",
    "汽配": "auto_accessories",
    "汽车用品": "auto_accessories",
    "玩具乐器": "toys_instruments",
    "图书文具": "books_stationery",
    "宠物用品": "pet_supplies",
    "家装建材": "home_improvement",
    "内衣配饰": "underwear_accessories",
}


def find_group_for_name(name: str):
    if not name:
        return None

    normalized = str(name).strip()
    if not normalized:
        return None

    alias = GROUP_NAME_ALIASES.get(normalized)
    if alias:
        return get_group_by_id(alias)

    lowered = normalized.lower()
    for group in CATEGORY_TREE.values():
        if normalized == group.name_zh or lowered == group.name_en.lower():
            return group
    return None


def resolve_group(product: dict):
    category_id = product.get("categoryId", "")
    platform = product.get("platform", "taobao")
    category_name = product.get("categoryName") or product.get("searchKeyword")
    if not category_id:
        return find_group_for_name(category_name)
    return (
        find_group_for_sub(category_id)
        or find_group_for_platform_id(platform, category_id)
        or find_group_for_name(category_name)
    )


async def backfill_group_categories(
    db,
    *,
    details_only: bool,
    only_missing: bool,
    dry_run: bool,
    batch_size: int,
) -> dict:
    query: dict = {}

    if details_only:
        query["detailsScraped"] = True
        query["detailedInfo"] = {"$exists": True, "$ne": None}

    if only_missing:
        query["$or"] = [
            {"groupCategoryId": {"$exists": False}},
            {"groupCategoryId": None},
            {"groupCategoryId": ""},
        ]

    projection = {
        "_id": 1,
        "itemId": 1,
        "platform": 1,
        "categoryId": 1,
        "categoryName": 1,
        "searchKeyword": 1,
        "groupCategoryId": 1,
    }

    total = await db.products.count_documents(query)
    matched = 0
    updated = 0
    unresolved = 0
    ops: list[UpdateOne] = []

    async for product in db.products.find(query, projection):
        matched += 1
        group = resolve_group(product)
        if not group:
            unresolved += 1
            continue

        changes = {
            "groupCategoryId": group.group_id,
            "groupCategoryName": group.name_zh,
            "groupCategoryNameEn": group.name_en,
            "updatedAt": datetime.now(UTC),
        }

        if product.get("groupCategoryId") == group.group_id and only_missing:
            continue

        updated += 1
        ops.append(UpdateOne(
            {"_id": product["_id"]},
            {
                "$set": changes,
                "$push": {
                    "migrationHistory": {
                        "version": 5,
                        "migratedAt": datetime.now(UTC),
                        "changes": {
                            "addedGroupCategoryId": group.group_id,
                            "addedGroupCategoryName": group.name_zh,
                        },
                    }
                },
            },
        ))

        if not dry_run and len(ops) >= batch_size:
            await db.products.bulk_write(ops, ordered=False)
            ops = []

    if not dry_run and ops:
        await db.products.bulk_write(ops, ordered=False)

    return {
        "query": query,
        "totalCandidates": total,
        "matched": matched,
        "updated": updated,
        "unresolved": unresolved,
        "dryRun": dry_run,
    }


async def main():
    parser = argparse.ArgumentParser(
        description="Backfill group category fields on scraped products in MongoDB."
    )
    parser.add_argument("--all-products", action="store_true",
                        help="Process all products, not only products with detailedInfo.")
    parser.add_argument("--include-existing", action="store_true",
                        help="Also recompute products that already have groupCategoryId.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview how many products would be updated without writing.")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Mongo bulk_write batch size.")
    args = parser.parse_args()

    load_dotenv()
    mongo_uri = os.getenv("MONGODB_URI")
    if not mongo_uri:
        raise RuntimeError("MONGODB_URI env var is required. Check your .env file.")

    client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
    db = client.get_default_database()

    try:
        result = await backfill_group_categories(
            db,
            details_only=not args.all_products,
            only_missing=not args.include_existing,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
        )
        print("Backfill complete")
        for key, value in result.items():
            print(f"{key}: {value}")
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
