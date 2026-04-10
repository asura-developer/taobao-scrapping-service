from datetime import datetime
from typing import Optional


class MigrationService:
    BATCH_SIZE = 500

    def __init__(self):
        self.migrations = [
            {"version": 2, "name": "add_platform_field",   "up": self._v2_up, "down": self._v2_down},
            {"version": 3, "name": "normalize_variants",   "up": self._v3_up, "down": self._v3_down},
            {"version": 4, "name": "add_category_name",    "up": self._v4_up, "down": self._v4_down},
            {"version": 5, "name": "add_group_category",   "up": self._v5_up, "down": self._v5_down},
        ]

    async def get_current_version(self, db) -> int:
        doc = await db.products.find_one({}, sort=[("migrationVersion", -1)], projection={"migrationVersion": 1})
        return doc.get("migrationVersion", 1) if doc else 1

    async def run_migrations(self, db, target_version: Optional[int] = None) -> dict:
        current = await self.get_current_version(db)
        target = target_version or max(m["version"] for m in self.migrations)
        to_run = [m for m in self.migrations if current < m["version"] <= target]
        to_run.sort(key=lambda m: m["version"])

        results = {"success": True, "migrationsRun": [], "errors": []}
        for m in to_run:
            try:
                print(f"Running migration: {m['name']} (v{m['version']})")
                result = await m["up"](db)
                results["migrationsRun"].append({"version": m["version"], "name": m["name"], "result": result})
            except Exception as e:
                results["success"] = False
                results["errors"].append({"version": m["version"], "name": m["name"], "error": str(e)})
                break
        return results

    async def rollback(self, db, target_version: int) -> dict:
        current = await self.get_current_version(db)
        to_rollback = [m for m in self.migrations if target_version < m["version"] <= current]
        to_rollback.sort(key=lambda m: -m["version"])

        results = {"success": True, "migrationsRolledBack": [], "errors": []}
        for m in to_rollback:
            try:
                result = await m["down"](db)
                results["migrationsRolledBack"].append({"version": m["version"], "name": m["name"], "result": result})
            except Exception as e:
                results["success"] = False
                results["errors"].append({"version": m["version"], "name": m["name"], "error": str(e)})
                break
        return results

    async def cleanup_duplicates(self, db) -> dict:
        pipeline = [
            {"$group": {"_id": "$itemId", "count": {"$sum": 1}, "docs": {"$push": "$_id"}}},
            {"$match": {"count": {"$gt": 1}}}
        ]
        dupes = await db.products.aggregate(pipeline).to_list(length=None)
        removed = 0
        for dup in dupes:
            to_remove = dup["docs"][1:]
            result = await db.products.delete_many({"_id": {"$in": to_remove}})
            removed += result.deleted_count
        return {"removed": removed, "message": f"Removed {removed} duplicate products"}

    async def _v2_up(self, db) -> dict:
        cursor = db.products.find({"platform": {"$exists": False}})
        ops = []
        updated = 0
        async for product in cursor:
            link = product.get("link", "")
            platform = "taobao"
            if "tmall.com" in link:
                platform = "tmall"
            elif "1688.com" in link:
                platform = "1688"
            elif "alibaba.com" in link:
                platform = "alibaba"

            from pymongo import UpdateOne
            ops.append(UpdateOne(
                {"_id": product["_id"]},
                {
                    "$set": {"platform": platform, "migrationVersion": 2},
                    "$push": {"migrationHistory": {
                        "version": 2, "migratedAt": datetime.utcnow(),
                        "changes": {"addedPlatform": platform}
                    }}
                }
            ))
            updated += 1
            if len(ops) >= self.BATCH_SIZE:
                await db.products.bulk_write(ops, ordered=False)
                ops = []

        if ops:
            await db.products.bulk_write(ops, ordered=False)
        return {"updated": updated, "message": f"Added platform field to {updated} products"}

    async def _v2_down(self, db) -> dict:
        await db.products.update_many(
            {"migrationVersion": {"$gte": 2}},
            {"$unset": {"platform": ""}, "$set": {"migrationVersion": 1}}
        )
        return {"message": "Removed platform field"}

    async def _v3_up(self, db) -> dict:
        from pymongo import UpdateOne
        cursor = db.products.find({"detailedInfo.variants": {"$exists": True, "$type": "object"}})
        ops = []
        updated = 0
        async for product in cursor:
            old_variants = product.get("detailedInfo", {}).get("variants", {})
            if isinstance(old_variants, list):
                continue
            variants_array = [
                {"type": k, "options": v if isinstance(v, list) else [v]}
                for k, v in old_variants.items()
            ]
            ops.append(UpdateOne(
                {"_id": product["_id"]},
                {
                    "$set": {"detailedInfo.variants": variants_array, "migrationVersion": 3},
                    "$push": {"migrationHistory": {
                        "version": 3, "migratedAt": datetime.utcnow(),
                        "changes": {"normalizedVariants": True, "variantCount": len(variants_array)}
                    }}
                }
            ))
            updated += 1
            if len(ops) >= self.BATCH_SIZE:
                await db.products.bulk_write(ops, ordered=False)
                ops = []

        if ops:
            await db.products.bulk_write(ops, ordered=False)
        return {"updated": updated, "message": f"Normalized variants for {updated} products"}

    async def _v3_down(self, db) -> dict:
        from pymongo import UpdateOne
        cursor = db.products.find({"migrationVersion": {"$gte": 3}})
        ops = []
        async for product in cursor:
            variants_list = product.get("detailedInfo", {}).get("variants", [])
            if not isinstance(variants_list, list):
                continue
            variants_obj = {v["type"]: v.get("options", []) for v in variants_list}
            ops.append(UpdateOne(
                {"_id": product["_id"]},
                {"$set": {"detailedInfo.variants": variants_obj, "migrationVersion": 2}}
            ))
            if len(ops) >= self.BATCH_SIZE:
                await db.products.bulk_write(ops, ordered=False)
                ops = []
        if ops:
            await db.products.bulk_write(ops, ordered=False)
        return {"message": "Reverted variants to object format"}

    async def _v4_up(self, db) -> dict:
        from pymongo import UpdateOne
        CATEGORY_MAPPING = {
            "50014866": "Baby Formula",
            "50025969": "Beauty & Skincare",
            "50010404": "Home & Living",
            "50010788": "Shoes & Bags",
            "50014811": "Electronics",
            "50016348": "Sports & Outdoor",
        }
        cursor = db.products.find({"categoryId": {"$exists": True}, "categoryName": {"$exists": False}})
        ops = []
        updated = 0
        async for product in cursor:
            cat_name = CATEGORY_MAPPING.get(product.get("categoryId", ""), "Unknown")
            ops.append(UpdateOne(
                {"_id": product["_id"]},
                {
                    "$set": {"categoryName": cat_name, "migrationVersion": 4},
                    "$push": {"migrationHistory": {
                        "version": 4, "migratedAt": datetime.utcnow(),
                        "changes": {"addedCategoryName": cat_name}
                    }}
                }
            ))
            updated += 1
            if len(ops) >= self.BATCH_SIZE:
                await db.products.bulk_write(ops, ordered=False)
                ops = []
        if ops:
            await db.products.bulk_write(ops, ordered=False)
        return {"updated": updated, "message": f"Added category names to {updated} products"}

    async def _v4_down(self, db) -> dict:
        await db.products.update_many(
            {"migrationVersion": {"$gte": 4}},
            {"$unset": {"categoryName": ""}, "$set": {"migrationVersion": 3}}
        )
        return {"message": "Removed category names"}


    async def _v5_up(self, db) -> dict:
        from pymongo import UpdateOne
        from data.category_tree import find_group_for_platform_id, find_group_for_sub

        cursor = db.products.find(
            {"groupCategoryId": {"$exists": False}},
            {"_id": 1, "categoryId": 1, "platform": 1},
        )
        ops = []
        updated = 0
        async for product in cursor:
            cat_id = product.get("categoryId", "")
            platform = product.get("platform", "taobao")

            # Try by sub_id first, then by platform-native ID
            group = find_group_for_sub(cat_id) or find_group_for_platform_id(platform, cat_id)

            group_cat_id   = group.group_id   if group else None
            group_cat_name = group.name_zh    if group else None
            group_cat_en   = group.name_en    if group else None

            ops.append(UpdateOne(
                {"_id": product["_id"]},
                {
                    "$set": {
                        "groupCategoryId":    group_cat_id,
                        "groupCategoryName":  group_cat_name,
                        "groupCategoryNameEn": group_cat_en,
                        "migrationVersion": 5,
                    },
                    "$push": {"migrationHistory": {
                        "version": 5,
                        "migratedAt": datetime.utcnow(),
                        "changes": {
                            "addedGroupCategoryId": group_cat_id,
                            "addedGroupCategoryName": group_cat_name,
                        },
                    }},
                },
            ))
            updated += 1
            if len(ops) >= self.BATCH_SIZE:
                await db.products.bulk_write(ops, ordered=False)
                ops = []

        if ops:
            await db.products.bulk_write(ops, ordered=False)
        return {"updated": updated, "message": f"Added group category to {updated} products"}

    async def _v5_down(self, db) -> dict:
        await db.products.update_many(
            {"migrationVersion": {"$gte": 5}},
            {
                "$unset": {
                    "groupCategoryId": "",
                    "groupCategoryName": "",
                    "groupCategoryNameEn": "",
                },
                "$set": {"migrationVersion": 4},
            },
        )
        return {"message": "Removed group category fields"}


migration_service = MigrationService()