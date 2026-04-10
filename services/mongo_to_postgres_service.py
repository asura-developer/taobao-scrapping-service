import asyncio
from datetime import datetime, UTC
import logging
from pathlib import Path
from typing import AsyncGenerator

from data.category_tree import find_group_for_sub, find_sub_by_platform_id, get_group_by_id, get_sub_by_id
from services.postgres_service import pg_service
from services.product_detail_extractor import cny_to_usd


def _normalize_label(val: str | None) -> str:
    return " ".join((val or "").strip().casefold().split())


def _to_numeric(val) -> float | None:
    """Safely coerce a price/rating value (str or number) to float, or None."""
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _to_int(val) -> int | None:
    """Safely coerce a review count value to int, or None."""
    if val is None:
        return None
    try:
        return int(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _to_sales_count(val) -> float | None:
    """Parse sales counts like '3000' or '1.2万' into numeric form."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if "万" in s:
        try:
            return float(s.replace("万", "").replace(",", "").strip()) * 10000
        except (ValueError, TypeError):
            return None
    return _to_numeric(s)


def _to_datetime(val):
    if val is None or val == "":
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _eligible_products_filter() -> dict:
    return {}


class MongoToPostgresMigrationService:
    BATCH_SIZE = 50

    def __init__(self):
        self.platform_cache: dict[str, int] = {}
        self._schema_lock = asyncio.Lock()
        self._schema_ready = False

    async def run_stream(self, db, force: bool = False) -> AsyncGenerator[dict, None]:
        yield {"type": "start", "message": "Migration started"}

        try:
            yield {"type": "phase", "phase": "schema", "message": "Applying PostgreSQL schema..."}
            await self.apply_schema()
            yield {"type": "phase_done", "phase": "schema", "message": "Schema ready"}

            await self.load_platform_cache()

            yield {"type": "phase", "phase": "products", "message": "Migrating products..."}
            product_stats = {"total": 0, "migrated": 0, "skipped": 0, "failed": 0}

            async for event in self._migrate_products_stream(db, force):
                if event.get("_stats"):
                    product_stats = event["_stats"]
                else:
                    yield event

            yield {"type": "phase_done", "phase": "products", **product_stats}

            yield {"type": "phase", "phase": "jobs", "message": "Migrating scraping jobs..."}
            job_stats = await self._migrate_jobs(db, force)
            yield {"type": "phase_done", "phase": "jobs", **job_stats}

            yield {"type": "phase", "phase": "verify", "message": "Verifying migration..."}
            verification = await self.verify(db)
            yield {"type": "phase_done", "phase": "verify", "verification": verification}

            yield {"type": "complete", "report": {
                "success": True,
                "products": product_stats,
                "jobs": job_stats,
                "verification": verification,
            }}

        except Exception as e:
            yield {"type": "error", "error": str(e)}
            raise

    async def verify(self, db) -> dict:
        import asyncio

        mongo_products, mongo_jobs, eligible_products = await asyncio.gather(
            db.products.count_documents({}),
            db.scraping_jobs.count_documents({}),
            db.products.count_documents(_eligible_products_filter()),
        )
        with_details = await db.products.count_documents({"detailsScraped": True})

        try:
            pg_rows = await pg_service.query("""
                SELECT
                    (SELECT COUNT(*) FROM products)         AS products,
                    (SELECT COUNT(*) FROM shops)            AS shops,
                    (SELECT COUNT(*) FROM categories)       AS categories,
                    (SELECT COUNT(*) FROM category_groups)  AS category_groups,
                    (SELECT COUNT(*) FROM scraping_jobs)    AS scraping_jobs,
                    (SELECT COUNT(*) FROM product_images)   AS product_images,
                    (SELECT COUNT(*) FROM product_specs)    AS product_specs,
                    (SELECT COUNT(*) FROM product_variants) AS product_variants,
                    (SELECT COUNT(*) FROM mongo_migration_log WHERE status = 'success') AS log_success,
                    (SELECT COUNT(*) FROM mongo_migration_log WHERE status = 'failed')  AS log_failed
            """)
            pg = pg_rows[0]
        except Exception:
            pg = {k: 0 for k in ["products","shops","categories","category_groups","scraping_jobs",
                                   "product_images","product_specs","product_variants",
                                   "log_success","log_failed"]}

        return {
            "mongo": {
                "products": mongo_products,
                "eligible_products": eligible_products,
                "jobs": mongo_jobs,
                "products_with_details": with_details,
            },
            "postgres": {
                "products":         int(pg["products"]),
                "shops":            int(pg["shops"]),
                "categories":       int(pg["categories"]),
                "category_groups":  int(pg["category_groups"]),
                "scraping_jobs":    int(pg["scraping_jobs"]),
                "product_images":   int(pg["product_images"]),
                "product_specs":    int(pg["product_specs"]),
                "product_variants": int(pg["product_variants"]),
            },
            "migration_log": {
                "success": int(pg["log_success"]),
                "failed":  int(pg["log_failed"]),
            },
        }

    async def apply_schema(self):
        if self._schema_ready:
            return

        async with self._schema_lock:
            if self._schema_ready:
                return

            sql_dir = Path(__file__).parent.parent / "sql"
            schema_path = sql_dir / "schema.sql"
            if schema_path.exists():
                await pg_service.execute(schema_path.read_text())

            for migration_file in sorted(sql_dir.glob("[0-9]*.sql")):
                try:
                    await pg_service.execute(migration_file.read_text())
                except Exception as e:
                    logging.getLogger("scraper").warning(
                        "PostgreSQL migration %s failed: %s", migration_file.name, e
                    )

            self._schema_ready = True

    async def load_platform_cache(self):
        try:
            rows = await pg_service.query("SELECT id, name FROM platforms")
            self.platform_cache = {r["name"]: r["id"] for r in rows}
        except Exception:
            self.platform_cache = {"taobao": 1, "tmall": 2, "1688": 3}

    def get_platform_id(self, name: str) -> int:
        return self.platform_cache.get(name, self.platform_cache.get("taobao", 1))

    def _canonicalize_category(
        self,
        *,
        platform: str,
        category_id: str | None,
        category_name: str | None,
        group_category_id: str | None = None,
        group_category_name: str | None = None,
        group_category_name_en: str | None = None,
    ) -> dict[str, str | None]:
        raw_category_id = (category_id or "").strip() or None
        sub = None
        if raw_category_id:
            sub = get_sub_by_id(raw_category_id) or find_sub_by_platform_id(platform, raw_category_id)

        canonical_category_id = sub.sub_id if sub else raw_category_id
        canonical_category_name = sub.name_zh if sub else category_name
        canonical_category_name_en = sub.name_en if sub else None

        group = None
        if sub:
            group = find_group_for_sub(sub.sub_id)
        elif group_category_id:
            group = get_group_by_id(group_category_id)

        canonical_group_id = group.group_id if group else group_category_id
        canonical_group_name = group.name_zh if group else group_category_name
        canonical_group_name_en = group.name_en if group else group_category_name_en

        return {
            "category_id": canonical_category_id,
            "category_name": canonical_category_name,
            "category_name_en": canonical_category_name_en,
            "group_id": canonical_group_id,
            "group_name": canonical_group_name,
            "group_name_en": canonical_group_name_en,
        }

    async def _get_or_create_group(
        self,
        conn,
        *,
        platform_id: int,
        group_id: str | None,
        group_name: str | None,
        group_name_en: str | None,
        icon: str | None,
    ) -> int | None:
        normalized_name = _normalize_label(group_name)
        normalized_name_en = _normalize_label(group_name_en)

        if normalized_name or normalized_name_en:
            existing = await conn.fetchrow(
                """
                SELECT id
                FROM category_groups
                WHERE (
                    group_id = $1
                    OR
                    ($2 <> '' AND lower(trim(coalesce(group_name, ''))) = $2)
                    OR
                    ($3 <> '' AND lower(trim(coalesce(group_name_en, ''))) = $3)
                  )
                ORDER BY id
                LIMIT 1
                """,
                group_id,
                normalized_name,
                normalized_name_en,
            )
            if existing:
                await conn.execute(
                    """
                    UPDATE category_groups
                    SET group_id = COALESCE(group_id, $2),
                        group_name = COALESCE(group_name, $3),
                        group_name_en = COALESCE(group_name_en, $4),
                        icon = COALESCE(icon, $5),
                        updated_at = NOW()
                    WHERE id = $1
                    """,
                    existing["id"],
                    group_id,
                    group_name,
                    group_name_en,
                    icon,
                )
                return existing["id"]

        if not group_id:
            return None

        row = await conn.fetchrow(
            """
            INSERT INTO category_groups (
                   platform_id, group_id, group_name, group_name_en, icon, source, updated_at
               )
               VALUES ($1,$2,$3,$4,$5,'seed',NOW())
               ON CONFLICT (group_id) DO UPDATE
                 SET group_name=COALESCE(EXCLUDED.group_name, category_groups.group_name),
                     group_name_en=COALESCE(EXCLUDED.group_name_en, category_groups.group_name_en),
                     icon=COALESCE(EXCLUDED.icon, category_groups.icon),
                     platform_id=COALESCE(category_groups.platform_id, EXCLUDED.platform_id),
                     updated_at=NOW()
               RETURNING id
            """,
            platform_id, group_id, group_name, group_name_en, icon,
        )
        return row["id"]

    async def _get_or_create_category(
        self,
        conn,
        *,
        platform_id: int,
        group_fk: int | None,
        category_id: str,
        category_name: str | None,
        category_name_en: str | None = None,
    ) -> int:
        normalized_name = _normalize_label(category_name)
        normalized_name_en = _normalize_label(category_name_en)

        if normalized_name or normalized_name_en:
            existing = await conn.fetchrow(
                """
                SELECT id
                FROM categories
                WHERE (
                    category_id = $1
                    OR
                    ($2 <> '' AND lower(trim(coalesce(category_name, ''))) = $2)
                    OR
                    ($3 <> '' AND lower(trim(coalesce(category_name_en, ''))) = $3)
                  )
                ORDER BY id
                LIMIT 1
                """,
                category_id,
                normalized_name,
                normalized_name_en,
            )
            if existing:
                await conn.execute(
                    """
                    UPDATE categories
                    SET group_fk = COALESCE(group_fk, $2),
                        category_name = COALESCE(category_name, $3),
                        category_name_en = COALESCE(category_name_en, $4),
                        updated_at = NOW()
                    WHERE id = $1
                    """,
                    existing["id"],
                    group_fk,
                    category_name,
                    category_name_en,
                )
                return existing["id"]

        row = await conn.fetchrow(
            """
           INSERT INTO categories (
                   platform_id, group_fk, category_id, category_name, category_name_en, source, updated_at
               )
               VALUES ($1,$2,$3,$4,$5,'seed',NOW())
               ON CONFLICT (category_id) DO UPDATE
                 SET group_fk=COALESCE(EXCLUDED.group_fk, categories.group_fk),
                     category_name=COALESCE(EXCLUDED.category_name, categories.category_name),
                     category_name_en=COALESCE(EXCLUDED.category_name_en, categories.category_name_en),
                     platform_id=COALESCE(categories.platform_id, EXCLUDED.platform_id),
                     updated_at=NOW()
               RETURNING id
            """,
            platform_id, group_fk, category_id, category_name, category_name_en,
        )
        return row["id"]

    async def _migrate_products_stream(self, db, force: bool) -> AsyncGenerator[dict, None]:
        eligible_filter = _eligible_products_filter()
        stats = {"total": 0, "migrated": 0, "skipped": 0, "failed": 0}
        stats["total"] = await db.products.count_documents(eligible_filter)

        yield {"type": "progress", "phase": "products",
               "message": f"Found {stats['total']} products with details in MongoDB"}

        existing_products = set()
        if not force:
            try:
                rows = await pg_service.query(
                    "SELECT item_id FROM products"
                )
                existing_products = {r["item_id"] for r in rows}
            except Exception:
                pass

        batch = []
        async for doc in db.products.find(eligible_filter):
            if not force and doc.get("itemId") in existing_products:
                stats["skipped"] += 1
                continue
            batch.append(doc)

            if len(batch) >= self.BATCH_SIZE:
                b_migrated, b_failed, batch_errors = await self._process_batch(batch)
                stats["migrated"] += b_migrated
                stats["failed"] += b_failed
                batch = []

                processed = stats["migrated"] + stats["skipped"] + stats["failed"]
                pct = round(processed / stats["total"] * 100) if stats["total"] else 0
                for item_id, error in batch_errors[:5]:
                    yield {
                        "type": "progress",
                        "phase": "products",
                        "message": f"Failed product {item_id}: {error}",
                    }
                yield {"type": "progress", "phase": "products",
                       "migrated": stats["migrated"], "skipped": stats["skipped"],
                       "failed": stats["failed"], "total": stats["total"], "percent": pct}

        if batch:
            b_migrated, b_failed, batch_errors = await self._process_batch(batch)
            stats["migrated"] += b_migrated
            stats["failed"] += b_failed
            for item_id, error in batch_errors[:5]:
                yield {
                    "type": "progress",
                    "phase": "products",
                    "message": f"Failed product {item_id}: {error}",
                }

        yield {"_stats": stats}

    async def _process_batch(self, docs: list) -> tuple[int, int, list[tuple[str, str]]]:
        migrated = 0
        failed = 0
        errors: list[tuple[str, str]] = []

        async def tx(conn):
            nonlocal migrated, failed
            for doc in docs:
                try:
                    async with conn.transaction():
                        await self._migrate_one_product(conn, doc)
                    await conn.execute(
                        """INSERT INTO mongo_migration_log (mongo_item_id, status)
                           VALUES ($1, 'success')
                           ON CONFLICT (mongo_item_id) DO UPDATE
                             SET migrated_at = NOW(), status = 'success', error_message = NULL""",
                        doc.get("itemId", "")
                    )
                    migrated += 1
                except Exception as e:
                    failed += 1
                    errors.append((doc.get("itemId", ""), str(e)[:500]))
                    try:
                        await conn.execute(
                            """INSERT INTO mongo_migration_log (mongo_item_id, status, error_message)
                               VALUES ($1, 'failed', $2)
                               ON CONFLICT (mongo_item_id) DO UPDATE
                                 SET status = 'failed', error_message = $2""",
                            doc.get("itemId", ""),
                            str(e)[:500]
                        )
                    except Exception:
                        pass

        try:
            await pg_service.with_transaction(tx)
        except Exception as e:
            print(f"[Migration] Batch transaction error: {e}")

        return migrated, failed, errors

    async def _migrate_one_product(self, conn, doc: dict):
        platform_id = self.get_platform_id(doc.get("platform", "taobao"))

        shop_id = None
        shop_info = doc.get("shopInfo") or {}
        shop_name = shop_info.get("shopName") or doc.get("shopName")
        shop_link = shop_info.get("shopLink")

        if shop_name or shop_link:
            row = await conn.fetchrow(
                """INSERT INTO shops (platform_id, shop_name, shop_link, shop_rating, shop_location, shop_age, updated_at)
                   VALUES ($1,$2,$3,$4,$5,$6,NOW())
                   ON CONFLICT (platform_id, shop_name) DO UPDATE
                     SET shop_link=EXCLUDED.shop_link,
                         shop_rating=EXCLUDED.shop_rating,
                         shop_location=EXCLUDED.shop_location,
                         shop_age=EXCLUDED.shop_age,
                         updated_at=NOW()
                   RETURNING id""",
                platform_id, shop_name or shop_link, shop_link,
                shop_info.get("shopRating"),
                shop_info.get("shopLocation") or doc.get("location"),
                shop_info.get("shopAge"),
            )
            shop_id = row["id"]

            seller_info = shop_info.get("sellerInfo") or {}
            if seller_info:
                await conn.execute(
                    """INSERT INTO seller_info (
                           shop_id, positive_feedback_rate, has_vip, avg_delivery_time, avg_refund_time
                       )
                       VALUES ($1,$2,$3,$4,$5)
                       ON CONFLICT (shop_id) DO UPDATE
                         SET positive_feedback_rate=EXCLUDED.positive_feedback_rate,
                             has_vip=EXCLUDED.has_vip,
                             avg_delivery_time=EXCLUDED.avg_delivery_time,
                             avg_refund_time=EXCLUDED.avg_refund_time""",
                    shop_id,
                    seller_info.get("positiveFeedbackRate"),
                    bool(seller_info.get("hasVIP", False)),
                    seller_info.get("averageDeliveryTime"),
                    seller_info.get("averageRefundTime"),
                )

            for badge in shop_info.get("badges") or []:
                if badge:
                    await conn.execute(
                        "INSERT INTO shop_badges (shop_id, badge) VALUES ($1,$2) ON CONFLICT DO NOTHING",
                        shop_id,
                        str(badge)[:500],
                    )

        cat_label = doc.get("categoryName") or doc.get("searchKeyword") or "unknown"
        resolved = self._canonicalize_category(
            platform=doc.get("platform", "taobao"),
            category_id=doc.get("categoryId") or self.generate_category_id(cat_label, doc.get("platform", "taobao")),
            category_name=doc.get("categoryName") or doc.get("searchKeyword"),
            group_category_id=doc.get("groupCategoryId"),
            group_category_name=doc.get("groupCategoryName"),
            group_category_name_en=doc.get("groupCategoryNameEn"),
        )
        cat_id = resolved["category_id"]
        cat_name = resolved["category_name"]

        group_cat_id = resolved["group_id"]
        group_cat_name = resolved["group_name"]
        group_cat_en = resolved["group_name_en"]

        # Fall back to the simpler JS reference behavior if category-group
        # resolution fails, so a bad group mapping does not block the product.
        group_fk = None
        try:
            group_fk = await self._get_or_create_group(
                conn,
                platform_id=platform_id,
                group_id=group_cat_id,
                group_name=group_cat_name,
                group_name_en=group_cat_en,
                icon=doc.get("groupCategoryIcon"),
            )
        except Exception as exc:
            logging.getLogger("scraper").warning(
                "Group resolution failed for item %s category %s: %s",
                doc.get("itemId"),
                cat_id,
                exc,
            )

        try:
            category_fk = await self._get_or_create_category(
                conn,
                platform_id=platform_id,
                group_fk=group_fk,
                category_id=cat_id,
                category_name=cat_name,
                category_name_en=resolved["category_name_en"],
            )
        except Exception as exc:
            logging.getLogger("scraper").warning(
                "Category upsert with group failed for item %s category %s: %s",
                doc.get("itemId"),
                cat_id,
                exc,
            )
            category_fk = await self._get_or_create_category(
                conn,
                platform_id=platform_id,
                group_fk=None,
                category_id=cat_id,
                category_name=cat_name,
                category_name_en=resolved["category_name_en"],
            )

        if group_cat_id and group_fk:
            try:
                await conn.execute(
                    """UPDATE categories
                       SET group_fk = COALESCE(group_fk, $1),
                           updated_at = NOW()
                       WHERE id = $2""",
                    group_fk, category_fk,
                )
            except Exception as exc:
                logging.getLogger("scraper").warning(
                    "Category group link failed for item %s category %s: %s",
                    doc.get("itemId"),
                    cat_id,
                    exc,
                )

        prod_row = await conn.fetchrow(
            """INSERT INTO products (
                   item_id, platform_id, shop_id, category_fk, title, price, price_usd, image, link,
                   sales_count, location, details_scraped, details_scraped_at,
                   extraction_quality, extracted_at, migration_version, updated_at
               )
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,NOW())
               ON CONFLICT (item_id) DO UPDATE
                 SET platform_id=EXCLUDED.platform_id, title=EXCLUDED.title,
                     price=EXCLUDED.price, price_usd=EXCLUDED.price_usd,
                     updated_at=NOW()
               RETURNING id""".replace("$6", "$6::numeric").replace("$7", "$7::numeric").replace("$10", "$10::numeric"),
            doc.get("itemId"), platform_id, shop_id, category_fk,
            doc.get("title"),
            _to_numeric(doc.get("price")),
            cny_to_usd(_to_numeric(doc.get("price"))) if _to_numeric(doc.get("price")) else None,
            doc.get("image"), doc.get("link"),
            _to_sales_count(doc.get("salesCount")), doc.get("location"),
            doc.get("detailsScraped", False), _to_datetime(doc.get("detailsScrapedAt")),
            doc.get("extractionQuality"), _to_datetime(doc.get("extractedAt")),
            doc.get("migrationVersion", 2),
        )
        product_id = prod_row["id"]

        await conn.execute(
            """INSERT INTO product_search_meta (
                   product_id, category_fk, search_keyword, page_number
               )
               VALUES ($1,$2,$3,$4)
               ON CONFLICT (product_id) DO UPDATE
                 SET category_fk=EXCLUDED.category_fk,
                     search_keyword=EXCLUDED.search_keyword,
                     page_number=EXCLUDED.page_number""",
            product_id,
            category_fk,
            doc.get("searchKeyword"),
            doc.get("pageNumber"),
        )

        if doc.get("detailsScraped") and doc.get("detailedInfo"):
            await self._migrate_detail_info(conn, product_id, doc["detailedInfo"])

    async def _migrate_detail_info(self, conn, product_id: int, di: dict):
        dq = di.get("dataQuality") or {}

        orig_price_cny = _to_numeric(di.get("originalPrice"))
        await conn.execute(
            """INSERT INTO product_details (
                   product_id, full_title, full_description, brand,
                   rating, review_count,
                   sales_volume, original_price, original_price_usd,
                   in_stock, shipping_info,
                   dq_has_title, dq_has_price, dq_has_images, dq_has_variants, dq_has_specs,
                   dq_has_brand, dq_has_reviews, dq_has_description, dq_has_sales_volume,
                   dq_has_shop_name, dq_completeness
               )
               VALUES ($1,$2,$3,$4,$5::numeric,$6::integer,$7,$8::numeric,$9::numeric,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
               ON CONFLICT (product_id) DO UPDATE
                 SET full_title=EXCLUDED.full_title,
                     full_description=EXCLUDED.full_description,
                     brand=EXCLUDED.brand,
                     rating=EXCLUDED.rating,
                     review_count=EXCLUDED.review_count,
                     sales_volume=EXCLUDED.sales_volume,
                     original_price=EXCLUDED.original_price,
                     original_price_usd=EXCLUDED.original_price_usd,
                     in_stock=EXCLUDED.in_stock,
                     shipping_info=EXCLUDED.shipping_info,
                     dq_completeness=EXCLUDED.dq_completeness""",
            product_id,
            di.get("fullTitle"), (di.get("fullDescription") or "")[:10000],
            di.get("brand"),
            _to_numeric(di.get("rating")),
            _to_int(di.get("reviewCount")),
            _to_sales_count(di.get("salesVolume")),
            orig_price_cny,
            cny_to_usd(orig_price_cny) if orig_price_cny is not None else None,
            di.get("inStock"),
            di.get("shippingInfo"),
            dq.get("hasTitle", False),
            dq.get("hasPrice", False),
            dq.get("hasImages", False),
            dq.get("hasVariants", False),
            dq.get("hasSpecs", False),
            dq.get("hasBrand", False),
            dq.get("hasReviews", False),
            dq.get("hasDescription", False),
            dq.get("hasSalesVolume", False),
            dq.get("hasShopName", False),
            dq.get("completeness", 0),
        )

        await conn.execute("DELETE FROM product_images WHERE product_id=$1", product_id)
        for i, url in enumerate(di.get("additionalImages") or []):
            if url:
                await conn.execute(
                    "INSERT INTO product_images (product_id, url, source_type, sort_order) VALUES ($1,$2,'gallery',$3)",
                    product_id, url, i
                )

        await conn.execute("DELETE FROM product_specs WHERE product_id=$1", product_id)
        for key, val in (di.get("specifications") or {}).items():
            if key:
                await conn.execute(
                    "INSERT INTO product_specs (product_id, spec_key, spec_value) VALUES ($1,$2,$3)",
                    product_id, key[:255], str(val)[:2000]
                )

        await conn.execute("DELETE FROM product_variants WHERE product_id=$1", product_id)
        variants = di.get("variants") or {}
        if isinstance(variants, list):
            normalized_variants = {v.get("type"): v.get("options", []) for v in variants if v.get("type")}
        else:
            normalized_variants = variants

        for variant_type, options in (normalized_variants or {}).items():
            if not variant_type or not isinstance(options, list) or not options:
                continue
            variant_row = await conn.fetchrow(
                "INSERT INTO product_variants (product_id, variant_type) VALUES ($1,$2) RETURNING id",
                product_id,
                str(variant_type)[:255],
            )
            variant_id = variant_row["id"]

            for option in options:
                if not option:
                    continue
                value = option if isinstance(option, str) else option.get("value")
                image_url = None if isinstance(option, str) else option.get("image")
                vid = None if isinstance(option, str) else option.get("vid")
                if not value:
                    continue
                await conn.execute(
                    "INSERT INTO variant_options (variant_id, value, image_url, vid) VALUES ($1,$2,$3,$4)",
                    variant_id,
                    str(value)[:500],
                    image_url,
                    vid,
                )

        await conn.execute("DELETE FROM product_guarantees WHERE product_id=$1", product_id)
        for guarantee in di.get("guarantees") or []:
            if guarantee:
                await conn.execute(
                    "INSERT INTO product_guarantees (product_id, guarantee) VALUES ($1,$2)",
                    product_id,
                    str(guarantee)[:500],
                )

    async def _migrate_jobs(self, db, force: bool) -> dict:
        stats = {"total": 0, "migrated": 0, "skipped": 0, "failed": 0}
        stats["total"] = await db.scraping_jobs.count_documents({})

        async for job in db.scraping_jobs.find({}):
            try:
                platform_id = self.get_platform_id(job.get("platform", "taobao"))
                sp = job.get("searchParams") or {}
                cat_label = sp.get("categoryName") or sp.get("keyword") or "unknown"
                resolved = self._canonicalize_category(
                    platform=job.get("platform", "taobao"),
                    category_id=sp.get("categoryId") or self.generate_category_id(cat_label, job.get("platform", "taobao")),
                    category_name=sp.get("categoryName") or sp.get("keyword"),
                    group_category_id=sp.get("groupCategoryId"),
                    group_category_name=sp.get("groupCategoryName"),
                    group_category_name_en=sp.get("groupCategoryNameEn"),
                )
                progress = job.get("progress") or {}
                results = job.get("results") or {}
                async def tx(conn):
                    category_fk = None
                    if resolved["category_id"]:
                        group_fk = await self._get_or_create_group(
                            conn,
                            platform_id=platform_id,
                            group_id=resolved["group_id"],
                            group_name=resolved["group_name"],
                            group_name_en=resolved["group_name_en"],
                            icon=None,
                        )
                        category_fk = await self._get_or_create_category(
                            conn,
                            platform_id=platform_id,
                            group_fk=group_fk,
                            category_id=resolved["category_id"],
                            category_name=resolved["category_name"],
                            category_name_en=resolved["category_name_en"],
                        )

                    await conn.execute(
                        """INSERT INTO scraping_jobs (
                               job_id, platform_id, category_fk, search_type, status,
                               keyword, max_products, max_pages,
                               current_page, products_scraped,
                               total_products, updated_products, error_message,
                               started_at, completed_at, created_at
                           )
                           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
                           ON CONFLICT (job_id) DO UPDATE
                             SET category_fk=EXCLUDED.category_fk,
                                 status=EXCLUDED.status,
                                 products_scraped=EXCLUDED.products_scraped""",
                        job.get("jobId"), platform_id, category_fk,
                        job.get("searchType", "keyword"), job.get("status"),
                        sp.get("keyword"),
                        sp.get("maxProducts"), sp.get("maxPages"),
                        progress.get("currentPage", 0), progress.get("productsScraped", 0),
                        results.get("totalProducts", 0), results.get("updatedProducts", 0),
                        (job.get("error") or "")[:2000] or None,
                        _to_datetime(job.get("startedAt")), _to_datetime(job.get("completedAt")),
                        _to_datetime(job.get("createdAt")) or datetime.now(UTC),
                    )

                await pg_service.with_transaction(tx)
                stats["migrated"] += 1
            except Exception as e:
                stats["failed"] += 1
                print(f"[Migration] Failed job {job.get('jobId')}: {e}")

        return stats

    def generate_category_id(self, label: str, platform: str) -> str:
        prefix = {"taobao": "1", "tmall": "2", "1688": "3"}.get(platform, "9")
        s = (label or "unknown").lower().strip()
        h = 5381
        for ch in s:
            h = ((h << 5) + h) ^ ord(ch)
            h &= 0xFFFFFFFF
        return f"{prefix}{h % 10_000_000:07d}"


mongo_to_pg_service = MongoToPostgresMigrationService()
