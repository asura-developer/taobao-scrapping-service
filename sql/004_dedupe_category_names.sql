-- Migration 004: collapse duplicate category/group display names per platform
-- Keeps the lowest id row as the canonical row and repoints foreign keys.

-- Merge duplicate category groups by normalized display name.
-- First enrich the canonical row with any missing values from duplicates.
WITH ranked_groups AS (
    SELECT
        id,
        platform_id,
        group_id,
        group_name,
        group_name_en,
        icon,
        lower(trim(COALESCE(NULLIF(group_name_en, ''), NULLIF(group_name, '')))) AS dedupe_name,
        MIN(id) OVER (
            PARTITION BY platform_id, lower(trim(COALESCE(NULLIF(group_name_en, ''), NULLIF(group_name, ''))))
        ) AS keep_id
    FROM category_groups
)
UPDATE category_groups keep
SET group_name = COALESCE(keep.group_name, src.group_name),
    group_name_en = COALESCE(keep.group_name_en, src.group_name_en),
    icon = COALESCE(keep.icon, src.icon),
    updated_at = NOW()
FROM ranked_groups src
WHERE src.keep_id = keep.id
  AND src.id <> keep.id;

WITH ranked_groups AS (
    SELECT
        id,
        platform_id,
        lower(trim(COALESCE(NULLIF(group_name_en, ''), NULLIF(group_name, '')))) AS dedupe_name,
        MIN(id) OVER (
            PARTITION BY platform_id, lower(trim(COALESCE(NULLIF(group_name_en, ''), NULLIF(group_name, ''))))
        ) AS keep_id
    FROM category_groups
),
group_dupes AS (
    SELECT id AS old_id, keep_id
    FROM ranked_groups
    WHERE dedupe_name IS NOT NULL
      AND dedupe_name <> ''
      AND id <> keep_id
)
UPDATE categories c
SET group_fk = gd.keep_id
FROM group_dupes gd
WHERE c.group_fk = gd.old_id;

WITH ranked_groups AS (
    SELECT
        id,
        platform_id,
        lower(trim(COALESCE(NULLIF(group_name_en, ''), NULLIF(group_name, '')))) AS dedupe_name,
        MIN(id) OVER (
            PARTITION BY platform_id, lower(trim(COALESCE(NULLIF(group_name_en, ''), NULLIF(group_name, ''))))
        ) AS keep_id
    FROM category_groups
),
group_dupes AS (
    SELECT id AS old_id, keep_id
    FROM ranked_groups
    WHERE dedupe_name IS NOT NULL
      AND dedupe_name <> ''
      AND id <> keep_id
)
DELETE FROM category_groups cg
USING group_dupes gd
WHERE cg.id = gd.old_id;

-- Merge duplicate categories by normalized display name.
-- First enrich the canonical row with any missing values from duplicates.
WITH ranked_categories AS (
    SELECT
        id,
        platform_id,
        group_fk,
        category_name,
        category_name_en,
        lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, '')))) AS dedupe_name,
        MIN(id) OVER (
            PARTITION BY platform_id, lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, ''))))
        ) AS keep_id
    FROM categories
)
UPDATE categories keep
SET group_fk = COALESCE(keep.group_fk, src.group_fk),
    category_name = COALESCE(keep.category_name, src.category_name),
    category_name_en = COALESCE(keep.category_name_en, src.category_name_en),
    updated_at = NOW()
FROM ranked_categories src
WHERE src.keep_id = keep.id
  AND src.id <> keep.id;

WITH ranked_categories AS (
    SELECT
        id,
        platform_id,
        lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, '')))) AS dedupe_name,
        MIN(id) OVER (
            PARTITION BY platform_id, lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, ''))))
        ) AS keep_id
    FROM categories
),
category_dupes AS (
    SELECT id AS old_id, keep_id
    FROM ranked_categories
    WHERE dedupe_name IS NOT NULL
      AND dedupe_name <> ''
      AND id <> keep_id
)
UPDATE products p
SET category_fk = cd.keep_id
FROM category_dupes cd
WHERE p.category_fk = cd.old_id;

WITH ranked_categories AS (
    SELECT
        id,
        platform_id,
        lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, '')))) AS dedupe_name,
        MIN(id) OVER (
            PARTITION BY platform_id, lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, ''))))
        ) AS keep_id
    FROM categories
),
category_dupes AS (
    SELECT id AS old_id, keep_id
    FROM ranked_categories
    WHERE dedupe_name IS NOT NULL
      AND dedupe_name <> ''
      AND id <> keep_id
)
UPDATE product_search_meta psm
SET category_fk = cd.keep_id
FROM category_dupes cd
WHERE psm.category_fk = cd.old_id;

WITH ranked_categories AS (
    SELECT
        id,
        platform_id,
        lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, '')))) AS dedupe_name,
        MIN(id) OVER (
            PARTITION BY platform_id, lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, ''))))
        ) AS keep_id
    FROM categories
),
category_dupes AS (
    SELECT id AS old_id, keep_id
    FROM ranked_categories
    WHERE dedupe_name IS NOT NULL
      AND dedupe_name <> ''
      AND id <> keep_id
)
UPDATE scraping_jobs sj
SET category_fk = cd.keep_id
FROM category_dupes cd
WHERE sj.category_fk = cd.old_id;

WITH ranked_categories AS (
    SELECT
        id,
        platform_id,
        lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, '')))) AS dedupe_name,
        MIN(id) OVER (
            PARTITION BY platform_id, lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, ''))))
        ) AS keep_id
    FROM categories
),
category_dupes AS (
    SELECT id AS old_id, keep_id
    FROM ranked_categories
    WHERE dedupe_name IS NOT NULL
      AND dedupe_name <> ''
      AND id <> keep_id
)
DELETE FROM categories c
USING category_dupes cd
WHERE c.id = cd.old_id;
