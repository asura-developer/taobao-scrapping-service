-- Migration 005: merge shared categories across platforms and drop duplicate child-table names

-- Remove old per-platform uniqueness so canonical global IDs can be reused.
ALTER TABLE category_groups DROP CONSTRAINT IF EXISTS category_groups_platform_id_group_id_key;
ALTER TABLE categories DROP CONSTRAINT IF EXISTS categories_platform_id_category_id_key;

WITH ranked_groups AS (
    SELECT
        id,
        group_id,
        MIN(id) OVER (PARTITION BY group_id) AS keep_id
    FROM category_groups
    WHERE group_id IS NOT NULL
      AND group_id <> ''
),
group_dupes AS (
    SELECT id AS old_id, keep_id
    FROM ranked_groups
    WHERE id <> keep_id
)
UPDATE categories c
SET group_fk = gd.keep_id
FROM group_dupes gd
WHERE c.group_fk = gd.old_id;

WITH ranked_groups AS (
    SELECT
        id,
        group_id,
        MIN(id) OVER (PARTITION BY group_id) AS keep_id
    FROM category_groups
    WHERE group_id IS NOT NULL
      AND group_id <> ''
),
group_dupes AS (
    SELECT id AS old_id, keep_id
    FROM ranked_groups
    WHERE id <> keep_id
)
DELETE FROM category_groups cg
USING group_dupes gd
WHERE cg.id = gd.old_id;

-- Merge category groups globally by normalized display name.
WITH ranked_groups AS (
    SELECT
        id,
        lower(trim(COALESCE(NULLIF(group_name_en, ''), NULLIF(group_name, '')))) AS dedupe_name,
        MIN(id) OVER (
            PARTITION BY lower(trim(COALESCE(NULLIF(group_name_en, ''), NULLIF(group_name, ''))))
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
        lower(trim(COALESCE(NULLIF(group_name_en, ''), NULLIF(group_name, '')))) AS dedupe_name,
        MIN(id) OVER (
            PARTITION BY lower(trim(COALESCE(NULLIF(group_name_en, ''), NULLIF(group_name, ''))))
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

WITH ranked_categories AS (
    SELECT
        id,
        category_id,
        MIN(id) OVER (PARTITION BY category_id) AS keep_id
    FROM categories
    WHERE category_id IS NOT NULL
      AND category_id <> ''
),
category_dupes AS (
    SELECT id AS old_id, keep_id
    FROM ranked_categories
    WHERE id <> keep_id
)
UPDATE products p
SET category_fk = cd.keep_id
FROM category_dupes cd
WHERE p.category_fk = cd.old_id;

WITH ranked_categories AS (
    SELECT
        id,
        category_id,
        MIN(id) OVER (PARTITION BY category_id) AS keep_id
    FROM categories
    WHERE category_id IS NOT NULL
      AND category_id <> ''
),
category_dupes AS (
    SELECT id AS old_id, keep_id
    FROM ranked_categories
    WHERE id <> keep_id
)
UPDATE product_search_meta psm
SET category_fk = cd.keep_id
FROM category_dupes cd
WHERE psm.category_fk = cd.old_id;

WITH ranked_categories AS (
    SELECT
        id,
        category_id,
        MIN(id) OVER (PARTITION BY category_id) AS keep_id
    FROM categories
    WHERE category_id IS NOT NULL
      AND category_id <> ''
),
category_dupes AS (
    SELECT id AS old_id, keep_id
    FROM ranked_categories
    WHERE id <> keep_id
)
UPDATE scraping_jobs sj
SET category_fk = cd.keep_id
FROM category_dupes cd
WHERE sj.category_fk = cd.old_id;

WITH ranked_categories AS (
    SELECT
        id,
        category_id,
        MIN(id) OVER (PARTITION BY category_id) AS keep_id
    FROM categories
    WHERE category_id IS NOT NULL
      AND category_id <> ''
),
category_dupes AS (
    SELECT id AS old_id, keep_id
    FROM ranked_categories
    WHERE id <> keep_id
)
DELETE FROM categories c
USING category_dupes cd
WHERE c.id = cd.old_id;

-- Merge categories globally by normalized display name.
WITH ranked_categories AS (
    SELECT
        id,
        lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, '')))) AS dedupe_name,
        MIN(id) OVER (
            PARTITION BY lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, ''))))
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
        lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, '')))) AS dedupe_name,
        MIN(id) OVER (
            PARTITION BY lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, ''))))
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
        lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, '')))) AS dedupe_name,
        MIN(id) OVER (
            PARTITION BY lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, ''))))
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
        lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, '')))) AS dedupe_name,
        MIN(id) OVER (
            PARTITION BY lower(trim(COALESCE(NULLIF(category_name_en, ''), NULLIF(category_name, ''))))
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

CREATE UNIQUE INDEX IF NOT EXISTS uq_category_groups_group_id ON category_groups(group_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_categories_category_id ON categories(category_id);

-- Drop duplicate copied fields from child tables. Canonical names now live only in categories/category_groups.
ALTER TABLE products DROP COLUMN IF EXISTS group_category_id;
ALTER TABLE products DROP COLUMN IF EXISTS group_category_name;
ALTER TABLE products DROP COLUMN IF EXISTS group_category_name_en;

ALTER TABLE product_search_meta DROP COLUMN IF EXISTS category_id;
ALTER TABLE product_search_meta DROP COLUMN IF EXISTS category_name;

ALTER TABLE scraping_jobs DROP COLUMN IF EXISTS category_id;
ALTER TABLE scraping_jobs DROP COLUMN IF EXISTS category_name;
