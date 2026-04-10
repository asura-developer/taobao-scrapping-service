-- Migration 002: Add category hierarchy columns
-- Idempotent — safe to re-run on existing databases.

DO $$ BEGIN

    -- group_id: parent group slug (e.g. "womens_clothing")
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='group_id'
    ) THEN
        ALTER TABLE categories ADD COLUMN group_id TEXT;
    END IF;

    -- group_name: parent group Chinese name
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='group_name'
    ) THEN
        ALTER TABLE categories ADD COLUMN group_name TEXT;
    END IF;

    -- group_name_en: parent group English name
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='group_name_en'
    ) THEN
        ALTER TABLE categories ADD COLUMN group_name_en TEXT;
    END IF;

    -- category_name_en: sub-category English name
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='category_name_en'
    ) THEN
        ALTER TABLE categories ADD COLUMN category_name_en TEXT;
    END IF;

    -- level: 1 = group category, 2 = sub-category
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='level'
    ) THEN
        ALTER TABLE categories ADD COLUMN level SMALLINT DEFAULT 2;
    END IF;

    -- source: "seed" | "discovered"
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='source'
    ) THEN
        ALTER TABLE categories ADD COLUMN source TEXT DEFAULT 'seed';
    END IF;

END $$;

-- Indexes for hierarchy queries
CREATE INDEX IF NOT EXISTS idx_categories_platform_level  ON categories(platform_id, level);
CREATE INDEX IF NOT EXISTS idx_categories_platform_group  ON categories(platform_id, group_id);

-- products: add group_category_id and group_category_name columns
DO $$ BEGIN

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='products' AND column_name='group_category_id'
    ) THEN
        ALTER TABLE products ADD COLUMN group_category_id TEXT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='products' AND column_name='group_category_name'
    ) THEN
        ALTER TABLE products ADD COLUMN group_category_name TEXT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='products' AND column_name='group_category_name_en'
    ) THEN
        ALTER TABLE products ADD COLUMN group_category_name_en TEXT;
    END IF;

END $$;

-- product_search_meta: add group columns
DO $$ BEGIN

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_search_meta' AND column_name='group_category_id'
    ) THEN
        ALTER TABLE product_search_meta ADD COLUMN group_category_id TEXT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_search_meta' AND column_name='group_category_name'
    ) THEN
        ALTER TABLE product_search_meta ADD COLUMN group_category_name TEXT;
    END IF;

END $$;
