-- Migration 003: split category groups into a dedicated table
-- Idempotent and safe to re-run.

CREATE TABLE IF NOT EXISTS category_groups (
    id                SERIAL PRIMARY KEY,
    platform_id       INTEGER REFERENCES platforms(id),
    group_id          TEXT NOT NULL,
    group_name        TEXT,
    group_name_en     TEXT,
    icon              TEXT,
    source            TEXT DEFAULT 'seed',
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (platform_id, group_id)
);

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='category_groups' AND column_name='icon'
    ) THEN
        ALTER TABLE category_groups ADD COLUMN icon TEXT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='group_fk'
    ) THEN
        ALTER TABLE categories ADD COLUMN group_fk INTEGER REFERENCES category_groups(id);
    END IF;
END $$;

-- Backfill category_groups from both legacy level-1 and level-2 category rows.
INSERT INTO category_groups (
    platform_id, group_id, group_name, group_name_en, source, created_at, updated_at
)
SELECT DISTINCT
    src.platform_id,
    src.group_id,
    src.group_name,
    src.group_name_en,
    COALESCE(src.source, 'seed') AS source,
    COALESCE(src.created_at, NOW()) AS created_at,
    COALESCE(src.updated_at, NOW()) AS updated_at
FROM (
    SELECT
        c.platform_id,
        c.category_id AS group_id,
        c.category_name AS group_name,
        c.category_name_en AS group_name_en,
        c.source,
        c.created_at,
        c.updated_at
    FROM categories c
    WHERE EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='level'
    )
    AND c.level = 1

    UNION

    SELECT
        c.platform_id,
        c.group_id,
        c.group_name,
        c.group_name_en,
        c.source,
        c.created_at,
        c.updated_at
    FROM categories c
    WHERE EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='group_id'
    )
    AND c.group_id IS NOT NULL
) AS src
WHERE src.group_id IS NOT NULL
ON CONFLICT (platform_id, group_id) DO UPDATE
SET group_name = COALESCE(EXCLUDED.group_name, category_groups.group_name),
    group_name_en = COALESCE(EXCLUDED.group_name_en, category_groups.group_name_en),
    source = COALESCE(EXCLUDED.source, category_groups.source),
    updated_at = NOW();

-- Attach categories to their group row.
UPDATE categories c
SET group_fk = cg.id
FROM category_groups cg
WHERE c.platform_id = cg.platform_id
  AND c.group_fk IS NULL
  AND (
      (c.category_id = cg.group_id)
      OR (
          EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_name='categories' AND column_name='group_id'
          )
          AND c.group_id = cg.group_id
      )
  );

CREATE INDEX IF NOT EXISTS idx_category_groups_platform_group ON category_groups(platform_id, group_id);
CREATE INDEX IF NOT EXISTS idx_categories_platform_group_fk   ON categories(platform_id, group_fk);

DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='level'
    ) THEN
        DELETE FROM categories WHERE level = 1;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='group_id'
    ) THEN
        ALTER TABLE categories DROP COLUMN group_id;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='group_name'
    ) THEN
        ALTER TABLE categories DROP COLUMN group_name;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='group_name_en'
    ) THEN
        ALTER TABLE categories DROP COLUMN group_name_en;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='level'
    ) THEN
        ALTER TABLE categories DROP COLUMN level;
    END IF;
END $$;
