-- Scraper OS PostgreSQL Schema
-- Safe to re-run (idempotent)

-- Platforms lookup
CREATE TABLE IF NOT EXISTS platforms (
    id   SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

INSERT INTO platforms (name) VALUES ('taobao'),('tmall'),('1688')
ON CONFLICT DO NOTHING;

-- Category groups
CREATE TABLE IF NOT EXISTS category_groups (
    id                SERIAL PRIMARY KEY,
    platform_id       INTEGER REFERENCES platforms(id),
    group_id          TEXT NOT NULL UNIQUE,
    group_name        TEXT,
    group_name_en     TEXT,
    icon              TEXT,
    source            TEXT DEFAULT 'seed',
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Categories
CREATE TABLE IF NOT EXISTS categories (
    id                SERIAL PRIMARY KEY,
    platform_id       INTEGER REFERENCES platforms(id),
    group_fk          INTEGER REFERENCES category_groups(id),
    category_id       TEXT NOT NULL UNIQUE,
    category_name     TEXT,
    category_name_en  TEXT,
    source            TEXT DEFAULT 'seed',
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Ensure normalized category columns exist on older databases before indexes run.
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='group_fk'
    ) THEN
        ALTER TABLE categories ADD COLUMN group_fk INTEGER REFERENCES category_groups(id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='category_name_en'
    ) THEN
        ALTER TABLE categories ADD COLUMN category_name_en TEXT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='source'
    ) THEN
        ALTER TABLE categories ADD COLUMN source TEXT DEFAULT 'seed';
    END IF;
END $$;

-- widen legacy varchar columns to TEXT so migrated product data matches the
-- current schema even on older PostgreSQL databases created by earlier builds
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='shops' AND column_name='shop_name' AND data_type='character varying'
    ) THEN
        ALTER TABLE shops ALTER COLUMN shop_name TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='shops' AND column_name='shop_link' AND data_type='character varying'
    ) THEN
        ALTER TABLE shops ALTER COLUMN shop_link TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='shops' AND column_name='shop_location' AND data_type='character varying'
    ) THEN
        ALTER TABLE shops ALTER COLUMN shop_location TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='shops' AND column_name='shop_age' AND data_type='character varying'
    ) THEN
        ALTER TABLE shops ALTER COLUMN shop_age TYPE TEXT;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='category_id' AND data_type='character varying'
    ) THEN
        ALTER TABLE categories ALTER COLUMN category_id TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='category_name' AND data_type='character varying'
    ) THEN
        ALTER TABLE categories ALTER COLUMN category_name TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='categories' AND column_name='category_name_en' AND data_type='character varying'
    ) THEN
        ALTER TABLE categories ALTER COLUMN category_name_en TYPE TEXT;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='products' AND column_name='title' AND data_type='character varying'
    ) THEN
        ALTER TABLE products ALTER COLUMN title TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='products' AND column_name='image' AND data_type='character varying'
    ) THEN
        ALTER TABLE products ALTER COLUMN image TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='products' AND column_name='link' AND data_type='character varying'
    ) THEN
        ALTER TABLE products ALTER COLUMN link TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='products' AND column_name='sales_count' AND data_type='character varying'
    ) THEN
        ALTER TABLE products ALTER COLUMN sales_count TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='products' AND column_name='location' AND data_type='character varying'
    ) THEN
        ALTER TABLE products ALTER COLUMN location TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_search_meta' AND column_name='search_keyword' AND data_type='character varying'
    ) THEN
        ALTER TABLE product_search_meta ALTER COLUMN search_keyword TYPE TEXT;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_details' AND column_name='full_title' AND data_type='character varying'
    ) THEN
        ALTER TABLE product_details ALTER COLUMN full_title TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_details' AND column_name='full_description' AND data_type='character varying'
    ) THEN
        ALTER TABLE product_details ALTER COLUMN full_description TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_details' AND column_name='brand' AND data_type='character varying'
    ) THEN
        ALTER TABLE product_details ALTER COLUMN brand TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_details' AND column_name='sales_volume' AND data_type='character varying'
    ) THEN
        ALTER TABLE product_details ALTER COLUMN sales_volume TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_details' AND column_name='shipping_info' AND data_type='character varying'
    ) THEN
        ALTER TABLE product_details ALTER COLUMN shipping_info TYPE TEXT;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_images' AND column_name='url' AND data_type='character varying'
    ) THEN
        ALTER TABLE product_images ALTER COLUMN url TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_specs' AND column_name='spec_key' AND data_type='character varying'
    ) THEN
        ALTER TABLE product_specs ALTER COLUMN spec_key TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_specs' AND column_name='spec_value' AND data_type='character varying'
    ) THEN
        ALTER TABLE product_specs ALTER COLUMN spec_value TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_variants' AND column_name='variant_type' AND data_type='character varying'
    ) THEN
        ALTER TABLE product_variants ALTER COLUMN variant_type TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='variant_options' AND column_name='value' AND data_type='character varying'
    ) THEN
        ALTER TABLE variant_options ALTER COLUMN value TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='variant_options' AND column_name='image_url' AND data_type='character varying'
    ) THEN
        ALTER TABLE variant_options ALTER COLUMN image_url TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='variant_options' AND column_name='vid' AND data_type='character varying'
    ) THEN
        ALTER TABLE variant_options ALTER COLUMN vid TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_guarantees' AND column_name='guarantee' AND data_type='character varying'
    ) THEN
        ALTER TABLE product_guarantees ALTER COLUMN guarantee TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='shop_badges' AND column_name='badge' AND data_type='character varying'
    ) THEN
        ALTER TABLE shop_badges ALTER COLUMN badge TYPE TEXT;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='scraping_jobs' AND column_name='job_id' AND data_type='character varying'
    ) THEN
        ALTER TABLE scraping_jobs ALTER COLUMN job_id TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='scraping_jobs' AND column_name='search_type' AND data_type='character varying'
    ) THEN
        ALTER TABLE scraping_jobs ALTER COLUMN search_type TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='scraping_jobs' AND column_name='status' AND data_type='character varying'
    ) THEN
        ALTER TABLE scraping_jobs ALTER COLUMN status TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='scraping_jobs' AND column_name='keyword' AND data_type='character varying'
    ) THEN
        ALTER TABLE scraping_jobs ALTER COLUMN keyword TYPE TEXT;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='scraping_jobs' AND column_name='error_message' AND data_type='character varying'
    ) THEN
        ALTER TABLE scraping_jobs ALTER COLUMN error_message TYPE TEXT;
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='category_groups' AND column_name='icon'
    ) THEN
        ALTER TABLE category_groups ADD COLUMN icon TEXT;
    END IF;
END $$;

-- Shops
CREATE TABLE IF NOT EXISTS shops (
    id            SERIAL PRIMARY KEY,
    platform_id   INTEGER REFERENCES platforms(id),
    shop_name     TEXT NOT NULL,
    shop_link     TEXT,
    shop_rating   NUMERIC(3,2),
    shop_location TEXT,
    shop_age      TEXT,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (platform_id, shop_name)
);

-- Seller info
CREATE TABLE IF NOT EXISTS seller_info (
    id                     SERIAL PRIMARY KEY,
    shop_id                INTEGER UNIQUE REFERENCES shops(id) ON DELETE CASCADE,
    positive_feedback_rate INTEGER,
    has_vip                BOOLEAN DEFAULT FALSE,
    avg_delivery_time      TEXT,
    avg_refund_time        TEXT
);

-- Shop badges
CREATE TABLE IF NOT EXISTS shop_badges (
    id      SERIAL PRIMARY KEY,
    shop_id INTEGER REFERENCES shops(id) ON DELETE CASCADE,
    badge   TEXT NOT NULL,
    UNIQUE (shop_id, badge)
);

-- Products
CREATE TABLE IF NOT EXISTS products (
    id                    SERIAL PRIMARY KEY,
    item_id               TEXT UNIQUE NOT NULL,
    platform_id           INTEGER REFERENCES platforms(id),
    shop_id               INTEGER REFERENCES shops(id),
    category_fk           INTEGER REFERENCES categories(id),
    title                 TEXT NOT NULL,
    price                 NUMERIC(12,2),
    price_usd             NUMERIC(12,2),
    image                 TEXT,
    link                  TEXT,
    sales_count           TEXT,
    location              TEXT,
    details_scraped       BOOLEAN DEFAULT FALSE,
    details_scraped_at    TIMESTAMPTZ,
    extraction_quality    INTEGER,
    extracted_at          TIMESTAMPTZ,
    migration_version     INTEGER DEFAULT 2,
    created_at            TIMESTAMPTZ DEFAULT NOW(),
    updated_at            TIMESTAMPTZ DEFAULT NOW()
);

-- Product search metadata
CREATE TABLE IF NOT EXISTS product_search_meta (
    id            SERIAL PRIMARY KEY,
    product_id    INTEGER UNIQUE REFERENCES products(id) ON DELETE CASCADE,
    category_fk   INTEGER REFERENCES categories(id),
    search_keyword TEXT,
    page_number   INTEGER
);

-- Product details (one-to-one)
CREATE TABLE IF NOT EXISTS product_details (
    id                  SERIAL PRIMARY KEY,
    product_id          INTEGER UNIQUE REFERENCES products(id) ON DELETE CASCADE,
    full_title          TEXT,
    full_description    TEXT,
    brand               TEXT,
    rating              NUMERIC(4,2),
    review_count        INTEGER,
    sales_volume        TEXT,
    original_price      NUMERIC(12,2),
    original_price_usd  NUMERIC(12,2),
    in_stock            BOOLEAN,
    shipping_info       TEXT,
    dq_has_title        BOOLEAN DEFAULT FALSE,
    dq_has_price        BOOLEAN DEFAULT FALSE,
    dq_has_images       BOOLEAN DEFAULT FALSE,
    dq_has_variants     BOOLEAN DEFAULT FALSE,
    dq_has_specs        BOOLEAN DEFAULT FALSE,
    dq_has_brand        BOOLEAN DEFAULT FALSE,
    dq_has_reviews      BOOLEAN DEFAULT FALSE,
    dq_has_description  BOOLEAN DEFAULT FALSE,
    dq_has_sales_volume BOOLEAN DEFAULT FALSE,
    dq_has_shop_name    BOOLEAN DEFAULT FALSE,
    dq_completeness     INTEGER DEFAULT 0
);

-- Product images
CREATE TABLE IF NOT EXISTS product_images (
    id          SERIAL PRIMARY KEY,
    product_id  INTEGER REFERENCES products(id) ON DELETE CASCADE,
    url         TEXT NOT NULL,
    source_type TEXT DEFAULT 'gallery',
    sort_order  INTEGER DEFAULT 0
);

-- Product specifications
CREATE TABLE IF NOT EXISTS product_specs (
    id         SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
    spec_key   TEXT NOT NULL,
    spec_value TEXT
);

-- Product variants
CREATE TABLE IF NOT EXISTS product_variants (
    id           SERIAL PRIMARY KEY,
    product_id   INTEGER REFERENCES products(id) ON DELETE CASCADE,
    variant_type TEXT NOT NULL
);

-- Variant options
CREATE TABLE IF NOT EXISTS variant_options (
    id         SERIAL PRIMARY KEY,
    variant_id INTEGER REFERENCES product_variants(id) ON DELETE CASCADE,
    value      TEXT NOT NULL,
    image_url  TEXT,
    vid        TEXT
);

-- Product guarantees
CREATE TABLE IF NOT EXISTS product_guarantees (
    id         SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
    guarantee  TEXT NOT NULL
);

-- Scraping jobs
CREATE TABLE IF NOT EXISTS scraping_jobs (
    id               SERIAL PRIMARY KEY,
    job_id           TEXT UNIQUE NOT NULL,
    platform_id      INTEGER REFERENCES platforms(id),
    category_fk      INTEGER REFERENCES categories(id),
    search_type      TEXT DEFAULT 'keyword',
    status           TEXT NOT NULL,
    keyword          TEXT,
    max_products     INTEGER,
    max_pages        INTEGER,
    current_page     INTEGER DEFAULT 0,
    products_scraped INTEGER DEFAULT 0,
    details_scraped  INTEGER DEFAULT 0,
    details_failed   INTEGER DEFAULT 0,
    total_products   INTEGER DEFAULT 0,
    updated_products INTEGER DEFAULT 0,
    error_message    TEXT,
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Migration log
CREATE TABLE IF NOT EXISTS mongo_migration_log (
    id             SERIAL PRIMARY KEY,
    mongo_item_id  TEXT UNIQUE NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    error_message  TEXT,
    migrated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_products_platform    ON products(platform_id);
CREATE INDEX IF NOT EXISTS idx_products_shop        ON products(shop_id);
CREATE INDEX IF NOT EXISTS idx_products_category    ON products(category_fk);
CREATE INDEX IF NOT EXISTS idx_products_details     ON products(details_scraped);
CREATE INDEX IF NOT EXISTS idx_products_quality     ON products(extraction_quality DESC);
CREATE INDEX IF NOT EXISTS idx_category_groups_platform_group ON category_groups(platform_id, group_id);
CREATE INDEX IF NOT EXISTS idx_categories_platform_group_fk   ON categories(platform_id, group_fk);
CREATE INDEX IF NOT EXISTS idx_product_images_pid   ON product_images(product_id);
CREATE INDEX IF NOT EXISTS idx_product_specs_pid    ON product_specs(product_id);
CREATE INDEX IF NOT EXISTS idx_scraping_jobs_status ON scraping_jobs(status);
CREATE INDEX IF NOT EXISTS idx_migration_log_status ON mongo_migration_log(status);

-- ─────────────────────────────────────────────────────────────────────────────
-- Safe column migrations for existing databases (idempotent)
-- ─────────────────────────────────────────────────────────────────────────────

-- products: convert price to numeric, add price_usd
DO $$ BEGIN
    -- price: TEXT → NUMERIC
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='products' AND column_name='price' AND data_type='text'
    ) THEN
        ALTER TABLE products
            ALTER COLUMN price TYPE NUMERIC(12,2) USING NULLIF(regexp_replace(price,'[^0-9.]','','g'),'')::NUMERIC(12,2);
    END IF;

    -- price_usd: new column
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='products' AND column_name='price_usd'
    ) THEN
        ALTER TABLE products ADD COLUMN price_usd NUMERIC(12,2);
    END IF;
END $$;

-- product_details: convert rating/review_count/original_price to numeric, add original_price_usd
DO $$ BEGIN
    -- rating: TEXT → NUMERIC
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_details' AND column_name='rating' AND data_type='text'
    ) THEN
        ALTER TABLE product_details
            ALTER COLUMN rating TYPE NUMERIC(4,2) USING NULLIF(regexp_replace(rating,'[^0-9.]','','g'),'')::NUMERIC(4,2);
    END IF;

    -- review_count: TEXT → INTEGER
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_details' AND column_name='review_count' AND data_type='text'
    ) THEN
        ALTER TABLE product_details
            ALTER COLUMN review_count TYPE INTEGER USING NULLIF(regexp_replace(review_count,'[^0-9]','','g'),'')::INTEGER;
    END IF;

    -- original_price: TEXT → NUMERIC
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_details' AND column_name='original_price' AND data_type='text'
    ) THEN
        ALTER TABLE product_details
            ALTER COLUMN original_price TYPE NUMERIC(12,2) USING NULLIF(regexp_replace(original_price,'[^0-9.]','','g'),'')::NUMERIC(12,2);
    END IF;

    -- original_price_usd: new column
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='product_details' AND column_name='original_price_usd'
    ) THEN
        ALTER TABLE product_details ADD COLUMN original_price_usd NUMERIC(12,2);
    END IF;
END $$;
