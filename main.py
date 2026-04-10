import os
from contextlib import asynccontextmanager

import motor.motor_asyncio
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from routes import scraper, products, migration, pg_migration, debug, price_history, scheduler, webhooks, retry, comparison, categories, logs
from services.postgres_service import pg_service
from services.auth_service import APIKeyMiddleware, is_auth_enabled
from services.proxy_service import proxy_service

load_dotenv()

# Initialize structured logging
from services.logging_service import setup_logging
setup_logging()

# ── MongoDB connection ─────────────────────────────────────────────────────
MONGO_URI = os.getenv("MONGODB_URI")
if not MONGO_URI:
    raise RuntimeError("MONGODB_URI env var is required. Check your .env file.")
mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
db = mongo_client.get_default_database()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("✅ MongoDB connecting...")
    try:
        await mongo_client.admin.command("ping")
        print("✅ MongoDB connected")
    except Exception as e:
        print(f"❌ MongoDB error: {e}")

    print("✅ PostgreSQL connecting...")
    try:
        await pg_service.connect()
        print("✅ PostgreSQL connected")
    except Exception as e:
        print(f"❌ PostgreSQL error: {e}")

    app.state.db = db
    app.state.mongo_client = mongo_client

    try:
        await proxy_service.load_provider_from_db(db)
    except Exception as e:
        print(f"⚠️  Proxy provider load error: {e}")

    # Create price history indexes
    from services.price_history_service import ensure_price_history_indexes
    try:
        await ensure_price_history_indexes(db)
        print("✅ Price history indexes created")
    except Exception as e:
        print(f"⚠️  Price history index error: {e}")

    from models.product import ensure_product_indexes, ensure_enrichment_queue_indexes
    try:
        await ensure_product_indexes(db)
        await ensure_enrichment_queue_indexes(db)
        print("✅ Product indexes created")
    except Exception as e:
        print(f"⚠️  Product index error: {e}")

    # Create category indexes and seed canonical tree
    from services.category_service import ensure_category_indexes, seed_all_platforms
    try:
        await ensure_category_indexes(db)
    except Exception:
        pass
    try:
        await seed_all_platforms(db)
        print("✅ Category tree seeded")
    except Exception as e:
        print(f"⚠️  Category seed error: {e}")

    # Create scheduler indexes and start job scheduler
    from services.scheduler_service import scheduler_service, ensure_scheduler_indexes
    try:
        await ensure_scheduler_indexes(db)
    except Exception:
        pass
    await scheduler_service.start(db)
    print("✅ Scheduler started")

    # Start retry worker
    from services.retry_service import retry_worker, ensure_retry_indexes
    try:
        await ensure_retry_indexes(db)
    except Exception:
        pass
    await retry_worker.start(db)
    print("✅ Retry worker started")

    try:
        await proxy_service.start()
        if proxy_service.enabled:
            print("✅ Proxy service started")
    except Exception as e:
        print(f"⚠️  Proxy service start error: {e}")

    yield

    await scheduler_service.stop()
    await retry_worker.stop()
    await proxy_service.stop()

    # Close any open QR login browser sessions
    from routes.scraper import cleanup_all_qr_sessions
    await cleanup_all_qr_sessions()

    mongo_client.close()
    await pg_service.close()
    print("Connections closed.")


app = FastAPI(
    title="Scraper OS API",
    description="Python/FastAPI scraping pipeline for Taobao, Tmall & 1688",
    version="1.0.0",
    lifespan=lifespan,
)

# ── Middleware ─────────────────────────────────────────────────────────────
app.add_middleware(APIKeyMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routes ─────────────────────────────────────────────────────────────────
app.include_router(scraper.router,      prefix="/api/scraper",      tags=["Scraper"])
app.include_router(products.router,     prefix="/api/products",     tags=["Products"])
app.include_router(migration.router,    prefix="/api/migration",    tags=["Migration"])
app.include_router(pg_migration.router, prefix="/api/pg-migration", tags=["PG Migration"])
app.include_router(debug.router,        prefix="/api/debug",        tags=["Debug"])
app.include_router(price_history.router, prefix="/api/prices",       tags=["Price History"])
app.include_router(scheduler.router,     prefix="/api/schedules",    tags=["Scheduler"])
app.include_router(webhooks.router,      prefix="/api/webhooks",     tags=["Webhooks"])
app.include_router(retry.router,         prefix="/api/retry",        tags=["Retry Queue"])
app.include_router(comparison.router,    prefix="/api/compare",      tags=["Comparison"])
app.include_router(categories.router,    prefix="/api/categories",   tags=["Categories"])
app.include_router(logs.router,          prefix="/api/logs",         tags=["Logs"])


# ── Serve static assets ───────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")


# ── Serve frontend ─────────────────────────────────────────────────────────
@app.get("/", response_class=FileResponse)
async def serve_frontend():
    return FileResponse("static/index.html")


# ── Health check ───────────────────────────────────────────────────────────
@app.get("/health")
async def health(request: Request):
    try:
        await request.app.state.mongo_client.admin.command("ping")
        mongo_status = "connected"
    except Exception:
        mongo_status = "disconnected"

    return {
        "status": "ok",
        "mongodb": mongo_status,
        "timestamp": __import__("datetime").datetime.utcnow().isoformat(),
    }


# ── Global error handler ───────────────────────────────────────────────────
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    import logging
    logging.getLogger("scraper").exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"success": False, "error": "Internal server error"},
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 3000))
    print(f"\n{'='*50}")
    print(f"🚀  Scraper OS  →  http://localhost:{port}")
    print(f"📡  Health      →  http://localhost:{port}/health")
    print(f"🔄  API Docs    →  http://localhost:{port}/docs")
    print(f"{'='*50}\n")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
