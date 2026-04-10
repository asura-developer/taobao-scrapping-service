import subprocess
import signal
from pathlib import Path

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from utils.serializer import clean

router = APIRouter()

# ── MCP server process tracking ───────────────────────────────────────────
_mcp_process: subprocess.Popen | None = None


@router.get("/job/{job_id}")
async def debug_job(request: Request, job_id: str):
    db = request.app.state.db
    job = await db.scraping_jobs.find_one({"jobId": job_id}, {"_id": 0})
    if not job:
        raise HTTPException(404, "Job not found")

    product_count = await db.products.count_documents(
        {"searchKeyword": job.get("searchParams", {}).get("keyword")}
    ) if job.get("searchParams", {}).get("keyword") else 0

    return JSONResponse({
        "success": True,
        "data": {
            "job": clean(job),
            "relatedProductCount": product_count,
        },
    })


@router.get("/jobs/failed")
async def failed_jobs(request: Request, limit: int = 20):
    db = request.app.state.db
    cursor = (
        db.scraping_jobs.find({"status": "failed"}, {"_id": 0})
        .sort("createdAt", -1)
        .limit(limit)
    )
    jobs = await cursor.to_list(length=limit)
    return JSONResponse({"success": True, "data": clean(jobs), "count": len(jobs)})


@router.get("/test-connection")
async def test_connection(request: Request):
    results = {}

    # Test MongoDB
    try:
        await request.app.state.mongo_client.admin.command("ping")
        results["mongodb"] = {"status": "connected"}
    except Exception as e:
        results["mongodb"] = {"status": "error", "error": str(e)}

    # Test Playwright browser launch
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
            version = browser.version
            await browser.close()
            results["browser"] = {"status": "ok", "version": version}
    except Exception as e:
        results["browser"] = {"status": "error", "error": str(e)}

    # Test PostgreSQL
    try:
        from services.postgres_service import pg_service
        rows = await pg_service.query("SELECT 1 AS ok")
        results["postgresql"] = {"status": "connected"}
    except Exception as e:
        results["postgresql"] = {"status": "error", "error": str(e)}

    return JSONResponse({"success": True, "data": results})


@router.post("/clear-failed-jobs")
async def clear_failed_jobs(request: Request):
    db = request.app.state.db
    result = await db.scraping_jobs.delete_many({"status": "failed"})
    return JSONResponse({
        "success": True,
        "data": {"deletedCount": result.deleted_count},
        "message": f"Deleted {result.deleted_count} failed jobs",
    })


# ── Scrapling MCP server management ─────────────────────────────────────

def _find_scrapling_cli() -> str | None:
    project_root = Path(__file__).parent.parent
    for venv in [".venv", "scrapling-env"]:
        path = project_root / venv / "bin" / "scrapling"
        if path.exists():
            return str(path)
    import shutil
    return shutil.which("scrapling")


@router.post("/mcp/start")
async def start_mcp_server(mode: str = "http", port: int = 8000):
    global _mcp_process
    if _mcp_process and _mcp_process.poll() is None:
        return JSONResponse({
            "success": True,
            "data": {"pid": _mcp_process.pid, "status": "already_running"},
            "message": "MCP server is already running",
        })

    cli = _find_scrapling_cli()
    if not cli:
        raise HTTPException(500, "scrapling CLI not found. Run: pip install 'scrapling[ai]'")

    cmd = [cli, "mcp"]
    if mode == "http":
        cmd += ["--http", "--port", str(port)]

    _mcp_process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return JSONResponse({
        "success": True,
        "data": {"pid": _mcp_process.pid, "mode": mode, "port": port if mode == "http" else None},
        "message": f"MCP server started in {mode} mode",
    })


@router.post("/mcp/stop")
async def stop_mcp_server():
    global _mcp_process
    if not _mcp_process or _mcp_process.poll() is not None:
        _mcp_process = None
        return JSONResponse({"success": True, "message": "MCP server is not running"})

    _mcp_process.send_signal(signal.SIGTERM)
    _mcp_process.wait(timeout=10)
    pid = _mcp_process.pid
    _mcp_process = None
    return JSONResponse({"success": True, "data": {"pid": pid}, "message": "MCP server stopped"})


@router.get("/mcp/status")
async def mcp_status():
    global _mcp_process
    if _mcp_process and _mcp_process.poll() is None:
        return JSONResponse({
            "success": True,
            "data": {"status": "running", "pid": _mcp_process.pid},
        })
    return JSONResponse({
        "success": True,
        "data": {"status": "stopped"},
    })


@router.get("/mcp/config")
async def mcp_config():
    """Return the Claude Code MCP configuration for Scrapling."""
    cli = _find_scrapling_cli()
    return JSONResponse({
        "success": True,
        "data": {
            "claude_code_command": f"claude mcp add ScraplingServer {cli} mcp" if cli else None,
            "claude_desktop_config": {
                "mcpServers": {
                    "ScraplingServer": {
                        "command": cli or "/path/to/scrapling",
                        "args": ["mcp"],
                    }
                }
            },
            "mcp_tools": [
                "get - HTTP GET with browser TLS fingerprint impersonation",
                "bulk_get - Async parallel HTTP GET for multiple URLs",
                "fetch - Dynamic content via Playwright Chromium browser",
                "bulk_fetch - Parallel dynamic fetching in multiple tabs",
                "stealthy_fetch - Anti-bot bypass (Cloudflare, etc.)",
                "bulk_stealthy_fetch - Parallel stealthy fetching",
                "open_session - Create persistent browser session",
                "close_session - Close a browser session",
                "list_sessions - List all active sessions",
            ],
            "key_parameters": {
                "extraction_type": "markdown | html | text",
                "css_selector": "Target specific elements (reduces AI token usage)",
                "main_content_only": "Extract only body content (default True)",
                "proxy": "Proxy support for requests",
                "session_id": "Reuse an existing browser session",
            },
        },
    })
