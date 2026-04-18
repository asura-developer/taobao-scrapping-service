from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from services.scraper_service import scraper_service
from services.translate_service import translate_detail, should_translate
from services.proxy_service import proxy_service
from services.image_service import download_product_images, download_batch_images, get_image_stats
from utils.serializer import clean

router = APIRouter()


SUPPORTED_LANGUAGES = ("en", "zh", "th", "ja", "ko", "ru")
_FRESH_LOGIN_TTL_SEC = 180
_fresh_login_grants: dict[str, dict] = {}


def _session_key(platform: str) -> str:
    if platform in ("taobao", "tmall"):
        return "taobao"
    if platform == "alibaba":
        return "alibaba"
    return "1688"


def _grant_fresh_login(platform: str, cookie_count: int = 0):
    key = _session_key(platform)
    _fresh_login_grants[key] = {
        "platform": platform,
        "grantedAt": __import__("time").time(),
        "cookieCount": cookie_count,
    }


def _consume_fresh_login(platform: str) -> bool:
    key = _session_key(platform)
    grant = _fresh_login_grants.get(key)
    if not grant:
        return False
    now = __import__("time").time()
    if now - grant.get("grantedAt", 0) > _FRESH_LOGIN_TTL_SEC:
        _fresh_login_grants.pop(key, None)
        return False
    _fresh_login_grants.pop(key, None)
    return True


def _clear_fresh_login(platform: str):
    _fresh_login_grants.pop(_session_key(platform), None)


def _require_fresh_login(platform: str):
    if not _consume_fresh_login(platform):
        raise HTTPException(
            409,
            f"Fresh QR login required for {platform}. Start /api/scraper/qr-login/{platform} and complete login before creating the job.",
        )


class SearchBody(BaseModel):
    platform: str
    keyword: Optional[str] = None
    categoryName: Optional[str] = None
    maxProducts: int = 100
    maxPages: int = 10
    startPage: int = 1
    includeDetails: bool = False
    language: str = "en"
    manualCaptcha: bool = False


class CategoryBody(BaseModel):
    platform: str
    categoryId: str
    categoryName: Optional[str] = None
    groupCategoryId: Optional[str] = None
    groupCategoryName: Optional[str] = None
    groupCategoryNameEn: Optional[str] = None
    maxProducts: int = 100
    maxPages: int = 10
    startPage: int = 1
    includeDetails: bool = False
    language: str = "en"
    manualCaptcha: bool = False


@router.post("/search")
async def search(request: Request, body: SearchBody):
    if body.platform not in ("taobao", "tmall", "1688", "alibaba"):
        raise HTTPException(400, "Invalid platform. Must be: taobao, tmall, 1688, or alibaba")
    if body.language not in SUPPORTED_LANGUAGES:
        raise HTTPException(400, f"Invalid language. Must be one of: {', '.join(SUPPORTED_LANGUAGES)}")
    search_query = body.keyword or body.categoryName
    if not search_query:
        raise HTTPException(400, "Either keyword or categoryName is required")
    if body.platform != "alibaba":
        _require_fresh_login(body.platform)

    db = request.app.state.db
    use_session = body.platform != "alibaba"
    try:
        result = await scraper_service.start_job(db, {
            "platform": body.platform,
            "keyword": search_query,
            "categoryName": body.categoryName or None,
            "searchType": "keyword",
            "maxProducts": body.maxProducts,
            "maxPages": body.maxPages,
            "startPage": body.startPage,
            "includeDetails": body.includeDetails,
            "language": body.language,
            "manualCaptcha": body.manualCaptcha,
            "useSession": use_session,
            "clearCookiesOnComplete": use_session,
        })
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return JSONResponse({"success": True, "data": result, "message": "Scraping job started"})


@router.post("/category")
async def category(request: Request, body: CategoryBody):
    if body.platform not in ("taobao", "tmall", "1688", "alibaba"):
        raise HTTPException(400, "Invalid platform. Must be: taobao, tmall, 1688, or alibaba")
    if body.language not in SUPPORTED_LANGUAGES:
        raise HTTPException(400, f"Invalid language. Must be one of: {', '.join(SUPPORTED_LANGUAGES)}")
    if body.platform != "alibaba":
        _require_fresh_login(body.platform)

    db = request.app.state.db
    use_session = body.platform != "alibaba"
    try:
        result = await scraper_service.start_job(db, {
            "platform":             body.platform,
            "categoryId":           str(body.categoryId),
            "categoryName":         body.categoryName or None,
            "groupCategoryId":      body.groupCategoryId or None,
            "groupCategoryName":    body.groupCategoryName or None,
            "groupCategoryNameEn":  body.groupCategoryNameEn or None,
            "searchType":           "category",
            "maxProducts":          body.maxProducts,
            "maxPages":             body.maxPages,
            "startPage":            body.startPage,
            "includeDetails":       body.includeDetails,
            "language":             body.language,
            "manualCaptcha":        body.manualCaptcha,
            "useSession":           use_session,
            "clearCookiesOnComplete": use_session,
        })
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return JSONResponse({"success": True, "data": result, "message": "Category scraping job started"})


@router.get("/jobs")
async def list_jobs(request: Request, status: Optional[str] = None, limit: int = 50):
    db = request.app.state.db
    filt = {}
    if status:
        filt["status"] = status
    cursor = db.scraping_jobs.find(filt, {"_id": 0}).sort("createdAt", -1).limit(limit)
    jobs = await cursor.to_list(length=limit)
    return JSONResponse({"success": True, "data": clean(jobs)})


@router.get("/job/{job_id}")
async def get_job(request: Request, job_id: str):
    db = request.app.state.db
    job = await scraper_service.get_job_status(db, job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return JSONResponse({"success": True, "data": clean(job)})


@router.delete("/job/{job_id}")
async def cancel_job(request: Request, job_id: str):
    db = request.app.state.db
    result = await scraper_service.cancel_job(db, job_id)
    return JSONResponse({"success": True, "data": result, "message": "Job cancelled"})


@router.post("/job/{job_id}/resume-captcha")
async def resume_captcha_job(request: Request, job_id: str):
    db = request.app.state.db
    result = await scraper_service.resume_captcha_job(db, job_id)
    status_code = 200 if result.get("success") else 409
    return JSONResponse({"success": result.get("success", False), "data": result, "message": result.get("message")}, status_code=status_code)


@router.post("/details/{item_id}")
async def scrape_details(request: Request, item_id: str, language: str = "en"):
    if language not in SUPPORTED_LANGUAGES:
        raise HTTPException(400, f"Invalid language. Must be one of: {', '.join(SUPPORTED_LANGUAGES)}")
    db = request.app.state.db
    product = await db.products.find_one({"itemId": item_id}, {"_id": 0})
    if not product or product.get("detailsScraped"):
        raise HTTPException(404, "Product not found or already scraped")

    details = await scraper_service._scrape_product_detail(product, product["platform"], language)
    if details:
        import dataclasses
        detail_dict = dataclasses.asdict(details)
        if should_translate(language):
            detail_dict = await translate_detail(detail_dict, language)
        await db.products.update_one(
            {"itemId": item_id},
            {"$set": {
                "detailedInfo": detail_dict,
                "detailsScraped": True,
                "extractionQuality": details.dataQuality.completeness,
            }}
        )
        product = await db.products.find_one({"itemId": item_id}, {"_id": 0})

    return JSONResponse({"success": True, "data": clean(product), "message": "Details scraped successfully"})


class BatchDetailsBody(BaseModel):
    platform: Optional[str] = None
    limit: Optional[int] = None        # how many products to scrape (None = all matching)
    mode: str = "pending"              # "pending" = only unscraped, "all" = re-scrape everything
    keyword: Optional[str] = None      # filter by search keyword
    categoryName: Optional[str] = None # filter by category
    minQuality: Optional[int] = None   # re-scrape products below this quality threshold
    delayMin: int = 5000               # min delay between products (ms)
    delayMax: int = 12000              # max delay between products (ms)
    language: str = "en"


class RequeueBody(BaseModel):
    itemIds: Optional[list[str]] = None
    platform: Optional[str] = None
    status: Optional[str] = "retry"
    limit: Optional[int] = None


@router.post("/scrape-pending-details")
async def scrape_pending_details(request: Request, body: BatchDetailsBody):
    """
    Batch scrape product details.

    Modes:
      - "pending"  → only products that haven't been scraped yet (detailsScraped=false)
      - "all"      → all products matching the filters (re-scrape existing details too)
      - "low"      → only products below minQuality threshold (default 50)

    Filters: platform, keyword, categoryName, minQuality, limit
    """
    if body.platform and body.platform not in ("taobao", "tmall", "1688", "alibaba"):
        raise HTTPException(400, "Invalid platform. Must be: taobao, tmall, 1688, or alibaba")
    if body.mode not in ("pending", "all", "low"):
        raise HTTPException(400, "Invalid mode. Must be: pending, all, or low")
    if body.language not in SUPPORTED_LANGUAGES:
        raise HTTPException(400, f"Invalid language. Must be one of: {', '.join(SUPPORTED_LANGUAGES)}")
    if body.platform and body.platform != "alibaba":
        _require_fresh_login(body.platform)

    db = request.app.state.db

    # Build filter based on mode
    filt: dict = {}
    if body.mode == "pending":
        filt["detailsScraped"] = False
    elif body.mode == "low":
        threshold = body.minQuality or 50
        filt["$or"] = [
            {"detailsScraped": False},
            {"extractionQuality": {"$lt": threshold}},
        ]

    if body.platform:
        filt["platform"] = body.platform
    if body.keyword:
        filt["searchKeyword"] = {"$regex": body.keyword, "$options": "i"}
    if body.categoryName:
        filt["categoryName"] = {"$regex": body.categoryName, "$options": "i"}

    matching_count = await db.products.count_documents(filt)
    actual_count = min(matching_count, body.limit) if body.limit else matching_count

    if matching_count == 0:
        return JSONResponse({
            "success": True,
            "data": {"message": "No products match the given filters"},
            "message": "Nothing to scrape",
        })

    use_session = bool(body.platform and body.platform != "alibaba")
    result = await scraper_service.start_pending_details_job(db, {
        "platform": body.platform or "all",
        "mode": body.mode,
        "keyword": body.keyword,
        "categoryName": body.categoryName,
        "minQuality": body.minQuality,
        "delayMin": body.delayMin,
        "delayMax": body.delayMax,
        "limit": body.limit,
        "pendingCount": actual_count,
        "language": body.language,
        "useSession": use_session,
        "clearCookiesOnComplete": use_session,
    })
    return JSONResponse({
        "success": True,
        "data": result,
        "message": f"Batch enrichment job started — up to {actual_count} candidates (mode={body.mode})",
    })


@router.get("/enrichment-queue")
async def list_enrichment_queue(
    request: Request,
    status: Optional[str] = None,
    platform: Optional[str] = None,
    limit: int = 100,
):
    db = request.app.state.db
    items = await scraper_service.list_enrichment_queue(
        db,
        status=status,
        platform=platform,
        limit=limit,
    )
    return JSONResponse({"success": True, "data": clean(items)})


@router.post("/enrichment-queue/requeue")
async def requeue_enrichment_queue(request: Request, body: RequeueBody):
    db = request.app.state.db
    result = await scraper_service.requeue_enrichment_items(
        db,
        item_ids=body.itemIds,
        platform=body.platform,
        status=body.status,
        limit=body.limit,
    )
    return JSONResponse({
        "success": True,
        "data": result,
        "message": f"Requeued {result['updated']} enrichment item(s)",
    })


@router.get("/session-status")
async def session_status():
    status = scraper_service.get_session_status()
    return JSONResponse({"success": True, "data": status})


@router.get("/captcha-status")
async def captcha_status():
    from services.captcha_solver import CAPTCHA_API_KEY
    try:
        import cv2
        opencv_available = True
    except ImportError:
        opencv_available = False

    if opencv_available and CAPTCHA_API_KEY:
        provider = "local+2captcha"
        message  = "Auto-solve enabled: local OpenCV (primary) + 2Captcha slider API (fallback)"
    elif opencv_available:
        provider = "local"
        message  = "Auto-solve enabled: local OpenCV. Set CAPTCHA_API_KEY for 2Captcha fallback."
    elif CAPTCHA_API_KEY:
        provider = "2captcha"
        message  = "2Captcha enabled (OpenCV not installed — run: pip install opencv-python-headless)"
    else:
        provider = None
        message  = "No auto-solve configured. Install opencv-python-headless and/or set CAPTCHA_API_KEY."

    return JSONResponse({
        "success": True,
        "data": {
            "autoSolveEnabled": opencv_available or bool(CAPTCHA_API_KEY),
            "localSolveEnabled": opencv_available,
            "fallbackEnabled":   bool(CAPTCHA_API_KEY),
            "provider":          provider,
            "message":           message,
        },
    })


@router.get("/proxy-status")
async def proxy_status():
    return JSONResponse({"success": True, "data": proxy_service.get_stats()})


class ProxyBody(BaseModel):
    proxy: str  # protocol://user:pass@host:port


class GatewayBody(BaseModel):
    url: str              # protocol://user:pass@gateway-host:port
    sticky: bool = False  # enable sticky sessions (session ID in username)


class ProviderBody(BaseModel):
    url: str
    protocol: str = "http"
    username: Optional[str] = None
    password: Optional[str] = None
    replaceExisting: bool = True
    refreshIntervalSec: int = 300
    minAvailable: int = 2


@router.post("/proxy")
async def add_proxy(body: ProxyBody):
    ok = proxy_service.add_proxy(body.proxy)
    if not ok:
        raise HTTPException(400, "Invalid proxy format. Use: protocol://user:pass@host:port")
    return JSONResponse({"success": True, "message": "Proxy added", "data": proxy_service.get_stats()})


@router.post("/proxy/gateway")
async def set_gateway(body: GatewayBody):
    """Set or replace the rotating gateway proxy.
    A gateway is a single endpoint where the provider rotates IPs for you.
    Examples: Smartproxy, Bright Data, Oxylabs, IPRoyal."""
    ok = proxy_service.set_gateway(body.url, sticky=body.sticky)
    if not ok:
        raise HTTPException(400, "Invalid gateway URL. Use: protocol://user:pass@host:port")
    return JSONResponse({
        "success": True,
        "message": f"Gateway proxy set (sticky={'on' if body.sticky else 'off'})",
        "data": proxy_service.get_stats(),
    })


@router.delete("/proxy/gateway")
async def remove_gateway():
    """Remove the rotating gateway proxy."""
    proxy_service.remove_gateway()
    return JSONResponse({"success": True, "message": "Gateway removed", "data": proxy_service.get_stats()})


@router.post("/proxy/provider")
async def set_proxy_provider(request: Request, body: ProviderBody):
    db = request.app.state.db
    ok = proxy_service.set_provider(
        body.url,
        protocol=body.protocol,
        username=body.username or "",
        password=body.password or "",
        refresh_interval_sec=body.refreshIntervalSec,
        min_available=body.minAvailable,
    )
    if not ok:
        raise HTTPException(400, "Invalid provider config. Supported protocols: http, https, socks5")

    await proxy_service.save_provider_to_db(db)

    result = await proxy_service.refresh_provider_proxies(replace_existing=body.replaceExisting)
    status_code = 200 if result.get("success") else 400
    return JSONResponse({
        "success": result.get("success", False),
        "message": result.get("message"),
        "data": {
            "refresh": result,
            "stats": proxy_service.get_stats(),
        },
    }, status_code=status_code)


@router.post("/proxy/provider/refresh")
async def refresh_proxy_provider(replace_existing: bool = True):
    result = await proxy_service.refresh_provider_proxies(replace_existing=replace_existing)
    status_code = 200 if result.get("success") else 400
    return JSONResponse({
        "success": result.get("success", False),
        "message": result.get("message"),
        "data": {
            "refresh": result,
            "stats": proxy_service.get_stats(),
        },
    }, status_code=status_code)


@router.delete("/proxy/provider")
async def clear_proxy_provider(request: Request):
    db = request.app.state.db
    proxy_service.clear_provider()
    await proxy_service.clear_provider_from_db(db)
    return JSONResponse({"success": True, "message": "Proxy provider removed", "data": proxy_service.get_stats()})


@router.delete("/proxy/{host_port}")
async def remove_proxy(host_port: str):
    ok = proxy_service.remove_proxy(host_port)
    if not ok:
        raise HTTPException(404, "Proxy not found")
    return JSONResponse({"success": True, "message": "Proxy removed", "data": proxy_service.get_stats()})


@router.post("/download-images/{item_id}")
async def download_images_single(request: Request, item_id: str):
    """Download all images for a single product."""
    db = request.app.state.db
    product = await db.products.find_one({"itemId": item_id}, {"_id": 0})
    if not product:
        raise HTTPException(404, "Product not found")
    stats = await download_product_images(product)
    if stats["localPaths"]:
        await db.products.update_one(
            {"itemId": item_id}, {"$set": {"localImages": stats["localPaths"]}}
        )
    return JSONResponse({"success": True, "data": stats})


class ImageBatchBody(BaseModel):
    platform: Optional[str] = None
    limit: int = 50


@router.post("/download-images")
async def download_images_batch(request: Request, body: ImageBatchBody):
    """Download images for multiple products."""
    db = request.app.state.db
    filt: dict = {}
    if body.platform:
        filt["platform"] = body.platform
    # Only products without cached images
    filt["localImages"] = {"$exists": False}
    products = await db.products.find(filt, {"_id": 0}).limit(body.limit).to_list(length=body.limit)
    if not products:
        return JSONResponse({"success": True, "data": {"message": "No products need image downloads"}})
    stats = await download_batch_images(db, products)
    return JSONResponse({"success": True, "data": stats})


@router.get("/image-stats")
async def image_stats():
    return JSONResponse({"success": True, "data": get_image_stats()})


# ── Cookie Management (headless-compatible login) ─────────────────────────

import json as _json
from pathlib import Path as _Path

_UTILS_DIR = _Path(__file__).parent.parent / "utils"
_COOKIE_FILES = {
    "taobao":  _UTILS_DIR / "cookies.json",
    "tmall":   _UTILS_DIR / "cookies.json",
    "1688":    _UTILS_DIR / "cookies-1688.json",
    "alibaba": _UTILS_DIR / "cookies-alibaba.json",
}

_LOGIN_URLS = {
    "taobao":  "https://login.taobao.com/member/login.jhtml",
    "tmall":   "https://login.tmall.com",
    "1688":    "https://passport.1688.com/member/signin.htm",
    "alibaba": "https://login.alibaba.com",
}


class ImportCookiesBody(BaseModel):
    platform: str  # "taobao" | "1688" | "alibaba"
    cookies: list   # list of cookie dicts [{name, value, domain, ...}]


@router.post("/import-cookies")
async def import_cookies(body: ImportCookiesBody):
    """Import cookies from a browser extension (e.g. EditThisCookie, Cookie-Editor).
    Works on headless servers — no browser window needed."""
    platform = body.platform.lower()
    if platform not in ("taobao", "1688", "alibaba"):
        raise HTTPException(400, "Platform must be 'taobao' (covers tmall too), '1688', or 'alibaba'")
    if not body.cookies or not isinstance(body.cookies, list):
        raise HTTPException(400, "cookies must be a non-empty list of cookie objects")

    # Validate minimum fields
    for i, c in enumerate(body.cookies):
        if not isinstance(c, dict) or "name" not in c or "value" not in c:
            raise HTTPException(400, f"Cookie at index {i} must have 'name' and 'value' fields")

    cookie_path = _COOKIE_FILES[platform]
    cookie_path.parent.mkdir(parents=True, exist_ok=True)
    cookie_path.write_text(_json.dumps(body.cookies, indent=2, ensure_ascii=False))

    # Invalidate scraper cookie cache so it picks up the new cookies
    scraper_service.clear_cookie_cache(platform)

    return JSONResponse({
        "success": True,
        "message": f"Imported {len(body.cookies)} cookies for {platform}",
        "data": {"platform": platform, "count": len(body.cookies), "file": cookie_path.name},
    })


@router.get("/export-cookies/{platform}")
async def export_cookies(platform: str):
    """Export current cookies for a platform (for backup or transfer)."""
    platform = platform.lower()
    if platform not in ("taobao", "tmall", "1688", "alibaba"):
        raise HTTPException(400, "Platform must be 'taobao', 'tmall', '1688', or 'alibaba'")
    cookie_path = _COOKIE_FILES[platform]
    if not cookie_path.exists():
        raise HTTPException(404, f"No cookies found for {platform}")
    try:
        cookies = _json.loads(cookie_path.read_text())
    except Exception as e:
        raise HTTPException(500, f"Failed to read cookies: {e}")
    return JSONResponse({"success": True, "data": {"platform": platform, "cookies": cookies}})


@router.delete("/clear-cookies/{platform}")
async def clear_cookies(platform: str):
    """Delete saved cookies for a platform."""
    platform = platform.lower()
    if platform not in ("taobao", "tmall", "1688", "alibaba"):
        raise HTTPException(400, "Platform must be 'taobao', 'tmall', '1688', or 'alibaba'")
    scraper_service.clear_saved_cookies(platform)
    _clear_fresh_login(platform)
    return JSONResponse({"success": True, "message": f"Cookies cleared for {platform}"})


# ── QR Code Login (headless) ──────────────────────────────────────────────

import asyncio as _asyncio
import base64 as _base64
import time as _time

# Track active QR login sessions
_qr_sessions: dict[str, dict] = {}
_QR_SESSION_TTL = 300  # 5 minutes — auto-close stale QR sessions


async def _cleanup_stale_qr_sessions():
    """Close QR sessions older than TTL."""
    now = _time.time()
    for key in list(_qr_sessions):
        session = _qr_sessions.get(key)
        if not session:
            continue
        if now - session.get("created_at", 0) > _QR_SESSION_TTL:
            try:
                await session["browser"].close()
                await session["pw"].stop()
            except Exception:
                pass
            _qr_sessions.pop(key, None)


async def cleanup_all_qr_sessions():
    """Close all QR sessions — called on app shutdown."""
    for key in list(_qr_sessions):
        session = _qr_sessions.pop(key, None)
        if not session:
            continue
        try:
            await session["browser"].close()
            await session["pw"].stop()
        except Exception:
            pass


@router.post("/qr-login/{platform}")
async def start_qr_login(platform: str):
    """Start a QR code login session. Returns a base64-encoded screenshot of the login page.
    The user scans the QR with their Taobao/Tmall/1688 mobile app.
    Poll GET /api/scraper/qr-login/{platform}/status to check completion."""
    platform = platform.lower()
    if platform not in ("taobao", "tmall", "1688", "alibaba"):
        raise HTTPException(400, "Invalid platform")
    _clear_fresh_login(platform)

    # Cancel any existing session for this platform
    session_key = _session_key(platform)
    existing = _qr_sessions.get(session_key)
    if existing and existing.get("browser"):
        try:
            await existing["browser"].close()
        except Exception:
            pass
        try:
            await existing["pw"].stop()
        except Exception:
            pass

    from playwright.async_api import TimeoutError as PlaywrightTimeoutError, async_playwright

    pw = browser = context = None
    try:
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )
        context = await browser.new_context(
            viewport={"width": 1200, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        page = await context.new_page()

        login_url = _LOGIN_URLS.get(platform, _LOGIN_URLS["taobao"])
        # Login pages keep long-polling/analytics requests open, so
        # networkidle can time out even when the QR UI is usable.
        await page.goto(login_url, wait_until="domcontentloaded", timeout=45000)
        try:
            await page.wait_for_load_state("load", timeout=10000)
        except PlaywrightTimeoutError:
            pass
        await page.wait_for_timeout(2000)
    except PlaywrightTimeoutError as e:
        if browser:
            try:
                await browser.close()
            except Exception:
                pass
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass
        return JSONResponse(
            {
                "success": False,
                "error": (
                    f"Timed out opening {platform} login page. "
                    "The site may be slow, blocked, or waiting on a proxy/VPN. Try again."
                ),
                "detail": str(e),
            },
            status_code=504,
        )
    except Exception as e:
        if browser:
            try:
                await browser.close()
            except Exception:
                pass
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass
        return JSONResponse(
            {"success": False, "error": f"Failed to start QR login for {platform}: {e}"},
            status_code=500,
        )

    # ── Try to switch to QR code login mode ──────────────────────────
    # Taobao/Tmall/1688 login pages often default to password form;
    # we need to click the QR code tab/switch to show the QR.
    _qr_switch_selectors = [
        # Taobao / Tmall
        '.icon-qrcode', '.qrcode-login', '#J_QRCodeLogin',
        '.login-switch', '.login-tab-qrcode',
        '[class*="qrcode-switch"]', '[class*="qr-switch"]',
        'div[class*="icon-qrcode"]', 'i[class*="qrcode"]',
        # Text-based
        ':text("扫码登录")', ':text("二维码登录")',
        'a:has-text("扫码")', 'div:has-text("扫码登录"):not(:has(div))',
        # 1688
        '.alipay-qrcode', '.alipay-login', '#J_LoginByQrcode',
        '[data-type="qrcode"]', '[data-action="qrcode"]',
    ]

    for selector in _qr_switch_selectors:
        try:
            el = page.locator(selector).first
            if await el.is_visible(timeout=800):
                await el.click()
                await page.wait_for_timeout(2000)
                break
        except Exception:
            continue

    # Also try iframes (Taobao login sometimes uses an iframe)
    for frame in page.frames:
        if frame == page.main_frame:
            continue
        for selector in _qr_switch_selectors[:8]:
            try:
                el = frame.locator(selector).first
                if await el.is_visible(timeout=500):
                    await el.click()
                    await page.wait_for_timeout(2000)
                    break
            except Exception:
                continue

    await page.wait_for_timeout(1000)

    # ── Capture QR code image ────────────────────────────────────────
    qr_image = None

    # Try to find the QR code element across main page and iframes
    _qr_img_selectors = [
        '#J_QRCodeImg', '#J_QRCodeImg img',
        '.qrcode-img', '.qrcode-img img',
        'img[src*="qrcode"]', 'img[src*="qr"]',
        'canvas', '.login-qrcode img',
        '#qrcode', '#qrcode img',
        '.qr-img', 'img.qr',
        '[class*="qrcode"] img', '[class*="qrcode"] canvas',
        'img[width="160"]', 'img[width="180"]', 'img[width="200"]',
        'img[width="140"]', 'img[width="220"]',
    ]

    # Search in main page
    for qr_sel in _qr_img_selectors:
        try:
            el = page.locator(qr_sel).first
            if await el.is_visible(timeout=600):
                qr_image = await el.screenshot()
                break
        except Exception:
            continue

    # Search in iframes
    if not qr_image:
        for frame in page.frames:
            if frame == page.main_frame:
                continue
            for qr_sel in _qr_img_selectors:
                try:
                    el = frame.locator(qr_sel).first
                    if await el.is_visible(timeout=500):
                        qr_image = await el.screenshot()
                        break
                except Exception:
                    continue
            if qr_image:
                break

    # Fallback: screenshot a login container
    if not qr_image:
        _container_selectors = [
            '.login-content', '.login-box', '.login-panel',
            '#login-form', '#J_LoginBox', '.qrcode-area',
            '.module-qrcode', '.login-main', '#content',
        ]
        for sel in _container_selectors:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=500):
                    qr_image = await el.screenshot()
                    break
            except Exception:
                continue

    # Last fallback: full page screenshot
    if not qr_image:
        qr_image = await page.screenshot(full_page=False)

    qr_b64 = _base64.b64encode(qr_image).decode("ascii")

    _qr_sessions[session_key] = {
        "pw": pw,
        "browser": browser,
        "context": context,
        "page": page,
        "platform": platform,
        "status": "waiting",
        "login_url": login_url,
        "created_at": _time.time(),
    }

    return JSONResponse({
        "success": True,
        "data": {
            "platform": platform,
            "qrImage": f"data:image/png;base64,{qr_b64}",
            "status": "waiting",
            "message": "Scan the QR code with your mobile app, then poll /qr-login/{platform}/status",
        },
    })


@router.get("/qr-login/{platform}/status")
async def qr_login_status(platform: str):
    """Poll this endpoint after starting a QR login to check if the user has scanned."""
    await _cleanup_stale_qr_sessions()
    session_key = _session_key(platform)
    session = _qr_sessions.get(session_key)
    if not session:
        return JSONResponse({"success": False, "error": "No active QR session. Start one first."}, status_code=404)

    page = session["page"]
    context = session["context"]

    try:
        current_url = page.url
        # Check if we've been redirected away from login (= success)
        is_still_login = any(kw in current_url for kw in ("login", "signin", "passport"))

        if not is_still_login:
            # Login succeeded — save cookies
            cookies = await context.cookies()
            cookie_path = _COOKIE_FILES[session["platform"]]
            cookie_path.parent.mkdir(parents=True, exist_ok=True)
            cookie_path.write_text(_json.dumps(cookies, indent=2, ensure_ascii=False))
            scraper_service.clear_cookie_cache(session["platform"])
            _grant_fresh_login(session["platform"], cookie_count=len(cookies))

            # Cleanup
            await session["browser"].close()
            await session["pw"].stop()
            _qr_sessions.pop(session_key, None)

            return JSONResponse({
                "success": True,
                "data": {
                    "status": "success",
                    "message": f"Login successful! {len(cookies)} cookies saved.",
                    "cookieCount": len(cookies),
                },
            })

        # Still on login page — check for expiry or error indicators
        expired = False
        for sel in ['.qrcode-expired', ':text("二维码已过期")', ':text("已过期")',
                     '.refresh-qrcode', ':text("expired")', ':text("刷新")']:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=500):
                    expired = True
                    break
            except Exception:
                continue

        if expired:
            # Try to click refresh button to get a new QR
            for sel in ['.refresh-qrcode', ':text("点击刷新")', ':text("刷新")',
                         '[class*="refresh"]', '.qrcode-expired']:
                try:
                    el = page.locator(sel).first
                    if await el.is_visible(timeout=500):
                        await el.click()
                        await page.wait_for_timeout(2000)
                        break
                except Exception:
                    continue

            return JSONResponse({
                "success": True,
                "data": {"status": "expired", "message": "QR code expired. Click 'QR Login' to start a new session."},
            })

        # Take a fresh screenshot so the UI stays updated
        try:
            fresh_img = await page.screenshot(full_page=False)
            fresh_b64 = _base64.b64encode(fresh_img).decode("ascii")
        except Exception:
            fresh_b64 = None

        resp_data = {"status": "waiting", "message": "Waiting for QR scan..."}
        if fresh_b64:
            resp_data["qrImage"] = f"data:image/png;base64,{fresh_b64}"

        return JSONResponse({"success": True, "data": resp_data})

    except Exception as e:
        # Cleanup on error
        try:
            await session["browser"].close()
            await session["pw"].stop()
        except Exception:
            pass
        _qr_sessions.pop(session_key, None)
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)


@router.post("/qr-login/{platform}/cancel")
async def cancel_qr_login(platform: str):
    """Cancel an active QR login session."""
    _clear_fresh_login(platform)
    session_key = _session_key(platform)
    session = _qr_sessions.pop(session_key, None)
    if not session:
        return JSONResponse({"success": True, "message": "No active session"})
    try:
        await session["browser"].close()
        await session["pw"].stop()
    except Exception:
        pass
    return JSONResponse({"success": True, "message": "QR login session cancelled"})
