"""
Image downloading and caching service.

Downloads product images locally before Taobao/Tmall CDN URLs expire.
Images are stored in data/images/{platform}/{itemId}/ directory.
"""

import asyncio
import hashlib
import logging
import os
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Base directory for cached images
IMAGE_DIR = Path(os.getenv("IMAGE_DIR", str(Path(__file__).parent.parent / "data" / "images")))
MAX_CONCURRENT_DOWNLOADS = 5
DOWNLOAD_TIMEOUT = 30


def _image_path(platform: str, item_id: str, url: str, index: int = 0) -> Path:
    """Generate a local file path for an image."""
    ext = ".jpg"
    for e in (".png", ".webp", ".gif", ".jpeg"):
        if e in url.lower():
            ext = e
            break
    # Use URL hash to avoid duplicates
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    filename = f"{index:03d}_{url_hash}{ext}"
    return IMAGE_DIR / platform / item_id / filename


async def download_image(url: str, dest: Path) -> bool:
    """Download a single image to the destination path."""
    if dest.exists():
        return True  # Already cached

    dest.parent.mkdir(parents=True, exist_ok=True)

    # Normalize URL
    if url.startswith("//"):
        url = "https:" + url

    try:
        async with httpx.AsyncClient(
            timeout=DOWNLOAD_TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0.0.0"},
        ) as client:
            response = await client.get(url)
            if response.status_code == 200 and len(response.content) > 100:
                dest.write_bytes(response.content)
                return True
            else:
                logger.warning(f"Image download failed ({response.status_code}): {url}")
                return False
    except Exception as e:
        logger.warning(f"Image download error: {url} -> {e}")
        return False


async def download_product_images(product: dict) -> dict:
    """
    Download all images for a product.
    Updates product dict with local paths.
    Returns download stats.
    """
    item_id = product.get("itemId", "unknown")
    platform = product.get("platform", "unknown")
    downloaded = 0
    failed = 0
    local_paths = []

    # Collect all image URLs
    urls: list[tuple[str, int]] = []

    # Main image
    main_img = product.get("image")
    if main_img:
        urls.append((main_img, 0))

    # Additional images from details
    detail = product.get("detailedInfo") or {}
    for i, img_url in enumerate(detail.get("additionalImages") or [], start=1):
        if img_url:
            urls.append((img_url, i))

    # Variant images
    variants = detail.get("variants") or {}
    img_idx = len(urls)
    if isinstance(variants, dict):
        for opts in variants.values():
            if isinstance(opts, list):
                for opt in opts:
                    if isinstance(opt, dict) and opt.get("image"):
                        urls.append((opt["image"], img_idx))
                        img_idx += 1

    if not urls:
        return {"downloaded": 0, "failed": 0, "total": 0}

    # Download with concurrency limit
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    async def _dl(url: str, idx: int):
        nonlocal downloaded, failed
        async with semaphore:
            dest = _image_path(platform, item_id, url, idx)
            ok = await download_image(url, dest)
            if ok:
                downloaded += 1
                local_paths.append(str(dest))
            else:
                failed += 1

    await asyncio.gather(*[_dl(url, idx) for url, idx in urls])

    return {
        "downloaded": downloaded,
        "failed": failed,
        "total": len(urls),
        "localPaths": local_paths,
    }


async def download_batch_images(db, products: list, job_id: Optional[str] = None) -> dict:
    """Download images for a batch of products."""
    total_downloaded = 0
    total_failed = 0

    for i, product in enumerate(products):
        stats = await download_product_images(product)
        total_downloaded += stats["downloaded"]
        total_failed += stats["failed"]

        # Update product in DB with local image paths
        if stats["localPaths"]:
            try:
                await db.products.update_one(
                    {"itemId": product["itemId"]},
                    {"$set": {"localImages": stats["localPaths"]}}
                )
            except Exception:
                pass

    return {
        "productsProcessed": len(products),
        "totalDownloaded": total_downloaded,
        "totalFailed": total_failed,
    }


def get_image_stats() -> dict:
    """Get image cache statistics."""
    if not IMAGE_DIR.exists():
        return {"enabled": True, "directory": str(IMAGE_DIR), "totalFiles": 0, "totalSizeMB": 0}

    total_files = 0
    total_size = 0
    for f in IMAGE_DIR.rglob("*"):
        if f.is_file():
            total_files += 1
            total_size += f.stat().st_size

    return {
        "enabled": True,
        "directory": str(IMAGE_DIR),
        "totalFiles": total_files,
        "totalSizeMB": round(total_size / (1024 * 1024), 2),
    }
