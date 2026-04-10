import asyncio
import base64
import hashlib
import json
import os
import random
import re
import uuid
from datetime import datetime, UTC, timedelta
from html import unescape
from pathlib import Path
from typing import Optional

from scrapling.fetchers import StealthyFetcher

from services.product_detail_extractor import ProductDetailExtractor
from services.translate_service import translate_products, translate_detail, should_translate
from services.proxy_service import proxy_service
from services.price_history_service import record_price_snapshots_bulk, record_price_snapshot
from services.webhook_service import notify_job_completed, notify_job_failed
from services.retry_service import enqueue_retry
from services.rate_limiter import rate_limiter
from data.category_tree import find_group_for_sub, find_group_for_platform_id


def _enrich_group_category(product: dict, params: dict) -> dict:
    """
    Return a new product dict enriched with groupCategoryId/Name/NameEn.

    Lookup order:
      1. params already has groupCategoryId (caller passed it explicitly)
      2. categoryId matches a known sub_id in the canonical tree
      3. categoryId matches a platform-native ID in the canonical tree
    """
    if product.get("groupCategoryId"):
        return product

    if params.get("groupCategoryId"):
        return {
            **product,
            "groupCategoryId":    params["groupCategoryId"],
            "groupCategoryName":  params.get("groupCategoryName"),
            "groupCategoryNameEn": params.get("groupCategoryNameEn"),
        }

    cat_id   = product.get("categoryId", "")
    platform = product.get("platform", params.get("platform", "taobao"))

    group = find_group_for_sub(cat_id) or find_group_for_platform_id(platform, cat_id)
    if not group:
        return product

    return {
        **product,
        "groupCategoryId":    group.group_id,
        "groupCategoryName":  group.name_zh,
        "groupCategoryNameEn": group.name_en,
    }


COOKIE_PATHS = {
    "taobao":   Path(__file__).parent.parent / "utils" / "cookies.json",
    "tmall":    Path(__file__).parent.parent / "utils" / "cookies.json",
    "1688":     Path(__file__).parent.parent / "utils" / "cookies-1688.json",
    "alibaba":  Path(__file__).parent.parent / "utils" / "cookies-alibaba.json",
}


def _session_scope(platform: str) -> str:
    if platform == "1688":
        return "1688"
    if platform == "alibaba":
        return "alibaba"
    return "taobao_tmall"

PLATFORM_CONFIG = {
    "taobao": {
        "searchUrl": "https://s.taobao.com/search",
        "baseUrl":   "https://www.taobao.com",
        "paginationType": "button",
    },
    "tmall": {
        "searchUrl": "https://list.tmall.com/search_product.htm",
        "baseUrl":   "https://www.tmall.com",
        "paginationType": "button",
    },
    "1688": {
        "searchUrl": "https://s.1688.com/selloffer/offer_search.htm",
        "baseUrl":   "https://www.1688.com",
        "paginationType": "url",
    },
    "alibaba": {
        "searchUrl": "https://www.alibaba.com/trade/search",
        "baseUrl":   "https://www.alibaba.com",
        "paginationType": "url",
    },
}

LANGUAGE_CONFIG = {
    "en": {
        "lang_param":      "en",
        "locale":          "en-US",
        "accept_language": "en-US,en;q=0.9",
        "hng_cookie":      "US%7Cen_US%7CUSD%7C840",
        "intl_locale":     "en_US",
        "currency":        "USD",
    },
    "zh": {
        "lang_param":      "zh",
        "locale":          "zh-CN",
        "accept_language": "zh-CN,zh;q=0.9",
        "hng_cookie":      "CN%7Czh_CN%7CCNY%7C156",
        "intl_locale":     "zh_CN",
        "currency":        "CNY",
    },
    "th": {
        "lang_param":      "th",
        "locale":          "th-TH",
        "accept_language": "th-TH,th;q=0.9,en;q=0.5",
        "hng_cookie":      "TH%7Cth_TH%7CTHB%7C764",
        "intl_locale":     "th_TH",
        "currency":        "THB",
    },
    "ja": {
        "lang_param":      "ja",
        "locale":          "ja-JP",
        "accept_language": "ja-JP,ja;q=0.9,en;q=0.5",
        "hng_cookie":      "JP%7Cja_JP%7CJPY%7C392",
        "intl_locale":     "ja_JP",
        "currency":        "JPY",
    },
    "ko": {
        "lang_param":      "ko",
        "locale":          "ko-KR",
        "accept_language": "ko-KR,ko;q=0.9,en;q=0.5",
        "hng_cookie":      "KR%7Cko_KR%7CKRW%7C410",
        "intl_locale":     "ko_KR",
        "currency":        "KRW",
    },
    "ru": {
        "lang_param":      "ru",
        "locale":          "ru-RU",
        "accept_language": "ru-RU,ru;q=0.9,en;q=0.5",
        "hng_cookie":      "RU%7Cru_RU%7CRUB%7C643",
        "intl_locale":     "ru_RU",
        "currency":        "RUB",
    },
}
DEFAULT_LANGUAGE = "en"

# Timezone to inject into the browser to match the expected proxy geo-location.
# Using China/HK timezones prevents Taobao from detecting timezone/IP mismatch.
_LANG_TIMEZONE: dict[str, str] = {
    "zh": "Asia/Shanghai",
    "en": "Asia/Hong_Kong",   # HK is common for English users on Alibaba platforms
    "th": "Asia/Bangkok",
    "ja": "Asia/Tokyo",
    "ko": "Asia/Seoul",
    "ru": "Europe/Moscow",
}

UA_POOL = [
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "platform": "Win32",
        "vendor": "Google Inc.",
    },
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "platform": "Win32",
        "vendor": "Google Inc.",
    },
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "platform": "Win32",
        "vendor": "Google Inc.",
    },
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "platform": "MacIntel",
        "vendor": "Google Inc.",
    },
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "platform": "MacIntel",
        "vendor": "Google Inc.",
    },
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "platform": "MacIntel",
        "vendor": "Google Inc.",
    },
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
        "platform": "Win32",
        "vendor": "Google Inc.",
    },
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
        "platform": "Win32",
        "vendor": "Google Inc.",
    },
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "platform": "Win32",
        "vendor": "Google Inc.",
    },
    {
        "ua": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "platform": "Linux x86_64",
        "vendor": "Google Inc.",
    },
]

VIEWPORT_POOL = [
    {"width": 1920, "height": 1080},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1366, "height": 768},
    {"width": 1680, "height": 1050},
    {"width": 1280, "height": 800},
    {"width": 1600, "height": 900},
]


def _pick_ua() -> dict:
    return random.choice(UA_POOL)


def _pick_viewport() -> dict:
    return random.choice(VIEWPORT_POOL)


def _compute_discovery_hash(product: dict) -> str:
    parts = [
        product.get("platform", ""),
        product.get("itemId", ""),
        product.get("title", ""),
        str(product.get("price", "") or ""),
        product.get("image", "") or "",
        product.get("link", "") or "",
        product.get("shopName", "") or "",
        product.get("categoryId", "") or "",
        product.get("categoryName", "") or "",
    ]
    payload = "||".join(parts)
    return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()


HUMAN = {
    "page_delay_base":      4.0,
    "page_delay_jitter":   (2.0, 6.0),
    "long_pause_every":     3,
    "long_pause_range":    (25.0, 55.0),
    "detail_delay_base":    3.5,
    "detail_delay_jitter": (1.5, 4.5),
    "detail_batch_delay":  (8.0, 16.0),
    "max_products_per_session": 80,
    "session_rest_range":       (180.0, 360.0),
    "page_count_variance": 2,
}


def _human_delay(base: float, jitter: tuple) -> float:
    return base + random.uniform(*jitter)


def _build_stealth_script(ua_entry: dict, accept_language: str, timezone: str = "Asia/Shanghai") -> str:
    platform = ua_entry["platform"]
    vendor   = ua_entry["vendor"]
    lang = accept_language.split(",")[0].strip()
    # Small per-session noise seed so Canvas/AudioContext outputs differ each run
    noise_seed = random.randint(1, 99)

    return f"""
// ── Stealth: patch all automation signals ────────────────────────────────────

Object.defineProperty(navigator, 'webdriver', {{ get: () => undefined }});
Object.defineProperty(navigator, 'platform', {{ get: () => '{platform}' }});
Object.defineProperty(navigator, 'vendor', {{ get: () => '{vendor}' }});
Object.defineProperty(navigator, 'languages', {{ get: () => ['{lang}', '{lang.split("-")[0]}'] }});
Object.defineProperty(navigator, 'language',  {{ get: () => '{lang}' }});

const _plugins = [
    {{ name: 'Chrome PDF Plugin',    filename: 'internal-pdf-viewer',   description: 'Portable Document Format' }},
    {{ name: 'Chrome PDF Viewer',    filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' }},
    {{ name: 'Native Client',        filename: 'internal-nacl-plugin',  description: '' }},
];
Object.defineProperty(navigator, 'plugins', {{
    get: () => {{
        const arr = Object.create(PluginArray.prototype);
        _plugins.forEach((p, i) => {{
            const plugin = Object.create(Plugin.prototype);
            Object.defineProperty(plugin, 'name',        {{ get: () => p.name }});
            Object.defineProperty(plugin, 'filename',    {{ get: () => p.filename }});
            Object.defineProperty(plugin, 'description', {{ get: () => p.description }});
            Object.defineProperty(arr, i, {{ get: () => plugin }});
        }});
        Object.defineProperty(arr, 'length', {{ get: () => _plugins.length }});
        return arr;
    }}
}});

Object.defineProperty(navigator, 'mimeTypes', {{
    get: () => {{
        const arr = Object.create(MimeTypeArray.prototype);
        Object.defineProperty(arr, 'length', {{ get: () => 2 }});
        return arr;
    }}
}});

if (!window.chrome) {{ window.chrome = {{}}; }}
if (!window.chrome.runtime) {{
    window.chrome.runtime = {{
        connect: () => {{}},
        sendMessage: () => {{}},
        id: undefined,
    }};
}}

const _origPermQuery = navigator.permissions && navigator.permissions.query
    ? navigator.permissions.query.bind(navigator.permissions)
    : null;
if (_origPermQuery) {{
    navigator.permissions.query = (params) => {{
        if (params && params.name === 'notifications') {{
            return Promise.resolve({{ state: Notification.permission, onchange: null }});
        }}
        return _origPermQuery(params);
    }};
}}

delete window.__playwright;
delete window.__pwInitScripts;
delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
Object.defineProperty(document, '$cdc_asdjflasutopfhvcZLmcfl_', {{ get: () => undefined }});

if (navigator.hardwareConcurrency < 2) {{
    Object.defineProperty(navigator, 'hardwareConcurrency', {{ get: () => 4 }});
}}

if (!navigator.deviceMemory || navigator.deviceMemory < 1) {{
    Object.defineProperty(navigator, 'deviceMemory', {{ get: () => 8 }});
}}

// ── WebGL fingerprint spoofing ────────────────────────────────────────────────
(function() {{
    const getParam = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(param) {{
        if (param === 37445) return 'Intel Inc.';           // UNMASKED_VENDOR_WEBGL
        if (param === 37446) return 'ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)';  // UNMASKED_RENDERER_WEBGL
        return getParam.call(this, param);
    }};
    if (typeof WebGL2RenderingContext !== 'undefined') {{
        const getParam2 = WebGL2RenderingContext.prototype.getParameter;
        WebGL2RenderingContext.prototype.getParameter = function(param) {{
            if (param === 37445) return 'Intel Inc.';
            if (param === 37446) return 'ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)';
            return getParam2.call(this, param);
        }};
    }}
}})();

// ── Canvas fingerprint noise ──────────────────────────────────────────────────
(function() {{
    const _noise = {noise_seed};
    const origToDataURL = HTMLCanvasElement.prototype.toDataURL;
    HTMLCanvasElement.prototype.toDataURL = function(type) {{
        const ctx = this.getContext('2d');
        if (ctx) {{
            const imageData = ctx.getImageData(0, 0, this.width || 1, this.height || 1);
            imageData.data[0] = (imageData.data[0] + _noise) % 256;
            imageData.data[1] = (imageData.data[1] + _noise) % 256;
            ctx.putImageData(imageData, 0, 0);
        }}
        return origToDataURL.apply(this, arguments);
    }};
    const origToBlob = HTMLCanvasElement.prototype.toBlob;
    HTMLCanvasElement.prototype.toBlob = function(callback, type, quality) {{
        const ctx = this.getContext('2d');
        if (ctx) {{
            const imageData = ctx.getImageData(0, 0, this.width || 1, this.height || 1);
            imageData.data[0] = (imageData.data[0] + _noise) % 256;
            ctx.putImageData(imageData, 0, 0);
        }}
        return origToBlob.call(this, callback, type, quality);
    }};
}})();

// ── AudioContext fingerprint randomization ────────────────────────────────────
(function() {{
    const _audioNoise = {noise_seed} * 0.0000001;
    const OrigAudioBuffer = window.AudioBuffer;
    if (typeof OfflineAudioContext !== 'undefined') {{
        const origGetChannelData = AudioBuffer.prototype.getChannelData;
        AudioBuffer.prototype.getChannelData = function(channel) {{
            const array = origGetChannelData.call(this, channel);
            for (let i = 0; i < array.length; i += 100) {{
                array[i] += _audioNoise;
            }}
            return array;
        }};
    }}
}})();

// ── Timezone and locale consistency ──────────────────────────────────────────
(function() {{
    const _tz = '{timezone}';
    const origDateTimeFormat = Intl.DateTimeFormat;
    window.Intl.DateTimeFormat = function(locales, options) {{
        options = options || {{}};
        if (!options.timeZone) {{
            options.timeZone = _tz;
        }}
        return new origDateTimeFormat(locales || '{lang}', options);
    }};
    window.Intl.DateTimeFormat.prototype = origDateTimeFormat.prototype;
    window.Intl.DateTimeFormat.supportedLocalesOf = origDateTimeFormat.supportedLocalesOf;

}})();
"""


def _normalize_href(href: str) -> str:
    href = unescape(href)
    if href.startswith("//"):
        return "https:" + href
    return href


def _html_content(page) -> str:
    for attr in ("html_content", "content", "text_content"):
        val = getattr(page, attr, None)
        if val and isinstance(val, str) and len(val) > 100:
            return val
    return str(page)


def _text_in_class(chunk: str, class_fragment: str) -> str:
    pattern = re.compile(
        r'class="[^"]*' + re.escape(class_fragment) + r'[^"]*"[^>]*>([\s\S]*?)</(?:div|span|p|h\d)>',
        re.IGNORECASE,
    )
    m = pattern.search(chunk)
    if not m:
        return ""
    return re.sub(r'<[^>]+>', '', m.group(1)).strip()


def _has_next_page(html: str, platform: str, current_page: int) -> bool:
    if platform in ("taobao", "tmall"):
        next_page = current_page + 1
        if re.search(rf'["\s]{next_page}["\s]', html):
            return True
        if re.search(r'(下一页|next-page|page-next|btnNext)(?![^<]*disabled)', html, re.IGNORECASE):
            return True
        if len(html) > 500_000:
            return True
        return False
    return len(html) > 300_000


# ── Taobao / Tmall extractor ───────────────────────────────────────────────

def _re_extract_taobao_tmall(html: str, params: dict, page_num: int) -> list[dict]:
    results = []
    seen_ids = set()

    anchor_pattern = re.compile(r'id="item_id_(\d+)"')
    anchors = list(anchor_pattern.finditer(html))

    if not anchors:
        href_pattern = re.compile(
            r'href="(//(?:item\.taobao\.com|detail\.tmall\.com)/item\.htm\?id=(\d+)[^"]*)"'
        )
        anchors_fb = list(href_pattern.finditer(html))
        for i, m in enumerate(anchors_fb):
            item_id = m.group(2)
            if item_id in seen_ids:
                continue
            seen_ids.add(item_id)
            href = _normalize_href(m.group(1))
            chunk_start = m.start()
            chunk_end = anchors_fb[i + 1].start() if i + 1 < len(anchors_fb) else chunk_start + 6000
            chunk = html[chunk_start:min(chunk_end, chunk_start + 6000)]
            product = _parse_taobao_card(chunk, item_id, href, params, page_num)
            if product:
                results.append(product)
        return results

    for i, anchor in enumerate(anchors):
        item_id = anchor.group(1)
        if item_id in seen_ids:
            continue
        seen_ids.add(item_id)

        chunk_start = anchor.start()
        chunk_end = anchors[i + 1].start() if i + 1 < len(anchors) else chunk_start + 6000
        chunk = html[chunk_start:min(chunk_end, chunk_start + 6000)]

        href_m = re.search(
            r'href="(//(?:item\.taobao\.com|detail\.tmall\.com)/item\.htm\?id=' + item_id + r'[^"]*)"',
            chunk,
        )
        if not href_m:
            href_m = re.search(r'href="(//(?:item\.taobao\.com|detail\.tmall\.com)/[^"]+)"', chunk)

        href = _normalize_href(href_m.group(1)) if href_m else f"https://item.taobao.com/item.htm?id={item_id}"
        product = _parse_taobao_card(chunk, item_id, href, params, page_num)
        if product:
            results.append(product)

    return results


def _parse_taobao_card(chunk: str, item_id: str, href: str, params: dict, page_num: int) -> Optional[dict]:
    title = ""
    title_attr_m = re.search(r'class="[^"]*title--[^"]*"[^>]*title="([^"]+)"', chunk)
    if title_attr_m:
        title = unescape(title_attr_m.group(1)).strip()
    if not title:
        title_span_m = re.search(
            r'class="[^"]*title--[^"]*"[^>]*>[\s\S]{0,100}?<span[^>]*>([\s\S]{3,200}?)</span>',
            chunk,
        )
        if title_span_m:
            title = re.sub(r'<[^>]+>', '', title_span_m.group(1)).strip()
    if not title:
        title = _text_in_class(chunk, "cardTitle--")
    if not title or len(title) < 3:
        return None

    price_int = _text_in_class(chunk, "innerPriceWrapper--")
    price_dec = _text_in_class(chunk, "priceFloat--")
    price = ""
    if price_int:
        price_int = re.sub(r'[^\d]', '', price_int)
        price_dec = re.sub(r'[^\d.]', '', price_dec)
        price = price_int + (price_dec if price_dec else "")
    else:
        pw_m = re.search(r'class="[^"]*[Pp]riceWrapper--[^"]*"[^>]*>([\s\S]{0,300}?)</div>', chunk)
        if pw_m:
            price_m = re.search(r'([\d,]+\.?\d*)', pw_m.group(1))
            if price_m:
                price = price_m.group(1).replace(",", "")
    if not price:
        return None

    img_m = re.search(r'class="[^"]*mainPic--[^"]*"[^>]*src="([^"]+)"', chunk)
    if not img_m:
        img_m = re.search(r'<img[^>]+src="([^"]+)"', chunk)
    image = _normalize_href(img_m.group(1)) if img_m else ""

    sales_text = _text_in_class(chunk, "realSales--")
    sales_m = re.search(r'([\d,]+)', sales_text) if sales_text else None
    sales_count = sales_m.group(1).replace(",", "") if sales_m else None
    shop_name = _text_in_class(chunk, "shopNameText--") or None
    location  = _text_in_class(chunk, "procity--") or None

    category_id = params.get("categoryId")
    if not category_id:
        cm = re.search(r'[?&](?:cat|catId|categoryId)=(\d+)', href)
        if cm:
            category_id = cm.group(1)

    actual_platform = "tmall" if "tmall.com" in href else "taobao"

    product: dict = _enrich_group_category({
        "itemId": item_id, "title": title[:200], "price": price,
        "image": image, "link": href, "platform": actual_platform,
        "searchKeyword": params.get("keyword"),
        "categoryId": category_id, "categoryName": params.get("categoryName"),
        "pageNumber": page_num, "location": location,
        "language": params.get("language", DEFAULT_LANGUAGE),
        "extractedAt": datetime.now(UTC).isoformat(), "detailsScraped": False,
    }, params)
    if shop_name:
        product["shopName"] = shop_name
        product["shopInfo"] = {"shopName": shop_name, "shopLink": None, "sellerInfo": {}, "badges": []}
    if sales_count:
        product["salesCount"] = sales_count
    return product


# ── 1688 extractor ─────────────────────────────────────────────────────────

def _re_extract_1688(html: str, params: dict, page_num: int) -> list[dict]:
    results = []
    seen_ids = set()
    pattern = re.compile(r'href="(//detail\.1688\.com[^"]*offer/(\d+)\.html[^"]*)"')
    matches = list(pattern.finditer(html))

    for i, m in enumerate(matches):
        item_id = m.group(2)
        if item_id in seen_ids:
            continue
        seen_ids.add(item_id)
        href = _normalize_href(m.group(1))
        chunk_start = m.start()
        chunk_end = matches[i + 1].start() if i + 1 < len(matches) else chunk_start + 6000
        chunk = html[chunk_start:min(chunk_end, chunk_start + 6000)]

        title = ""
        for cls_frag in ["title--", "Title--", "subject--", "name--"]:
            title = _text_in_class(chunk, cls_frag)
            if len(title) >= 3:
                break
        if not title:
            ta = re.search(r'\btitle="([^"]{3,200})"', chunk)
            if ta:
                title = unescape(ta.group(1)).strip()
        if not title or len(title) < 3:
            continue

        price = ""
        for cls_frag in ["price--", "Price--"]:
            pt = _text_in_class(chunk, cls_frag)
            pm = re.search(r'([\d,]+\.?\d*)', pt)
            if pm:
                price = pm.group(1).replace(",", "")
                break
        if not price:
            continue

        img_m = re.search(r'<img[^>]+src="([^"]+)"', chunk)
        image = _normalize_href(img_m.group(1)) if img_m else ""

        results.append(_enrich_group_category({
            "itemId": item_id, "title": title[:200], "price": price,
            "image": image, "link": href, "platform": "1688",
            "searchKeyword": params.get("keyword"),
            "categoryId": params.get("categoryId"),
            "categoryName": params.get("categoryName"),
            "pageNumber": page_num,
            "language": params.get("language", DEFAULT_LANGUAGE),
            "extractedAt": datetime.now(UTC).isoformat(),
            "detailsScraped": False,
        }, params))
    return results


# ── Alibaba.com extractor ──────────────────────────────────────────────────
#
# HTML structure (from debug_alibaba.json):
#   <h2 class="search-card-e-title">
#     <a href="//www.alibaba.com/product-detail/Name_ID.html" data-spm="d_title">
#       <span>Product Title</span>
#     </a>
#   </h2>
#   ... (price is nearby as plain text "US$XX-YY" or "US$XX.XX")
#
# Each product URL appears 5-6 times per card; we anchor on the unique h2 title
# element (one per product) and scan backward/forward for the URL and price.

_ALIBABA_TITLE_RE = re.compile(
    r'<h2[^>]*class="search-card-e-title"[^>]*>([\s\S]{0,600}?)</h2>',
    re.IGNORECASE,
)
_ALIBABA_HREF_RE = re.compile(
    r'href="(//www\.alibaba\.com/product-detail/[^"]*?_(\d{5,})\.html[^"]*)"'
)
_ALIBABA_PRICE_RE = re.compile(
    r'US\$\s*([\d,]+\.?\d*)(?:\s*[-–]\s*[\d,]+\.?\d*)?'
)


def _re_extract_alibaba(html: str, params: dict, page_num: int) -> list[dict]:
    results = []
    seen_ids: set[str] = set()

    title_matches = list(_ALIBABA_TITLE_RE.finditer(html))

    for i, tm in enumerate(title_matches):
        # Title text is inside <span> within the <h2>
        inner = tm.group(1)
        span_m = re.search(r'<span[^>]*>([\s\S]{3,200}?)</span>', inner)
        title = re.sub(r'<[^>]+>', '', span_m.group(1)).strip() if span_m else ""
        if not title:
            title = re.sub(r'<[^>]+>', '', inner).strip()
        title = re.sub(r'\s+', ' ', title).strip()
        if not title or len(title) < 3:
            continue

        # Product URL is inside the <a> within the title h2
        href_m = _ALIBABA_HREF_RE.search(inner)
        if not href_m:
            continue
        item_id = href_m.group(2)
        if item_id in seen_ids:
            continue
        seen_ids.add(item_id)
        href = _normalize_href(href_m.group(1))

        # Alibaba card layout:  [image slider] → [title h2] → [price / supplier]
        # Images live in search-card-e-slider__wrapper which is BEFORE the h2.
        # Scan 3000 chars backward for the image, 2000 chars forward for price/shop.
        prev_end = title_matches[i - 1].end() if i > 0 else max(0, tm.start() - 3000)
        back_window = html[max(prev_end, tm.start() - 3000):tm.start()]

        window_start = tm.end()
        window_end   = (
            title_matches[i + 1].start() if i + 1 < len(title_matches)
            else window_start + 2000
        )
        fwd_window = html[window_start:min(window_end, window_start + 2000)]

        price_m = _ALIBABA_PRICE_RE.search(fwd_window)
        price = price_m.group(1).replace(",", "") if price_m else ""
        if not price:
            bare = re.search(r'\b(\d{1,6}(?:\.\d{1,2})?)\b', fwd_window)
            if bare:
                price = bare.group(1)
        if not price:
            continue

        # Image: search backward (slider) first, then forward as fallback.
        # Alibaba uses src, data-src, and lazy-src for lazy-loaded images.
        _img_re = re.compile(
            r'<img[^>]+(?:src|data-src|lazy-src)="((?:https?:|//)[^"]+\.(?:jpg|jpeg|png|webp)[^"]*)"',
            re.IGNORECASE,
        )
        img_m = _img_re.search(back_window) or _img_re.search(fwd_window)
        image = _normalize_href(img_m.group(1)) if img_m else ""

        # Supplier name
        shop_m = re.search(
            r'class="[^"]*(?:supplier|company|shop|store)[^"]*"[^>]*>([\s\S]{2,80}?)</(?:span|div|a)>',
            fwd_window, re.IGNORECASE,
        )
        shop_name = re.sub(r'<[^>]+>', '', shop_m.group(1)).strip() if shop_m else None

        results.append(_enrich_group_category({
            "itemId":        item_id,
            "title":         title[:200],
            "price":         price,
            "image":         image,
            "link":          href,
            "platform":      "alibaba",
            "searchKeyword": params.get("keyword"),
            "categoryId":    params.get("categoryId"),
            "categoryName":  params.get("categoryName"),
            "pageNumber":    page_num,
            "language":      params.get("language", DEFAULT_LANGUAGE),
            "extractedAt":   datetime.now(UTC).isoformat(),
            "detailsScraped": False,
            **({"shopName": shop_name} if shop_name else {}),
        }, params))

    return results


# ── ScraperService ─────────────────────────────────────────────────────────

class ScraperService:
    def __init__(self):
        self.active_jobs: dict[str, dict] = {}
        self.detail_extractor = ProductDetailExtractor()
        self._cookie_cache: dict[str, Optional[list]] = {
            "taobao_tmall": None,
            "1688":         None,
            "alibaba":      None,
        }
        self.config = {
            "headless":               True,
            "page_load_delay":        HUMAN["page_delay_base"],
            "detail_page_delay":      HUMAN["detail_delay_base"],
            "max_retries":            2,
            "min_extraction_quality": 50,
            "auto_scrape_details":    False,
            "details_per_batch":      10,
            "details_batch_delay":    HUMAN["detail_batch_delay"][0],
            "taobao_http_discovery":  True,
            "taobao_browser_fallback": True,
            "enrichment_retry_base_minutes": 15,
            "enrichment_retry_max_attempts": 5,
        }
        self._session_product_count: int = 0

    def log(self, job_id: Optional[str], message: str, level: str = "info"):
        prefix = f"[Job: {job_id[:8]}]" if job_id else "[Scraper]"
        print(f"{datetime.now(UTC).isoformat()} {prefix} [{level.upper()}] {message}")
        import logging as _logging
        _logger = _logging.getLogger("scraper")
        log_level = getattr(_logging, level.upper(), _logging.INFO)
        _logger.log(log_level, message, extra={"job_id": job_id[:8] if job_id else None})

    # ── Cookies ────────────────────────────────────────────────────────────

    def clear_cookie_cache(self, platform: str = None):
        if platform == "1688":
            self._cookie_cache["1688"] = None
        elif platform == "alibaba":
            self._cookie_cache["alibaba"] = None
        elif platform in ("taobao", "tmall"):
            self._cookie_cache["taobao_tmall"] = None
        else:
            self._cookie_cache["taobao_tmall"] = None
            self._cookie_cache["1688"] = None
            self._cookie_cache["alibaba"] = None

    def clear_saved_cookies(self, platform: str, job_id: Optional[str] = None) -> bool:
        path = COOKIE_PATHS.get(platform)
        if not path:
            return False
        try:
            if path.exists():
                path.unlink()
            self.clear_cookie_cache(platform)
            self.log(job_id, f"🧹 Cleared saved cookies for session scope '{_session_scope(platform)}'")
            return True
        except Exception as e:
            self.log(job_id, f"⚠️  Failed to clear cookies for {platform}: {e}", "warn")
            return False

    def has_active_session_job(self, platform: str) -> bool:
        scope = _session_scope(platform)
        for job in self.active_jobs.values():
            if job.get("status") != "running":
                continue
            if job.get("sessionScope") != scope:
                continue
            if not job.get("useSession", True):
                continue
            if not job.get("clearCookiesOnComplete"):
                continue
            return True
        return False

    def load_cookies(self, platform: str) -> list:
        if platform == "1688":
            cache_key = "1688"
        elif platform == "alibaba":
            cache_key = "alibaba"
        else:
            cache_key = "taobao_tmall"
        if self._cookie_cache[cache_key] is not None:
            return self._cookie_cache[cache_key]

        cookie_path = COOKIE_PATHS.get(platform)
        if not cookie_path or not cookie_path.exists():
            # Alibaba scrapes in guest mode — missing cookie file is expected
            if cookie_path and platform != "alibaba":
                self.log(None, f"⚠️  {cookie_path.name} not found.", "warn")
            return []

        try:
            raw = json.loads(cookie_path.read_text())
            if not isinstance(raw, list) or not raw:
                return []
            cookies = []
            for c in raw:
                if "name" not in c or "value" not in c:
                    continue
                domain_defaults = {"taobao": ".taobao.com", "tmall": ".tmall.com", "1688": ".1688.com", "alibaba": ".alibaba.com"}
                cookie = {
                    "name":   c["name"],
                    "value":  c["value"],
                    "domain": c.get("domain") or domain_defaults.get(platform, ".taobao.com"),
                    "path":   c.get("path", "/"),
                }
                if c.get("expires") and c["expires"] > 0:
                    cookie["expires"] = int(c["expires"])
                if "httpOnly" in c:
                    cookie["httpOnly"] = bool(c["httpOnly"])
                if "secure" in c:
                    cookie["secure"] = bool(c["secure"])
                if c.get("sameSite") in ("Strict", "Lax", "None"):
                    cookie["sameSite"] = c["sameSite"]
                cookies.append(cookie)
            self._cookie_cache[cache_key] = cookies
            self.log(None, f"✅ [{platform}] Loaded {len(cookies)} cookies")
            return cookies
        except Exception as e:
            self.log(None, f"⚠️  Failed to parse cookies: {e}", "warn")
            return []

    def get_locale_cookies(self, platform: str, language: str = DEFAULT_LANGUAGE) -> list:
        lang_cfg = LANGUAGE_CONFIG.get(language, LANGUAGE_CONFIG[DEFAULT_LANGUAGE])
        domain_map = {"taobao": ".taobao.com", "tmall": ".tmall.com", "1688": ".1688.com", "alibaba": ".alibaba.com"}
        domain = domain_map.get(platform, ".taobao.com")
        return [
            {"name": "hng",         "value": lang_cfg["hng_cookie"],  "domain": domain, "path": "/"},
            {"name": "intl_locale", "value": lang_cfg["intl_locale"], "domain": domain, "path": "/"},
            {"name": "language",    "value": lang_cfg["intl_locale"], "domain": domain, "path": "/"},
            {"name": "INTL_LOCALE", "value": lang_cfg["intl_locale"], "domain": domain, "path": "/"},
        ]

    def get_all_cookies(self, platform: str, language: str = DEFAULT_LANGUAGE) -> list:
        session = self.load_cookies(platform)
        locale  = self.get_locale_cookies(platform, language)
        session_names = {c["name"] for c in session}
        return session + [c for c in locale if c["name"] not in session_names]

    # ── URL building ───────────────────────────────────────────────────────

    def build_search_url(self, platform: str, params: dict) -> str:
        config = PLATFORM_CONFIG[platform]
        search_type = params.get("searchType", "keyword")
        keyword     = params.get("keyword", "")
        category_id = params.get("categoryId", "")
        lang        = LANGUAGE_CONFIG.get(
            params.get("language", DEFAULT_LANGUAGE),
            LANGUAGE_CONFIG[DEFAULT_LANGUAGE],
        )["lang_param"]

        if search_type == "keyword":
            if platform == "1688":
                return f"{config['searchUrl']}?keywords={keyword}&lang={lang}"
            if platform == "alibaba":
                return f"{config['searchUrl']}?SearchText={keyword}&fsb=y&IndexArea=product_en"
            return f"{config['searchUrl']}?q={keyword}&lang={lang}"

        if search_type == "category":
            if platform == "taobao":
                return self._build_taobao_category_url(category_id, params, lang)
            if platform == "tmall":
                return self._build_tmall_category_url(category_id, params, lang)
            if platform == "1688":
                return f"https://s.1688.com/selloffer/offer_search.htm?categoryId={category_id}&lang={lang}"
            if platform == "alibaba":
                return f"https://www.alibaba.com/trade/search?CatId={category_id}&fsb=y&IndexArea=product_en"

        raise ValueError("Invalid search type or missing parameters")

    def _build_taobao_category_url(self, category_id: str, params: dict, lang: str) -> str:
        from urllib.parse import quote
        from data.category_tree import get_sub_by_id

        if category_id and category_id.isdigit():
            return f"https://s.taobao.com/search?catId={category_id}&tab=all&lang={lang}"

        cat_name_zh = params.get("categoryName") or ""
        if not cat_name_zh and category_id:
            sub = get_sub_by_id(category_id)
            if sub:
                cat_name_zh = sub.name_zh

        if cat_name_zh:
            return f"https://s.taobao.com/search?q={quote(cat_name_zh)}&tab=all&lang={lang}"

        return f"https://s.taobao.com/search?q={quote(category_id)}&tab=all&lang={lang}"

    def _build_tmall_category_url(self, category_id: str, params: dict, lang: str) -> str:
        from urllib.parse import quote
        from data.category_tree import get_sub_by_id

        if category_id and category_id.isdigit():
            return f"https://list.tmall.com/search_product.htm?cat={category_id}&lang={lang}"

        cat_name_zh = params.get("categoryName") or ""
        if not cat_name_zh and category_id:
            sub = get_sub_by_id(category_id)
            if sub:
                cat_name_zh = sub.name_zh

        if cat_name_zh:
            return f"https://s.taobao.com/search?q={quote(cat_name_zh)}&tab=tmall&lang={lang}"

        return f"https://s.taobao.com/search?q={quote(category_id)}&tab=tmall&lang={lang}"

    def _build_page_url(self, base_url: str, platform: str, page_num: int) -> str:
        if "page=" in base_url:
            return re.sub(r"([?&])page=\d+", f"\\1page={page_num}", base_url)
        sep = "&" if "?" in base_url else "?"
        return f"{base_url}{sep}page={page_num}"

    # ── Category ID ────────────────────────────────────────────────────────

    def generate_category_id(self, label: str, platform: str) -> str:
        prefix = {"taobao": "1", "tmall": "2", "1688": "3", "alibaba": "4"}.get(platform, "9")
        s = (label or "unknown").lower().strip()
        h = 5381
        for ch in s:
            h = ((h << 5) + h) ^ ord(ch)
            h &= 0xFFFFFFFF
        return f"{prefix}{h % 10_000_000:07d}"

    # ── Human-like delays ──────────────────────────────────────────────────

    async def _page_delay(self, job_id: str, next_page: int):
        if next_page > 1 and next_page % HUMAN["long_pause_every"] == 0:
            rest = random.uniform(*HUMAN["long_pause_range"])
            self.log(job_id, f"☕ Long pause before page {next_page}: {rest:.1f}s (anti-detection)")
            await asyncio.sleep(rest)
        else:
            delay = _human_delay(HUMAN["page_delay_base"], HUMAN["page_delay_jitter"])
            self.log(job_id, f"⏱  Page delay: {delay:.1f}s")
            await asyncio.sleep(delay)

    async def _detail_delay(self, job_id: str, index: int, total: int, is_batch_end: bool):
        is_last = index == total - 1
        if is_batch_end and not is_last:
            rest = random.uniform(*HUMAN["detail_batch_delay"])
            self.log(job_id, f"☕ Detail batch break: {rest:.1f}s")
            await asyncio.sleep(rest)
        elif not is_last:
            delay = _human_delay(HUMAN["detail_delay_base"], HUMAN["detail_delay_jitter"])
            await asyncio.sleep(delay)

    async def _check_session_limit(self, job_id: str):
        n = self._session_product_count
        limit = HUMAN["max_products_per_session"]
        if n > 0 and n % limit == 0:
            rest = random.uniform(*HUMAN["session_rest_range"])
            self.log(
                job_id,
                f"🛑 Session limit hit ({n} products scraped). "
                f"Resting {rest:.0f}s before continuing…",
                "warn",
            )
            await asyncio.sleep(rest)

    def _randomise_max_pages(self, requested: int) -> int:
        v = HUMAN["page_count_variance"]
        return random.randint(max(1, requested - v), requested + v)

    # ── Job management ─────────────────────────────────────────────────────

    async def start_job(self, db, params: dict) -> dict:
        if (
            params.get("useSession", True)
            and params.get("clearCookiesOnComplete", False)
            and self.has_active_session_job(params["platform"])
        ):
            raise RuntimeError(
                f"Another authenticated job is already running for session scope '{_session_scope(params['platform'])}'."
            )

        job_id = str(uuid.uuid4())
        self.log(job_id, f"Creating job: {params}")
        job_doc = {
            "jobId": job_id, "platform": params["platform"],
            "searchType": params.get("searchType", "keyword"),
            "searchParams": params, "status": "pending",
            "progress": {"currentPage": 0, "productsScraped": 0, "detailsScraped": 0, "detailsFailed": 0},
            "results": {
                "totalProducts": 0,
                "updatedProducts": 0,
                "detailsScraped": 0,
                "discoveredProducts": 0,
                "changedProducts": 0,
                "queuedForEnrichment": 0,
            },
            "error": None, "startedAt": None, "completedAt": None,
            "createdAt": datetime.now(UTC), "updatedAt": datetime.now(UTC),
        }
        await db.scraping_jobs.insert_one(job_doc)
        asyncio.create_task(self._execute_job(db, job_id, params))
        return {"jobId": job_id, "status": "started"}

    async def _execute_job(self, db, job_id: str, params: dict):
        await db.scraping_jobs.update_one(
            {"jobId": job_id},
            {"$set": {"status": "running", "startedAt": datetime.now(UTC)}}
        )
        self.active_jobs[job_id] = {
            "status": "running",
            "cancelRequested": False,
            "platform": params["platform"],
            "sessionScope": _session_scope(params["platform"]),
            "useSession": params.get("useSession", True),
            "clearCookiesOnComplete": params.get("clearCookiesOnComplete", False),
        }
        try:
            if proxy_service.enabled and not proxy_service.has_gateway:
                refresh = await proxy_service.ensure_provider_proxies(reason=f"job:{job_id[:8]}")
                if refresh.get("success") and refresh.get("added", 0):
                    self.log(job_id, f"🔀 Refreshed provider proxies: +{refresh['added']} loaded")

            platform = params["platform"]
            use_session = params.get("useSession", True)
            if use_session and not self.check_cookie_health_for_job(platform, job_id):
                raise RuntimeError(
                    f"Session cookies for {platform} are fully expired. "
                    "Re-login to continue."
                )
            if not use_session:
                self.log(job_id, "👤 Guest mode — scraping without session cookies")

            self.log(job_id, "=== PHASE 1: SEARCH & COLLECT ===")
            products = await self.scrape_products(db, job_id, params)
            if self.active_jobs.get(job_id, {}).get("cancelRequested"):
                await db.scraping_jobs.update_one(
                    {"jobId": job_id},
                    {"$set": {"status": "cancelled", "completedAt": datetime.now(UTC)}}
                )
                self.log(job_id, "Job cancelled during search phase", "warn")
                return
            self.log(job_id, f"✅ Found {len(products)} products")

            lang = params.get("language", DEFAULT_LANGUAGE)
            if should_translate(lang) and products:
                self.log(job_id, f"=== TRANSLATING {len(products)} products → {lang} ===")
                products = await translate_products(products, lang)
                self.log(job_id, f"✅ Translation complete")

            self.log(job_id, "=== PHASE 2: BULK SAVE ===")
            save_stats = await self.save_products_bulk(db, products, params["platform"])

            queued_count = 0
            if params.get("includeDetails") and products:
                self.log(job_id, "=== PHASE 3: ENRICHMENT QUEUE ===")
                queued_count = await self.enqueue_enrichment_candidates(
                    db,
                    products,
                    params["platform"],
                    save_stats["changed_item_ids"],
                    reason="discovery_changed",
                    source_policy=["seller_feed", "official_api", "http_parse", "browser_fallback"],
                    discovery_job_id=job_id,
                )

            await db.scraping_jobs.update_one(
                {"jobId": job_id},
                {"$set": {
                    "status": "completed",
                    "results.totalProducts": save_stats["inserted"],
                    "results.updatedProducts": save_stats["updated"],
                    "results.detailsScraped": 0,
                    "results.discoveredProducts": len(products),
                    "results.changedProducts": save_stats["changed"],
                    "results.queuedForEnrichment": queued_count,
                    "completedAt": datetime.now(UTC),
                }}
            )
            try:
                job_doc = await db.scraping_jobs.find_one({"jobId": job_id}, {"_id": 0})
                if job_doc:
                    await notify_job_completed(db, job_doc)
            except Exception:
                pass
        except Exception as e:
            self.log(job_id, f"Job failed: {e}", "error")
            await db.scraping_jobs.update_one(
                {"jobId": job_id},
                {"$set": {"status": "failed", "error": str(e)}}
            )
            try:
                await notify_job_failed(db, {"jobId": job_id, "platform": params.get("platform")}, str(e))
            except Exception:
                pass
        finally:
            if params.get("useSession", True) and params.get("clearCookiesOnComplete", False):
                self.clear_saved_cookies(params["platform"], job_id=job_id)
            self.active_jobs.pop(job_id, None)
            self.log(job_id, "Job finished")

    async def cancel_job(self, db, job_id: str) -> dict:
        if job_id in self.active_jobs:
            self.active_jobs[job_id]["cancelRequested"] = True
        await db.scraping_jobs.update_one(
            {"jobId": job_id, "status": {"$in": ["running"]}},
            {"$set": {"status": "cancelled"}}
        )
        return {"success": True}

    async def get_job_status(self, db, job_id: str) -> Optional[dict]:
        return await db.scraping_jobs.find_one({"jobId": job_id}, {"_id": 0})

    # ── Product scraping ───────────────────────────────────────────────────

    async def scrape_products(self, db, job_id: str, params: dict) -> list[dict]:
        platform = params["platform"]

        if not params.get("categoryName") and params.get("keyword"):
            params["categoryName"] = params["keyword"]

        start_page = max(1, params.get("startPage", 1))
        max_pages_req = params.get("maxPages", 10)
        max_pages = self._randomise_max_pages(max_pages_req)
        end_page = start_page + max_pages - 1
        self.log(job_id, f"📋 Pages {start_page}→{end_page} (max {max_pages}, ±{HUMAN['page_count_variance']})")

        if platform == "1688":
            return await self._scrape_1688_url_based(db, job_id, params, max_pages, start_page)
        if platform == "alibaba":
            return await self._scrape_alibaba_url_based(db, job_id, params, max_pages, start_page)
        if self.config.get("taobao_http_discovery", True):
            return await self._scrape_taobao_tmall_url_based(db, job_id, params, max_pages, start_page)
        return await self._scrape_taobao_tmall_with_clicks(db, job_id, params, max_pages, start_page)

    def _looks_like_taobao_block_page(self, html: str) -> bool:
        if not html:
            return True
        lowered = html.lower()
        markers = [
            "login.taobao.com",
            "nocaptcha",
            "captcha",
            "verify",
            "baxia",
            "loginname",
            "punish",
            "sec.taobao.com",
        ]
        return any(marker in lowered for marker in markers)

    async def _scrape_taobao_tmall_url_based(
        self, db, job_id: str, params: dict, max_pages: int, start_page: int = 1
    ) -> list[dict]:
        platform     = params["platform"]
        language     = params.get("language", DEFAULT_LANGUAGE)
        lang_cfg     = LANGUAGE_CONFIG.get(language, LANGUAGE_CONFIG[DEFAULT_LANGUAGE])
        max_products = params.get("maxProducts", 100)
        search_url   = self.build_search_url(platform, params)
        cookies      = self.get_all_cookies(platform, language)
        all_products: list[dict] = []
        seen_ids: set[str] = set()
        current_page = start_page
        end_page     = start_page + max_pages - 1

        self.log(job_id, f"Starting {platform} HTTP discovery (pages {start_page}→{end_page}): {search_url}")

        while current_page <= end_page and len(all_products) < max_products:

            await self._check_session_limit(job_id)
            if self.active_jobs.get(job_id, {}).get("cancelRequested"):
                self.log(job_id, "🛑 Cancel requested")
                break

            fetch_url = (
                search_url if current_page == 1
                else self._build_page_url(search_url, platform, current_page)
            )
            self.log(job_id, f"📄 Page {current_page}: {fetch_url}")

            page_proxy = proxy_service.get_next(session_id=job_id) if proxy_service.enabled else None
            if page_proxy:
                self.log(job_id, f"  🔀 Proxy: {page_proxy.host}:{page_proxy.port}")

            try:
                fetch_kwargs: dict = dict(
                    url=fetch_url,
                    headless=self.config["headless"],
                    network_idle=True,
                    extra_headers={"Accept-Language": lang_cfg["accept_language"]},
                )
                if cookies:
                    fetch_kwargs["cookies"] = cookies
                if page_proxy:
                    fetch_kwargs["proxy"] = page_proxy.to_url()

                await rate_limiter.acquire(platform)
                page = await StealthyFetcher.async_fetch(**fetch_kwargs)

                if not page or page.status != 200:
                    self.log(job_id, f"⚠️  Bad response: {getattr(page, 'status', 'None')}", "warn")
                    if page_proxy:
                        proxy_service.mark_failure(page_proxy)
                    break

                html = _html_content(page)
                self.log(job_id, f"  HTML: {len(html):,} chars")

                if self._looks_like_taobao_block_page(html):
                    self.log(job_id, "  ⚠️  HTTP discovery returned login/captcha/interstitial page", "warn")
                    if page_proxy:
                        proxy_service.mark_failure(page_proxy)
                    if (
                        current_page == start_page
                        and self.config.get("taobao_browser_fallback", True)
                    ):
                        self.log(job_id, "  ↩ Falling back to browser discovery for this job", "warn")
                        return await self._scrape_taobao_tmall_with_clicks(db, job_id, params, max_pages, start_page)
                    break

                page_products = self.extract_products_from_page(html, platform, params, current_page)
                self.log(job_id, f"  Extracted: {len(page_products)}")

                if not page_products:
                    if current_page == start_page and self.config.get("taobao_browser_fallback", True):
                        self.log(job_id, "  ⚠️  HTTP discovery found no products on first page; falling back to browser", "warn")
                        return await self._scrape_taobao_tmall_with_clicks(db, job_id, params, max_pages, start_page)
                    self.log(job_id, "No products — stopping")
                    break

                new_products = [p for p in page_products if p["itemId"] not in seen_ids]
                for p in new_products:
                    seen_ids.add(p["itemId"])
                all_products.extend(new_products)
                self._session_product_count += len(new_products)
                self.log(
                    job_id,
                    f"  New: {len(new_products)} | Job total: {len(all_products)} | Session: {self._session_product_count}"
                )

                await db.scraping_jobs.update_one(
                    {"jobId": job_id},
                    {"$set": {
                        "progress.currentPage": current_page,
                        "progress.productsScraped": len(all_products),
                    }}
                )

                if len(all_products) >= max_products:
                    self.log(job_id, f"Reached max_products ({max_products})")
                    break
                if not new_products and current_page > 1:
                    self.log(job_id, "No new products — last page")
                    break
                if not _has_next_page(html, platform, current_page):
                    self.log(job_id, f"No next page after page {current_page}")
                    break

                if page_proxy:
                    proxy_service.mark_success(page_proxy)

                current_page += 1
                await self._page_delay(job_id, current_page)

            except Exception as e:
                import traceback
                self.log(job_id, f"❌ Page {current_page} error: {e}\n{traceback.format_exc()}", "error")
                if page_proxy:
                    proxy_service.mark_failure(page_proxy)
                if (
                    current_page == start_page
                    and self.config.get("taobao_browser_fallback", True)
                ):
                    self.log(job_id, "  ↩ HTTP discovery failed on first page; falling back to browser", "warn")
                    return await self._scrape_taobao_tmall_with_clicks(db, job_id, params, max_pages, start_page)
                break

        return all_products[:max_products]

    _NEXT_BTN_SELECTORS = [
        "button[aria-label*='下一页']",
        "button.next-pagination-item.next-next",
        "button.next-next:not([disabled])",
        ".next-pagination-item.next-next",
        "[class*='next-pagination-item'][class*='next-next']",
        ".ui-page-next:not(.ui-page-disabled)",
        "a.ui-page-next",
        ".J_nextPage",
        "a.next",
    ]

    async def _scrape_taobao_tmall_with_clicks(
        self, db, job_id: str, params: dict, max_pages: int, start_page: int = 1
    ) -> list[dict]:
        from playwright.async_api import async_playwright

        platform     = params["platform"]
        language     = params.get("language", DEFAULT_LANGUAGE)
        lang_cfg     = LANGUAGE_CONFIG.get(language, LANGUAGE_CONFIG[DEFAULT_LANGUAGE])
        max_products = params.get("maxProducts", 100)
        search_url   = self.build_search_url(platform, params)
        all_products: list[dict] = []
        seen_ids: set[str] = set()

        use_session = params.get("useSession", True)
        pw_cookies = []
        if use_session:
            for c in self.get_all_cookies(platform, language):
                pw_c = {
                    "name":   c["name"],
                    "value":  c["value"],
                    "domain": c.get("domain", ".taobao.com"),
                    "path":   c.get("path", "/"),
                }
                if "expires" in c:
                    pw_c["expires"] = float(c["expires"])
                pw_cookies.append(pw_c)

        search_proxy = proxy_service.get_next(session_id=job_id) if proxy_service.enabled else None
        if search_proxy:
            self.log(job_id, f"🔀 Using proxy: {search_proxy.host}:{search_proxy.port}")

        ua_entry = _pick_ua()
        viewport = _pick_viewport()
        self.log(job_id, f"🖥  Playwright session ({language}, {ua_entry['platform']}, {viewport['width']}x{viewport['height']}): {search_url}")

        async with async_playwright() as pw:
            launch_kwargs = dict(
                headless=self.config["headless"],
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--disable-infobars",
                    "--disable-extensions",
                ],
            )
            if search_proxy:
                launch_kwargs["proxy"] = search_proxy.to_playwright_proxy()

            browser = await pw.chromium.launch(**launch_kwargs)
            context = await browser.new_context(
                user_agent=ua_entry["ua"],
                locale=lang_cfg["locale"],
                viewport=viewport,  # type: ignore[arg-type]
                extra_http_headers={"Accept-Language": lang_cfg["accept_language"]},
            )
            if pw_cookies:
                await context.add_cookies(pw_cookies)

            # Block Alibaba telemetry beacons that report real device fingerprints,
            # bypassing DOM-level patches. Return fake 200s so page JS doesn't error.
            async def _fake_ok(route, request):
                await route.fulfill(status=200, body="", content_type="text/plain")

            await context.route("**/*mmstat*", _fake_ok)
            await context.route("**/*arms-retcode*", _fake_ok)
            await context.route("**/*aplus*", _fake_ok)
            await context.route("**/*tbcdn*logstat*", _fake_ok)

            pw_page = await context.new_page()
            tz = _LANG_TIMEZONE.get(language, "Asia/Shanghai")
            stealth_script = _build_stealth_script(ua_entry, lang_cfg["accept_language"], timezone=tz)
            await pw_page.add_init_script(stealth_script)

            try:
                # Warm-up: visit the homepage first so Taobao sees a realistic
                # navigation chain (real users don't jump straight to search URLs).
                base_url = PLATFORM_CONFIG[platform]["baseUrl"]
                self.log(job_id, f"🏠 Warm-up: visiting {base_url}...")
                await rate_limiter.acquire(platform)
                await pw_page.goto(base_url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(random.uniform(2.0, 5.0))
                await self._add_random_mouse_movements(pw_page, job_id)

                self.log(job_id, f"🔍 Navigating to search: {search_url}")
                await rate_limiter.acquire(platform)
                await pw_page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                await self._wait_for_products(pw_page, job_id)
                await self._add_random_mouse_movements(pw_page, job_id)
                await asyncio.sleep(random.uniform(1.0, 2.0))

                current_page = 1

                if self._DEBUG_DETAIL:
                    await self._debug_dump_pagination(pw_page, job_id)

                # ── Skip to startPage by clicking Next repeatedly ─────────
                if start_page > 1:
                    self.log(job_id, f"⏩ Skipping to page {start_page} ({start_page - 1} clicks)...")
                    for skip in range(1, start_page):
                        if self.active_jobs.get(job_id, {}).get("cancelRequested"):
                            break
                        await self._smart_scroll(pw_page, job_id)
                        first_id = await pw_page.evaluate("""
                            () => {
                                const el = document.querySelector("[id^='item_id_']");
                                return el ? el.id : null;
                            }
                        """)
                        url_before = pw_page.url
                        clicked = await self._playwright_click_next(pw_page, job_id, skip)
                        if not clicked:
                            self.log(job_id, f"  ⚠️  Could not skip past page {skip} — starting from here", "warn")
                            break
                        changed = await self._wait_for_dom_change(pw_page, job_id, first_id, url_before)
                        if not changed:
                            self.log(job_id, f"  ⚠️  Page did not change at skip {skip} — starting from here", "warn")
                            break
                        current_page = skip + 1
                        await asyncio.sleep(random.uniform(1.5, 3.0))
                    self.log(job_id, f"✅ Now on page {current_page}")

                end_page = start_page + max_pages - 1

                while current_page <= end_page and len(all_products) < max_products:

                    await self._check_session_limit(job_id)
                    if self.active_jobs.get(job_id, {}).get("cancelRequested"):
                        self.log(job_id, "🛑 Cancel requested")
                        break

                    html = await pw_page.content()
                    self.log(job_id, f"📄 Page {current_page} — HTML: {len(html):,} chars")

                    page_products = self.extract_products_from_page(html, platform, params, current_page)
                    self.log(job_id, f"  Extracted: {len(page_products)}")

                    if not page_products:
                        self.log(job_id, "  No products — stopping")
                        break

                    new_products = [p for p in page_products if p["itemId"] not in seen_ids]
                    for p in new_products:
                        seen_ids.add(p["itemId"])
                    all_products.extend(new_products)
                    self._session_product_count += len(new_products)
                    self.log(
                        job_id,
                        f"  New: {len(new_products)} | Job total: {len(all_products)} | Session: {self._session_product_count}"
                    )

                    await db.scraping_jobs.update_one(
                        {"jobId": job_id},
                        {"$set": {
                            "progress.currentPage":     current_page,
                            "progress.productsScraped": len(all_products),
                        }}
                    )

                    if len(all_products) >= max_products:
                        self.log(job_id, f"Reached max_products ({max_products})")
                        break
                    if not new_products and current_page > 1:
                        self.log(job_id, "No new products — last page")
                        break
                    if current_page >= end_page:
                        self.log(job_id, f"Reached page limit ({max_pages} pages from {start_page})")
                        break

                    await self._page_delay(job_id, current_page + 1)
                    await self._smart_scroll(pw_page, job_id)

                    first_id_before = await pw_page.evaluate("""
                        () => {
                            const el = document.querySelector("[id^='item_id_']");
                            return el ? el.id : null;
                        }
                    """)
                    url_before = pw_page.url
                    self.log(job_id, f"  📌 Before click — item: {first_id_before} | url: {url_before[-60:]}")

                    clicked = await self._playwright_click_next(pw_page, job_id, current_page)
                    if not clicked:
                        break

                    changed = await self._wait_for_dom_change(
                        pw_page, job_id, first_id_before, url_before
                    )
                    if not changed:
                        self.log(job_id, "  ⚠️  Page did not change after click — last page or blocked", "warn")
                        break

                    await asyncio.sleep(random.uniform(1.5, 2.5))
                    await self._add_random_mouse_movements(pw_page, job_id)
                    current_page += 1

                if params.get("includeDetails") and all_products:
                    self.log(job_id, "=== PHASE 2: ENRICHMENT DEFERRED ===")
                    self.log(job_id, "Detail scraping skipped in discovery jobs; candidates will be queued after save.")

            except Exception as e:
                import traceback
                self.log(job_id, f"❌ Playwright session error: {e}\n{traceback.format_exc()}", "error")
                if search_proxy:
                    proxy_service.mark_failure(search_proxy)
            else:
                if search_proxy:
                    proxy_service.mark_success(search_proxy)
            finally:
                await browser.close()

        return all_products[:max_products]

    _PRODUCT_GRID_SELECTORS = [
        "[id^='item_id_']",
        "[class*='doubleCardWrapper']",
        "[class*='item--']",
        "[class*='Card--']",
        ".search-item",
        "[class*='itemWrap']",
    ]

    async def _wait_for_products(self, pw_page, job_id: str, timeout: int = 20000):
        for sel in self._PRODUCT_GRID_SELECTORS:
            try:
                await pw_page.wait_for_selector(sel, timeout=timeout)
                self.log(job_id, f"  ✅ Products ready [{sel}]")
                return
            except Exception:
                continue
        self.log(job_id, "  ⚠️  Product grid not detected", "warn")

    async def _playwright_click_next(self, pw_page, job_id: str, current_page: int) -> bool:

        async def _real_mouse_click(locator, label: str) -> bool:
            try:
                count = await locator.count()
                if count == 0:
                    return False

                el = locator.first
                cls = await el.get_attribute("class") or ""
                is_disabled = await el.is_disabled()
                if is_disabled or "disabled" in cls:
                    self.log(job_id, f"  ⚫ [{label}] found but disabled")
                    return False

                await el.scroll_into_view_if_needed()
                await asyncio.sleep(random.uniform(0.2, 0.5))

                bbox = await el.bounding_box()
                if bbox and bbox["width"] > 0 and bbox["height"] > 0:
                    x = bbox["x"] + bbox["width"]  / 2 + random.uniform(-3, 3)
                    y = bbox["y"] + bbox["height"] / 2 + random.uniform(-2, 2)
                    await pw_page.mouse.move(x, y)
                    await asyncio.sleep(random.uniform(0.05, 0.15))
                    await pw_page.mouse.click(x, y)
                    self.log(job_id, f"  🖱  Real mouse click [{label}] at ({x:.0f},{y:.0f})")
                    return True

                await el.click(force=True, timeout=5000)
                self.log(job_id, f"  🖱  Force click [{label}]")
                return True

            except Exception as e:
                self.log(job_id, f"  ⚠️  [{label}] click error: {e}", "warn")
                return False

        for sel in self._NEXT_BTN_SELECTORS:
            try:
                if await _real_mouse_click(pw_page.locator(sel), sel):
                    return True
            except Exception:
                continue

        for label_fragment in ["下一页", "next page", "Next page"]:
            try:
                loc = pw_page.locator(f"button[aria-label*='{label_fragment}']")
                if await _real_mouse_click(loc, f"aria-label*={label_fragment}"):
                    return True
            except Exception:
                continue

        try:
            span_loc = pw_page.locator("span.next-btn-helper", has_text="下一页")
            count = await span_loc.count()
            if count > 0:
                parent_loc = span_loc.locator("xpath=..")
                if await _real_mouse_click(parent_loc, "parent-of-span[下一页]"):
                    return True
        except Exception:
            pass

        try:
            rect = await pw_page.evaluate("""
                () => {
                    const byAria = document.querySelector(
                        'button[aria-label*="下一页"]:not([disabled])'
                    );
                    if (byAria) {
                        const r = byAria.getBoundingClientRect();
                        if (r.width > 0 && r.height > 0)
                            return { x: r.left + r.width/2, y: r.top + r.height/2,
                                     tag: byAria.tagName, cls: byAria.className.slice(0,60) };
                    }
                    for (const span of document.querySelectorAll('span.next-btn-helper')) {
                        if (span.textContent.trim() !== '下一页') continue;
                        let btn = span.parentElement;
                        while (btn && btn.tagName !== 'BUTTON') btn = btn.parentElement;
                        if (!btn || btn.disabled) continue;
                        const r = btn.getBoundingClientRect();
                        if (r.width > 0 && r.height > 0)
                            return { x: r.left + r.width/2, y: r.top + r.height/2,
                                     tag: btn.tagName, cls: btn.className.slice(0,60) };
                    }
                    const byClass = document.querySelector(
                        'button.next-next:not([disabled])'
                    );
                    if (byClass) {
                        const r = byClass.getBoundingClientRect();
                        if (r.width > 0 && r.height > 0)
                            return { x: r.left + r.width/2, y: r.top + r.height/2,
                                     tag: byClass.tagName, cls: byClass.className.slice(0,60) };
                    }
                    return null;
                }
            """)
            if rect:
                x = rect["x"] + random.uniform(-3, 3)
                y = rect["y"] + random.uniform(-2, 2)
                await pw_page.mouse.move(x, y)
                await asyncio.sleep(random.uniform(0.05, 0.15))
                await pw_page.mouse.click(x, y)
                self.log(job_id, f"  🖱  JS-rect mouse click [{rect['tag']} .{rect['cls']}] at ({x:.0f},{y:.0f})")
                return True
        except Exception as e:
            self.log(job_id, f"  ⚠️  JS-rect strategy failed: {e}", "warn")

        try:
            fired = await pw_page.evaluate("""
                () => {
                    const btn =
                        document.querySelector('button[aria-label*="下一页"]:not([disabled])') ||
                        document.querySelector('button.next-next:not([disabled])') ||
                        (() => {
                            for (const span of document.querySelectorAll('span.next-btn-helper')) {
                                if (span.textContent.trim() !== '下一页') continue;
                                let b = span.parentElement;
                                while (b && b.tagName !== 'BUTTON') b = b.parentElement;
                                if (b && !b.disabled) return b;
                            }
                            return null;
                        })();
                    if (!btn) return null;
                    const opts = { bubbles: true, cancelable: true, view: window, detail: 1 };
                    btn.dispatchEvent(new MouseEvent('pointerover',  opts));
                    btn.dispatchEvent(new MouseEvent('mouseover',    opts));
                    btn.dispatchEvent(new MouseEvent('mouseenter',   opts));
                    btn.dispatchEvent(new MouseEvent('mousemove',    opts));
                    btn.dispatchEvent(new MouseEvent('mousedown',    opts));
                    btn.dispatchEvent(new MouseEvent('mouseup',      opts));
                    btn.dispatchEvent(new MouseEvent('click',        opts));
                    return btn.tagName + ' ' + (btn.className||'').slice(0,60);
                }
            """)
            if fired:
                self.log(job_id, f"  🖱  dispatchEvent click [{fired}]")
                return True
        except Exception as e:
            self.log(job_id, f"  ⚠️  dispatchEvent failed: {e}", "warn")

        self.log(job_id, f"  ⛔ All click strategies failed on page {current_page}")
        return False

    async def _wait_for_dom_change(
        self, pw_page, job_id: str,
        first_id_before: str,
        url_before: str = None,
        timeout: float = 20.0,
    ) -> bool:
        import time
        deadline = time.monotonic() + timeout
        poll_interval = 0.3

        while time.monotonic() < deadline:
            try:
                first_id_now = await pw_page.evaluate("""
                    () => {
                        const el = document.querySelector("[id^='item_id_']");
                        return el ? el.id : null;
                    }
                """)
                if first_id_now and first_id_now != first_id_before:
                    self.log(job_id, f"  ✅ Grid changed: {first_id_before} → {first_id_now}")
                    return True
                if first_id_before is None and first_id_now:
                    self.log(job_id, f"  ✅ Grid populated: {first_id_now}")
                    return True

                if url_before:
                    url_now = pw_page.url
                    if url_now != url_before:
                        self.log(job_id, f"  ✅ URL changed → {url_now[-80:]}")
                        await self._wait_for_products(pw_page, job_id)
                        return True

            except Exception:
                pass

            await asyncio.sleep(poll_interval)

        self.log(
            job_id,
            f"  ⏰ Page change timeout ({timeout}s) — item: {first_id_before} | url: {(url_before or '')[-60:]}",
            "warn",
        )
        return False

    async def _smart_scroll(self, pw_page, job_id: Optional[str], steps: int = 6):
        try:
            for i in range(steps):
                await pw_page.evaluate(f"""
                    () => window.scrollBy({{
                        top: {400 + i * 80},
                        behavior: 'smooth'
                    }})
                """)
                await asyncio.sleep(random.uniform(0.2, 0.45))

            await pw_page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(random.uniform(0.5, 1.0))

            await pw_page.evaluate("""
                () => {
                    const footer = document.querySelector(
                        '[class*="pagination"],[class*="page"],[class*="next"]'
                    );
                    if (footer) {
                        footer.scrollIntoView({behavior: "smooth", block: "center"});
                    } else {
                        window.scrollBy({top: -300, behavior: "smooth"});
                    }
                }
            """)
            await asyncio.sleep(random.uniform(0.3, 0.6))
            self.log(job_id, f"  📜 Scrolled to bottom ({steps} steps)")
        except Exception as e:
            self.log(job_id, f"  ⚠️  Scroll error: {e}", "warn")

    async def _add_random_mouse_movements(self, pw_page, job_id: Optional[str] = None, moves: int = 3):
        """Move the mouse to a few random positions to simulate human scanning behaviour."""
        try:
            vp = pw_page.viewport_size or {"width": 1280, "height": 800}
            for _ in range(moves):
                x = random.randint(100, max(110, vp["width"] - 100))
                y = random.randint(100, max(110, vp["height"] - 100))
                await pw_page.mouse.move(x, y)
                await asyncio.sleep(random.uniform(0.1, 0.3))
        except Exception:
            pass  # non-critical — never interrupt the main flow

    async def _debug_dump_pagination(self, pw_page, job_id: str):
        try:
            debug_path = Path(__file__).parent.parent / "debug_pagination.json"
            pagination_data = await pw_page.evaluate("""
                () => {
                    const results = {};
                    const paginationEls = document.querySelectorAll(
                        '[class*="page"],[class*="navi"],[class*="next"],[class*="prev"],[class*="pager"]'
                    );
                    results.all_pagination_elements = Array.from(paginationEls).slice(0, 30).map(el => ({
                        tag: el.tagName,
                        class: el.className,
                        text: el.textContent.trim().slice(0, 80),
                        disabled: el.disabled || false,
                        html: el.outerHTML.slice(0, 300),
                    }));
                    const nextEls = [];
                    for (const el of document.querySelectorAll('*')) {
                        if (el.children.length === 0 && el.textContent.trim() === '下一页') {
                            nextEls.push({
                                tag: el.tagName,
                                class: el.className,
                                disabled: el.disabled || false,
                                parent_html: (el.parentElement || el).outerHTML.slice(0, 400),
                            });
                        }
                    }
                    results.next_page_elements = nextEls;
                    results.current_url = window.location.href;
                    const dataPageEls = document.querySelectorAll('[data-page],[data-num],[data-index]');
                    results.data_page_attrs = Array.from(dataPageEls).slice(0, 20).map(el => ({
                        tag: el.tagName,
                        class: el.className,
                        data: el.dataset,
                        html: el.outerHTML.slice(0, 200),
                    }));
                    return results;
                }
            """)

            import json as _json
            debug_path.write_text(
                _json.dumps(pagination_data, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8"
            )
            self.log(
                job_id,
                f"🔍 Pagination debug → {debug_path} "
                f"({len(pagination_data.get('all_pagination_elements', []))} pagination els, "
                f"{len(pagination_data.get('next_page_elements', []))} 下一页 els)"
            )
        except Exception as e:
            self.log(job_id, f"⚠️  Pagination debug failed: {e}", "warn")

    # ── 1688: URL-based pagination ─────────────────────────────────────────

    async def _scrape_1688_url_based(
        self, db, job_id: str, params: dict, max_pages: int, start_page: int = 1
    ) -> list[dict]:
        platform     = params["platform"]
        language     = params.get("language", DEFAULT_LANGUAGE)
        lang_cfg     = LANGUAGE_CONFIG.get(language, LANGUAGE_CONFIG[DEFAULT_LANGUAGE])
        max_products = params.get("maxProducts", 100)
        search_url   = self.build_search_url(platform, params)
        cookies      = self.get_all_cookies(platform, language)
        all_products: list[dict] = []
        seen_ids: set[str] = set()
        current_page = start_page
        end_page     = start_page + max_pages - 1

        self.log(job_id, f"Starting 1688 scrape (pages {start_page}→{end_page}): {search_url}")

        while current_page <= end_page and len(all_products) < max_products:

            await self._check_session_limit(job_id)
            if self.active_jobs.get(job_id, {}).get("cancelRequested"):
                self.log(job_id, "🛑 Cancel requested")
                break

            fetch_url = (
                search_url if current_page == 1
                else self._build_page_url(search_url, platform, current_page)
            )

            self.log(job_id, f"📄 Page {current_page}: {fetch_url}")

            page_proxy = proxy_service.get_next() if proxy_service.enabled else None
            if page_proxy:
                self.log(job_id, f"  🔀 Proxy: {page_proxy.host}:{page_proxy.port}")

            try:
                fetch_kwargs = dict(
                    url=fetch_url,
                    headless=self.config["headless"],
                    network_idle=True,
                    extra_headers={"Accept-Language": lang_cfg["accept_language"]},
                )
                if cookies:
                    fetch_kwargs["cookies"] = cookies  # type: ignore[assignment]
                if page_proxy:
                    fetch_kwargs["proxy"] = page_proxy.to_url()

                await rate_limiter.acquire("1688")
                page = await StealthyFetcher.async_fetch(**fetch_kwargs)

                if not page or page.status != 200:
                    self.log(job_id, f"⚠️  Bad response: {getattr(page, 'status', 'None')}", "warn")
                    if page_proxy:
                        proxy_service.mark_failure(page_proxy)
                    break

                html = _html_content(page)
                self.log(job_id, f"  HTML: {len(html):,} chars")

                page_products = self.extract_products_from_page(html, platform, params, current_page)
                self.log(job_id, f"  Extracted: {len(page_products)}")

                if not page_products:
                    self.log(job_id, "No products — stopping")
                    break

                new_products = [p for p in page_products if p["itemId"] not in seen_ids]
                for p in new_products:
                    seen_ids.add(p["itemId"])
                all_products.extend(new_products)
                self._session_product_count += len(new_products)
                self.log(
                    job_id,
                    f"  New: {len(new_products)} | Job total: {len(all_products)} | Session: {self._session_product_count}"
                )

                await db.scraping_jobs.update_one(
                    {"jobId": job_id},
                    {"$set": {
                        "progress.currentPage":     current_page,
                        "progress.productsScraped": len(all_products),
                    }}
                )

                if len(all_products) >= max_products:
                    self.log(job_id, f"Reached max_products ({max_products})")
                    break
                if not new_products and current_page > 1:
                    self.log(job_id, "No new products — last page")
                    break
                if not _has_next_page(html, platform, current_page):
                    self.log(job_id, f"No next page after page {current_page}")
                    break

                if page_proxy:
                    proxy_service.mark_success(page_proxy)

                current_page += 1
                await self._page_delay(job_id, current_page)

            except Exception as e:
                import traceback
                self.log(job_id, f"❌ Page {current_page} error: {e}\n{traceback.format_exc()}", "error")
                if page_proxy:
                    proxy_service.mark_failure(page_proxy)
                break

        return all_products[:max_products]

    # ── Alibaba debug dump ─────────────────────────────────────────────────

    def _dump_alibaba_debug(self, html: str, url: str, job_id: Optional[str]):
        """Dump HTML structure to JSON for selector debugging when 0 products extracted."""
        try:
            import json as _json

            debug_path = Path(__file__).parent.parent / "debug_alibaba.json"

            # Collect all <a href> tags that contain alibaba.com
            all_hrefs = re.findall(r'href="([^"]*alibaba\.com[^"]*)"', html)[:60]

            # Collect all unique class names (first 100)
            all_classes = list(dict.fromkeys(re.findall(r'class="([^"]{3,80})"', html)))[:100]

            # Collect anchor + 200-char context for product-detail links
            pd_matches = re.findall(
                r'(.{0,60}href="[^"]*product-detail[^"]*".{0,60})',
                html,
            )[:30]

            # Collect tags with "price" in class
            price_tags = re.findall(
                r'<[^>]*class="[^"]*price[^"]*"[^>]*>([^<]{0,80})',
                html, re.IGNORECASE,
            )[:20]

            # Collect tags with "title" in class
            title_tags = re.findall(
                r'<[^>]*class="[^"]*title[^"]*"[^>]*>([^<]{0,120})',
                html, re.IGNORECASE,
            )[:20]

            # Collect image src / data-src / lazy-src from img tags
            img_srcs = re.findall(
                r'<img[^>]+(?:src|data-src|lazy-src)="((?:https?:|//)[^"]+)"',
                html, re.IGNORECASE,
            )[:30]

            # 400-char context around each search-card-e-title h2 (first 3 products)
            title_contexts = re.findall(
                r'(.{0,200}<h2[^>]*class="search-card-e-title"[^>]*>[\s\S]{0,400}?</h2>.{0,200})',
                html,
            )[:3]

            debug_data = {
                "url":                   url,
                "html_length":           len(html),
                "alibaba_hrefs":         all_hrefs,
                "product_detail_contexts": pd_matches,
                "price_tag_values":      [v.strip() for v in price_tags if v.strip()],
                "title_tag_values":      [v.strip() for v in title_tags if v.strip()],
                "image_srcs":            img_srcs,
                "title_h2_contexts":     title_contexts,
                "class_names":           all_classes,
            }

            debug_path.write_text(
                _json.dumps(debug_data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            self.log(job_id, f"🔍 Alibaba debug → {debug_path} "
                             f"({len(all_hrefs)} alibaba hrefs, {len(pd_matches)} product-detail contexts)")
        except Exception as e:
            self.log(job_id, f"⚠️  Alibaba debug dump failed: {e}", "warn")

    # ── Alibaba.com: URL-based pagination ──────────────────────────────────

    async def _scrape_alibaba_url_based(
        self, db, job_id: str, params: dict, max_pages: int, start_page: int = 1
    ) -> list[dict]:
        platform     = params["platform"]
        language     = params.get("language", DEFAULT_LANGUAGE)
        lang_cfg     = LANGUAGE_CONFIG.get(language, LANGUAGE_CONFIG[DEFAULT_LANGUAGE])
        max_products = params.get("maxProducts", 100)
        search_url   = self.build_search_url(platform, params)
        cookies      = self.get_all_cookies(platform, language)
        all_products: list[dict] = []
        seen_ids: set[str] = set()
        current_page = start_page
        end_page     = start_page + max_pages - 1

        self.log(job_id, f"Starting Alibaba scrape (pages {start_page}→{end_page}): {search_url}")

        while current_page <= end_page and len(all_products) < max_products:

            await self._check_session_limit(job_id)
            if self.active_jobs.get(job_id, {}).get("cancelRequested"):
                self.log(job_id, "🛑 Cancel requested")
                break

            fetch_url = (
                search_url if current_page == 1
                else self._build_page_url(search_url, platform, current_page)
            )

            self.log(job_id, f"📄 Page {current_page}: {fetch_url}")

            page_proxy = proxy_service.get_next() if proxy_service.enabled else None
            if page_proxy:
                self.log(job_id, f"  🔀 Proxy: {page_proxy.host}:{page_proxy.port}")

            try:
                fetch_kwargs: dict = dict(
                    url=fetch_url,
                    headless=self.config["headless"],
                    # network_idle=True causes Alibaba timeouts — the page keeps
                    # firing XHR requests and never reaches networkidle.
                    # Instead wait for the first product card title to appear.
                    network_idle=False,
                    wait_selector="h2.search-card-e-title",
                    wait_selector_state="attached",
                    timeout=60000,
                    extra_headers={"Accept-Language": lang_cfg["accept_language"]},
                )
                if cookies:
                    fetch_kwargs["cookies"] = cookies
                if page_proxy:
                    fetch_kwargs["proxy"] = page_proxy.to_url()

                await rate_limiter.acquire("alibaba")
                page = await StealthyFetcher.async_fetch(**fetch_kwargs)

                if not page or page.status != 200:
                    self.log(job_id, f"⚠️  Bad response: {getattr(page, 'status', 'None')}", "warn")
                    if page_proxy:
                        proxy_service.mark_failure(page_proxy)
                    break

                html = _html_content(page)
                self.log(job_id, f"  HTML: {len(html):,} chars")

                page_products = self.extract_products_from_page(html, platform, params, current_page)
                self.log(job_id, f"  Extracted: {len(page_products)}")

                if not page_products:
                    if current_page == 1:
                        self._dump_alibaba_debug(html, fetch_url, job_id)
                    self.log(job_id, "No products — stopping")
                    break

                new_products = [p for p in page_products if p["itemId"] not in seen_ids]
                for p in new_products:
                    seen_ids.add(p["itemId"])
                all_products.extend(new_products)
                self._session_product_count += len(new_products)
                self.log(
                    job_id,
                    f"  New: {len(new_products)} | Job total: {len(all_products)} | Session: {self._session_product_count}"
                )

                await db.scraping_jobs.update_one(
                    {"jobId": job_id},
                    {"$set": {
                        "progress.currentPage":     current_page,
                        "progress.productsScraped": len(all_products),
                    }}
                )

                if len(all_products) >= max_products:
                    self.log(job_id, f"Reached max_products ({max_products})")
                    break
                if not new_products and current_page > 1:
                    self.log(job_id, "No new products — last page")
                    break
                if not _has_next_page(html, platform, current_page):
                    self.log(job_id, f"No next page after page {current_page}")
                    break

                if page_proxy:
                    proxy_service.mark_success(page_proxy)

                current_page += 1
                await self._page_delay(job_id, current_page)

            except Exception as e:
                import traceback
                self.log(job_id, f"❌ Page {current_page} error: {e}\n{traceback.format_exc()}", "error")
                if page_proxy:
                    proxy_service.mark_failure(page_proxy)
                break

        return all_products[:max_products]

    def extract_products_from_page(self, html: str, platform: str, params: dict, page_num: int) -> list[dict]:
        if platform == "1688":
            products = _re_extract_1688(html, params, page_num)
        elif platform == "alibaba":
            products = _re_extract_alibaba(html, params, page_num)
        else:
            products = _re_extract_taobao_tmall(html, params, page_num)

        if not products and len(html) > 50_000:
            products = self._extract_with_find_similar(html, platform, params, page_num)

        return products

    def _extract_with_find_similar(
        self, html: str, platform: str, params: dict, page_num: int
    ) -> list[dict]:
        try:
            from scrapling.parser import Selector

            page = Selector(content=html)
            reference = None
            for sel in [
                "[id^='item_id_']",
                "a[href*='item.taobao.com'][href*='id=']",
                "a[href*='detail.tmall.com'][href*='id=']",
                "a[href*='detail.1688.com'][href*='offer/']",
                "a[href*='alibaba.com/product-detail/']",
                "[class*='doubleCardWrapper']",
                "[class*='Card--']",
                "[class*='item--']",
                "[class*='product-title']",
            ]:
                els = page.css(sel)
                if els and els.first:
                    reference = els.first
                    break

            if not reference:
                return []

            similar = reference.find_similar(similarity_threshold=0.2)
            all_cards = [reference] + list(similar) if similar else [reference]
            self.log(None, f"  🔍 find_similar found {len(all_cards)} cards")

            results = []
            seen_ids: set[str] = set()
            for card in all_cards:
                try:
                    card_html = str(card.html_content or "")
                    id_m = re.search(r'(?:id=|offer/)(\d{5,})', card_html)
                    if not id_m:
                        continue
                    item_id = id_m.group(1)
                    if item_id in seen_ids:
                        continue
                    seen_ids.add(item_id)

                    title = ""
                    title_el = card.css("[class*='title--']") or card.css("[title]")
                    if title_el:
                        title = (title_el.first.attrib.get("title")
                                 or title_el.first.text or "").strip()
                    if not title or len(title) < 3:
                        continue

                    price = ""
                    price_el = card.css("[class*='price']")
                    if price_el:
                        price_text = (price_el.first.text or "").strip()
                        pm = re.search(r"([\d,]+\.?\d*)", price_text)
                        if pm:
                            price = pm.group(1).replace(",", "")
                    if not price:
                        continue

                    href_m = re.search(
                        r'href="([^"]*(?:item\.taobao|detail\.tmall|detail\.1688|alibaba\.com/product-detail)[^"]*)"',
                        card_html
                    )
                    link = _normalize_href(href_m.group(1)) if href_m else ""
                    if not link:
                        if platform == "1688":
                            link = f"https://detail.1688.com/offer/{item_id}.html"
                        elif platform == "alibaba":
                            link = f"https://www.alibaba.com/product-detail/product_{item_id}.html"
                        else:
                            link = f"https://item.taobao.com/item.htm?id={item_id}"

                    image = ""
                    img_el = card.css("img")
                    if img_el:
                        src = (img_el.first.attrib.get("src")
                               or img_el.first.attrib.get("data-src") or "")
                        if src:
                            image = _normalize_href(src)

                    actual_platform = (
                        "1688" if "1688.com" in link
                        else "alibaba" if "alibaba.com" in link
                        else "tmall" if "tmall.com" in link
                        else "taobao"
                    )

                    results.append(_enrich_group_category({
                        "itemId": item_id, "title": title[:200], "price": price,
                        "image": image, "link": link, "platform": actual_platform,
                        "searchKeyword": params.get("keyword"),
                        "categoryId": params.get("categoryId"),
                        "categoryName": params.get("categoryName"),
                        "pageNumber": page_num,
                        "language": params.get("language", DEFAULT_LANGUAGE),
                        "extractedAt": datetime.now(UTC).isoformat(),
                        "detailsScraped": False,
                        "_extractionMethod": "find_similar",
                    }, params))
                except Exception:
                    continue

            return results
        except Exception as e:
            self.log(None, f"  ⚠️  find_similar fallback failed: {e}", "warn")
            return []

    # ── Detail scraping ────────────────────────────────────────────────────

    async def scrape_all_product_details(
        self, db, job_id: str, products: list, platform: str,
        language: str = DEFAULT_LANGUAGE,
        shared_context=None,
    ) -> list:
        success = 0
        failed  = 0
        consecutive_failures = 0

        for i, product in enumerate(products):
            if self.active_jobs.get(job_id, {}).get("cancelRequested"):
                break

            self.log(job_id, f"  [{i+1}/{len(products)}] {product['itemId']}")

            if consecutive_failures >= 3:
                self.log(job_id, f"  ⚠️  {consecutive_failures} consecutive failures — cooling down 30s", "warn")
                await asyncio.sleep(30.0)
                consecutive_failures = 0

            try:
                details = await self._scrape_product_detail(product, platform, language, shared_context=shared_context)
                if details and details.dataQuality.completeness >= self.config["min_extraction_quality"]:
                    import dataclasses
                    detail_dict = dataclasses.asdict(details)
                    if should_translate(language):
                        try:
                            detail_dict = await translate_detail(detail_dict, language)
                            self.log(job_id, f"    🌐 Translated details → {language}")
                        except Exception as te:
                            self.log(job_id, f"    ⚠️  Detail translation failed: {te}", "warn")
                    product["detailedInfo"]      = detail_dict
                    product["detailsScraped"]    = True
                    product["detailsScrapedAt"]  = datetime.now(UTC)
                    product["extractionQuality"] = details.dataQuality.completeness
                    success += 1
                    consecutive_failures = 0
                    self.log(job_id, f"    ✅ quality={details.dataQuality.completeness}%")
                else:
                    product["detailsScraped"] = False
                    failed += 1
                    consecutive_failures += 1
                    self.log(job_id, f"    ⚠️  low quality or no data")
                    try:
                        await enqueue_retry(db, product["itemId"], product.get("platform", "taobao"), language, "Low quality")
                    except Exception:
                        pass
            except Exception as e:
                product["detailsScraped"] = False
                failed += 1
                consecutive_failures += 1
                self.log(job_id, f"    ❌ {e}", "warn")
                try:
                    await enqueue_retry(db, product["itemId"], product.get("platform", "taobao"), language, str(e))
                except Exception:
                    pass

            is_batch_end = (i + 1) % self.config["details_per_batch"] == 0

            if is_batch_end or (i + 1) % 5 == 0:
                await db.scraping_jobs.update_one(
                    {"jobId": job_id},
                    {"$set": {
                        "progress.detailsScraped": success,
                        "progress.detailsFailed": failed,
                        "results.successfulDetails": success,
                        "results.failedDetails": failed,
                    }}
                )

            await self._detail_delay(job_id, i, len(products), is_batch_end)

        return products

    _DETAIL_READY_SELECTORS = [
        "[class*='mainTitle--']",
        "[class*='ItemTitle--']",
        "[class*='GeneralSkuPanel--']",
        "[class*='bodyWrap--']",
        "[class*='highlightPrice']",
        "[class*='MainTitle']",
        "h1[class*='title']",
        ".tb-detail-hd h1",
    ]
    _DETAIL_READY_TIMEOUT_MS = 8000
    _DEBUG_DETAIL = os.getenv("DEBUG_DETAIL_PAGES", "").lower() in ("true", "1", "yes")

    async def _scrape_product_detail(
        self, product: dict, platform: str, language: str = DEFAULT_LANGUAGE,
        shared_context=None,
    ):
        meta = await self._scrape_product_detail_with_meta(
            product,
            platform,
            language=language,
            shared_context=shared_context,
        )
        return meta.get("result")

    def _classify_detail_failure(self, html: str, result=None) -> tuple[str, str]:
        lowered = (html or "").lower()
        if not html or len(html.strip()) < 500:
            return "empty_dom", "detail page returned almost no HTML"
        if any(marker in lowered for marker in ("login.taobao.com", "sec.taobao.com", "punish", "verify", "baxia")):
            return "interstitial", "detail page appears to be a login, verify, or punishment page"
        if result is None:
            return "selector_miss", "detail page loaded but no known selector appeared"

        quality = getattr(getattr(result, "dataQuality", None), "completeness", 0) or 0
        if quality <= 0:
            return "no_data", "detail extractor returned no usable fields"
        if quality < self.config["min_extraction_quality"]:
            return "low_quality", f"detail quality below threshold ({quality}% < {self.config['min_extraction_quality']}%)"
        return "unknown", "detail extraction failed for an unknown reason"

    async def _scrape_product_detail_with_meta(
        self, product: dict, platform: str, language: str = DEFAULT_LANGUAGE,
        shared_context=None,
    ) -> dict:
        from scrapling import Selector

        lang_cfg = LANGUAGE_CONFIG.get(language, LANGUAGE_CONFIG[DEFAULT_LANGUAGE])
        lang_param = lang_cfg["lang_param"]
        url = product["link"]
        if f"lang={lang_param}" not in url:
            url = url + ("&" if "?" in url else "?") + f"lang={lang_param}"

        detail_proxy = proxy_service.get_next() if (proxy_service.enabled and not shared_context) else None

        for attempt in range(self.config["max_retries"] + 1):
            pw_page = None
            own_browser = None
            own_pw = None
            try:
                if shared_context:
                    pw_page = await shared_context.new_page()
                    search_url = PLATFORM_CONFIG[platform]["baseUrl"]
                    await pw_page.set_extra_http_headers({"Referer": search_url})
                    stealth_script = _build_stealth_script(
                        _pick_ua(), lang_cfg["accept_language"],
                        timezone=_LANG_TIMEZONE.get(language, "Asia/Shanghai"),
                    )
                    await pw_page.add_init_script(stealth_script)
                else:
                    from playwright.async_api import async_playwright as _apw
                    own_pw = await _apw().__aenter__()
                    ua_entry = _pick_ua()
                    viewport = _pick_viewport()
                    detail_launch_kwargs = dict(
                        headless=self.config["headless"],
                        args=[
                            "--no-sandbox", "--disable-setuid-sandbox",
                            "--disable-blink-features=AutomationControlled",
                            "--disable-dev-shm-usage",
                            "--disable-infobars",
                            "--disable-extensions",
                            f"--lang={lang_cfg['locale']}",
                        ],
                    )
                    if detail_proxy:
                        detail_launch_kwargs["proxy"] = detail_proxy.to_playwright_proxy()

                    own_browser = await own_pw.chromium.launch(**detail_launch_kwargs)

                    pw_cookies = []
                    for c in self.get_all_cookies(platform, language):
                        pw_c = {
                            "name":   c["name"],
                            "value":  c["value"],
                            "domain": c.get("domain", ".taobao.com"),
                            "path":   c.get("path", "/"),
                        }
                        if c.get("expires") and c["expires"] > 0:
                            pw_c["expires"] = float(c["expires"])
                        if "httpOnly" in c:
                            pw_c["httpOnly"] = bool(c["httpOnly"])
                        if "secure" in c:
                            pw_c["secure"] = bool(c["secure"])
                        pw_cookies.append(pw_c)

                    own_ctx = await own_browser.new_context(
                        user_agent=ua_entry["ua"],
                        locale=lang_cfg["locale"],
                        viewport=viewport,  # type: ignore[arg-type]
                        extra_http_headers={"Accept-Language": lang_cfg["accept_language"]},
                    )
                    if pw_cookies:
                        await own_ctx.add_cookies(pw_cookies)

                    pw_page = await own_ctx.new_page()
                    stealth_script = _build_stealth_script(
                        ua_entry, lang_cfg["accept_language"],
                        timezone=_LANG_TIMEZONE.get(language, "Asia/Shanghai"),
                    )
                    await pw_page.add_init_script(stealth_script)

                try:
                    await rate_limiter.acquire(platform)
                    await pw_page.goto(url, wait_until="domcontentloaded", timeout=30000)

                    page_ready = False
                    pre_ready_html = ""
                    for sel in self._DETAIL_READY_SELECTORS:
                        try:
                            await pw_page.wait_for_selector(sel, timeout=self._DETAIL_READY_TIMEOUT_MS)
                            self.log(None, f"    ⏳ Detail ready [{sel}]")
                            page_ready = True
                            break
                        except Exception:
                            continue

                    if not page_ready:
                        try:
                            pre_ready_html = await pw_page.content()
                        except Exception:
                            pre_ready_html = ""
                        self.log(None, "    ⚠️  Detail page: no known selector appeared — skipping", "warn")
                        failure_type, failure_reason = self._classify_detail_failure(pre_ready_html, None)
                        return {
                            "result": None,
                            "failureType": failure_type,
                            "failureReason": failure_reason,
                            "htmlLength": len(pre_ready_html),
                        }

                    await self._smart_scroll(pw_page, None, steps=8)
                    await asyncio.sleep(random.uniform(2.5, 3.5))
                    await pw_page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(random.uniform(1.5, 2.0))

                    html = await pw_page.content()

                    if self._DEBUG_DETAIL:
                        await self._debug_dump_detail(pw_page, product["itemId"], html)

                    page_selector = Selector(html, url=url)
                    result = self.detail_extractor.extract_product_details(page_selector, html, platform)
                    if detail_proxy:
                        proxy_service.mark_success(detail_proxy)
                    if result and getattr(getattr(result, "dataQuality", None), "completeness", 0) >= self.config["min_extraction_quality"]:
                        return {
                            "result": result,
                            "failureType": None,
                            "failureReason": None,
                            "htmlLength": len(html),
                        }
                    failure_type, failure_reason = self._classify_detail_failure(html, result)
                    return {
                        "result": result,
                        "failureType": failure_type,
                        "failureReason": failure_reason,
                        "htmlLength": len(html),
                    }

                finally:
                    if pw_page:
                        try:
                            await pw_page.close()
                        except Exception:
                            pass
                    if own_browser:
                        await own_browser.close()
                    if own_pw:
                        try:
                            await own_pw.__aexit__(None, None, None)
                        except Exception:
                            pass

            except Exception:
                if detail_proxy:
                    proxy_service.mark_failure(detail_proxy)
                if attempt < self.config["max_retries"]:
                    await asyncio.sleep(_human_delay(3.0, (1.0, 4.0)))
                    continue
                raise

    async def _debug_dump_detail(self, pw_page, item_id: str, html: str):
        try:
            debug_path = Path(__file__).parent.parent / f"debug_detail_{item_id}.json"

            structure = await pw_page.evaluate("""
                () => {
                    const keywords = [
                        'title','Title','price','Price','Price--','priceWrap','priceText',
                        'highlightPrice','subPrice','originPrice',
                        'sales','Sales','salesDesc','salesNumber','soldCount','tradeCount',
                        'payCount','countText','salesVolume',
                        'rating','Rating','star','Star','shop','Shop',
                        'brand','Brand','spec','Spec','param','Param',
                        'sku','Sku','variant','Variant','gallery','Gallery',
                        'thumb','Thumb','image','Image','desc','Desc',
                        'guarantee','Guarantee','review','Review','comment','Comment',
                        'commentNum','rateCount','reviewCount',
                        'seller','Seller','store','Store','highlight',
                        'Purchase','purchase','GeneralSku','bodyWrap','attrItem',
                        'emphasisParams','generalParams','paramsInfo',
                    ];

                    const results = {};

                    for (const kw of keywords) {
                        const els = document.querySelectorAll(`[class*="${kw}"]`);
                        if (!els.length) continue;
                        results[kw] = Array.from(els).slice(0, 5).map(el => ({
                            tag:  el.tagName,
                            cls:  el.className.slice(0, 120),
                            text: el.textContent.trim().slice(0, 150),
                            html: el.outerHTML.slice(0, 300),
                        }));
                    }

                    results._meta = {
                        url:   window.location.href,
                        title: document.title,
                        bodyLen: document.body.innerHTML.length,
                        htmlLang: document.documentElement.lang || null,
                        contentLanguage: document.querySelector('meta[http-equiv="content-language"]')?.content || null,
                        detectedLocale: navigator.language || null,
                    };

                    return results;
                }
            """)

            import json as _json
            debug_data = {
                "item_id":   item_id,
                "url":       pw_page.url,
                "html_len":  len(html),
                "structure": structure,
            }
            debug_path.write_text(
                _json.dumps(debug_data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            self.log(None, f"    🔍 Detail debug → {debug_path} ({len(html):,} chars)")
        except Exception as e:
            self.log(None, f"    ⚠️  Detail debug dump failed: {e}", "warn")

    # ── Bulk save ──────────────────────────────────────────────────────────

    async def save_products_bulk(self, db, products: list, platform: str) -> dict:
        if not products:
            return {"inserted": 0, "updated": 0, "changed": 0, "changed_item_ids": []}
        from pymongo import UpdateOne

        now = datetime.now(UTC)
        item_ids = [p["itemId"] for p in products]
        existing_map = {
            doc["itemId"]: doc
            async for doc in db.products.find(
                {"itemId": {"$in": item_ids}},
                {
                    "itemId": 1,
                    "detailsScraped": 1,
                    "categoryId": 1,
                    "contentHash": 1,
                    "enrichmentStatus": 1,
                    "lastSuccessfulDetailAt": 1,
                },
            )
        }
        ops = []
        changed_item_ids: list[str] = []
        for p in products:
            existing    = existing_map.get(p["itemId"])
            category_id = (
                p.get("categoryId")
                or (existing.get("categoryId") if existing else None)
                or self.generate_category_id(
                    p.get("categoryName") or p.get("searchKeyword") or "unknown", platform
                )
            )
            details_scraped = (
                True
                if (existing and existing.get("detailsScraped") and not p.get("detailsScraped"))
                else p.get("detailsScraped", False)
            )
            enriched = _enrich_group_category(
                {**p, "categoryId": category_id},
                {"platform": platform},
            )
            discovery_hash = _compute_discovery_hash({**enriched, "categoryId": category_id})
            is_changed = not existing or existing.get("contentHash") != discovery_hash
            if is_changed:
                changed_item_ids.append(p["itemId"])
            existing_status = (existing or {}).get("enrichmentStatus")
            enrichment_status = (
                "pending"
                if is_changed
                else existing_status or ("completed" if details_scraped else "pending")
            )
            ops.append(UpdateOne(
                {"itemId": p["itemId"]},
                {
                    "$set": {
                        **enriched,
                        "categoryId":     category_id,
                        "platform":       enriched.get("platform", platform),
                        "detailsScraped": details_scraped,
                        "contentHash": discovery_hash,
                        "lastSeenAt": now,
                        "lastDiscoveryAt": now,
                        "enrichmentStatus": enrichment_status,
                        "updatedAt": now,
                    },
                    "$setOnInsert": {
                        "createdAt": now,
                        "enrichmentAttempts": 0,
                        "enrichmentSource": None,
                        "lastEnrichmentError": None,
                        "lastSuccessfulDetailAt": (existing or {}).get("lastSuccessfulDetailAt"),
                    },
                },
                upsert=True,
            ))
        result = await db.products.bulk_write(ops, ordered=False)

        try:
            await record_price_snapshots_bulk(db, products)
        except Exception as e:
            self.log(None, f"⚠️  Price history recording failed: {e}", "warn")

        return {
            "inserted": result.upserted_count or 0,
            "updated": result.modified_count or 0,
            "changed": len(changed_item_ids),
            "changed_item_ids": changed_item_ids,
        }

    async def enqueue_enrichment_candidates(
        self,
        db,
        products: list[dict],
        platform: str,
        changed_item_ids: list[str],
        *,
        reason: str,
        source_policy: list[str],
        discovery_job_id: Optional[str] = None,
    ) -> int:
        if not products or not changed_item_ids:
            return 0

        from pymongo import UpdateOne

        changed = {item_id for item_id in changed_item_ids}
        now = datetime.now(UTC)
        queue_ops = []
        product_ops = []
        queued = 0
        for product in products:
            item_id = product["itemId"]
            if item_id not in changed:
                continue
            queued += 1
            queue_ops.append(UpdateOne(
                {"itemId": item_id},
                {
                    "$set": {
                        "platform": product.get("platform", platform),
                        "status": "pending",
                        "priority": 100,
                        "reason": reason,
                        "sourcePolicy": source_policy,
                        "discoveryJobId": discovery_job_id,
                        "title": product.get("title"),
                        "updatedAt": now,
                        "nextAttemptAt": now,
                    },
                    "$setOnInsert": {
                        "createdAt": now,
                        "attempts": 0,
                    },
                },
                upsert=True,
            ))
            product_ops.append(UpdateOne(
                {"itemId": item_id},
                {
                    "$set": {
                        "enrichmentStatus": "queued",
                        "updatedAt": now,
                    }
                },
            ))

        if queue_ops:
            await db.enrichment_queue.bulk_write(queue_ops, ordered=False)
            await db.products.bulk_write(product_ops, ordered=False)
        return queued

    def _compute_retry_schedule(self, attempts: int) -> tuple[str, datetime]:
        max_attempts = self.config.get("enrichment_retry_max_attempts", 5)
        if attempts >= max_attempts:
            return "failed", datetime.now(UTC)
        base_minutes = self.config.get("enrichment_retry_base_minutes", 15)
        delay_minutes = base_minutes * (2 ** max(0, attempts - 1))
        return "retry", datetime.now(UTC) + timedelta(minutes=delay_minutes)

    async def list_enrichment_queue(
        self,
        db,
        *,
        status: Optional[str] = None,
        platform: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        filt: dict = {}
        if status:
            filt["status"] = status
        if platform and platform != "all":
            filt["platform"] = platform
        cursor = db.enrichment_queue.find(filt, {"_id": 0}).sort(
            [("priority", -1), ("nextAttemptAt", 1), ("updatedAt", -1)]
        ).limit(limit)
        return await cursor.to_list(length=limit)

    async def requeue_enrichment_items(
        self,
        db,
        *,
        item_ids: Optional[list[str]] = None,
        platform: Optional[str] = None,
        status: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> dict:
        now = datetime.now(UTC)
        filt: dict = {}
        if item_ids:
            filt["itemId"] = {"$in": item_ids}
        if platform and platform != "all":
            filt["platform"] = platform
        if status:
            filt["status"] = status

        selected_ids: list[str]
        if limit:
            docs = await db.enrichment_queue.find(filt, {"_id": 0, "itemId": 1}).limit(limit).to_list(length=limit)
            selected_ids = [doc["itemId"] for doc in docs]
            if not selected_ids:
                return {"matched": 0, "updated": 0}
            filt = {"itemId": {"$in": selected_ids}}
        else:
            docs = await db.enrichment_queue.find(filt, {"_id": 0, "itemId": 1}).to_list(length=10000)
            selected_ids = [doc["itemId"] for doc in docs]
            if not selected_ids:
                return {"matched": 0, "updated": 0}

        result = await db.enrichment_queue.update_many(
            filt,
            {"$set": {
                "status": "pending",
                "nextAttemptAt": now,
                "lastError": None,
                "updatedAt": now,
            }}
        )
        await db.products.update_many(
            {"itemId": {"$in": selected_ids}},
            {"$set": {
                "enrichmentStatus": "queued",
                "lastEnrichmentError": None,
                "updatedAt": now,
            }}
        )
        return {"matched": result.matched_count, "updated": result.modified_count}

    # ── Pending details job ───────────────────────────────────────────────

    async def start_pending_details_job(self, db, params: dict) -> dict:
        if (
            params.get("useSession", True)
            and params.get("clearCookiesOnComplete", False)
            and params.get("platform")
            and params.get("platform") != "all"
            and self.has_active_session_job(params["platform"])
        ):
            raise RuntimeError(
                f"Another authenticated job is already running for session scope '{_session_scope(params['platform'])}'."
            )

        job_id = str(uuid.uuid4())
        self.log(job_id, f"Creating pending-details job: {params}")
        job_doc = {
            "jobId": job_id, "platform": params["platform"],
            "searchType": "pending_details",
            "searchParams": params, "status": "pending",
            "progress": {"currentPage": 0, "productsScraped": 0, "detailsScraped": 0, "detailsFailed": 0},
            "results": {"totalProducts": params.get("pendingCount", 0), "updatedProducts": 0,
                         "successfulDetails": 0, "failedDetails": 0, "detailsScraped": 0},
            "error": None, "startedAt": None, "completedAt": None,
            "createdAt": datetime.now(UTC), "updatedAt": datetime.now(UTC),
        }
        await db.scraping_jobs.insert_one(job_doc)
        asyncio.create_task(self._execute_pending_details_job(db, job_id, params))
        return {"jobId": job_id, "status": "started", "pendingCount": params.get("pendingCount", 0)}

    async def _execute_pending_details_job(self, db, job_id: str, params: dict):
        import dataclasses
        import re as _re

        await db.scraping_jobs.update_one(
            {"jobId": job_id},
            {"$set": {"status": "running", "startedAt": datetime.now(UTC)}}
        )
        platform_scope = params.get("platform") if params.get("platform") != "all" else None
        self.active_jobs[job_id] = {
            "status": "running",
            "cancelRequested": False,
            "platform": platform_scope,
            "sessionScope": _session_scope(platform_scope) if platform_scope else None,
            "useSession": params.get("useSession", True),
            "clearCookiesOnComplete": params.get("clearCookiesOnComplete", False),
        }

        try:
            mode = params.get("mode", "pending")
            filt: dict = {}
            platform_filter = params.get("platform")
            now = datetime.now(UTC)

            if mode == "pending":
                queue_filt: dict = {
                    "status": {"$in": ["pending", "retry"]},
                    "nextAttemptAt": {"$lte": now},
                }
                if platform_scope:
                    queue_filt["platform"] = platform_scope

                queue_cursor = db.enrichment_queue.find(queue_filt, {"_id": 0, "itemId": 1}).sort(
                    [("priority", -1), ("nextAttemptAt", 1), ("updatedAt", 1)]
                )
                if params.get("limit"):
                    queue_cursor = queue_cursor.limit(params["limit"])
                queue_entries = await queue_cursor.to_list(length=params.get("limit") or 10000)
                queued_ids = [entry["itemId"] for entry in queue_entries]
                if not queued_ids:
                    products = []
                else:
                    product_filt: dict = {"itemId": {"$in": queued_ids}}
                    keyword = params.get("keyword")
                    if keyword:
                        product_filt["searchKeyword"] = _re.compile(keyword, _re.I)
                    category_name = params.get("categoryName")
                    if category_name:
                        product_filt["categoryName"] = _re.compile(category_name, _re.I)
                    projection = {
                        "_id": 0, "itemId": 1, "link": 1, "platform": 1,
                        "detailsScraped": 1, "title": 1, "searchKeyword": 1,
                        "categoryName": 1, "extractionQuality": 1,
                    }
                    products = await db.products.find(product_filt, projection).to_list(length=len(queued_ids))
                    by_id = {p["itemId"]: p for p in products}
                    products = [by_id[item_id] for item_id in queued_ids if item_id in by_id]
            elif mode == "low":
                threshold = params.get("minQuality") or 50
                filt["$or"] = [
                    {"detailsScraped": False},
                    {"extractionQuality": {"$lt": threshold}},
                ]
                if platform_filter and platform_filter != "all":
                    filt["platform"] = platform_filter

                keyword = params.get("keyword")
                if keyword:
                    filt["searchKeyword"] = _re.compile(keyword, _re.I)

                category_name = params.get("categoryName")
                if category_name:
                    filt["categoryName"] = _re.compile(category_name, _re.I)

                limit = params.get("limit")
                projection = {
                    "_id": 0, "itemId": 1, "link": 1, "platform": 1,
                    "detailsScraped": 1, "title": 1, "searchKeyword": 1,
                    "categoryName": 1, "extractionQuality": 1,
                }
                cursor = db.products.find(filt, projection).sort("createdAt", -1)
                if limit:
                    cursor = cursor.limit(limit)
                products = await cursor.to_list(length=limit or 10000)
            else:
                if platform_filter and platform_filter != "all":
                    filt["platform"] = platform_filter

                keyword = params.get("keyword")
                if keyword:
                    filt["searchKeyword"] = _re.compile(keyword, _re.I)

                category_name = params.get("categoryName")
                if category_name:
                    filt["categoryName"] = _re.compile(category_name, _re.I)

                limit = params.get("limit")
                projection = {
                    "_id": 0, "itemId": 1, "link": 1, "platform": 1,
                    "detailsScraped": 1, "title": 1, "searchKeyword": 1,
                    "categoryName": 1, "extractionQuality": 1,
                }
                cursor = db.products.find(filt, projection).sort("createdAt", -1)
                if limit:
                    cursor = cursor.limit(limit)
                products = await cursor.to_list(length=limit or 10000)

            total = len(products)
            self.log(job_id, f"=== BATCH DETAILS: {total} products (mode={mode}) ===")

            await db.scraping_jobs.update_one(
                {"jobId": job_id},
                {"$set": {
                    "results.totalProducts": total,
                    "progress.totalPages": total,
                }}
            )

            delay_min = params.get("delayMin", 5000) / 1000.0
            delay_max = params.get("delayMax", 12000) / 1000.0
            success = 0
            failed = 0
            skipped = 0
            consecutive_failures = 0

            for i, product in enumerate(products):
                if self.active_jobs.get(job_id, {}).get("cancelRequested"):
                    self.log(job_id, "🛑 Cancel requested")
                    break

                platform = product.get("platform", "taobao")
                item_id = product["itemId"]
                old_quality = product.get("extractionQuality", 0) or 0

                re_scrape = product.get("detailsScraped", False)
                label = f"re-scrape q={old_quality}%" if re_scrape else "new"
                self.log(job_id, f"  [{i+1}/{total}] {item_id} ({platform}) [{label}]")

                if consecutive_failures >= 3:
                    self.log(job_id, f"  ⚠️  {consecutive_failures} consecutive failures — cooling down 30s", "warn")
                    await asyncio.sleep(30.0)
                    consecutive_failures = 0

                try:
                    language = params.get("language", DEFAULT_LANGUAGE)
                    detail_meta = await self._scrape_product_detail_with_meta(product, platform, language)
                    details = detail_meta.get("result")
                    if details and details.dataQuality.completeness >= self.config["min_extraction_quality"]:
                        new_quality = details.dataQuality.completeness

                        if re_scrape and new_quality <= old_quality:
                            skipped += 1
                            consecutive_failures = 0
                            await db.products.update_one(
                                {"itemId": item_id},
                                {"$set": {
                                    "enrichmentStatus": "completed",
                                    "lastEnrichmentError": None,
                                    "updatedAt": datetime.now(UTC),
                                }}
                            )
                            await db.enrichment_queue.update_one(
                                {"itemId": item_id},
                                {"$set": {
                                    "status": "completed",
                                    "completedAt": datetime.now(UTC),
                                    "updatedAt": datetime.now(UTC),
                                }}
                            )
                            self.log(job_id, f"    ⏭  skipped — new quality ({new_quality}%) <= existing ({old_quality}%)")
                        else:
                            detail_dict = dataclasses.asdict(details)
                            if should_translate(language):
                                try:
                                    detail_dict = await translate_detail(detail_dict, language)
                                    self.log(job_id, f"    🌐 Translated details → {language}")
                                except Exception as te:
                                    self.log(job_id, f"    ⚠️  Detail translation failed: {te}", "warn")
                            await db.products.update_one(
                                {"itemId": item_id},
                                {"$set": {
                                    "detailedInfo": detail_dict,
                                    "detailsScraped": True,
                                    "detailsScrapedAt": datetime.now(UTC),
                                    "lastSuccessfulDetailAt": datetime.now(UTC),
                                    "extractionQuality": new_quality,
                                    "enrichmentStatus": "completed",
                                    "enrichmentSource": "browser_fallback",
                                    "lastEnrichmentError": None,
                                    "updatedAt": datetime.now(UTC),
                                },
                                "$inc": {
                                    "enrichmentAttempts": 1,
                                }}
                            )
                            await db.enrichment_queue.update_one(
                                {"itemId": item_id},
                                {"$set": {
                                    "status": "completed",
                                    "completedAt": datetime.now(UTC),
                                    "updatedAt": datetime.now(UTC),
                                }}
                            )
                            try:
                                await record_price_snapshot(db, {**product, "detailedInfo": detail_dict})
                            except Exception:
                                pass
                            success += 1
                            consecutive_failures = 0
                            improvement = f" (+{new_quality - old_quality}%)" if re_scrape else ""
                            self.log(job_id, f"    ✅ quality={new_quality}%{improvement}")
                    else:
                        failed += 1
                        consecutive_failures += 1
                        failure_type = detail_meta.get("failureType") or "low_quality"
                        failure_reason = detail_meta.get("failureReason") or "low quality or no data"
                        queue_doc = await db.enrichment_queue.find_one({"itemId": item_id}, {"_id": 0, "attempts": 1})
                        attempts = (queue_doc or {}).get("attempts", 0) + 1
                        next_status, next_attempt_at = self._compute_retry_schedule(attempts)
                        await db.products.update_one(
                            {"itemId": item_id},
                            {
                                "$set": {
                                    "enrichmentStatus": next_status,
                                    "lastEnrichmentError": failure_reason,
                                    "lastEnrichmentFailureType": failure_type,
                                    "updatedAt": datetime.now(UTC),
                                },
                                "$inc": {"enrichmentAttempts": 1},
                            }
                        )
                        await db.enrichment_queue.update_one(
                            {"itemId": item_id},
                            {"$set": {
                                "status": next_status,
                                "lastError": failure_reason,
                                "lastFailureType": failure_type,
                                "nextAttemptAt": next_attempt_at,
                                "updatedAt": datetime.now(UTC),
                            },
                            "$inc": {"attempts": 1}}
                        )
                        self.log(job_id, f"    ⚠️  {failure_type}: {failure_reason}")
                except Exception as e:
                    failed += 1
                    consecutive_failures += 1
                    queue_doc = await db.enrichment_queue.find_one({"itemId": item_id}, {"_id": 0, "attempts": 1})
                    attempts = (queue_doc or {}).get("attempts", 0) + 1
                    next_status, next_attempt_at = self._compute_retry_schedule(attempts)
                    await db.products.update_one(
                        {"itemId": item_id},
                        {
                            "$set": {
                                "enrichmentStatus": next_status,
                                "lastEnrichmentError": str(e),
                                "lastEnrichmentFailureType": "fetch_error",
                                "updatedAt": datetime.now(UTC),
                            },
                            "$inc": {"enrichmentAttempts": 1},
                        }
                    )
                    await db.enrichment_queue.update_one(
                        {"itemId": item_id},
                        {"$set": {
                            "status": next_status,
                            "lastError": str(e),
                            "lastFailureType": "fetch_error",
                            "nextAttemptAt": next_attempt_at,
                            "updatedAt": datetime.now(UTC),
                        },
                        "$inc": {"attempts": 1}}
                    )
                    self.log(job_id, f"    ❌ {e}", "warn")

                if (i + 1) % 5 == 0 or i == total - 1:
                    await db.scraping_jobs.update_one(
                        {"jobId": job_id},
                        {"$set": {
                            "progress.detailsScraped": success,
                            "progress.detailsFailed": failed,
                            "progress.productsScraped": i + 1,
                            "results.successfulDetails": success,
                            "results.failedDetails": failed,
                            "results.updatedProducts": skipped,
                            "updatedAt": datetime.now(UTC),
                        }}
                    )

                if i < total - 1:
                    delay = random.uniform(delay_min, delay_max)
                    await asyncio.sleep(delay)

            await db.scraping_jobs.update_one(
                {"jobId": job_id},
                {"$set": {
                    "status": "completed",
                    "results.successfulDetails": success,
                    "results.failedDetails": failed,
                    "results.detailsScraped": success,
                    "results.updatedProducts": skipped,
                    "completedAt": datetime.now(UTC),
                }}
            )
            self.log(
                job_id,
                f"✅ Batch details done: {success} success, {failed} failed, {skipped} skipped (unchanged)"
            )
        except Exception as e:
            self.log(job_id, f"Batch details job failed: {e}", "error")
            await db.scraping_jobs.update_one(
                {"jobId": job_id},
                {"$set": {"status": "failed", "error": str(e)}}
            )
        finally:
            if (
                params.get("useSession", True)
                and params.get("clearCookiesOnComplete", False)
                and params.get("platform")
                and params.get("platform") != "all"
            ):
                self.clear_saved_cookies(params["platform"], job_id=job_id)
            self.active_jobs.pop(job_id, None)

    # ── Session status ─────────────────────────────────────────────────────

    def check_cookie_health_for_job(self, platform: str, job_id: str) -> bool:
        import time as _time
        path = COOKIE_PATHS.get(platform)
        if not path or not path.exists():
            self.log(job_id, f"⚠️  Cookie file missing for {platform} — scrape may fail without a session", "warn")
            return True

        try:
            raw = json.loads(path.read_text())
            cookies = raw if isinstance(raw, list) else []
        except Exception as e:
            self.log(job_id, f"⚠️  Could not read cookies for {platform}: {e}", "warn")
            return True

        if not cookies:
            self.log(job_id, f"⚠️  Cookie file for {platform} is empty — results may be limited", "warn")
            return True

        now = int(_time.time())
        timed = [c for c in cookies if c.get("expires", 0) > 0]
        expired = [c for c in timed if c["expires"] < now]
        near_expiry = [c for c in timed if now <= c["expires"] < now + 86400 * 3]

        if timed and len(expired) == len(timed):
            self.log(
                job_id,
                f"❌ All {len(expired)} session cookies for {platform} are EXPIRED — aborting job. "
                f"Re-login via /api/sessions/qr-login or upload fresh cookies.",
                "error",
            )
            return False

        if expired:
            self.log(
                job_id,
                f"⚠️  {len(expired)}/{len(timed)} cookies for {platform} have expired — session may be degraded",
                "warn",
            )

        if near_expiry:
            hours = min(
                max(0, c["expires"] - now) for c in near_expiry
            ) // 3600
            self.log(
                job_id,
                f"⚠️  {len(near_expiry)} cookie(s) for {platform} expire within 3 days "
                f"(soonest: ~{hours}h) — consider refreshing your session",
                "warn",
            )

        return True

    def get_session_status(self) -> dict:
        import time as time_mod

        def check(label: str, path: Path, cache_key: str) -> dict:
            if not path.exists():
                return {"label": label, "file": path.name, "status": "missing",
                        "message": f"File not found. Run: python utils/login_helper.py --platform {label}"}
            try:
                raw     = json.loads(path.read_text())
                cookies = raw if isinstance(raw, list) else []
            except Exception as e:
                return {"label": label, "file": path.name, "status": "error", "message": str(e)}
            if not cookies:
                return {"label": label, "file": path.name, "status": "empty", "message": "Cookie file is empty"}

            now         = int(time_mod.time())
            timed       = [c for c in cookies if c.get("expires", 0) > 0]
            expired     = [c for c in timed if c["expires"] < now]
            near_expiry = [c for c in timed if now <= c["expires"] < now + 86400 * 3]
            oldest      = min((c["expires"] for c in timed), default=None)
            expires_in  = max(0, oldest - now) if oldest else None
            status = (
                "expired"  if timed and len(expired) == len(timed) else
                "expiring" if expired or near_expiry else "ok"
            )
            return {
                "label": label, "file": path.name, "status": status,
                "total": len(cookies), "expired": len(expired), "nearExpiry": len(near_expiry),
                "expiresInHours": round(expires_in / 3600) if expires_in else None,
                "message": f"{len(cookies)} cookies loaded" if status == "ok" else f"{len(expired)} expired",
            }

        return {
            "taobao_tmall": check("taobao/tmall", COOKIE_PATHS["taobao"],   "taobao_tmall"),
            "1688":         check("1688",         COOKIE_PATHS["1688"],     "1688"),
            "alibaba":      check("alibaba",      COOKIE_PATHS["alibaba"],  "alibaba"),
        }


# Singleton
scraper_service = ScraperService()
