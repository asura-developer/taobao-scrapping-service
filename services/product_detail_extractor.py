import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# ── Adaptive storage path ─────────────────────────────────────────────────
ADAPTIVE_DB_PATH = str(
    Path(__file__).parent.parent / "data" / "scrapling_elements.db"
)


@dataclass
class ShopInfoResult:
    shopName: Optional[str] = None
    shopLink: Optional[str] = None
    shopRating: Optional[float] = None
    shopLocation: Optional[str] = None
    shopAge: Optional[str] = None
    sellerInfo: dict = field(default_factory=dict)
    badges: list = field(default_factory=list)
    extractedFrom: list = field(default_factory=list)


@dataclass
class DataQuality:
    hasTitle: bool = False
    hasPrice: bool = False
    hasImages: bool = False
    hasVariants: bool = False
    hasSpecs: bool = False
    hasBrand: bool = False
    hasReviews: bool = False
    hasDescription: bool = False
    hasSalesVolume: bool = False
    hasShopName: bool = False
    completeness: int = 0


# CNY → USD conversion rate — configurable via env var
CNY_TO_USD_RATE: float = float(os.getenv("CNY_TO_USD_RATE", "0.1376"))


def cny_to_usd(cny: float) -> float:
    """Convert Chinese Yuan to USD, rounded to 2 decimal places."""
    return round(cny * CNY_TO_USD_RATE, 2)


@dataclass
class ProductDetailResult:
    platform: str = ""
    extractedAt: str = ""
    fullTitle: Optional[str] = None
    price: Optional[float] = None          # CNY numeric
    priceUsd: Optional[float] = None       # USD numeric
    originalPrice: Optional[float] = None  # CNY numeric
    originalPriceUsd: Optional[float] = None  # USD numeric
    salesVolume: Optional[str] = None
    rating: Optional[float] = None         # numeric e.g. 4.9
    fullDescription: Optional[str] = None
    brand: Optional[str] = None
    reviewCount: Optional[int] = None      # numeric integer
    additionalImages: list = field(default_factory=list)
    variants: dict = field(default_factory=dict)
    specifications: dict = field(default_factory=dict)
    guarantees: list = field(default_factory=list)
    shopInfo: Optional[ShopInfoResult] = None
    dataQuality: Optional[DataQuality] = None
    extractionStrategies: dict = field(default_factory=dict)


# ══════════════════════════════════════════════════════════════════════════════
# CONFIRMED SELECTORS — sourced from debug_detail_1013388934740.json
# Each selector has a comment showing the exact class/element from the debug dump
# ══════════════════════════════════════════════════════════════════════════════

class ProductDetailExtractor:

    # ── Title ─────────────────────────────────────────────────────────────────
    # CONFIRMED: <span class="mainTitle--R75fTcZL" title="...">PRODUCT NAME</span>
    #            inside <div class="MainTitle--PiA4nmJz ...">
    #            inside <div class="ItemTitle--n4_pxQxz ">
    TITLE_SELECTORS = [
        "span.mainTitle--R75fTcZL",                    # ① exact — span with the title text
        "[class*='mainTitle--']",                       # ② hash-agnostic variant
        "[class*='ItemTitle--'] span[title]",           # ③ span with title attr inside ItemTitle
        "[class*='MainTitle--'] span",                  # ④ any span inside MainTitle div
        "[class*='ItemTitle--']",                       # ⑤ whole ItemTitle block (fallback)
        "h1",                                           # ⑥ old Taobao
        ".tb-detail-hd h1",
        ".item-title",
    ]

    # ── Price ─────────────────────────────────────────────────────────────────
    # CONFIRMED from debug: ￥170 is inside bodyWrap--otVC8M5m text.
    # No dedicated price element in the initial DOM — must use ￥ regex on
    # the confirmed containers, then fall back to raw HTML scan.
    PRICE_SELECTORS = [
        "[class*='highlightPrice--'] [class*='text--']",   # ① hashed highlight price text
        "[class*='highlightPrice--']",                     # ② whole highlight block
        "[class*='priceText--']",                          # ③ priceText span
        "[class*='priceWrap--'] [class*='text--']",        # ④ priceWrap > text
        "[class*='priceWrap--']",                          # ⑤ price wrapper
        "[class*='price--'] [class*='text']",              # ⑥ any hashed price > text
        "[class*='PurchasePanel--'] [class*='price']",     # ⑦ inside purchase panel
        "[class*='bodyWrap--']",                           # ⑧ CONFIRMED — contains ￥170
        "[class*='PurchasePanel--cKj4V']",                 # ⑨ CONFIRMED inner panel
        "[class*='GeneralSkuPanel--']",                    # ⑩ whole SKU panel fallback
        ".tb-rmb-num",                                     # ⑪ old Taobao
        "em[class*='price']",
        ".price strong",
    ]

    # ── Original / crossed-out price ─────────────────────────────────────────
    ORIG_PRICE_SELECTORS = [
        "[class*='subPrice--'] [class*='text--']",
        "[class*='subPrice--']",
        "[class*='originPrice--']",
        "[class*='originalPrice--']",
        "[class*='linePrice--']",
        ".tb-price del",
        "del[class*='price']",
        "s[class*='price']",
    ]

    # ── Sales volume ─────────────────────────────────────────────────────────
    # Tmall/Taobao show "X人付款", "已售X件", "X+ sold" near the price.
    # Class names vary across versions — add all known variants.
    SALES_SELECTORS = [
        "[class*='salesDesc--']",      # hashed salesDesc
        "[class*='salesNumber--']",    # hashed salesNumber
        "[class*='soldCount--']",      # hashed soldCount
        "[class*='tradeCount--']",     # hashed tradeCount
        "[class*='payCount--']",       # hashed payCount
        "[class*='countText--']",      # hashed countText
        "[class*='sold--']",
        "[class*='sales--']",
        "[class*='salesVolume']",
        "[class*='buyCount--']",
        "[class*='saleCount--']",
        ".tb-sold-out",
        # Fallback: scan the whole SKU panel text for "X人付款" / "已售X"
        "[class*='GeneralSkuPanel--']",
        "[class*='PurchasePanel--']",
    ]

    # ── Rating ────────────────────────────────────────────────────────────────
    # CONFIRMED: <div class="StoreComprehensiveRating--If5wS20L">4.4</div>
    #            text content IS the rating number
    RATING_SELECTORS = [
        "[class*='StoreComprehensiveRating--']",   # ① confirmed — direct text is "4.4"
        "[class*='starsWrap--']",                  # ② starsWrap contains rating text
        "[class*='starNum--']",
        "[class*='ratingNum--']",
        "[class*='ratingValue--']",
        ".tb-rate-star",
        "[class*='score--']",
    ]

    # ── Shop name ─────────────────────────────────────────────────────────────
    # CONFIRMED: <span class="shopName--cSjM9uKk f-els-1" title="浅茶家旗舰店">浅茶家旗舰店</span>
    #            inside shopNameWrap--_4tEwrTc → shopNameLevelWrapper--pPrqPaSN
    SHOP_NAME_SELECTORS = [
        "span[class*='shopName--']",                    # ① confirmed — span with shop name
        "[class*='shopNameWrap--'] span[title]",        # ② span with title attr in shopNameWrap
        "[class*='shopNameLevelWrapper--'] span[title]",# ③ inside level wrapper
        "[class*='shopName--']",                        # ④ any shopName-- element
        "[class*='shopHeader--'] a span[title]",        # ⑤ span inside shopHeader link
        ".tb-shop-name",
        "[class*='storeName--']",
        "[class*='sellerName--']",
    ]

    # ── Shop link ─────────────────────────────────────────────────────────────
    # CONFIRMED: <a class="detailWrap--svoEjPUO" href="//shop256790389.taobao.com">
    #            inside shopHeader--J_nfJZjm
    SHOP_LINK_SELECTORS = [
        "[class*='shopHeader--'] a[href*='taobao.com']:not([href*='openshop'])",  # ① confirmed
        "[class*='shopHeader--'] a[href*='tmall.com']:not([href*='openshop'])",
        "a[href*='shop.taobao.com']",
        "a[href*='shop.tmall.com']",
        "a[href*='store.tmall.com']",
        "a[href*='shop.1688.com']",
        "a[href*='store.taobao.com']",
    ]

    # ── Store rating / label items ─────────────────────────────────────────────
    # CONFIRMED: <div class="storeLabelItem--IcqpWWIy">88VIP好评率94%</div>
    #            <div class="storeLabelItem--IcqpWWIy">平均20小时发货</div>
    #            <div class="storeLabelItem--IcqpWWIy">客服满意度96%</div>
    STORE_LABEL_SELECTOR = "[class*='storeLabelItem--']"

    # ── Brand ─────────────────────────────────────────────────────────────────
    BRAND_SELECTORS = [
        "[class*='brandName--']",
        "[class*='brandLogo--']",
        "[class*='brandTitle--']",
        ".tb-brand",
        "[class*='brand--']",
        "[class*='Brand--']",
    ]

    # ── Review / comment count ────────────────────────────────────────────────
    # Tmall shows "X条评价" or "X Reviews" in the comment section header.
    REVIEW_SELECTORS = [
        "[class*='rateCount--']",
        "[class*='reviewCount--']",
        "[class*='commentNum--']",
        "[class*='commentCount--']",
        "[class*='commentTitle--']",
        "[class*='comment--'] [class*='count']",
        "[class*='comment--'] [class*='num']",
        "[class*='rate--'] [class*='count']",
        "[class*='rateTotal--']",
        "[class*='review--'] [class*='count']",
        ".tb-rate-counter",
    ]

    # ── Description ───────────────────────────────────────────────────────────
    # Lazy-loaded below the fold; broader fallback list.
    DESCRIPTION_SELECTORS = [
        "[class*='imageTextInfo--']",
        "#imageTextInfo-content",
        "[class*='descRoot--']",
        "[class*='tabDetailItem--']",
        "[class*='descV8--']",
        "[class*='itemDesc--']",
        "[class*='detail-desc']",
        ".desc-root",
        "#description",
        ".item-desc",
        "[class*='description--']",
        "[class*='Description--']",
    ]

    # ── Specs ─────────────────────────────────────────────────────────────────
    # emphasisParams / generalParams confirmed in Tmall 2025. Also scan skuItem
    # for dimension-like entries that look like product attributes.
    SPEC_SELECTORS = [
        "[class*='emphasisParams--']",
        "[class*='generalParams--']",
        "[class*='paramsInfo--']",
        "[class*='attrItem--']",
        "[class*='specItem--']",
        "[class*='paramItem--']",
        "[class*='propItem--']",
        "[class*='property--']",
        ".attributes-list li",
        "dl.tb-prop",
    ]

    # ── Guarantees ────────────────────────────────────────────────────────────
    # CONFIRMED: <span class="guaranteeText--hqmmjLTB">破损包退</span>
    #            <span class="guaranteeText--hqmmjLTB">7天无理由退换</span>  etc.
    #            All inside <div class="GuaranteeInfo--OYtWvOEt">
    GUARANTEE_SELECTORS = [
        "span[class*='guaranteeText--']",               # ① confirmed — individual spans
        "[class*='GuaranteeInfo--'] span",              # ② all spans inside guarantee block
        "[class*='GuaranteeInfo--']",                   # ③ whole block as fallback
        ".service-promise",
        "[class*='servicePromise--']",
    ]

    # ── Gallery / thumbnail images ─────────────────────────────────────────────
    # CONFIRMED: <img class="thumbnailPic--QasTmWDm" src="//gw.alicdn.com/...">
    #            inside thumbnailItem--WQyauvvr → thumbnails--v976to2t → picGallery--qY53_w0u
    GALLERY_SELECTORS = [
        "img[class*='thumbnailPic--']",                 # ① confirmed — exact img class
        "[class*='thumbnailItem--'] img",               # ② img inside each thumbnail item
        "[class*='thumbnails--'] img",                  # ③ all imgs in thumbnails strip
        "[class*='picGallery--'] img",                  # ④ whole gallery container
        ".tb-thumb img",
        "#J_UlThumb img",
    ]

    # ── SKU / variants ────────────────────────────────────────────────────────
    # CONFIRMED:
    #   Container: <div class="skuItem--Z2AJB9Ew ">
    #   Label:     <span title="口味" class="f-els-2">口味</span>
    #              inside <div class="ItemLabel--psS1SOyC"> → labelWrap--ffBEejeJ
    #   Values:    <div class="valueItem--smR4pNt4 hasImg--K82HLg1O"
    #                   data-vid="42683583476" data-disabled="false">
    #              inside skuValueWrap--aEfxuhNr → contentWrap--jRII6Vhf → content--DIGuLqdf
    SKU_ITEM_SELECTOR    = "[class*='skuItem--']"           # one per variant dimension
    SKU_LABEL_SELECTOR   = "[class*='ItemLabel--'] span[title]"  # label span has title attr
    SKU_VALUE_SELECTOR   = "[class*='valueItem--']"         # each option button/div

    # ── Specs ─────────────────────────────────────────────────────────────────
    # NOT in debug — specs are lazy-loaded via scroll. Keep broad fallbacks.
    SPEC_SELECTORS = [
        "[class*='emphasisParams--']",
        "[class*='generalParams--']",
        "[class*='paramsInfo--']",
        "[class*='attrItem--']",
        "[class*='specItem--']",
        ".attributes-list li",
        "dl.tb-prop",
        "[class*='property--']",
    ]

    # ─────────────────────────────────────────────────────────────────────────
    # Adaptive + standard helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _make_adaptive_page(raw_html: str, url: str = ""):
        """Create a Scrapling Selector with adaptive tracking enabled."""
        from scrapling.parser import Selector
        os.makedirs(Path(ADAPTIVE_DB_PATH).parent, exist_ok=True)
        return Selector(
            content=raw_html,
            url=url,
            adaptive=True,
            storage_args={"storage_file": ADAPTIVE_DB_PATH, "url": url},
        )

    def _css_text(self, page, selector: str, *, adaptive: bool = False,
                  identifier: str = "") -> str:
        """Extract text via CSS. If adaptive=True, auto_save on hit and
        try adaptive relocation on miss."""
        try:
            if adaptive and identifier:
                el = page.css(selector, auto_save=True, identifier=identifier)
            else:
                el = page.css(selector)
            if el:
                return (el.first.text or "").strip()
            # Adaptive fallback: try to relocate from saved fingerprint
            if adaptive and identifier:
                el = page.css(selector, adaptive=True, identifier=identifier)
                if el:
                    return (el.first.text or "").strip()
        except Exception:
            pass
        return ""

    def _css_attr(self, page, selector: str, attr: str, *,
                  adaptive: bool = False, identifier: str = "") -> str:
        try:
            if adaptive and identifier:
                el = page.css(selector, auto_save=True, identifier=identifier)
            else:
                el = page.css(selector)
            if el:
                return (el.first.attrib.get(attr) or "").strip()
            if adaptive and identifier:
                el = page.css(selector, adaptive=True, identifier=identifier)
                if el:
                    return (el.first.attrib.get(attr) or "").strip()
        except Exception:
            pass
        return ""

    def _css_all_texts(self, page, selector: str) -> list[str]:
        try:
            els = page.css(selector)
            return [(el.text or "").strip() for el in els if (el.text or "").strip()]
        except Exception:
            return []

    # ── Scrapling smart search helpers ────────────────────────────────────

    @staticmethod
    def _find_by_text(page, text: str, *, partial: bool = True,
                      case_sensitive: bool = False) -> str:
        """Use Scrapling's find_by_text for text-based element discovery."""
        try:
            el = page.find_by_text(text, partial=partial,
                                   case_sensitive=case_sensitive)
            if el:
                return (el.text or "").strip()
        except Exception:
            pass
        return ""

    @staticmethod
    def _find_by_regex(page, pattern: str, *, first_match: bool = True) -> str:
        """Use Scrapling's find_by_regex for regex-based element discovery."""
        try:
            el = page.find_by_regex(pattern, first_match=first_match)
            if el:
                return (el.text or "").strip()
        except Exception:
            pass
        return ""

    @staticmethod
    def _find_similar_texts(element, **kwargs) -> list[str]:
        """Use find_similar to discover structurally similar elements."""
        try:
            similar = element.find_similar(**kwargs)
            return [(el.text or "").strip() for el in similar
                    if (el.text or "").strip()]
        except Exception:
            return []

    # ─────────────────────────────────────────────────────────────────────────

    def extract_product_details(self, page, raw_html: str, platform: str) -> ProductDetailResult:
        from datetime import datetime, UTC
        result = ProductDetailResult(
            platform=platform,
            extractedAt=datetime.now(UTC).isoformat(),
        )

        # Build adaptive page for smart element tracking
        url = getattr(page, 'url', '') or ''
        try:
            adaptive_page = self._make_adaptive_page(raw_html, url)
        except Exception:
            adaptive_page = page  # fallback to non-adaptive

        # ── Shop info (uses adaptive selectors) ──────────────────────────────
        result.shopInfo = self._extract_shop_info(adaptive_page)

        # ── Title (adaptive + find_by_text fallback) ──────────────────────────
        for sel in self.TITLE_SELECTORS:
            # Prefer the title="" attribute — it has the clean full title
            attr = self._css_attr(adaptive_page, sel, "title",
                                  adaptive=True, identifier=f"detail_title_{sel}")
            if attr and len(attr) >= 5:
                result.fullTitle = attr
                result.extractionStrategies["title"] = f"{sel}@title[adaptive]"
                break
            text = self._css_text(adaptive_page, sel,
                                  adaptive=True, identifier=f"detail_title_{sel}")
            if text and len(text) >= 5:
                result.fullTitle = text
                result.extractionStrategies["title"] = f"{sel}[adaptive]"
                break

        # ── Price (adaptive + find_by_regex fallback) ─────────────────────────
        for sel in self.PRICE_SELECTORS:
            text = self._css_text(adaptive_page, sel,
                                  adaptive=True, identifier=f"detail_price_{sel}")
            if not text:
                continue
            m = re.search(r"[¥￥]\s*([\d,]+\.?\d*)", text)
            if not m:
                m = re.search(r"(?:^|[¥￥])\s*([\d,]+\.?\d*)", text)
            if m:
                val = float(m.group(1).replace(",", ""))
                if 0.01 < val < 1_000_000:
                    result.price = round(val, 2)
                    result.priceUsd = cny_to_usd(val)
                    result.extractionStrategies["price"] = f"{sel}[adaptive]"
                    break
        # find_by_regex fallback for price
        if not result.price:
            price_text = self._find_by_regex(adaptive_page, r"[¥￥]\s*[\d,]+\.?\d*")
            if price_text:
                m = re.search(r"[¥￥]\s*([\d,]+\.?\d*)", price_text)
                if m:
                    val = float(m.group(1).replace(",", ""))
                    if 0.01 < val < 1_000_000:
                        result.price = round(val, 2)
                        result.priceUsd = cny_to_usd(val)
                        result.extractionStrategies["price"] = "find_by_regex"

        # ── Original price (adaptive) ────────────────────────────────────────
        for sel in self.ORIG_PRICE_SELECTORS:
            text = self._css_text(adaptive_page, sel,
                                  adaptive=True, identifier=f"detail_origprice_{sel}")
            m = re.search(r"[¥￥]\s*([\d,]+\.?\d*)", text) or re.search(r"[\d,]+\.?\d*", text)
            if m:
                raw = m.group(1) if m.lastindex else m.group()
                val = float(raw.replace(",", ""))
                if 0.01 < val < 1_000_000 and val != result.price:
                    result.originalPrice = round(val, 2)
                    result.originalPriceUsd = cny_to_usd(val)
                    break

        # ── Sales volume (adaptive + find_by_text fallback) ──────────────────
        _SALES_PATTERNS = [
            r"([\d,]+)\+?\s*(?:人付款|人已付款|已购|笔交易)",
            r"(?:已售|销量)\s*([\d,]+)\+?\s*(?:件|个)?",
            r"([\d,]+)\+?\s*(?:sold|orders?)",
            r"([\d,]+)\+?\s*(?:purchases?|buyers?)",
        ]
        for sel in self.SALES_SELECTORS:
            text = self._css_text(adaptive_page, sel,
                                  adaptive=True, identifier=f"detail_sales_{sel}")
            if not text:
                continue
            matched = False
            for pattern in _SALES_PATTERNS:
                m = re.search(pattern, text, re.IGNORECASE)
                if m:
                    result.salesVolume = m.group(1).replace(",", "")
                    result.extractionStrategies["salesVolume"] = f"{sel}[adaptive]"
                    matched = True
                    break
            if matched:
                break
            if "GeneralSkuPanel" not in sel and "PurchasePanel" not in sel:
                m = re.search(r"(\d[\d,]*)", text)
                if m:
                    result.salesVolume = m.group().replace(",", "")
                    result.extractionStrategies["salesVolume"] = f"{sel}[adaptive]"
                    break
        # find_by_text fallback for sales
        if not result.salesVolume:
            for phrase in ["人付款", "已售", "sold", "orders"]:
                text = self._find_by_text(adaptive_page, phrase, partial=True)
                if text:
                    for pattern in _SALES_PATTERNS:
                        m = re.search(pattern, text, re.IGNORECASE)
                        if m:
                            result.salesVolume = m.group(1).replace(",", "")
                            result.extractionStrategies["salesVolume"] = f"find_by_text({phrase})"
                            break
                if result.salesVolume:
                    break

        # ── Rating (adaptive) ────────────────────────────────────────────────
        for sel in self.RATING_SELECTORS:
            text = self._css_text(adaptive_page, sel,
                                  adaptive=True, identifier=f"detail_rating_{sel}")
            m = re.search(r"[\d.]+", text)
            if m:
                try:
                    val = float(m.group())
                    if 0 < val <= 5:
                        result.rating = val
                        result.extractionStrategies["rating"] = f"{sel}[adaptive]"
                        break
                except ValueError:
                    pass

        # ── Description (adaptive) ───────────────────────────────────────────
        for sel in self.DESCRIPTION_SELECTORS:
            text = self._css_text(adaptive_page, sel,
                                  adaptive=True, identifier=f"detail_desc_{sel}")
            if len(text) > 50:
                result.fullDescription = text[:10000]
                result.extractionStrategies["description"] = f"{sel}[adaptive]"
                break

        # ── Brand (adaptive) ─────────────────────────────────────────────────
        for sel in self.BRAND_SELECTORS:
            text = self._css_text(adaptive_page, sel,
                                  adaptive=True, identifier=f"detail_brand_{sel}")
            if text and 1 < len(text) < 80:
                result.brand = text
                result.extractionStrategies["brand"] = f"{sel}[adaptive]"
                break

        # ── Review count (adaptive + find_by_text fallback) ──────────────────
        _REVIEW_PATTERNS = [
            r"([\d,]+)\s*(?:条评价|条评论|个评价|reviews?|ratings?|comments?)",
            r"(?:评价|评论|reviews?|ratings?)\s*[:(（]\s*([\d,]+)",
            r"^([\d,]+)$",
        ]
        for sel in self.REVIEW_SELECTORS:
            text = self._css_text(adaptive_page, sel,
                                  adaptive=True, identifier=f"detail_review_{sel}")
            if not text:
                continue
            matched = False
            for pattern in _REVIEW_PATTERNS:
                m = re.search(pattern, text, re.IGNORECASE)
                if m:
                    try:
                        result.reviewCount = int(m.group(1).replace(",", ""))
                        result.extractionStrategies["reviewCount"] = f"{sel}[adaptive]"
                        matched = True
                    except (IndexError, ValueError):
                        pass
                    break
            if matched:
                break
        # find_by_text fallback for reviews
        if not result.reviewCount:
            for phrase in ["条评价", "条评论", "reviews", "ratings"]:
                text = self._find_by_text(adaptive_page, phrase, partial=True)
                if text:
                    for pattern in _REVIEW_PATTERNS:
                        m = re.search(pattern, text, re.IGNORECASE)
                        if m:
                            try:
                                result.reviewCount = int(m.group(1).replace(",", ""))
                                result.extractionStrategies["reviewCount"] = f"find_by_text({phrase})"
                            except (IndexError, ValueError):
                                pass
                            break
                if result.reviewCount:
                    break

        # ── Images (adaptive page for gallery discovery) ─────────────────────
        result.additionalImages = self._extract_images(adaptive_page)

        # ── Variants (adaptive page) ──────────────────────────────────────────
        result.variants = self._extract_variants(adaptive_page)

        # ── Specs (adaptive page) ─────────────────────────────────────────────
        result.specifications = self._extract_specs(adaptive_page)

        # ── Brand fallback: check specifications dict ─────────────────────────
        if not result.brand and result.specifications:
            for key in result.specifications:
                if re.search(r"brand|品牌", key, re.IGNORECASE):
                    result.brand = result.specifications[key]
                    result.extractionStrategies["brand"] = f"specs[{key}]"
                    break

        # ── Guarantees (with find_similar fallback) ────────────────────────────
        texts = self._css_all_texts(adaptive_page, "span[class*='guaranteeText--']")
        if texts:
            result.guarantees = texts[:10]
            result.extractionStrategies["guarantees"] = "span[class*='guaranteeText--']"
        else:
            for sel in self.GUARANTEE_SELECTORS[1:]:
                texts = self._css_all_texts(adaptive_page, sel)
                clean = [t for t in texts if 2 < len(t) < 60]
                if clean:
                    result.guarantees = clean[:10]
                    break
        # find_similar fallback: if we found one guarantee, find others like it
        if len(result.guarantees) == 1:
            try:
                el = adaptive_page.css("span[class*='guaranteeText--']")
                if el and el.first:
                    similar_texts = self._find_similar_texts(
                        el.first, similarity_threshold=0.3)
                    for t in similar_texts:
                        if 2 < len(t) < 60 and t not in result.guarantees:
                            result.guarantees.append(t)
                    if len(result.guarantees) > 1:
                        result.extractionStrategies["guarantees"] = "find_similar"
            except Exception:
                pass

        # ── HTML-level regex fallbacks (when CSS class hashes have rotated) ───
        # Use the raw HTML string passed from the scraper (full rendered page).
        if raw_html:
            # Price fallback: find ¥79.2 or ￥79.2 in the raw HTML
            if not result.price:
                pm = re.search(r"[¥￥]([\d,]+\.?\d*)", raw_html)
                if pm:
                    try:
                        val = float(pm.group(1).replace(",", ""))
                        if 0.01 < val < 1_000_000:
                            result.price = round(val, 2)
                            result.priceUsd = cny_to_usd(val)
                            result.extractionStrategies["price"] = "html_regex"
                    except ValueError:
                        pass

            # Sales volume fallback: find "X人付款" / "已售X件" / "X+ sold" in HTML
            if not result.salesVolume:
                for pat in [
                    r"([\d,]+)\+?\s*(?:人付款|人已付款|已购|笔交易)",
                    r"(?:已售|销量)[^\d]*([\d,]+)\+?",
                    r"([\d,]+)\+?\s*(?:sold|orders?)\b",
                ]:
                    sm = re.search(pat, raw_html, re.IGNORECASE)
                    if sm:
                        result.salesVolume = sm.group(1).replace(",", "")
                        result.extractionStrategies["salesVolume"] = "html_regex"
                        break

            # Review count fallback
            if not result.reviewCount:
                for pat in [
                    r"([\d,]+)\s*(?:条评价|条评论|个评价)",
                    r"([\d,]+)\s*(?:reviews?|ratings?)\b",
                ]:
                    rm = re.search(pat, raw_html, re.IGNORECASE)
                    if rm:
                        try:
                            result.reviewCount = int(rm.group(1).replace(",", ""))
                            result.extractionStrategies["reviewCount"] = "html_regex"
                        except ValueError:
                            pass
                        break

        result.dataQuality = self._calc_quality(result)
        return result

    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _clean_shop_name(raw: str) -> str:
        """
        Strip leading noise such as repeat-customer counts that Tmall/Taobao
        prepends to the shop name, e.g.:
            "100,000 repeat customersLijieyou cosmetics flagship store"
            "10万回头客丽洁优化妆旗舰店"
        Returns only the actual store name that follows the noise.
        """
        if not raw:
            return raw

        # Pattern: one or more digit groups (with optional commas/dots/万/千/百)
        # followed by optional spaces + common "repeat customer" phrases in
        # English or Chinese, then the real name starts.
        noise_pattern = re.compile(
            r"^[\d,.+\s万千百]+\s*"                         # leading number  e.g. 100,000 / 10万
            r"(?:"
            r"repeat\s+customers?|"                        # English
            r"回头客|复购客|老顾客|忠实顾客|"                # Chinese variants
            r"(?:loyal|returning)\s+(?:customers?|buyers?)"# more English
            r")\s*",
            re.IGNORECASE,
        )
        cleaned = noise_pattern.sub("", raw).strip()
        return cleaned if cleaned else raw

    def _extract_shop_info(self, page) -> ShopInfoResult:
        info = ShopInfoResult()

        # ── Shop name (adaptive) ─────────────────────────────────────────────
        for sel in self.SHOP_NAME_SELECTORS:
            name = self._css_attr(page, sel, "title",
                                  adaptive=True, identifier=f"shop_name_{sel}")
            if not name:
                name = self._css_text(page, sel,
                                      adaptive=True, identifier=f"shop_name_{sel}")
            if name and 2 < len(name) < 200 and "open a store" not in name.lower():
                # Strip repeat-customer count prefix (e.g. "100,000 repeat customers")
                name = self._clean_shop_name(name)
                age_m = re.search(r"(\d+)\s*(?:years?\s*old\s*store|年老店)", name, re.I)
                if age_m:
                    info.shopAge = age_m.group()
                    info.shopName = name.replace(age_m.group(), "").strip()
                else:
                    info.shopName = name
                info.extractedFrom.append({"field": "shopName", "selector": sel})
                break

        # ── Shop link (adaptive) ─────────────────────────────────────────────
        for sel in self.SHOP_LINK_SELECTORS:
            href = self._css_attr(page, sel, "href",
                                  adaptive=True, identifier=f"shop_link_{sel}")
            if href:
                if href.startswith("//"):
                    href = "https:" + href
                info.shopLink = href
                info.extractedFrom.append({"field": "shopLink", "selector": sel})
                break

        # ── Store comprehensive rating — CONFIRMED: "4.9" ─────────────────────
        rating_text = self._css_text(page, "[class*='StoreComprehensiveRating--']")
        if rating_text:
            m = re.search(r"[\d.]+", rating_text)
            if m:
                try:
                    r = float(m.group())
                    if 0 < r <= 5:
                        info.shopRating = r
                        info.extractedFrom.append({"field": "shopRating", "selector": "StoreComprehensiveRating--"})
                except ValueError:
                    pass

        # ── Store label items — CONFIRMED: storeLabelItem-- divs ─────────────
        # Text examples: "88VIP好评率94%", "平均20小时发货", "客服满意度96%"
        label_texts = self._css_all_texts(page, self.STORE_LABEL_SELECTOR)
        for text in label_texts:
            if not text:
                continue
            # Positive feedback rate: "好评率94%"
            m = re.search(r"好评率(\d+)%", text)
            if m:
                info.sellerInfo["positiveFeedbackRate"] = int(m.group(1))
            # Average ship time: "平均20小时发货"
            m = re.search(r"平均(\d+)\s*(小时|天|日|hours?|days?)", text, re.I)
            if m:
                info.sellerInfo["averageDeliveryTime"] = m.group(0)
            # Service satisfaction: "客服满意度96%"
            m = re.search(r"满意度(\d+)%", text)
            if m:
                info.sellerInfo["serviceSatisfaction"] = int(m.group(1))
            # VIP badge: "88VIP"
            if "VIP" in text or "vip" in text:
                info.sellerInfo["hasVIP"] = True
                info.badges.append(text)
            # Generic badges (short texts)
            elif 1 < len(text) < 30 and text not in info.badges:
                info.badges.append(text)

        return info

    def _extract_images(self, page) -> list[str]:
        images: set[str] = set()

        # CONFIRMED: img[class*='thumbnailPic--'] with src="//gw.alicdn.com/..."
        for sel in self.GALLERY_SELECTORS:
            try:
                els = page.css(sel)
                if not els:
                    continue
                for img in els:
                    src = (
                        img.attrib.get("src")
                        or img.attrib.get("data-src")
                        or img.attrib.get("data-lazy-src")
                        or ""
                    ).strip()
                    if not src or "s.gif" in src or len(src) < 10:
                        continue
                    if src.startswith("//"):
                        src = "https:" + src
                    # Strip thumbnail size suffix: _q50.jpg_.webp → clean URL
                    src = re.sub(r'_q\d+\.jpg.*$', '', src)
                    src = re.sub(r'_\d+x\d+.*$', '', src)
                    images.add(src)
                if images:
                    break
            except Exception:
                continue

        # Description section images (lazy loaded)
        for sel in [
            "[class*='descV8--'] img",
            "[class*='imageTextInfo--'] img",
            "#imageTextInfo-container img",
        ]:
            try:
                els = page.css(sel)
                for img in (els or []):
                    src = (img.attrib.get("data-src") or img.attrib.get("src") or "").strip()
                    if src and "s.gif" not in src and len(src) > 10:
                        if src.startswith("//"):
                            src = "https:" + src
                        images.add(src)
                if len(images) > 5:
                    break
            except Exception:
                continue

        return list(images)[:30]

    # Noise patterns appended to SKU value text by Tmall (social proof labels)
    _SKU_VALUE_NOISE = re.compile(
        r"\s*(?:\d+%\s*of\s+customers?\s+choose|thousands?\s+of\s+people|"
        r"[0-9]+\+?\s*(?:people|customers?|buyers?)\s+(?:add|chose|selected)|"
        r"热销|推荐|人选择|人购买|人加购|好评率|最受欢迎|爆款|新品|限时).*",
        re.IGNORECASE,
    )

    def _extract_variants(self, page) -> dict:
        """
        CONFIRMED DOM structure:
          <div class="skuItem--Z2AJB9Ew ">           ← one per dimension (口味, 净含量...)
            <div class="labelWrap--ffBEejeJ">
              <div class="ItemLabel--psS1SOyC">
                <span title="口味">口味</span>         ← label text (use title attr)
              </div>
            </div>
            <div class="skuValueWrap--aEfxuhNr">
              <div class="contentWrap--jRII6Vhf">
                <div class="content--DIGuLqdf">
                  <div class="valueItem--smR4pNt4 hasImg--K82HLg1O"
                       data-vid="42683583476"
                       data-disabled="false">  ← each option
        """
        variants: dict = {}
        try:
            sku_items = page.css(self.SKU_ITEM_SELECTOR)
            if not sku_items:
                return variants

            for item in sku_items:
                # ── Label ──────────────────────────────────────────────────
                label = None
                try:
                    lel = item.css(self.SKU_LABEL_SELECTOR)
                    if lel:
                        # title attr is cleanest
                        label = lel.first.attrib.get("title") or (lel.first.text or "").strip()
                        label = label.rstrip(":：").strip() if label else None
                except Exception:
                    pass
                if not label:
                    # Fallback: any span inside ItemLabel
                    try:
                        lel = item.css("[class*='ItemLabel--'] span")
                        if lel:
                            label = (lel.first.text or "").strip().rstrip(":：")
                    except Exception:
                        pass
                if not label:
                    continue

                # ── Values ─────────────────────────────────────────────────
                options: list = []
                try:
                    vels = item.css(self.SKU_VALUE_SELECTOR)
                    for vel in (vels or []):
                        if vel.attrib.get("data-disabled") == "true":
                            continue

                        # Try to get the clean label text — preferring span[title]
                        # or the first text-only child span (avoids social-proof noise)
                        text = ""
                        try:
                            sp = vel.css("span[title]")
                            if sp:
                                text = (sp.first.attrib.get("title") or sp.first.text or "").strip()
                        except Exception:
                            pass
                        if not text:
                            try:
                                sp = vel.css("span")
                                if sp:
                                    text = (sp.first.text or "").strip()
                            except Exception:
                                pass
                        if not text:
                            text = (vel.text or "").strip()

                        # Strip social-proof noise appended by Tmall
                        text = self._SKU_VALUE_NOISE.sub("", text).strip()

                        if not text or len(text) >= 120:
                            continue
                        option: dict = {"value": text}
                        # Image (swatch)
                        try:
                            img = vel.css("img")
                            if img:
                                src = (
                                    img.first.attrib.get("src")
                                    or img.first.attrib.get("data-src")
                                    or ""
                                )
                                if src:
                                    option["image"] = ("https:" + src) if src.startswith("//") else src
                        except Exception:
                            pass
                        vid = vel.attrib.get("data-vid") or vel.attrib.get("data-value")
                        if vid:
                            option["vid"] = vid
                        options.append(option)
                except Exception:
                    pass

                if options:
                    seen: set = set()
                    deduped = []
                    for opt in options:
                        if opt["value"] not in seen:
                            seen.add(opt["value"])
                            deduped.append(opt)
                    variants[label] = deduped

        except Exception:
            pass
        return variants

    def _extract_specs(self, page) -> dict:
        specs: dict = {}
        for sel in self.SPEC_SELECTORS:
            try:
                els = page.css(sel)
                if not els:
                    continue
                for item in els:
                    # Structured children approach
                    key_el = item.css(
                        "[class*='title'],[class*='Title'],[class*='name'],[class*='Name'],dt"
                    )
                    val_el = item.css(
                        "[class*='value'],[class*='Value'],[class*='subtitle'],[class*='SubTitle'],dd"
                    )
                    if key_el and val_el:
                        k = (key_el.first.text or "").strip()
                        v = (val_el.first.text or "").strip()
                        if k and v and k != v and len(k) < 60:
                            specs[k] = v[:200]
                    else:
                        # Plain text "key: value" or "key：value"
                        text = (item.text or "").strip()
                        if ":" in text or "：" in text:
                            parts = re.split(r"[:：]", text, maxsplit=1)
                            if len(parts) == 2 and parts[0].strip() and parts[1].strip():
                                k = parts[0].strip()
                                if len(k) < 60:
                                    specs[k] = parts[1].strip()[:200]
                if len(specs) >= 3:
                    break
            except Exception:
                continue

        # ── Fallback: scan all elements for "Key: Value" attribute pairs ───────
        if not specs:
            try:
                # Look for any element pairs that have label+value class structure
                for pair_sel in [
                    "[class*='paramLabel--'],[class*='paramValue--']",
                    "[class*='specLabel--'],[class*='specValue--']",
                    "[class*='attrLabel--'],[class*='attrValue--']",
                ]:
                    els = page.css(pair_sel)
                    texts = [(el.text or "").strip() for el in (els or []) if (el.text or "").strip()]
                    # Expect alternating label/value
                    if len(texts) >= 2:
                        for i in range(0, len(texts) - 1, 2):
                            k, v = texts[i], texts[i + 1]
                            if k and v and k != v and len(k) < 60:
                                specs[k] = v[:200]
                    if specs:
                        break
            except Exception:
                pass

        return specs

    def _calc_quality(self, r: ProductDetailResult) -> DataQuality:
        dq = DataQuality(
            hasTitle=bool(r.fullTitle),
            hasPrice=bool(r.price),
            hasImages=len(r.additionalImages) > 0,
            hasVariants=len(r.variants) > 0,
            hasSpecs=len(r.specifications) > 0,
            hasBrand=bool(r.brand),
            hasReviews=bool(r.reviewCount),
            hasDescription=bool(r.fullDescription),
            hasSalesVolume=bool(r.salesVolume),
            hasShopName=bool(r.shopInfo and r.shopInfo.shopName),
        )
        checks = [
            dq.hasTitle, dq.hasPrice, dq.hasImages, dq.hasVariants,
            dq.hasSpecs, dq.hasSalesVolume, dq.hasShopName,
            dq.hasBrand, dq.hasReviews, dq.hasDescription,
        ]
        dq.completeness = round(sum(1 for c in checks if c) / len(checks) * 100)
        return dq