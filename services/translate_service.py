"""
Translation service using deep_translator.

Translates scraped product data from Chinese to the target language.
Skips translation when target is 'zh' since source content is already Chinese.
All fields are translated in-place — titles, descriptions, specs, variants,
guarantees, shop info, badges, sales volume, etc.
"""

import asyncio
import logging
from typing import Optional

from deep_translator import GoogleTranslator

logger = logging.getLogger(__name__)

# Map our language codes to deep_translator language codes
LANG_MAP = {
    "en": "en",
    "zh": "zh-CN",
    "th": "th",
    "ja": "ja",
    "ko": "ko",
    "ru": "ru",
}

# Max characters per translation request (Google Translate limit is 5000)
_MAX_CHARS = 4900


def _translate_sync(text: str, target: str) -> str:
    """Synchronous translation via GoogleTranslator."""
    translator = GoogleTranslator(source="auto", target=target)
    if len(text) <= _MAX_CHARS:
        return translator.translate(text)

    # Split long text into chunks at sentence boundaries
    chunks = []
    remaining = text
    while remaining:
        if len(remaining) <= _MAX_CHARS:
            chunks.append(remaining)
            break
        split_at = remaining.rfind("\n", 0, _MAX_CHARS)
        if split_at == -1:
            split_at = remaining.rfind("。", 0, _MAX_CHARS)
        if split_at == -1:
            split_at = remaining.rfind(".", 0, _MAX_CHARS)
        if split_at == -1:
            split_at = _MAX_CHARS
        chunks.append(remaining[: split_at + 1])
        remaining = remaining[split_at + 1:]

    translated_chunks = [translator.translate(c) for c in chunks if c.strip()]
    return " ".join(translated_chunks)


async def translate_text(text: str, target_lang: str) -> str:
    """
    Translate a single text string to the target language.
    Runs in a thread executor since deep_translator is synchronous.
    """
    if not text or not text.strip():
        return text

    target = LANG_MAP.get(target_lang, "en")
    try:
        result = await asyncio.to_thread(_translate_sync, text, target)
        return result or text
    except Exception as e:
        logger.warning(f"Translation failed for text [{text[:50]}...]: {e}")
        return text


async def translate_batch(texts: list[str], target_lang: str) -> list[str]:
    """
    Translate multiple texts using separator-based batching to minimize API calls.
    """
    if not texts:
        return texts

    target = LANG_MAP.get(target_lang, "en")

    # Use a separator that Google Translate won't mangle
    SEPARATOR = "\n###SPLIT###\n"
    batches: list[list[tuple[int, str]]] = [[]]
    current_len = 0

    for i, text in enumerate(texts):
        if not text or not text.strip():
            continue
        text_len = len(text) + len(SEPARATOR)
        if current_len + text_len > _MAX_CHARS and batches[-1]:
            batches.append([])
            current_len = 0
        batches[-1].append((i, text))
        current_len += text_len

    results = list(texts)  # copy originals as fallback

    for batch in batches:
        if not batch:
            continue
        combined = SEPARATOR.join(t for _, t in batch)
        try:
            translated = await asyncio.to_thread(
                lambda c=combined: GoogleTranslator(source="auto", target=target).translate(c)
            )
            if translated:
                parts = translated.split("###SPLIT###")
                if len(parts) == len(batch):
                    # Count matches — safe to apply
                    for j, (orig_idx, _) in enumerate(batch):
                        results[orig_idx] = parts[j].strip()
                else:
                    # Separator was mangled — fall back to individual translation
                    logger.debug(
                        f"Batch split mismatch: expected {len(batch)} parts, got {len(parts)}. "
                        "Falling back to individual translation."
                    )
                    for orig_idx, text in batch:
                        try:
                            t = await asyncio.to_thread(
                                lambda t=text: GoogleTranslator(source="auto", target=target).translate(t)
                            )
                            if t:
                                results[orig_idx] = t.strip()
                        except Exception:
                            pass  # keep original
        except Exception as e:
            logger.warning(f"Batch translation failed: {e}")

    return results


def should_translate(language: str) -> bool:
    """Check if translation is needed. Skip for Chinese since source is already Chinese."""
    return language != "zh"


# ── Product listing translation (Phase 1) ────────────────────────────────


async def translate_product(product: dict, language: str) -> dict:
    """Translate a single product's listing fields in-place."""
    if not should_translate(language):
        return product

    fields = ["title", "shopName", "location"]
    texts = [product.get(f, "") or "" for f in fields]
    translated = await translate_batch(texts, language)

    for i, f in enumerate(fields):
        if product.get(f) and translated[i]:
            product[f] = translated[i]

    if product.get("shopInfo") and product["shopInfo"].get("shopName"):
        product["shopInfo"]["shopName"] = product.get("shopName", product["shopInfo"]["shopName"])

    return product


async def translate_products(products: list[dict], language: str) -> list[dict]:
    """
    Translate all product listing fields in batch.
    Batches titles, shop names, locations, salesCount together for efficiency.
    """
    if not should_translate(language) or not products:
        return products

    # Gather all translatable fields
    titles = [p.get("title", "") or "" for p in products]
    shop_names = [p.get("shopName", "") or "" for p in products]
    locations = [p.get("location", "") or "" for p in products]
    sales = [p.get("salesCount", "") or "" for p in products]

    # Translate in parallel batches
    t_titles, t_shops, t_locations, t_sales = await asyncio.gather(
        translate_batch(titles, language),
        translate_batch(shop_names, language),
        translate_batch(locations, language),
        translate_batch(sales, language),
    )

    for i, product in enumerate(products):
        if product.get("title") and t_titles[i]:
            product["title"] = t_titles[i]
        if product.get("shopName") and t_shops[i]:
            product["shopName"] = t_shops[i]
            if product.get("shopInfo") and product["shopInfo"].get("shopName"):
                product["shopInfo"]["shopName"] = t_shops[i]
        if product.get("location") and t_locations[i]:
            product["location"] = t_locations[i]
        if product.get("salesCount") and t_sales[i]:
            product["salesCount"] = t_sales[i]

    return products


# ── Detail translation (Phase 2) ─────────────────────────────────────────


async def translate_detail(detail_dict: dict, language: str) -> dict:
    """
    Translate ALL text fields in detailed product info in-place.

    Covers:
      - fullTitle, fullDescription, brand, shippingInfo, salesVolume
      - guarantees (list of strings)
      - specifications (dict: key→value, both translated)
      - variants (dict: label→[{value, image?, vid?}], label + all values translated)
      - shopInfo: shopName, shopLocation, shopAge, badges
    """
    if not should_translate(language) or not detail_dict:
        return detail_dict

    # ── Simple text fields ────────────────────────────────────────────────
    text_fields = ["fullTitle", "fullDescription", "brand", "shippingInfo", "salesVolume"]
    texts = [str(detail_dict.get(f, "") or "") for f in text_fields]
    translated = await translate_batch(texts, language)
    for i, f in enumerate(text_fields):
        if detail_dict.get(f) and translated[i]:
            detail_dict[f] = translated[i]

    # ── Guarantees (list[str]) ────────────────────────────────────────────
    guarantees = detail_dict.get("guarantees") or []
    if guarantees:
        detail_dict["guarantees"] = await translate_batch(
            [str(g) for g in guarantees], language
        )

    # ── Specifications (dict[str, str]) — translate both keys and values ──
    specs = detail_dict.get("specifications")
    if specs and isinstance(specs, dict):
        keys = list(specs.keys())
        values = [str(v) for v in specs.values()]
        t_keys, t_values = await asyncio.gather(
            translate_batch(keys, language),
            translate_batch(values, language),
        )
        detail_dict["specifications"] = dict(zip(t_keys, t_values))

    # ── Variants (dict[str, list[dict]]) ──────────────────────────────────
    # Structure: {"口味": [{"value": "辣味", "image": "...", "vid": "123"}, ...]}
    # Translate: dimension labels (keys) + every option's "value" field
    variants = detail_dict.get("variants")
    if variants and isinstance(variants, dict):
        old_keys = list(variants.keys())
        # Collect all option value texts across all dimensions
        all_option_values: list[str] = []
        option_map: list[tuple[str, int]] = []  # (old_key, index_in_option_list)
        for key in old_keys:
            options = variants[key]
            if isinstance(options, list):
                for opt in options:
                    if isinstance(opt, dict) and opt.get("value"):
                        option_map.append((key, len(all_option_values)))
                        all_option_values.append(str(opt["value"]))

        # Translate keys and all option values in parallel
        if all_option_values:
            t_keys, t_option_values = await asyncio.gather(
                translate_batch(old_keys, language),
                translate_batch(all_option_values, language),
            )
        else:
            t_keys = await translate_batch(old_keys, language)
            t_option_values = []

        # Rebuild variants dict with translated keys
        new_variants: dict = {}
        key_mapping = dict(zip(old_keys, t_keys))
        for old_key in old_keys:
            new_key = key_mapping.get(old_key, old_key)
            new_variants[new_key] = variants[old_key]
        detail_dict["variants"] = new_variants

        # Apply translated option values back
        val_idx = 0
        for old_key in old_keys:
            new_key = key_mapping.get(old_key, old_key)
            options = detail_dict["variants"][new_key]
            if isinstance(options, list):
                for opt in options:
                    if isinstance(opt, dict) and opt.get("value"):
                        if val_idx < len(t_option_values) and t_option_values[val_idx]:
                            opt["value"] = t_option_values[val_idx]
                        val_idx += 1

    # ── ShopInfo ──────────────────────────────────────────────────────────
    shop_info = detail_dict.get("shopInfo")
    if shop_info and isinstance(shop_info, dict):
        shop_text_fields = ["shopName", "shopLocation", "shopAge"]
        shop_texts = [str(shop_info.get(f, "") or "") for f in shop_text_fields]
        translated_shop = await translate_batch(shop_texts, language)
        for i, f in enumerate(shop_text_fields):
            if shop_info.get(f) and translated_shop[i]:
                shop_info[f] = translated_shop[i]

        # Translate badges list
        badges = shop_info.get("badges") or []
        if badges:
            shop_info["badges"] = await translate_batch(
                [str(b) for b in badges], language
            )

        # Translate sellerInfo fields if present
        seller_info = shop_info.get("sellerInfo")
        if seller_info and isinstance(seller_info, dict):
            seller_text_fields = ["averageDeliveryTime", "averageRefundTime"]
            seller_texts = [str(seller_info.get(f, "") or "") for f in seller_text_fields]
            t_seller = await translate_batch(seller_texts, language)
            for i, f in enumerate(seller_text_fields):
                if seller_info.get(f) and t_seller[i]:
                    seller_info[f] = t_seller[i]

    return detail_dict
