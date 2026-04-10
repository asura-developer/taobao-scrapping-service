"""
Debug script - dumps Taobao search page card structure to JSON.
Run from project root: python debug_cards.py
Output: debug_cards.json
"""
import asyncio
import json
import re
from pathlib import Path


COOKIE_PATH = Path("utils/cookies.json")


def load_cookies():
    raw = json.loads(COOKIE_PATH.read_text())
    cookies = []
    for c in raw:
        if "name" not in c or "value" not in c:
            continue
        cookie = {
            "name":   c["name"],
            "value":  c["value"],
            "domain": c.get("domain", ".taobao.com"),
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
    locale = [
        {"name": "hng",         "value": "US%7Cen_US%7CUSD%7C840", "domain": ".taobao.com", "path": "/"},
        {"name": "intl_locale", "value": "en_US",                   "domain": ".taobao.com", "path": "/"},
    ]
    existing = {c["name"] for c in cookies}
    cookies += [c for c in locale if c["name"] not in existing]
    print(f"Loaded {len(cookies)} cookies")
    return cookies


def parse_element(el) -> dict:
    """Recursively parse a Scrapling element into a dict."""
    try:
        tag  = el.root.tag if hasattr(el, "root") else "unknown"
        attrs = dict(el.attrib) if hasattr(el, "attrib") else {}
        text  = (el.text or "").strip()

        children = []
        try:
            for child in el.css(":scope > *") or []:
                children.append(parse_element(child))
        except Exception:
            pass

        return {
            "tag":      tag,
            "class":    attrs.get("class", ""),
            "id":       attrs.get("id", ""),
            "href":     attrs.get("href", ""),
            "src":      attrs.get("src", attrs.get("data-src", "")),
            "text":     text[:200] if text else "",
            "attrs":    {k: v for k, v in attrs.items() if k not in ("class","id","href","src","data-src")},
            "children": children,
        }
    except Exception as e:
        return {"error": str(e)}


def extract_cards_from_html(html: str) -> list[dict]:
    """
    Extract raw HTML chunks between each product link.
    Returns list of {item_id, href, chunk_html} dicts.
    """
    pattern = re.compile(
        r'href=["\']?(//(?:item\.taobao\.com|detail\.tmall\.com)/[^"\'>\s]*id=(\d+)[^"\'>\s]*)',
        re.IGNORECASE,
    )
    matches = list(pattern.finditer(html))
    print(f"Found {len(matches)} product link matches in HTML")

    cards = []
    seen = set()
    for i, m in enumerate(matches):
        item_id = m.group(2)
        if item_id in seen:
            continue
        seen.add(item_id)

        href = "https:" + m.group(1)
        chunk_start = m.start()
        chunk_end   = matches[i + 1].start() if i + 1 < len(matches) else chunk_start + 5000
        chunk = html[chunk_start:min(chunk_end, chunk_start + 5000)]

        cards.append({
            "item_id": item_id,
            "href":    href,
            "chunk":   chunk,
        })

    return cards


def analyse_chunk(chunk: str) -> dict:
    """
    Extract all class names, tag names, and text snippets from a HTML chunk.
    Returns structured dict for easy reading.
    """
    # All tags with classes
    tag_classes = []
    for m in re.finditer(r'<(\w+)[^>]*class="([^"]*)"[^>]*>(.*?)</', chunk, re.DOTALL):
        tag      = m.group(1)
        cls      = m.group(2).strip()
        inner    = re.sub(r'<[^>]+>', '', m.group(3)).strip()[:100]
        tag_classes.append({
            "tag":   tag,
            "class": cls,
            "text":  inner,
        })

    # All img srcs
    imgs = re.findall(r'<img[^>]+(?:src|data-src|data-lazy-src)=["\']([^"\']+)["\']', chunk, re.IGNORECASE)

    # All hrefs
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', chunk)

    return {
        "tag_classes": tag_classes,
        "images": imgs,
        "hrefs":  hrefs,
    }


async def main():
    from scrapling.fetchers import StealthyFetcher

    cookies = load_cookies()
    url = "https://s.taobao.com/search?q=%E9%A3%9F%E5%93%81&lang=en"  # 食品 (food)

    print(f"\nFetching: {url}")
    page = await StealthyFetcher.async_fetch(
        url=url,
        headless=True,
        network_idle=True,
        cookies=cookies,
        extra_headers={"Accept-Language": "en-US,en;q=0.9"},
    )
    print(f"Status: {page.status}")

    # Get raw HTML
    html = ""
    for attr in ("html_content", "content", "text_content"):
        val = getattr(page, attr, None)
        if val and isinstance(val, str) and len(val) > 100:
            html = val
            break
    print(f"HTML size: {len(html):,} chars")

    # Save full HTML for browser inspection
    Path("debug_page.html").write_text(html, encoding="utf-8")
    print("✅ Saved full HTML → debug_page.html")

    # Extract card chunks
    cards = extract_cards_from_html(html)
    print(f"Unique product cards found: {len(cards)}")

    # Analyse first 5 cards
    output = []
    for card in cards[:5]:
        analysis = analyse_chunk(card["chunk"])
        output.append({
            "item_id":   card["item_id"],
            "href":      card["href"],
            "analysis":  analysis,
            "raw_chunk": card["chunk"][:2000],  # First 2000 chars of raw HTML
        })

    # Also try Scrapling CSS selectors on full page for comparison
    css_probes = {}
    probe_selectors = [
        "a[href*='item.taobao.com']",
        "a[href*='detail.tmall.com']",
        "[class*='card']",
        "[class*='Card']",
        "[class*='title']",
        "[class*='Title']",
        "[class*='price']",
        "[class*='Price']",
        "[class*='priceWrapper']",
        "[class*='img']",
        "img",
    ]
    for sel in probe_selectors:
        try:
            els = page.css(sel)
            count = len(els) if els else 0
            sample_class = ""
            sample_text  = ""
            if count:
                first = els[0]
                sample_class = first.attrib.get("class", "")[:80] if hasattr(first, "attrib") else ""
                sample_text  = (first.text or "")[:80] if hasattr(first, "text") else ""
            css_probes[sel] = {
                "count": count,
                "sample_class": sample_class,
                "sample_text":  sample_text,
            }
        except Exception as e:
            css_probes[sel] = {"error": str(e)}

    result = {
        "url":        url,
        "status":     page.status,
        "html_size":  len(html),
        "card_count": len(cards),
        "css_probes": css_probes,
        "cards":      output,
    }

    out_path = Path("debug_cards.json")
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n✅ Saved → debug_cards.json ({out_path.stat().st_size:,} bytes)")

    # Print a quick summary to console
    print("\n── CSS probe summary ───────────────────────────────────────────")
    for sel, info in css_probes.items():
        count = info.get("count", "ERR")
        cls   = info.get("sample_class", "")[:60]
        txt   = info.get("sample_text",  "")[:40]
        print(f"  {count:4}  {sel:<45} class={cls!r}  text={txt!r}")

    print("\n── First card tag+class list ───────────────────────────────────")
    if output:
        for tc in output[0]["analysis"]["tag_classes"][:30]:
            print(f"  <{tc['tag']}> class={tc['class']!r}  text={tc['text']!r}")

    print("\nOpen debug_cards.json for full detail.")


if __name__ == "__main__":
    asyncio.run(main())