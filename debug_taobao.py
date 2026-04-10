"""
Run this standalone to inspect what Taobao's search page actually returns.
Usage:  python debug_taobao.py
Output: debug_page.html  (open in browser to inspect)
"""
import asyncio
import json
from pathlib import Path

from scrapling.fetchers import StealthyFetcher

COOKIE_PATH = Path("utils/cookies.json")


def load_cookies() -> list:
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

    # Add locale cookies
    locale = [
        {"name": "hng",         "value": "US%7Cen_US%7CUSD%7C840", "domain": ".taobao.com", "path": "/"},
        {"name": "intl_locale", "value": "en_US",                   "domain": ".taobao.com", "path": "/"},
        {"name": "language",    "value": "en_US",                   "domain": ".taobao.com", "path": "/"},
    ]
    existing = {c["name"] for c in cookies}
    cookies += [c for c in locale if c["name"] not in existing]
    print(f"Loaded {len(cookies)} cookies")
    return cookies


async def main():
    cookies = load_cookies()
    url = "https://s.taobao.com/search?q=家电&lang=en"

    print(f"Fetching: {url}")
    page = await StealthyFetcher.async_fetch(
        url=url,
        headless=True,
        network_idle=True,
        cookies=cookies,
        extra_headers={"Accept-Language": "en-US,en;q=0.9"},
    )

    print(f"Status: {page.status}")
    html = page.html_content if hasattr(page, "html_content") else str(page.content)

    # Save full HTML
    Path("debug_page.html").write_text(html, encoding="utf-8")
    print(f"✅ Saved full HTML → debug_page.html ({len(html):,} chars)")

    # ── Probe common selectors ─────────────────────────────────────────────
    probes = [
        # Product links
        ("a[href*='item.taobao.com']",          "taobao item links"),
        ("a[href*='detail.tmall.com']",          "tmall detail links"),
        # Common wrapper selectors across Taobao redesigns
        ("[data-item-id]",                       "data-item-id attrs"),
        (".card--doubleCard--L1GQLx3",           "card doubleCard"),
        (".item",                                "generic .item"),
        (".product",                             "generic .product"),
        ("[class*='Card']",                      "class contains Card"),
        ("[class*='card']",                      "class contains card"),
        ("[class*='item']",                      "class contains item"),
        ("[class*='product']",                   "class contains product"),
        ("[class*='result']",                    "class contains result"),
        # Price selectors
        ("[class*='price']",                     "class contains price"),
        ("[class*='Price']",                     "class contains Price"),
        ("strong",                               "strong tags"),
        # Check if it's a login wall
        ("input[name='loginName']",              "login form (blocked!)"),
        ("#J_SubmitStatic",                      "login submit btn"),
        ("[class*='login']",                     "login classes"),
    ]

    print("\n── Selector probe results ──────────────────────────────────────")
    for sel, label in probes:
        try:
            els = page.css(sel)
            count = len(els) if els else 0
            sample = ""
            if count:
                el = els[0] if hasattr(els, '__getitem__') else els.first
                href = el.attrib.get("href", "") if hasattr(el, 'attrib') else ""
                text = (el.text or "")[:60] if hasattr(el, 'text') else ""
                cls  = el.attrib.get("class", "")[:60] if hasattr(el, 'attrib') else ""
                sample = f" | href={href[:80]}" if href else f" | text={text!r}" if text else f" | class={cls!r}"
            print(f"  {count:4d}  {label:<40s}{sample}")
        except Exception as e:
            print(f"  ERR   {label:<40s} → {e}")

    # ── Print first 3 item.taobao.com hrefs ───────────────────────────────
    print("\n── First 3 item.taobao.com links ───────────────────────────────")
    try:
        links = page.css("a[href*='item.taobao.com']")
        for i, link in enumerate(links[:3]):
            print(f"  [{i}] href={link.attrib.get('href','')[:120]}")
            print(f"       text={link.text!r}")
            parent = link.parent
            if parent:
                print(f"       parent.class={parent.attrib.get('class','')[:80]}")
    except Exception as e:
        print(f"  Error: {e}")

    # ── Print first 500 chars of body text (detect login wall) ────────────
    print("\n── Body text preview (first 500 chars) ─────────────────────────")
    try:
        body = page.css("body")
        if body:
            print(repr((body.first.text or "")[:500]))
    except Exception as e:
        print(f"  Error: {e}")

    print("\nDone. Open debug_page.html in your browser to inspect the full DOM.")


if __name__ == "__main__":
    asyncio.run(main())