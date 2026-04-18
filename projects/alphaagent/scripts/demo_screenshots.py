"""
Generate demo screenshots for the README.

Requires: `pip install playwright && playwright install chromium`
Requires: API + Streamlit running locally (`make serve` in another terminal).

Usage:
    python3 scripts/demo_screenshots.py
    open docs/screenshots/

Produces:
    docs/screenshots/01-ui-ask.png
    docs/screenshots/02-ui-portfolio.png
    docs/screenshots/03-ui-dq.png
    docs/screenshots/04-ui-lineage.png
    docs/screenshots/05-ui-eval.png
    docs/screenshots/06-api-swagger.png

These are the images the Substack / LinkedIn post references.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

from playwright.async_api import async_playwright

OUT = Path(__file__).resolve().parent.parent / "docs" / "screenshots"
OUT.mkdir(parents=True, exist_ok=True)

TARGETS = [
    ("01-ui-ask.png", "http://localhost:8501/", "🤖 Ask"),
    ("02-ui-portfolio.png", "http://localhost:8501/", "📊 Portfolio Explorer"),
    ("03-ui-dq.png", "http://localhost:8501/", "🧪 Data Quality"),
    ("04-ui-lineage.png", "http://localhost:8501/", "🕸 Lineage"),
    ("05-ui-eval.png", "http://localhost:8501/", "🎯 Agent Eval"),
    ("06-api-swagger.png", "http://localhost:8000/docs", None),
]


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        context = await browser.new_context(viewport={"width": 1440, "height": 900})
        page = await context.new_page()

        for filename, url, tab_label in TARGETS:
            out = OUT / filename
            print(f"→ {filename}")
            try:
                await page.goto(url, wait_until="networkidle", timeout=15000)
                if tab_label:
                    # Streamlit tab-button click
                    await page.get_by_role("tab", name=tab_label).click()
                    await page.wait_for_timeout(1500)  # let charts render
                await page.screenshot(path=str(out), full_page=True)
            except Exception as e:
                print(f"  ✗ {e}")
                continue
        await browser.close()

    print(f"\n✓ Screenshots written to {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
