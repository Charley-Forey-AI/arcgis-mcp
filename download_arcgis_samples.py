from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


DEFAULT_LIST_URL = "https://developers.arcgis.com/javascript/latest/sample-code/"
DEFAULT_OUT_DIR = Path("mcp-apps/examples")


def _unique(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _slug_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    slug = (path.split("/")[-1] or "sample").strip()
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", slug).strip("._-")
    return slug or "sample"


def _sanitize_filename(name: str) -> str:
    name = name.strip().replace("\0", "")
    # Windows reserved characters: < > : " / \ | ? *
    name = re.sub(r'[<>:"/\\\\|?*]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    if not name:
        return "download"
    # Avoid reserved filenames on Windows
    reserved = {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{i}" for i in range(1, 10)),
        *(f"LPT{i}" for i in range(1, 10)),
    }
    base = name.split(".")[0].upper()
    if base in reserved:
        return f"_{name}"
    return name


def scrape_sample_urls(list_url: str, *, headless: bool) -> list[str]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()

        page.goto(list_url, wait_until="domcontentloaded")
        _try_dismiss_cookie_banner(page)
        page.wait_for_selector("#sample-card-group a[href]", timeout=60_000)

        # Some listings lazy-load as you scroll. Scroll until the link count stabilizes.
        last_count = 0
        stable_rounds = 0
        for _ in range(60):
            count = page.locator("#sample-card-group a[href]").count()
            if count == last_count:
                stable_rounds += 1
            else:
                stable_rounds = 0
                last_count = count

            if stable_rounds >= 3:
                break

            page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(800)

        hrefs: list[str] = page.eval_on_selector_all(
            "#sample-card-group a[href]",
            "els => els.map(e => e.getAttribute('href')).filter(Boolean)",
        )

        context.close()
        browser.close()

    urls = [urljoin(list_url, h) for h in hrefs]
    urls = [u for u in urls if "/javascript/latest/sample-code/" in u]
    return _unique(urls)


def _try_dismiss_cookie_banner(page) -> None:
    candidates = [
        "#onetrust-accept-btn-handler",
        'button:has-text("Accept all")',
        'button:has-text("Accept All")',
        'button:has-text("Accept")',
        'button:has-text("I agree")',
    ]
    for sel in candidates:
        loc = page.locator(sel)
        try:
            if loc.count() > 0:
                loc.first.click(timeout=1500)
                return
        except Exception:
            continue


def _download_from_page(page, download_btn):
    """
    The ArcGIS sample "Download" control isn't consistent across all pages.
    Sometimes it downloads immediately; sometimes it opens a menu first.
    This tries a few strategies before giving up.
    """

    def _try_click(locator, timeout_ms: int):
        with page.expect_download(timeout=timeout_ms) as dl_info:
            locator.click()
        return dl_info.value

    # Attempt 1: direct download (fast path)
    try:
        return _try_click(download_btn, timeout_ms=15_000)
    except PlaywrightTimeoutError:
        pass

    # Attempt 2: menu/popover item (e.g., "Download" becomes a menu)
    try:
        menu_item = page.get_by_role("menuitem", name=re.compile(r"download", re.I))
        if menu_item.count() > 0 and menu_item.first.is_visible():
            return _try_click(menu_item.first, timeout_ms=180_000)
    except Exception:
        pass

    # Attempt 3: visible link that looks like a download target
    try:
        link = page.locator('a[download], a[href*="download"]').filter(has_not=download_btn)
        if link.count() > 0:
            link.first.wait_for(state="visible", timeout=5_000)
            return _try_click(link.first, timeout_ms=180_000)
    except Exception:
        pass

    # Attempt 4: retry the original button with a longer timeout (some samples generate bundles server-side)
    return _try_click(download_btn, timeout_ms=300_000)


def _load_url_list(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [u.strip() for u in path.read_text(encoding="utf-8").splitlines() if u.strip()]


def _append_line(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as f:
        f.write(line.rstrip() + "\n")


def _looks_downloaded(out_dir: Path, sample_url: str) -> bool:
    """
    Best-effort "already downloaded?" check.
    Most downloads are <slug>.html, so this is usually enough.
    """
    slug = _slug_from_url(sample_url)
    patterns = [
        f"{slug}.*",
        f"{slug}__*",
    ]
    for pat in patterns:
        if any(out_dir.glob(pat)):
            return True
    return False


def _apply_sharding(urls: list[str], shard: int, shards: int) -> list[str]:
    if shards <= 1:
        return urls
    if not (0 <= shard < shards):
        raise ValueError(f"shard must be in [0, {shards - 1}]")
    return [u for i, u in enumerate(urls) if i % shards == shard]


def download_samples(sample_urls: list[str], *, out_dir: Path, headless: bool, limit: int | None) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)

    urls_txt = out_dir / "sample_urls.txt"
    urls_txt.write_text("\n".join(sample_urls) + ("\n" if sample_urls else ""), encoding="utf-8")

    failures: list[str] = []
    completed_urls_path = out_dir / "completed_urls.txt"
    completed_urls = set(_load_url_list(completed_urls_path))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(accept_downloads=True)
        # Speed up by blocking heavy resources (images/fonts/media). DOM + JS still load.
        context.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in {"image", "media", "font"}
            else route.continue_(),
        )

        total = len(sample_urls) if limit is None else min(len(sample_urls), limit)
        for idx, sample_url in enumerate(sample_urls[:total], start=1):
            try:
                if sample_url in completed_urls:
                    print(f"[{idx}/{total}] (skip: completed) {sample_url}")
                    continue
                if _looks_downloaded(out_dir, sample_url):
                    print(f"[{idx}/{total}] (skip: file exists) {sample_url}")
                    completed_urls.add(sample_url)
                    _append_line(completed_urls_path, sample_url)
                    continue

                page = context.new_page()
                print(f"[{idx}/{total}] {sample_url}")
                page.goto(sample_url, wait_until="domcontentloaded")
                _try_dismiss_cookie_banner(page)

                download_btn = page.get_by_role("button", name="Download")
                if download_btn.count() == 0:
                    download_btn = page.locator('button[aria-label="Download"], button:has-text("Download")')

                download_btn.first.wait_for(state="visible", timeout=60_000)

                download = _download_from_page(page, download_btn.first)
                suggested = download.suggested_filename or "download"
                slug = _slug_from_url(sample_url)

                out_name = suggested
                if slug.lower() not in suggested.lower():
                    out_name = f"{slug}__{suggested}"

                save_path = out_dir / _sanitize_filename(out_name)
                download.save_as(str(save_path))

                completed_urls.add(sample_url)
                _append_line(completed_urls_path, sample_url)
            except PlaywrightTimeoutError as e:
                failures.append(sample_url)
                print(f"  !! timeout: {e}", file=sys.stderr)
            except Exception as e:
                failures.append(sample_url)
                print(f"  !! error: {e}", file=sys.stderr)
            finally:
                try:
                    page.close()
                except Exception:
                    pass

        context.close()
        browser.close()

    if failures:
        (out_dir / "failed_urls.txt").write_text("\n".join(failures) + "\n", encoding="utf-8")
        print(f"\nDone with failures: {len(failures)} (see {out_dir / 'failed_urls.txt'})", file=sys.stderr)
        return 2

    print("\nDone. All downloads succeeded.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape ArcGIS JS sample URLs from the listing page, then click Download on each sample page "
            "and save the downloaded file(s) into an output folder."
        )
    )
    parser.add_argument("--list-url", default=DEFAULT_LIST_URL, help=f"Listing page URL (default: {DEFAULT_LIST_URL})")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Output directory for downloaded files")
    parser.add_argument("--headful", action="store_true", help="Run with a visible browser window")
    parser.add_argument("--limit", type=int, default=None, help="Only download the first N samples")
    parser.add_argument("--dry-run", action="store_true", help="Only print/write the URLs; do not download")
    parser.add_argument(
        "--only-failed",
        action="store_true",
        help="Download only URLs in <out-dir>/failed_urls.txt (from a previous run)",
    )
    parser.add_argument("--shards", type=int, default=1, help="Split work into N shards (run in parallel terminals)")
    parser.add_argument("--shard", type=int, default=0, help="Which shard index to run (0..N-1)")

    args = parser.parse_args()
    out_dir = Path(args.out_dir)
    headless = not args.headful

    if args.only_failed:
        failed_path = Path(args.out_dir) / "failed_urls.txt"
        if not failed_path.exists():
            print(f"failed list not found: {failed_path}", file=sys.stderr)
            return 2
        urls = [u.strip() for u in failed_path.read_text(encoding="utf-8").splitlines() if u.strip()]
    else:
        urls = scrape_sample_urls(args.list_url, headless=headless)

    urls = _apply_sharding(urls, shard=args.shard, shards=max(args.shards, 1))

    if args.limit is not None:
        urls = urls[: max(args.limit, 0)]

    if args.dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "sample_urls.txt").write_text("\n".join(urls) + ("\n" if urls else ""), encoding="utf-8")
        # Print in the exact "full URL per line" format you requested.
        for u in urls:
            print(u)
        return 0

    return download_samples(urls, out_dir=out_dir, headless=headless, limit=None)


if __name__ == "__main__":
    raise SystemExit(main())

