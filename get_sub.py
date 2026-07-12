"""
get_sub.py — Fetch & filter live VPN nodes from v2nodes.com
Strategy:
  1. Scrape trang 1 của country → detect tổng số trang ("1 of N")
  2. Fetch toàn bộ trang song song → gom tất cả server ID
  3. Với mỗi server: GET trang → lấy node URL + CSRF → POST check
  4. Chỉ giữ node Online → ghi {country}_sub.txt

Dependencies: aiohttp, beautifulsoup4, lxml
"""

import asyncio
import re
import sys
import logging
from pathlib import Path
from typing import Optional

import aiohttp
from bs4 import BeautifulSoup

# ═══════════════════════════ Config ═══════════════════════════
BASE_URL        = "https://www.v2nodes.com"
COUNTRIES       = ["hk", "jp", "sg", "vn"]   # thêm/bớt tuỳ ý
CONCURRENCY     = 15    # request song song tối đa (cả page + check)
RETRY_TIMES     = 2     # số lần retry mỗi request
CONNECT_TIMEOUT = 10    # giây
READ_TIMEOUT    = 20    # giây

# Regex trích xuất
_RE_SERVER_ID   = re.compile(r'/servers/(\d+)/')
_RE_TOTAL_PAGES = re.compile(r'\b1\s+of\s+(\d+)\b', re.IGNORECASE)
_RE_NODE_URL    = re.compile(
    r'(vmess|vless|trojan|ss|ssr)://[A-Za-z0-9+/=%@:._\-?&#]+'
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ═══════════════════════════ Logging ══════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ═══════════════════════ Network helpers ══════════════════════
def _timeout() -> aiohttp.ClientTimeout:
    return aiohttp.ClientTimeout(connect=CONNECT_TIMEOUT, total=READ_TIMEOUT)


async def _get(
    session: aiohttp.ClientSession,
    url: str,
    *,
    retries: int = RETRY_TIMES,
    **kw,
) -> Optional[str]:
    """GET với exponential-backoff retry. Trả về text hoặc None."""
    for attempt in range(retries + 1):
        try:
            async with session.get(url, timeout=_timeout(), **kw) as r:
                r.raise_for_status()
                return await r.text()
        except Exception as exc:
            if attempt == retries:
                log.debug("GET %s | fail(%d): %s", url, attempt + 1, exc)
                return None
            await asyncio.sleep(1.5 ** attempt)   # 1s, 1.5s, 2.25s…
    return None


async def _post(
    session: aiohttp.ClientSession,
    url: str,
    *,
    data: dict | None = None,
    retries: int = RETRY_TIMES,
    **kw,
) -> Optional[str]:
    """POST với retry. Trả về text hoặc None."""
    for attempt in range(retries + 1):
        try:
            async with session.post(url, data=data, timeout=_timeout(), **kw) as r:
                r.raise_for_status()
                return await r.text()
        except Exception as exc:
            if attempt == retries:
                log.debug("POST %s | fail(%d): %s", url, attempt + 1, exc)
                return None
            await asyncio.sleep(1.5 ** attempt)
    return None


# ═══════════════════════ Page scraping ════════════════════════
def _parse_server_ids(html: str) -> list[str]:
    """Trả về danh sách server ID không trùng, giữ thứ tự."""
    return list(dict.fromkeys(_RE_SERVER_ID.findall(html)))


def _parse_total_pages(html: str) -> int:
    """
    Tìm dòng dạng '1 of 6' trong HTML → trả về tổng số trang.
    Trả về 1 nếu không tìm thấy (chỉ có 1 trang).
    """
    m = _RE_TOTAL_PAGES.search(html)
    return int(m.group(1)) if m else 1


async def _fetch_page(
    session: aiohttp.ClientSession,
    country: str,
    page: int,
    sem: asyncio.Semaphore,
) -> list[str]:
    """Fetch 1 trang của country, trả về danh sách server ID."""
    async with sem:
        url = (
            f"{BASE_URL}/country/{country}/"
            if page == 1
            else f"{BASE_URL}/country/{country}/?page={page}"
        )
        html = await _get(session, url)
        if not html:
            log.warning("[%s] Không tải được trang %d", country.upper(), page)
            return []
        return _parse_server_ids(html)


async def collect_all_server_ids(
    session: aiohttp.ClientSession,
    country: str,
    sem: asyncio.Semaphore,
) -> list[str]:
    """
    Bước 1+2:
      - Fetch trang 1 → detect tổng số trang
      - Fetch các trang còn lại song song
      - Gom + dedup toàn bộ server ID
    """
    # Trang 1 luôn cần trước để biết tổng số trang
    url_p1 = f"{BASE_URL}/country/{country}/"
    html_p1 = await _get(session, url_p1)
    if not html_p1:
        log.warning("[%s] Không tải được trang 1", country.upper())
        return []

    total_pages = _parse_total_pages(html_p1)
    ids_p1 = _parse_server_ids(html_p1)

    log.info(
        "[%s] Tổng số trang: %d | Trang 1: %d servers",
        country.upper(), total_pages, len(ids_p1),
    )

    # Fetch các trang 2..N song song (nếu có)
    extra_ids: list[str] = []
    if total_pages > 1:
        tasks = [
            _fetch_page(session, country, page, sem)
            for page in range(2, total_pages + 1)
        ]
        pages_results = await asyncio.gather(*tasks)
        for page_ids in pages_results:
            extra_ids.extend(page_ids)

    # Gom lại, loại trùng toàn cục, giữ thứ tự
    all_ids = list(dict.fromkeys(ids_p1 + extra_ids))
    log.info(
        "[%s] Tổng server ID sau gom: %d",
        country.upper(), len(all_ids),
    )
    return all_ids


# ═══════════════════════ Server checking ══════════════════════
def _extract_csrf(html: str) -> Optional[str]:
    """Lấy CSRF token từ hidden input hoặc meta tag."""
    soup = BeautifulSoup(html, "lxml")
    tag = soup.find("input", {"name": "csrfmiddlewaretoken"})
    if tag:
        return tag.get("value")
    meta = soup.find("meta", {"name": "csrf-token"})
    if meta:
        return meta.get("content")
    return None


def _extract_node_url(html: str) -> Optional[str]:
    """Tìm URL node (vmess/vless/trojan/ss/ssr) trong trang server."""
    m = _RE_NODE_URL.search(html)
    return m.group(0) if m else None


async def check_server(
    session: aiohttp.ClientSession,
    server_id: str,
    sem: asyncio.Semaphore,
) -> Optional[str]:
    """
    Bước 3:
      - GET trang server → node URL + CSRF token
      - POST /servers/{id}/check/ (AJAX)
      - Parse "online" / "offline" từ response
    Trả về node URL nếu Online, None nếu Offline/lỗi.
    """
    async with sem:
        server_url = f"{BASE_URL}/servers/{server_id}/"

        # ── GET trang server ──
        html = await _get(session, server_url)
        if not html:
            return None

        node_url = _extract_node_url(html)
        if not node_url:
            log.debug("Server %s: không có node URL, bỏ qua", server_id)
            return None

        csrf = _extract_csrf(html)

        # ── POST check endpoint ──
        check_url = f"{BASE_URL}/servers/{server_id}/check/"
        ajax_headers = {
            "Referer":            server_url,
            "X-Requested-With":   "XMLHttpRequest",
            "Accept":             "application/json, text/javascript, */*; q=0.01",
        }
        resp = await _post(
            session, check_url,
            data={"csrfmiddlewaretoken": csrf} if csrf else None,
            headers=ajax_headers,
        )

        # Fallback GET nếu POST không được
        if resp is None:
            resp = await _get(session, check_url, headers=ajax_headers)

        if not resp:
            log.debug("Server %s: không có phản hồi check", server_id)
            return None

        low = resp.lower()
        if "offline" in low:
            log.debug("Server %s → OFFLINE", server_id)
            return None
        if "online" in low:
            log.debug("Server %s → ONLINE  %s…", server_id, node_url[:55])
            return node_url

        log.debug("Server %s → trạng thái không rõ: %r", server_id, resp[:80])
        return None


# ═══════════════════════ Per-country pipeline ═════════════════
async def process_country(
    session: aiohttp.ClientSession,
    country: str,
    sem: asyncio.Semaphore,
) -> None:
    log.info("━━━ [%s] Bắt đầu ━━━", country.upper())

    # Bước 1+2: thu thập toàn bộ server ID từ mọi trang
    server_ids = await collect_all_server_ids(session, country, sem)
    if not server_ids:
        log.warning("[%s] Không có server nào, bỏ qua.", country.upper())
        return

    # Bước 3: check song song
    results = await asyncio.gather(
        *[check_server(session, sid, sem) for sid in server_ids],
        return_exceptions=True,
    )

    online_nodes = [r for r in results if isinstance(r, str) and r]
    total  = len(server_ids)
    online = len(online_nodes)

    log.info(
        "[%s] Kết quả: %d/%d online | %d offline/lỗi",
        country.upper(), online, total, total - online,
    )

    if online_nodes:
        out = Path(f"{country}_sub.txt")
        out.write_text("\n".join(online_nodes), encoding="utf-8")
        log.info("[%s] ✔ Lưu %d node → %s", country.upper(), online, out)
    else:
        log.warning("[%s] Không có node online, không ghi file.", country.upper())

    log.info("━━━ [%s] Xong ━━━", country.upper())


# ═══════════════════════════ Main ═════════════════════════════
async def main() -> None:
    log.info("┌─────────────────────────────────────────┐")
    log.info("│   BẮT ĐẦU FETCH & CHECK VPN NODES       │")
    log.info("└─────────────────────────────────────────┘")

    sem = asyncio.Semaphore(CONCURRENCY)

    connector = aiohttp.TCPConnector(
        limit=60,
        ssl=False,          # bỏ SSL verify → giảm overhead
        ttl_dns_cache=300,  # cache DNS 5 phút
    )

    async with aiohttp.ClientSession(
        headers=HEADERS,
        connector=connector,
        cookie_jar=aiohttp.CookieJar(),  # giữ cookie session (cho CSRF)
    ) as session:
        for country in COUNTRIES:
            await process_country(session, country, sem)
            log.info("")

    log.info("┌─────────────────────────────────────────┐")
    log.info("│               HOÀN TẤT                  │")
    log.info("└─────────────────────────────────────────┘")


if __name__ == "__main__":
    asyncio.run(main())
