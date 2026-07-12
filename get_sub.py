"""
get_sub.py — Fetch & filter live VPN nodes from v2nodes.com

Phân tích HTML thực tế (server page):
  - Node URL nằm trong: <textarea id="config" data-config="ss://...">
  - Check button: <button id="checkButton" data-id="{id}"> — KHÔNG có CSRF form
  - Check endpoint: GET /servers/{id}/check/  (JS gọi AJAX GET, không POST)
  - Result injected vào: <div id="testResult">
  - Response chứa "Server Status: Online!" hoặc "Server Status: Offline"

Flow:
  1. GET /country/{cc}/          → detect "X of N" pages
  2. GET /country/{cc}/?page=K   → collect server IDs  (song song)
  3. GET /servers/{id}/          → parse node URL từ data-config attribute
  4. GET /servers/{id}/check/    → parse Online / Offline
  5. Ghi node Online → {cc}_sub.txt

Dependencies: aiohttp>=3.9, beautifulsoup4>=4.12, lxml>=5.0
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
COUNTRIES       = ["hk", "jp", "sg", "vn"]
CONCURRENCY     = 15
RETRY_TIMES     = 2
CONNECT_TIMEOUT = 10    # giây
READ_TIMEOUT    = 25    # giây (check đôi khi chậm)

# Bắt "1 of 6", "2 of 10", ... (phòng trường hợp trang hiện tại khác 1)
_RE_TOTAL_PAGES = re.compile(r'\b\d+\s+of\s+(\d+)\b', re.IGNORECASE)
_RE_SERVER_ID   = re.compile(r'/servers/(\d+)/')

# Fallback regex nếu BeautifulSoup không parse được textarea
_RE_NODE_URL    = re.compile(
    r'(?:vmess|vless|trojan|ss|ssr)://[^\s<>"\']+',
    re.IGNORECASE,
)

# Headers giả lập browser Chrome — dùng chung cho mọi request
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection":      "keep-alive",
}

# Headers bổ sung khi gọi check endpoint (giống AJAX từ browser)
_AJAX_EXTRA = {
    "X-Requested-With": "XMLHttpRequest",
    "Accept":           "text/html, */*; q=0.01",
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
    extra: dict | None = None,
    retries: int = RETRY_TIMES,
) -> Optional[str]:
    """GET với exponential-backoff retry. Trả về text hoặc None."""
    hdrs = {**_HEADERS, **(extra or {})}
    for attempt in range(retries + 1):
        try:
            async with session.get(url, headers=hdrs, timeout=_timeout()) as r:
                r.raise_for_status()
                return await r.text()
        except Exception as exc:
            if attempt == retries:
                log.debug("GET %s | fail x%d: %s", url, attempt + 1, exc)
                return None
            await asyncio.sleep(1.5 ** attempt)
    return None


# ═══════════════════════ HTML parsers ═════════════════════════
def _parse_total_pages(html: str) -> int:
    """Parse 'X of N' → N. Trả về 1 nếu không tìm thấy."""
    m = _RE_TOTAL_PAGES.search(html)
    return int(m.group(1)) if m else 1


def _parse_server_ids(html: str) -> list[str]:
    """Server IDs không trùng, giữ thứ tự."""
    return list(dict.fromkeys(_RE_SERVER_ID.findall(html)))


def _extract_node_url(html: str) -> Optional[str]:
    """
    Lấy node URL từ trang server.

    Ưu tiên: <textarea id="config" data-config="ss://...">
    Vì đây là field chứa chính xác URL mà user copy để dùng.
    Fallback: regex trên toàn HTML.
    """
    soup = BeautifulSoup(html, "lxml")

    # Cách chính xác nhất: đọc attribute data-config
    textarea = soup.find("textarea", {"id": "config"})
    if textarea:
        node = textarea.get("data-config") or textarea.get_text(strip=True)
        if node and "://" in node:
            return node.strip()

    # Fallback regex
    m = _RE_NODE_URL.search(html)
    return m.group(0).strip() if m else None


# ═══════════════════════ Page collection ══════════════════════
async def _fetch_page_ids(
    session: aiohttp.ClientSession,
    country: str,
    page: int,
    sem: asyncio.Semaphore,
) -> list[str]:
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


async def collect_server_ids(
    session: aiohttp.ClientSession,
    country: str,
    sem: asyncio.Semaphore,
) -> list[str]:
    """Fetch tất cả trang của country, gom server ID."""
    html_p1 = await _get(session, f"{BASE_URL}/country/{country}/")
    if not html_p1:
        log.warning("[%s] Không tải được trang country", country.upper())
        return []

    total  = _parse_total_pages(html_p1)
    ids_p1 = _parse_server_ids(html_p1)
    log.info("[%s] %d trang | trang 1: %d servers", country.upper(), total, len(ids_p1))

    extra: list[str] = []
    if total > 1:
        pages = await asyncio.gather(*[
            _fetch_page_ids(session, country, p, sem)
            for p in range(2, total + 1)
        ])
        for pg in pages:
            extra.extend(pg)

    all_ids = list(dict.fromkeys(ids_p1 + extra))
    log.info("[%s] Tổng server IDs: %d", country.upper(), len(all_ids))
    return all_ids


# ═══════════════════════ Server check ═════════════════════════
async def check_server(
    session: aiohttp.ClientSession,
    server_id: str,
    sem: asyncio.Semaphore,
) -> Optional[str]:
    """
    Bước 3+4:

    a) GET /servers/{id}/
       → Parse node URL từ <textarea id="config" data-config="...">
         (đây là field v2nodes dùng để user copy — chính xác nhất)

    b) GET /servers/{id}/check/
       → Gửi kèm headers AJAX (X-Requested-With: XMLHttpRequest)
         và Referer như browser thật
       → KHÔNG cần CSRF (trang không có csrfmiddlewaretoken)
       → Parse response: "Server Status: Online!" / "Server Status: Offline"

    Trả về node URL nếu Online, None nếu Offline/lỗi.
    """
    async with sem:
        server_url = f"{BASE_URL}/servers/{server_id}/"

        # ── a) GET trang server ──────────────────────────────
        html = await _get(session, server_url)
        if not html:
            return None

        node_url = _extract_node_url(html)
        if not node_url:
            log.debug("Server %s: không tìm thấy node URL", server_id)
            return None

        # ── b) GET check endpoint (= JS gọi khi bấm Check Server) ──
        check_url  = f"{BASE_URL}/servers/{server_id}/check/"
        check_hdrs = {**_AJAX_EXTRA, "Referer": server_url}

        resp = await _get(session, check_url, extra=check_hdrs)

        if not resp:
            log.debug("Server %s: check endpoint không phản hồi", server_id)
            return None

        resp_lower = resp.lower()
        log.debug("Server %s check response: %r", server_id, resp[:120])

        # Parse kết quả — ưu tiên tìm cụm "server status" trước
        if "server status" in resp_lower:
            if "online" in resp_lower:
                log.debug("Server %s → ✔ ONLINE", server_id)
                return node_url
            if "offline" in resp_lower:
                log.debug("Server %s → ✘ OFFLINE", server_id)
                return None

        # Fallback heuristic
        if "offline" in resp_lower:
            return None
        if "online" in resp_lower:
            return node_url

        log.debug("Server %s: response không rõ trạng thái → %r", server_id, resp[:120])
        return None


# ═══════════════════════ Per-country pipeline ═════════════════
async def process_country(
    session: aiohttp.ClientSession,
    country: str,
    sem: asyncio.Semaphore,
) -> None:
    log.info("━━━ [%s] Bắt đầu ━━━", country.upper())

    server_ids = await collect_server_ids(session, country, sem)
    if not server_ids:
        log.warning("[%s] Không có server nào.", country.upper())
        return

    results = await asyncio.gather(
        *[check_server(session, sid, sem) for sid in server_ids],
        return_exceptions=True,
    )

    online_nodes = [r for r in results if isinstance(r, str) and r]
    total  = len(server_ids)
    online = len(online_nodes)

    log.info(
        "[%s] ✔ %d/%d online  |  ✘ %d offline/lỗi",
        country.upper(), online, total, total - online,
    )

    if online_nodes:
        out = Path(f"{country}_sub.txt")
        out.write_text("\n".join(online_nodes), encoding="utf-8")
        log.info("[%s] Đã lưu %d node → %s", country.upper(), online, out)
    else:
        log.warning("[%s] Không có node online — bỏ qua ghi file.", country.upper())

    log.info("━━━ [%s] Xong ━━━\n", country.upper())


# ═════════════════════════ Main ═══════════════════════════════
async def main() -> None:
    log.info("┌──────────────────────────────────────────┐")
    log.info("│   FETCH & CHECK VPN NODES — v2nodes.com  │")
    log.info("└──────────────────────────────────────────┘")

    sem = asyncio.Semaphore(CONCURRENCY)

    connector = aiohttp.TCPConnector(
        limit=60,
        ssl=False,
        ttl_dns_cache=300,
    )

    async with aiohttp.ClientSession(
        connector=connector,
        cookie_jar=aiohttp.CookieJar(unsafe=True),
    ) as session:
        for country in COUNTRIES:
            await process_country(session, country, sem)

    log.info("┌──────────────────────────────────────────┐")
    log.info("│               HOÀN TẤT                  │")
    log.info("└──────────────────────────────────────────┘")


if __name__ == "__main__":
    asyncio.run(main())
