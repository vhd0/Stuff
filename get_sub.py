"""
get_sub.py — Fetch live VPN nodes from v2nodes.com

Phân tích:
  - /servers/{id}/check/ bị Cloudflare chặn với automated request
  - v2nodes đã tự test server mỗi vài phút — kết quả hiển thị trên
    country page qua badge ping:
      <span class="text-success fw-bold">📡 60ms</span>  → Online
      <span class="text-danger  fw-bold">📡 ...</span>   → Offline
      (không có badge)                                   → Unknown/skip

Flow tối ưu (không cần gọi /check/):
  1. GET /country/{cc}/?page=N  → parse từng server card:
       - href  → server_id
       - badge text-success → is_online = True
     (song song toàn bộ trang)
  2. GET /servers/{id}/         → parse node URL từ <textarea data-config>
     CHỈ cho server is_online = True
  3. Ghi {cc}_sub.txt

Dependencies: aiohttp>=3.9, beautifulsoup4>=4.12, lxml>=5.0
"""

import asyncio
import re
import sys
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import aiohttp
from bs4 import BeautifulSoup

# ═══════════════════════════ Config ═══════════════════════════
BASE_URL        = "https://www.v2nodes.com"
COUNTRIES       = ["hk", "jp", "sg", "vn"]
CONCURRENCY     = 20       # request song song tối đa
RETRY_TIMES     = 2
CONNECT_TIMEOUT = 10
READ_TIMEOUT    = 20

_RE_TOTAL_PAGES = re.compile(r'\b\d+\s+of\s+(\d+)\b', re.IGNORECASE)
_RE_SERVER_HREF = re.compile(r'^/servers/(\d+)/$')

# Node URL fallback regex (nếu textarea không parse được)
_RE_NODE_URL = re.compile(
    r'(?:vmess|vless|trojan|ss|ssr)://[^\s<>"\']+',
    re.IGNORECASE,
)

_HEADERS = {
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


# ═══════════════════════════ Models ═══════════════════════════
@dataclass
class ServerEntry:
    server_id: str
    is_online: bool   # True nếu badge text-success xuất hiện trong card


# ═══════════════════════ Network helpers ══════════════════════
def _timeout() -> aiohttp.ClientTimeout:
    return aiohttp.ClientTimeout(connect=CONNECT_TIMEOUT, total=READ_TIMEOUT)


async def _get(
    session: aiohttp.ClientSession,
    url: str,
    *,
    retries: int = RETRY_TIMES,
) -> Optional[str]:
    """GET với exponential-backoff retry."""
    for attempt in range(retries + 1):
        try:
            async with session.get(url, headers=_HEADERS, timeout=_timeout()) as r:
                r.raise_for_status()
                return await r.text()
        except Exception as exc:
            if attempt == retries:
                log.debug("GET %s | fail x%d: %s", url, attempt + 1, exc)
                return None
            await asyncio.sleep(1.5 ** attempt)
    return None


# ═══════════════════════ Country page parser ══════════════════
def _parse_page(html: str) -> tuple[int, list[ServerEntry]]:
    """
    Parse một trang country. Trả về (total_pages, [ServerEntry]).

    Mỗi server card có dạng:
      <div class="card ...">
        <div class="card-header ...">
          <a href="/servers/{id}/">...</a>
        </div>
        <div class="card-body ...">
          <!-- Online:  --><span class="text-success fw-bold">📡 60ms</span>
          <!-- Offline: --><span class="text-danger  fw-bold">...</span>
        </div>
      </div>
    """
    soup = BeautifulSoup(html, "lxml")
    total_pages = 1
    m = _RE_TOTAL_PAGES.search(html)
    if m:
        total_pages = int(m.group(1))

    entries: list[ServerEntry] = []

    for card in soup.find_all("div", class_="card"):
        # Tìm link /servers/{id}/
        link = card.find("a", href=_RE_SERVER_HREF)
        if not link:
            continue
        m_id = _RE_SERVER_HREF.match(link["href"])
        if not m_id:
            continue
        server_id = m_id.group(1)

        # Online nếu có badge text-success (ping xanh lá)
        # Offline nếu chỉ có text-danger hoặc không có badge nào
        online_badge  = card.find("span", class_="text-success")
        is_online = online_badge is not None

        entries.append(ServerEntry(server_id=server_id, is_online=is_online))

    return total_pages, entries


async def _fetch_page(
    session: aiohttp.ClientSession,
    country: str,
    page: int,
    sem: asyncio.Semaphore,
) -> list[ServerEntry]:
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
        _, entries = _parse_page(html)
        return entries


async def collect_entries(
    session: aiohttp.ClientSession,
    country: str,
    sem: asyncio.Semaphore,
) -> list[ServerEntry]:
    """Fetch tất cả trang, gom ServerEntry."""
    # Trang 1 trước để biết total_pages
    html_p1 = await _get(session, f"{BASE_URL}/country/{country}/")
    if not html_p1:
        log.warning("[%s] Không tải được trang country", country.upper())
        return []

    total_pages, entries_p1 = _parse_page(html_p1)
    log.info(
        "[%s] %d trang | trang 1: %d servers (%d online)",
        country.upper(), total_pages, len(entries_p1),
        sum(1 for e in entries_p1 if e.is_online),
    )

    all_entries = list(entries_p1)

    if total_pages > 1:
        pages = await asyncio.gather(*[
            _fetch_page(session, country, p, sem)
            for p in range(2, total_pages + 1)
        ])
        for pg in pages:
            all_entries.extend(pg)

    # Dedup by server_id (giữ entry đầu tiên)
    seen: set[str] = set()
    deduped: list[ServerEntry] = []
    for e in all_entries:
        if e.server_id not in seen:
            seen.add(e.server_id)
            deduped.append(e)

    total   = len(deduped)
    online  = sum(1 for e in deduped if e.is_online)
    offline = total - online
    log.info(
        "[%s] Tổng: %d servers | ✔ %d online | ✘ %d offline (theo badge v2nodes)",
        country.upper(), total, online, offline,
    )
    return deduped


# ═══════════════════════ Node URL fetcher ═════════════════════
def _extract_node_url(html: str) -> Optional[str]:
    """
    Lấy node URL từ <textarea id="config" data-config="ss://...">.
    Fallback: regex trên toàn HTML.
    """
    soup = BeautifulSoup(html, "lxml")
    textarea = soup.find("textarea", {"id": "config"})
    if textarea:
        node = textarea.get("data-config") or textarea.get_text(strip=True)
        if node and "://" in node:
            return node.strip()

    m = _RE_NODE_URL.search(html)
    return m.group(0).strip() if m else None


async def fetch_node_url(
    session: aiohttp.ClientSession,
    server_id: str,
    sem: asyncio.Semaphore,
) -> Optional[str]:
    """GET trang server → trả về node URL, hoặc None nếu lỗi."""
    async with sem:
        html = await _get(session, f"{BASE_URL}/servers/{server_id}/")
        if not html:
            return None
        return _extract_node_url(html)


# ═══════════════════════ Per-country pipeline ═════════════════
async def process_country(
    session: aiohttp.ClientSession,
    country: str,
    sem: asyncio.Semaphore,
) -> None:
    log.info("━━━ [%s] Bắt đầu ━━━", country.upper())

    # Bước 1+2: Gom entries từ tất cả trang
    entries = await collect_entries(session, country, sem)
    if not entries:
        log.warning("[%s] Không có server nào.", country.upper())
        return

    # Chỉ fetch node URL cho server is_online = True
    online_entries = [e for e in entries if e.is_online]
    log.info("[%s] Fetching node URL cho %d server online...", country.upper(), len(online_entries))

    if not online_entries:
        log.warning("[%s] Không có server online.", country.upper())
        log.info("━━━ [%s] Xong ━━━\n", country.upper())
        return

    results = await asyncio.gather(
        *[fetch_node_url(session, e.server_id, sem) for e in online_entries],
        return_exceptions=True,
    )

    nodes = [r for r in results if isinstance(r, str) and r]
    log.info(
        "[%s] ✔ Thu được %d/%d node URL",
        country.upper(), len(nodes), len(online_entries),
    )

    if nodes:
        out = Path(f"{country}_sub.txt")
        out.write_text("\n".join(nodes), encoding="utf-8")
        log.info("[%s] Đã lưu → %s", country.upper(), out)
    else:
        log.warning("[%s] Không lấy được node URL nào.", country.upper())

    log.info("━━━ [%s] Xong ━━━\n", country.upper())


# ═════════════════════════ Main ═══════════════════════════════
async def main() -> None:
    log.info("┌──────────────────────────────────────────┐")
    log.info("│   FETCH LIVE VPN NODES — v2nodes.com     │")
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
