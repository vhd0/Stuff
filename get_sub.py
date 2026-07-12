"""
get_sub.py — Fetch & filter live VPN nodes from v2nodes.com
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Vấn đề với các phương pháp trước:
  - /servers/{id}/check/ → Cloudflare block GitHub Actions IPs
  - Badge text-success trên country page → không phân biệt được online/offline
    (v2nodes chỉ list server đang online nên gần như mọi badge đều xanh)

Giải pháp tối ưu — 2 bước độc lập:

  BƯỚC 1 — GET NODES (2 request/country, không per-server):
    a. GET /country/{cc}/  → tìm subscription URL có ?key=...
    b. GET subscription URL → base64-decode → danh sách node URLs

  BƯỚC 2 — CHECK CONNECTIVITY (không cần HTTP đến v2nodes):
    - Parse host:port từ mỗi node URL (vmess/vless/trojan/ss/ssr)
    - asyncio TCP connect thực tế đến host:port
    - Online = TCP handshake thành công trong timeout
    - Offline = connection refused / timeout

  Lợi ích:
    ✔ Bypass hoàn toàn Cloudflare (check step không gọi v2nodes)
    ✔ Check thực tế (TCP đến server thật, không phụ thuộc v2nodes API)
    ✔ Rất nhanh (50 TCP test song song, mỗi cái chỉ 3-5 giây)
    ✔ Chỉ 2 HTTP request đến v2nodes mỗi country

Dependencies: aiohttp>=3.9, beautifulsoup4>=4.12, lxml>=5.0
"""

import asyncio
import base64
import json
import re
import sys
import logging
from pathlib import Path
from urllib.parse import urlparse
from typing import Optional

import aiohttp

# ═══════════════════════════ Config ═══════════════════════════
BASE_URL         = "https://www.v2nodes.com"
COUNTRIES        = ["hk", "jp", "sg", "vn"]
HTTP_CONCURRENCY = 8    # request HTTP song song (lấy sub + page)
TCP_CONCURRENCY  = 50   # TCP test song song (nhẹ, không phụ thuộc server)
TCP_TIMEOUT      = 5    # giây cho mỗi TCP connect test
HTTP_RETRY       = 2
CONNECT_TIMEOUT  = 10
READ_TIMEOUT     = 20

_RE_TOTAL_PAGES = re.compile(r'\b\d+\s+of\s+(\d+)\b', re.IGNORECASE)
_RE_SERVER_ID   = re.compile(r'/servers/(\d+)/')

# Subscription URL patterns (absolute và relative)
_RE_SUB_ABS = re.compile(
    r'https://www\.v2nodes\.com/subscriptions/country/\w+/\?key=[A-Za-z0-9]+'
)
_RE_SUB_REL = re.compile(
    r'/subscriptions/country/\w+/\?key=[A-Za-z0-9]+'
)

# Node URL từ server page (fallback)
_RE_NODE = re.compile(
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


# ═══════════════════════ Network helpers ══════════════════════
def _http_timeout() -> aiohttp.ClientTimeout:
    return aiohttp.ClientTimeout(connect=CONNECT_TIMEOUT, total=READ_TIMEOUT)


async def _get(
    session: aiohttp.ClientSession,
    url: str,
    *,
    retries: int = HTTP_RETRY,
) -> Optional[str]:
    for attempt in range(retries + 1):
        try:
            async with session.get(url, headers=_HEADERS, timeout=_http_timeout()) as r:
                r.raise_for_status()
                return await r.text()
        except Exception as exc:
            if attempt == retries:
                log.debug("GET %s | fail x%d: %s", url, attempt + 1, exc)
                return None
            await asyncio.sleep(1.5 ** attempt)
    return None


# ═══════════════════════ Node URL parsing ═════════════════════
def _decode_b64(s: str) -> Optional[str]:
    """Base64 decode với auto-padding."""
    try:
        pad = (-len(s)) % 4
        return base64.b64decode(s + "=" * pad).decode("utf-8", errors="ignore")
    except Exception:
        return None


def parse_host_port(node_url: str) -> Optional[tuple[str, int]]:
    """
    Trích xuất (host, port) từ node URL.
    Hỗ trợ: vmess, vless, trojan, ss, ssr
    """
    try:
        scheme = node_url.split("://")[0].lower()

        # ── vmess://BASE64JSON ────────────────────────────────
        if scheme == "vmess":
            raw = node_url[8:].split("#")[0].strip()
            decoded = _decode_b64(raw)
            if not decoded:
                return None
            cfg = json.loads(decoded)
            host = str(cfg.get("add", "")).strip()
            port = int(cfg.get("port", 0))
            return (host, port) if host and port else None

        # ── vless, trojan — standard URL format ──────────────
        if scheme in ("vless", "trojan"):
            parsed = urlparse(node_url)
            host = parsed.hostname or ""
            port = parsed.port or 0
            return (host, port) if host and port else None

        # ── ss — hai format: @-style và pure base64 ──────────
        if scheme == "ss":
            raw = node_url[5:].split("#")[0]
            if "@" in raw:
                # ss://BASE64(method:pass)@host:port  OR
                # ss://method:pass@host:port  (SIP002)
                parsed = urlparse("ss://" + raw)
                host = parsed.hostname or ""
                port = parsed.port or 0
                return (host, port) if host and port else None
            else:
                # ss://BASE64(method:pass@host:port)
                decoded = _decode_b64(raw)
                if not decoded or "@" not in decoded:
                    return None
                addr = decoded.split("@", 1)[1]
                host, port_s = addr.rsplit(":", 1)
                return (host.strip("[]"), int(port_s))

        # ── ssr://BASE64 ──────────────────────────────────────
        if scheme == "ssr":
            raw = node_url[6:]
            decoded = _decode_b64(raw)
            if not decoded:
                return None
            # host:port:protocol:method:obfs:BASE64(pass)/...
            parts = decoded.split(":")
            host = parts[0]
            port = int(parts[1])
            return (host, port) if host and port else None

    except Exception as exc:
        log.debug("parse_host_port %r → %s", node_url[:60], exc)
    return None


# ═════════════════════ TCP connectivity test ══════════════════
async def tcp_is_alive(
    host: str,
    port: int,
    sem: asyncio.Semaphore,
    *,
    timeout: float = TCP_TIMEOUT,
) -> bool:
    """
    Thử mở TCP connection đến host:port.
    True = server đang lắng nghe (Online).
    False = refused / timeout / DNS fail (Offline).
    """
    async with sem:
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout,
            )
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return True
        except Exception:
            return False


# ═══════════════════ Subscription URL strategy ════════════════
def _find_sub_url(html: str) -> Optional[str]:
    """Tìm subscription URL trong HTML trang country."""
    m = _RE_SUB_ABS.search(html)
    if m:
        return m.group(0)
    m = _RE_SUB_REL.search(html)
    if m:
        return BASE_URL + m.group(0)
    return None


def _decode_subscription(raw: str) -> list[str]:
    """
    Subscription content: thường là base64 của các node URLs, mỗi dòng 1 URL.
    Nếu không phải base64 thì xử lý trực tiếp.
    """
    stripped = raw.strip()

    # Thử base64 decode
    decoded = _decode_b64(stripped)
    if decoded:
        lines = [l.strip() for l in decoded.splitlines() if "://" in l]
        if lines:
            return lines

    # Không phải base64 → dùng trực tiếp
    return [l.strip() for l in stripped.splitlines() if "://" in l]


async def get_nodes_via_subscription(
    session: aiohttp.ClientSession,
    country: str,
    http_sem: asyncio.Semaphore,
) -> list[str]:
    """
    Lấy tất cả node URL của country qua subscription endpoint.
    Trả về list node URLs, rỗng nếu thất bại.
    """
    async with http_sem:
        html = await _get(session, f"{BASE_URL}/country/{country}/")
    if not html:
        return []

    sub_url = _find_sub_url(html)
    if not sub_url:
        log.warning("[%s] Không tìm thấy subscription URL", country.upper())
        return []

    log.info("[%s] Sub URL: %s", country.upper(), sub_url)

    async with http_sem:
        raw = await _get(session, sub_url)
    if not raw:
        log.warning("[%s] Không tải được subscription content", country.upper())
        return []

    nodes = _decode_subscription(raw)
    log.info("[%s] Subscription trả về %d nodes", country.upper(), len(nodes))
    return nodes


# ════════════════════ Fallback: scrape per-server ═════════════
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
        return _RE_SERVER_ID.findall(html) if html else []


async def _fetch_node_from_server_page(
    session: aiohttp.ClientSession,
    server_id: str,
    sem: asyncio.Semaphore,
) -> Optional[str]:
    async with sem:
        html = await _get(session, f"{BASE_URL}/servers/{server_id}/")
    if not html:
        return None

    # Ưu tiên data-config attribute của textarea
    m = re.search(r'data-config="([^"]+)"', html)
    if m:
        val = m.group(1)
        if "://" in val:
            return val.strip()

    # Fallback regex
    n = _RE_NODE.search(html)
    return n.group(0).strip() if n else None


async def get_nodes_via_scraping(
    session: aiohttp.ClientSession,
    country: str,
    http_sem: asyncio.Semaphore,
) -> list[str]:
    """
    Fallback khi subscription URL không có:
    Scrape từng trang country → IDs → từng server page → node URL
    """
    log.info("[%s] Dùng fallback scraping...", country.upper())

    html_p1 = await _get(session, f"{BASE_URL}/country/{country}/")
    if not html_p1:
        return []

    total = 1
    m = _RE_TOTAL_PAGES.search(html_p1)
    if m:
        total = int(m.group(1))

    ids_p1 = list(dict.fromkeys(_RE_SERVER_ID.findall(html_p1)))
    extra_ids: list[str] = []

    if total > 1:
        pages = await asyncio.gather(*[
            _fetch_page_ids(session, country, p, http_sem)
            for p in range(2, total + 1)
        ])
        for pg in pages:
            extra_ids.extend(pg)

    all_ids = list(dict.fromkeys(ids_p1 + extra_ids))
    log.info("[%s] Fallback: %d server IDs tổng cộng", country.upper(), len(all_ids))

    results = await asyncio.gather(*[
        _fetch_node_from_server_page(session, sid, http_sem)
        for sid in all_ids
    ])
    return [r for r in results if isinstance(r, str) and r]


# ═══════════════════════ Per-country pipeline ═════════════════
async def process_country(
    session: aiohttp.ClientSession,
    country: str,
    http_sem: asyncio.Semaphore,
    tcp_sem: asyncio.Semaphore,
) -> None:
    log.info("━━━ [%s] Bắt đầu ━━━", country.upper())

    # ── Bước 1: Lấy tất cả node URLs ─────────────────────────
    nodes = await get_nodes_via_subscription(session, country, http_sem)

    if not nodes:
        nodes = await get_nodes_via_scraping(session, country, http_sem)

    if not nodes:
        log.warning("[%s] Không lấy được node nào.", country.upper())
        log.info("━━━ [%s] Xong ━━━\n", country.upper())
        return

    log.info("[%s] Tổng nodes cần check: %d", country.upper(), len(nodes))

    # ── Bước 2: Parse host:port + TCP test song song ──────────
    host_ports = [(n, parse_host_port(n)) for n in nodes]
    parseable  = [(n, hp) for n, hp in host_ports if hp is not None]
    unparsed   = len(nodes) - len(parseable)

    if unparsed:
        log.info("[%s] Bỏ qua %d node không parse được host:port", country.upper(), unparsed)

    if not parseable:
        log.warning("[%s] Không có node nào parse được.", country.upper())
        log.info("━━━ [%s] Xong ━━━\n", country.upper())
        return

    log.info("[%s] TCP testing %d nodes (timeout=%ds)...", country.upper(), len(parseable), TCP_TIMEOUT)

    tcp_results = await asyncio.gather(*[
        tcp_is_alive(hp[0], hp[1], tcp_sem)
        for _, hp in parseable
    ])

    online_nodes = [
        node for (node, _), alive in zip(parseable, tcp_results)
        if alive
    ]

    total   = len(parseable)
    online  = len(online_nodes)
    offline = total - online

    log.info(
        "[%s] ✔ %d online  |  ✘ %d offline  (TCP test)",
        country.upper(), online, offline,
    )

    if online_nodes:
        out = Path(f"{country}_sub.txt")
        out.write_text("\n".join(online_nodes), encoding="utf-8")
        log.info("[%s] Đã lưu %d node → %s", country.upper(), online, out)
    else:
        log.warning("[%s] Không có node online.", country.upper())

    log.info("━━━ [%s] Xong ━━━\n", country.upper())


# ═════════════════════════ Main ═══════════════════════════════
async def main() -> None:
    log.info("┌──────────────────────────────────────────────┐")
    log.info("│   FETCH + TCP-CHECK VPN NODES  v2nodes.com  │")
    log.info("└──────────────────────────────────────────────┘")

    http_sem = asyncio.Semaphore(HTTP_CONCURRENCY)
    tcp_sem  = asyncio.Semaphore(TCP_CONCURRENCY)

    connector = aiohttp.TCPConnector(
        limit=30,
        ssl=False,
        ttl_dns_cache=300,
    )

    async with aiohttp.ClientSession(
        connector=connector,
        cookie_jar=aiohttp.CookieJar(unsafe=True),
    ) as session:
        for country in COUNTRIES:
            await process_country(session, country, http_sem, tcp_sem)

    log.info("┌──────────────────────────────────────────────┐")
    log.info("│                 HOÀN TẤT                    │")
    log.info("└──────────────────────────────────────────────┘")


if __name__ == "__main__":
    asyncio.run(main())
