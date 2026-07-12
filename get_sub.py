"""
get_sub.py — Scrape + health-check VPN nodes from v2nodes.com
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Bài học từ các cách đã thử:
  ✘ /servers/{id}/check/  → Cloudflare block GitHub IPs
  ✘ TCP connect test      → False negative cao (CDN nodes, custom handshake)
  ✔ sing-box proxy test   → Test VPN protocol thực tế qua HTTP request

Architecture:
  BƯỚC 1 — SCRAPE (confirmed working):
    - Paginate /country/{cc}/ → server IDs
    - GET /servers/{id}/ → node URL từ data-config attribute

  BƯỚC 2 — HEALTH CHECK qua sing-box:
    - Parse node URL → sing-box outbound config
    - Batch 30 nodes/lần: generate config → start sing-box →
      HTTP test qua mỗi inbound port → kill sing-box
    - Test URL: http://connectivitycheck.gstatic.com/generate_204
      (Android connectivity check, trả về 204 nếu internet thông)

Dependencies: aiohttp>=3.9, beautifulsoup4>=4.12, lxml>=5.0
Requires:     sing-box binary tại SINGBOX_BIN (installed in workflow)
"""

import asyncio
import base64
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

import aiohttp
from bs4 import BeautifulSoup

# ═══════════════════════════ Config ═══════════════════════════
BASE_URL         = "https://www.v2nodes.com"
COUNTRIES        = ["hk", "jp", "sg", "vn"]
HTTP_CONCURRENCY = 10
HTTP_RETRY       = 3
CONNECT_TIMEOUT  = 12
READ_TIMEOUT     = 25

SINGBOX_BIN      = os.environ.get("SINGBOX_BIN", "/usr/local/bin/sing-box")
BATCH_SIZE       = 30        # nodes per sing-box instance
BASE_PORT        = 20000     # HTTP inbound ports: 20000, 20001, …
SINGBOX_STARTUP  = 3.0       # giây chờ sing-box khởi động
TEST_URL         = "http://connectivitycheck.gstatic.com/generate_204"
TEST_TIMEOUT     = 10        # giây timeout mỗi HTTP test
TEST_CONCURRENCY = 30        # HTTP tests song song trong 1 batch

_RE_TOTAL_PAGES = re.compile(r'\b\d+\s+of\s+(\d+)\b', re.IGNORECASE)
_RE_SERVER_HREF = re.compile(r'^/servers/(\d+)/$')
_RE_DATA_CONFIG = re.compile(r'data-config="([^"]+)"')
_RE_NODE        = re.compile(r'^(?:vmess|vless|trojan|ss|ssr)://.+', re.IGNORECASE)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ══════════════════════════ HTTP helpers ══════════════════════
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
                log.debug("GET %s fail x%d: %s", url, attempt + 1, exc)
                return None
            await asyncio.sleep(2.0 ** attempt)
    return None


# ═══════════════════════ Scraping ═════════════════════════════
def _server_ids(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    ids: list[str] = []
    for a in soup.find_all("a", href=_RE_SERVER_HREF):
        m = _RE_SERVER_HREF.match(a["href"])
        if m:
            ids.append(m.group(1))
    return list(dict.fromkeys(ids))


async def _fetch(session, url, sem):
    async with sem:
        return await _get(session, url)


async def _node_url(session, server_id, sem) -> Optional[str]:
    html = await _fetch(session, f"{BASE_URL}/servers/{server_id}/", sem)
    if not html:
        return None
    m = _RE_DATA_CONFIG.search(html)
    if m:
        v = m.group(1).strip()
        if _RE_NODE.match(v):
            return v
    soup = BeautifulSoup(html, "lxml")
    ta = soup.find("textarea", {"id": "config"})
    if ta:
        v = (ta.get("data-config") or ta.get_text()).strip()
        if _RE_NODE.match(v):
            return v
    return None


async def scrape_country(
    session: aiohttp.ClientSession,
    country: str,
    sem: asyncio.Semaphore,
) -> list[str]:
    """Paginate country pages → server IDs → node URLs."""
    html1 = await _fetch(session, f"{BASE_URL}/country/{country}/", sem)
    if not html1:
        return []

    total = 1
    m = _RE_TOTAL_PAGES.search(html1)
    if m:
        total = int(m.group(1))
    log.info("[%s] %d trang country", country.upper(), total)

    all_ids = _server_ids(html1)
    if total > 1:
        extra = await asyncio.gather(*[
            _fetch(session, f"{BASE_URL}/country/{country}/?page={p}", sem)
            for p in range(2, total + 1)
        ])
        for h in extra:
            if h:
                all_ids.extend(_server_ids(h))
    all_ids = list(dict.fromkeys(all_ids))
    log.info("[%s] %d server IDs", country.upper(), len(all_ids))

    nodes_raw = await asyncio.gather(*[
        _node_url(session, sid, sem) for sid in all_ids
    ])
    nodes = [r for r in nodes_raw if isinstance(r, str) and r]
    log.info("[%s] %d node URLs scraped", country.upper(), len(nodes))
    return nodes


# ═══════════════════════ Node URL parsers ═════════════════════
def _b64d(s: str) -> Optional[str]:
    try:
        return base64.b64decode(s + "=" * ((-len(s)) % 4)).decode("utf-8", errors="ignore")
    except Exception:
        return None


def _parse_ss(url: str, tag: str) -> Optional[dict]:
    try:
        raw = url.split("#")[0][5:]   # strip "ss://" and fragment
        if "@" in raw:
            ui, hostinfo = raw.rsplit("@", 1)
            # userinfo may be base64(method:pass) or plain method:pass
            dec = _b64d(ui)
            if dec and ":" in dec:
                method, password = dec.split(":", 1)
            elif ":" in ui:
                method, password = ui.split(":", 1)
            else:
                return None
        else:
            dec = _b64d(raw)
            if not dec or "@" not in dec:
                return None
            mp, hostinfo = dec.rsplit("@", 1)
            method, password = mp.split(":", 1)

        # hostinfo: host:port or [ipv6]:port
        if hostinfo.startswith("["):
            host = hostinfo[1:hostinfo.index("]")]
            port = int(hostinfo[hostinfo.index("]") + 2:])
        else:
            host, port_s = hostinfo.rsplit(":", 1)
            port = int(port_s)

        return {
            "type": "shadowsocks", "tag": tag,
            "server": host, "server_port": port,
            "method": method, "password": password,
        }
    except Exception:
        return None


def _parse_vmess(url: str, tag: str) -> Optional[dict]:
    try:
        raw  = url[8:].split("#")[0]
        cfg  = json.loads(_b64d(raw) or "")
        host = str(cfg.get("add", "")).strip()
        port = int(cfg.get("port", 0))
        if not host or not port:
            return None

        ob = {
            "type": "vmess", "tag": tag,
            "server": host, "server_port": port,
            "uuid": cfg.get("id", ""),
            "security": cfg.get("scy", cfg.get("security", "auto")),
            "alter_id": int(cfg.get("aid", 0)),
        }

        net  = cfg.get("net", "tcp")
        h    = cfg.get("host", "")
        path = cfg.get("path", "/") or "/"
        sni  = cfg.get("sni", h) or host
        tls  = cfg.get("tls", "")

        if net == "ws":
            ob["transport"] = {
                "type": "ws", "path": path,
                "headers": {"Host": h} if h else {},
            }
        elif net == "grpc":
            ob["transport"] = {"type": "grpc", "service_name": path}
        elif net in ("h2", "http"):
            ob["transport"] = {"type": "http", "host": [h] if h else [], "path": path}

        if tls:
            ob["tls"] = {"enabled": True, "server_name": sni, "insecure": True}

        return ob
    except Exception:
        return None


def _parse_vless(url: str, tag: str) -> Optional[dict]:
    try:
        parsed = urlparse(url)
        server, port, uuid = parsed.hostname, parsed.port, parsed.username
        if not server or not port or not uuid:
            return None

        p = parse_qs(parsed.query)
        def q(k): return p.get(k, [""])[0]

        net      = q("type") or "tcp"
        sni      = q("sni") or q("peer") or server
        host     = q("host")
        path     = q("path") or "/"
        security = q("security")
        fp       = q("fp")
        flow     = q("flow")

        ob = {
            "type": "vless", "tag": tag,
            "server": server, "server_port": port,
            "uuid": uuid,
        }
        if flow:
            ob["flow"] = flow

        if net == "ws":
            ob["transport"] = {
                "type": "ws", "path": path,
                "headers": {"Host": host} if host else {},
            }
        elif net == "grpc":
            ob["transport"] = {
                "type": "grpc",
                "service_name": q("serviceName") or path,
            }
        elif net in ("h2", "http"):
            ob["transport"] = {"type": "http", "host": [host] if host else [], "path": path}

        if security in ("tls", "reality", "xtls"):
            tls_cfg: dict = {
                "enabled": True,
                "server_name": sni,
                "insecure": True,
            }
            if security == "reality":
                tls_cfg["reality"] = {
                    "enabled": True,
                    "public_key": q("pbk"),
                    "short_id": q("sid"),
                }
            if fp:
                tls_cfg["utls"] = {"enabled": True, "fingerprint": fp}
            ob["tls"] = tls_cfg

        return ob
    except Exception:
        return None


def _parse_trojan(url: str, tag: str) -> Optional[dict]:
    try:
        parsed = urlparse(url)
        server, port, password = parsed.hostname, parsed.port, parsed.username
        if not server or not port:
            return None

        p = parse_qs(parsed.query)
        def q(k): return p.get(k, [""])[0]

        net  = q("type") or "tcp"
        sni  = q("sni") or q("peer") or server
        host = q("host")
        path = q("path") or "/"
        fp   = q("fp")

        ob: dict = {
            "type": "trojan", "tag": tag,
            "server": server, "server_port": port,
            "password": password or "",
            "tls": {"enabled": True, "server_name": sni, "insecure": True},
        }
        if fp:
            ob["tls"]["utls"] = {"enabled": True, "fingerprint": fp}

        if net == "ws":
            ob["transport"] = {
                "type": "ws", "path": path,
                "headers": {"Host": host} if host else {},
            }
        elif net == "grpc":
            ob["transport"] = {
                "type": "grpc",
                "service_name": q("serviceName") or path,
            }

        return ob
    except Exception:
        return None


def parse_node(url: str, tag: str) -> Optional[dict]:
    """Dispatch node URL → sing-box outbound config."""
    scheme = url.split("://")[0].lower()
    if scheme == "vmess":
        return _parse_vmess(url, tag)
    if scheme == "vless":
        return _parse_vless(url, tag)
    if scheme == "trojan":
        return _parse_trojan(url, tag)
    if scheme == "ss":
        return _parse_ss(url, tag)
    return None   # ssr not supported by sing-box natively, skip


# ═══════════════════════ sing-box testing ═════════════════════
def _singbox_config(batch: list[tuple[str, dict, int]]) -> dict:
    """
    Generate sing-box config for a batch.
    Each node → 1 HTTP inbound (127.0.0.1:port) → 1 outbound.
    Routing: inbound tag → matching outbound tag.
    """
    inbounds  = []
    outbounds = []
    rules     = []

    for _url, ob, port in batch:
        in_tag = f"in_{ob['tag']}"
        inbounds.append({
            "type": "http",
            "tag": in_tag,
            "listen": "127.0.0.1",
            "listen_port": port,
        })
        outbounds.append(ob)
        rules.append({"inbound": [in_tag], "outbound": ob["tag"]})

    return {
        "log": {"disabled": True},
        "inbounds": inbounds,
        "outbounds": outbounds + [{"type": "block", "tag": "block"}],
        "route": {"rules": rules, "final": "block"},
    }


async def _http_test(port: int, sem: asyncio.Semaphore) -> bool:
    """HTTP GET qua proxy 127.0.0.1:port → True nếu trả về 200/204."""
    async with sem:
        try:
            conn = aiohttp.TCPConnector(ssl=False)
            timeout = aiohttp.ClientTimeout(total=TEST_TIMEOUT)
            async with aiohttp.ClientSession(connector=conn) as s:
                async with s.get(
                    TEST_URL,
                    proxy=f"http://127.0.0.1:{port}",
                    timeout=timeout,
                    allow_redirects=True,
                ) as r:
                    return r.status in (200, 204)
        except Exception:
            return False


async def test_batch(
    batch: list[tuple[str, dict, int]],
    sem: asyncio.Semaphore,
) -> list[bool]:
    """
    Chạy sing-box cho 1 batch, test song song, trả về list[bool].
    """
    config  = _singbox_config(batch)
    tmp     = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    )
    try:
        json.dump(config, tmp)
        tmp.close()

        proc = subprocess.Popen(
            [SINGBOX_BIN, "run", "-c", tmp.name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        await asyncio.sleep(SINGBOX_STARTUP)

        results = await asyncio.gather(*[
            _http_test(port, sem) for _, _, port in batch
        ])

        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()

        return list(results)

    finally:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass


async def health_check(nodes: list[str]) -> list[str]:
    """
    Chia nodes thành batches → test qua sing-box → trả về nodes online.
    Nếu sing-box không có → warning + trả về toàn bộ nodes.
    """
    if not Path(SINGBOX_BIN).exists():
        log.warning(
            "sing-box không tìm thấy tại %s — bỏ qua health check, "
            "giữ toàn bộ %d nodes.", SINGBOX_BIN, len(nodes)
        )
        return nodes

    # Parse node URLs → sing-box configs
    parsed: list[tuple[str, dict, int]] = []
    skipped = 0
    for i, url in enumerate(nodes):
        ob = parse_node(url, f"n{i}")
        if ob:
            parsed.append((url, ob, BASE_PORT + i))
        else:
            skipped += 1

    if skipped:
        log.info("Bỏ qua %d nodes không parse được (ssr hoặc format lạ)", skipped)

    if not parsed:
        return []

    log.info("Health-check %d nodes qua sing-box (batch=%d)...", len(parsed), BATCH_SIZE)

    sem     = asyncio.Semaphore(TEST_CONCURRENCY)
    online  : list[str] = []
    batches = [parsed[i:i + BATCH_SIZE] for i in range(0, len(parsed), BATCH_SIZE)]

    for idx, batch in enumerate(batches, 1):
        log.info("  Batch %d/%d (%d nodes)...", idx, len(batches), len(batch))
        t0      = time.monotonic()
        results = await test_batch(batch, sem)
        elapsed = time.monotonic() - t0

        ok  = sum(results)
        log.info(
            "  Batch %d: %d/%d online (%.1fs)",
            idx, ok, len(batch), elapsed,
        )
        online.extend(url for (url, _, _), alive in zip(batch, results) if alive)

    return online


# ═══════════════════════ Per-country pipeline ═════════════════
async def process_country(
    session: aiohttp.ClientSession,
    country: str,
    sem: asyncio.Semaphore,
) -> None:
    log.info("━━━ [%s] Bắt đầu ━━━", country.upper())

    nodes = await scrape_country(session, country, sem)
    if not nodes:
        log.warning("[%s] Không scrape được node nào.", country.upper())
        log.info("━━━ [%s] Xong ━━━\n", country.upper())
        return

    live = await health_check(nodes)

    total  = len(nodes)
    online = len(live)
    log.info(
        "[%s] Kết quả: ✔ %d/%d online | ✘ %d offline",
        country.upper(), online, total, total - online,
    )

    if live:
        out = Path(f"{country}_sub.txt")
        out.write_text("\n".join(live), encoding="utf-8")
        log.info("[%s] Lưu %d nodes → %s", country.upper(), online, out)
    else:
        log.warning("[%s] Không có node online.", country.upper())

    log.info("━━━ [%s] Xong ━━━\n", country.upper())


# ═════════════════════════ Main ═══════════════════════════════
async def main() -> None:
    log.info("┌──────────────────────────────────────────────────┐")
    log.info("│  SCRAPE + SINGBOX HEALTH-CHECK — v2nodes.com     │")
    log.info("│  Test: HTTP route qua VPN node thực tế          │")
    log.info("└──────────────────────────────────────────────────┘")

    sem = asyncio.Semaphore(HTTP_CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=30, ssl=False, ttl_dns_cache=300)

    async with aiohttp.ClientSession(
        connector=connector,
        cookie_jar=aiohttp.CookieJar(unsafe=True),
    ) as session:
        for country in COUNTRIES:
            await process_country(session, country, sem)

    log.info("┌──────────────────────────────────────────────────┐")
    log.info("│                    HOÀN TẤT                     │")
    log.info("└──────────────────────────────────────────────────┘")


if __name__ == "__main__":
    asyncio.run(main())
