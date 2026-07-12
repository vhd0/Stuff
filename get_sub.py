"""
get_sub.py — Scrape → Dedup → GeoIP filter → sing-box test → Rename
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Pipeline mỗi country:
  1. Scrape  : paginate /country/{cc}/ → server IDs → node URLs (data-config)
  2. Dedup   : loại node trùng (scheme, host, port)
  3. GeoIP   : DNS resolve host → maxminddb lookup → giữ đúng quốc gia
               CDN IPs (Cloudflare/Fastly) luôn giữ dù GeoIP ≠ country
  4. sing-box: batch test HTTP qua VPN protocol thực tế
  5. Rename  : strip label cũ → "JP | vmess | 01"

Dependencies: aiohttp, beautifulsoup4, lxml, maxminddb
External:     sing-box binary, GeoLite2-Country.mmdb
"""

import asyncio
import base64
import json
import logging
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

import aiohttp
import maxminddb
from bs4 import BeautifulSoup

# ═══════════════════════════════ Config ═══════════════════════════════
BASE_URL         = "https://www.v2nodes.com"
COUNTRIES        = ["hk", "jp", "sg", "vn"]

COUNTRY_CC   = {"hk": "HK",  "jp": "JP",  "sg": "SG",  "vn": "VN"}
COUNTRY_FLAG = {"hk": "🇭🇰", "jp": "🇯🇵", "sg": "🇸🇬", "vn": "🇻🇳"}

GEOIP_DB         = os.environ.get("GEOIP_DB", "GeoLite2-Country.mmdb")
DNS_CONCURRENCY  = 60
DNS_TIMEOUT      = 5.0

HTTP_CONCURRENCY = 10
HTTP_RETRY       = 3
CONNECT_TIMEOUT  = 12
READ_TIMEOUT     = 25

SINGBOX_BIN      = os.environ.get("SINGBOX_BIN", "/usr/local/bin/sing-box")
BATCH_SIZE       = 30
BASE_PORT        = 20000
SINGBOX_STARTUP  = 3.0
TEST_URL         = "http://connectivitycheck.gstatic.com/generate_204"
TEST_TIMEOUT     = 10
TEST_CONCURRENCY = 30

# CDN prefixes → giữ node bất kể GeoIP (Cloudflare/Fastly/Akamai có thể
# front cho bất kỳ country nào nhưng VPN vẫn kết nối được)
_CDN_PREFIXES = (
    "104.16.", "104.17.", "104.18.", "104.19.", "104.20.", "104.21.",
    "104.22.", "104.23.", "104.24.", "104.25.", "104.26.", "104.27.",
    "108.162.", "141.101.", "162.158.",
    "172.64.",  "172.65.",  "172.66.",  "172.67.",
    "173.245.", "188.114.", "190.93.",  "197.234.", "198.41.",
    "151.101.", "199.232.",             # Fastly
    "23.32.",   "23.64.",   "23.192.",  "23.200.",  # Akamai
)

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


# ═══════════════════════ Helpers ══════════════════════════════════════
def _b64d(s: str) -> Optional[str]:
    try:
        return base64.b64decode(s + "=" * ((-len(s)) % 4)).decode("utf-8", errors="ignore")
    except Exception:
        return None


def _http_timeout() -> aiohttp.ClientTimeout:
    return aiohttp.ClientTimeout(connect=CONNECT_TIMEOUT, total=READ_TIMEOUT)


async def _get(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    for attempt in range(HTTP_RETRY + 1):
        try:
            async with session.get(url, headers=_HEADERS, timeout=_http_timeout()) as r:
                r.raise_for_status()
                return await r.text()
        except Exception as exc:
            if attempt == HTTP_RETRY:
                log.debug("GET %s fail x%d: %s", url, attempt + 1, exc)
                return None
            await asyncio.sleep(2.0 ** attempt)
    return None


# ═══════════════════════ Node endpoint parsing ════════════════════════
def parse_endpoint(url: str) -> Optional[tuple[str, str, int]]:
    """
    Trả về (scheme, host, port) từ bất kỳ node URL nào.
    Dùng cho cả dedup key lẫn GeoIP host extraction.
    """
    try:
        clean  = url.split("#")[0].strip()
        scheme = clean.split("://")[0].lower()

        if scheme == "vmess":
            cfg  = json.loads(_b64d(clean[8:]) or "{}")
            host = str(cfg.get("add", "")).strip()
            port = int(cfg.get("port", 0))
            return (scheme, host, port) if host and port else None

        if scheme in ("vless", "trojan"):
            p = urlparse(clean)
            return (scheme, p.hostname or "", p.port or 0) if p.hostname and p.port else None

        if scheme == "ss":
            rest = clean[5:]
            if "@" in rest:
                hostinfo = rest.rsplit("@", 1)[1]
            else:
                dec = _b64d(rest)
                if not dec or "@" not in dec:
                    return None
                hostinfo = dec.rsplit("@", 1)[1]
            if hostinfo.startswith("["):
                host = hostinfo[1:hostinfo.index("]")]
                port = int(hostinfo[hostinfo.index("]") + 2:])
            else:
                host, ps = hostinfo.rsplit(":", 1)
                port = int(ps)
            return (scheme, host, port)

        if scheme == "ssr":
            dec = _b64d(clean[6:])
            if dec:
                parts = dec.split(":")
                return (scheme, parts[0], int(parts[1]))

    except Exception:
        pass
    return None


# ═══════════════════════ 1. Scraping ══════════════════════════════════
def _parse_server_ids(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    ids: list[str] = []
    for a in soup.find_all("a", href=_RE_SERVER_HREF):
        m = _RE_SERVER_HREF.match(a["href"])
        if m:
            ids.append(m.group(1))
    return list(dict.fromkeys(ids))


async def _node_from_page(
    session: aiohttp.ClientSession,
    server_id: str,
    sem: asyncio.Semaphore,
) -> Optional[str]:
    async with sem:
        html = await _get(session, f"{BASE_URL}/servers/{server_id}/")
    if not html:
        return None
    m = _RE_DATA_CONFIG.search(html)
    if m:
        v = m.group(1).strip()
        if _RE_NODE.match(v):
            return v
    soup = BeautifulSoup(html, "lxml")
    ta   = soup.find("textarea", {"id": "config"})
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
    """Scrape tất cả trang → server IDs → node URLs."""
    async with sem:
        html1 = await _get(session, f"{BASE_URL}/country/{country}/")
    if not html1:
        return []

    total = 1
    m = _RE_TOTAL_PAGES.search(html1)
    if m:
        total = int(m.group(1))
    log.info("[%s] %d trang country", country.upper(), total)

    all_ids = _parse_server_ids(html1)
    if total > 1:
        extra_htmls = await asyncio.gather(*[
            (lambda p: (asyncio.create_task(_get(session, f"{BASE_URL}/country/{country}/?page={p}"))))(p)
            for p in range(2, total + 1)
        ])
        for h in extra_htmls:
            if h:
                all_ids.extend(_parse_server_ids(h))
    all_ids = list(dict.fromkeys(all_ids))
    log.info("[%s] %d server IDs", country.upper(), len(all_ids))

    raw = await asyncio.gather(*[_node_from_page(session, sid, sem) for sid in all_ids])
    nodes = [r for r in raw if isinstance(r, str) and r]
    log.info("[%s] %d nodes scraped", country.upper(), len(nodes))
    return nodes


# ═══════════════════════ 2. Deduplication ════════════════════════════
def deduplicate(nodes: list[str]) -> list[str]:
    """Loại node trùng cùng (scheme, host, port). Giữ lần xuất hiện đầu."""
    seen:   set[tuple]  = set()
    unique: list[str]   = []
    dups = 0
    for url in nodes:
        ep = parse_endpoint(url)
        if ep is None:
            unique.append(url)   # không parse được → giữ để không mất node
            continue
        if ep in seen:
            dups += 1
            continue
        seen.add(ep)
        unique.append(url)
    if dups:
        log.info("  Dedup: loại %d node trùng", dups)
    return unique


# ═══════════════════════ 3. GeoIP filtering ═══════════════════════════
def _is_cdn(ip: str) -> bool:
    return ip.startswith(_CDN_PREFIXES)


async def _resolve(hostname: str) -> Optional[str]:
    """Resolve hostname → IP (IPv4 ưu tiên). Trả về None nếu DNS fail."""
    # Đã là IP?
    for family in (socket.AF_INET, socket.AF_INET6):
        try:
            socket.inet_pton(family, hostname)
            return hostname
        except OSError:
            pass
    try:
        loop    = asyncio.get_event_loop()
        infos   = await asyncio.wait_for(
            loop.getaddrinfo(hostname, None, type=socket.SOCK_STREAM),
            timeout=DNS_TIMEOUT,
        )
        # Ưu tiên IPv4
        for info in infos:
            if info[0] == socket.AF_INET:
                return info[4][0]
        return infos[0][4][0] if infos else None
    except Exception:
        return None


async def geoip_filter(
    nodes: list[str],
    expected_cc: str,
    reader: maxminddb.Reader,
) -> list[str]:
    """
    Giữ node nếu:
      - IP resolve được VÀ GeoIP == expected_cc
      - HOẶC IP là CDN (Cloudflare/Fastly/Akamai) → front cho mọi country
    Loại bỏ:
      - DNS fail (node không resolv được)
      - GeoIP sai country VÀ không phải CDN
    """
    sem = asyncio.Semaphore(DNS_CONCURRENCY)

    async def _check(url: str) -> Optional[str]:
        ep = parse_endpoint(url)
        if not ep:
            return url          # không parse → giữ (safe default)
        _, host, _ = ep

        async with sem:
            ip = await _resolve(host)

        if ip is None:
            log.debug("DNS fail: %s → loại", host)
            return None

        if _is_cdn(ip):
            log.debug("CDN %s (%s) → giữ", ip, host)
            return url

        try:
            record = reader.get(ip) or {}
            cc     = record.get("country", {}).get("iso_code", "")
        except Exception:
            return url          # GeoIP lỗi → giữ (safe default)

        if cc == expected_cc:
            return url
        log.debug("GeoIP %s → %s ≠ %s → loại", ip, cc, expected_cc)
        return None

    results  = await asyncio.gather(*[_check(n) for n in nodes])
    filtered = [r for r in results if r is not None]
    return filtered


# ═══════════════════════ 4. sing-box health check ═════════════════════
def _parse_ss(url: str, tag: str) -> Optional[dict]:
    try:
        rest = url.split("#")[0][5:]
        if "@" in rest:
            ui, hostinfo = rest.rsplit("@", 1)
            dec = _b64d(ui)
            if dec and ":" in dec:
                method, password = dec.split(":", 1)
            elif ":" in ui:
                method, password = ui.split(":", 1)
            else:
                return None
        else:
            dec = _b64d(rest)
            if not dec or "@" not in dec:
                return None
            mp, hostinfo = dec.rsplit("@", 1)
            method, password = mp.split(":", 1)

        if hostinfo.startswith("["):
            host = hostinfo[1:hostinfo.index("]")]
            port = int(hostinfo[hostinfo.index("]") + 2:])
        else:
            host, ps = hostinfo.rsplit(":", 1)
            port = int(ps)
        return {"type": "shadowsocks", "tag": tag,
                "server": host, "server_port": port,
                "method": method, "password": password}
    except Exception:
        return None


def _parse_vmess(url: str, tag: str) -> Optional[dict]:
    try:
        cfg  = json.loads(_b64d(url[8:].split("#")[0]) or "{}")
        host = str(cfg.get("add", "")).strip()
        port = int(cfg.get("port", 0))
        if not host or not port:
            return None
        ob: dict = {"type": "vmess", "tag": tag,
                    "server": host, "server_port": port,
                    "uuid": cfg.get("id", ""),
                    "security": cfg.get("scy", cfg.get("security", "auto")),
                    "alter_id": int(cfg.get("aid", 0))}
        net  = cfg.get("net", "tcp")
        h    = cfg.get("host", "")
        path = cfg.get("path", "/") or "/"
        sni  = cfg.get("sni", h) or host
        if net == "ws":
            ob["transport"] = {"type": "ws", "path": path,
                               "headers": {"Host": h} if h else {}}
        elif net == "grpc":
            ob["transport"] = {"type": "grpc", "service_name": path}
        elif net in ("h2", "http"):
            ob["transport"] = {"type": "http",
                               "host": [h] if h else [], "path": path}
        if cfg.get("tls"):
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
        net, sni  = q("type") or "tcp", q("sni") or q("peer") or server
        host, path = q("host"), q("path") or "/"
        security, fp, flow = q("security"), q("fp"), q("flow")
        ob: dict = {"type": "vless", "tag": tag,
                    "server": server, "server_port": port, "uuid": uuid}
        if flow:
            ob["flow"] = flow
        if net == "ws":
            ob["transport"] = {"type": "ws", "path": path,
                               "headers": {"Host": host} if host else {}}
        elif net == "grpc":
            ob["transport"] = {"type": "grpc",
                               "service_name": q("serviceName") or path}
        elif net in ("h2", "http"):
            ob["transport"] = {"type": "http",
                               "host": [host] if host else [], "path": path}
        if security in ("tls", "reality", "xtls"):
            tls: dict = {"enabled": True, "server_name": sni, "insecure": True}
            if security == "reality":
                tls["reality"] = {"enabled": True,
                                  "public_key": q("pbk"), "short_id": q("sid")}
            if fp:
                tls["utls"] = {"enabled": True, "fingerprint": fp}
            ob["tls"] = tls
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
        net, sni  = q("type") or "tcp", q("sni") or q("peer") or server
        host, path = q("host"), q("path") or "/"
        fp = q("fp")
        ob: dict = {"type": "trojan", "tag": tag,
                    "server": server, "server_port": port,
                    "password": password or "",
                    "tls": {"enabled": True, "server_name": sni, "insecure": True}}
        if fp:
            ob["tls"]["utls"] = {"enabled": True, "fingerprint": fp}
        if net == "ws":
            ob["transport"] = {"type": "ws", "path": path,
                               "headers": {"Host": host} if host else {}}
        elif net == "grpc":
            ob["transport"] = {"type": "grpc",
                               "service_name": q("serviceName") or path}
        return ob
    except Exception:
        return None


def _to_singbox(url: str, tag: str) -> Optional[dict]:
    scheme = url.split("://")[0].lower()
    if scheme == "vmess":   return _parse_vmess(url, tag)
    if scheme == "vless":   return _parse_vless(url, tag)
    if scheme == "trojan":  return _parse_trojan(url, tag)
    if scheme == "ss":      return _parse_ss(url, tag)
    return None   # ssr: not natively supported by sing-box


def _build_singbox_cfg(batch: list[tuple[str, dict, int]]) -> dict:
    inbounds, outbounds, rules = [], [], []
    for _, ob, port in batch:
        in_tag = f"in_{ob['tag']}"
        inbounds.append({"type": "http", "tag": in_tag,
                         "listen": "127.0.0.1", "listen_port": port})
        outbounds.append(ob)
        rules.append({"inbound": [in_tag], "outbound": ob["tag"]})
    return {
        "log": {"disabled": True},
        "inbounds": inbounds,
        "outbounds": outbounds + [{"type": "block", "tag": "block"}],
        "route": {"rules": rules, "final": "block"},
    }


async def _http_test(port: int, sem: asyncio.Semaphore) -> bool:
    async with sem:
        try:
            conn    = aiohttp.TCPConnector(ssl=False)
            timeout = aiohttp.ClientTimeout(total=TEST_TIMEOUT)
            async with aiohttp.ClientSession(connector=conn) as s:
                async with s.get(TEST_URL,
                                 proxy=f"http://127.0.0.1:{port}",
                                 timeout=timeout,
                                 allow_redirects=True) as r:
                    return r.status in (200, 204)
        except Exception:
            return False


async def _test_batch(
    batch: list[tuple[str, dict, int]],
    sem: asyncio.Semaphore,
) -> list[bool]:
    cfg = _build_singbox_cfg(batch)
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    try:
        json.dump(cfg, tmp)
        tmp.close()
        proc = subprocess.Popen(
            [SINGBOX_BIN, "run", "-c", tmp.name],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        await asyncio.sleep(SINGBOX_STARTUP)
        results = await asyncio.gather(*[_http_test(port, sem) for _, _, port in batch])
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
    """sing-box batch test. Nếu binary không có → giữ hết nodes."""
    if not Path(SINGBOX_BIN).exists():
        log.warning("sing-box không tìm thấy → bỏ qua health check, giữ %d nodes", len(nodes))
        return nodes

    parsed: list[tuple[str, dict, int]] = []
    skipped = 0
    for i, url in enumerate(nodes):
        ob = _to_singbox(url, f"n{i}")
        if ob:
            parsed.append((url, ob, BASE_PORT + i))
        else:
            skipped += 1
    if skipped:
        log.info("  sing-box: bỏ qua %d nodes không parse được (ssr/unknown)", skipped)
    if not parsed:
        return []

    sem     = asyncio.Semaphore(TEST_CONCURRENCY)
    online: list[str] = []
    batches = [parsed[i:i + BATCH_SIZE] for i in range(0, len(parsed), BATCH_SIZE)]

    for idx, batch in enumerate(batches, 1):
        t0      = time.monotonic()
        results = await _test_batch(batch, sem)
        ok      = sum(results)
        log.info("  Batch %d/%d: %d/%d online (%.1fs)",
                 idx, len(batches), ok, len(batch), time.monotonic() - t0)
        online.extend(url for (url, _, _), alive in zip(batch, results) if alive)

    return online


# ═══════════════════════ 5. Rename nodes ══════════════════════════════

# Schemes hợp lệ của VPN protocols
_VALID_SCHEMES = {"vmess", "vless", "trojan", "ss", "ssr"}

# Regex bắt fragment cũ: từ ký tự # đầu tiên đến hết
# Cũng bắt %23 (# bị URL-encode trong một số node)
_RE_FRAGMENT = re.compile(r'(?:#|%23).*$')


def _strip_fragment(url: str) -> str:
    """
    Loại bỏ fragment cũ khỏi node URL một cách an toàn.

    Xử lý các trường hợp:
      • URL bình thường   : ss://...@host:443#OldLabel  → ss://...@host:443
      • Không có fragment : vmess://BASE64               → vmess://BASE64
      • # encoded         : ...%23OldLabel               → ...
      • Nhiều # liên tiếp : vless://...#part1#part2      → vless://...
    """
    return _RE_FRAGMENT.sub("", url).strip()


def _scheme_of(base_url: str) -> str:
    """Trích scheme từ URL đã strip fragment. Trả về 'vpn' nếu không nhận ra."""
    if "://" not in base_url:
        return "vpn"
    scheme = base_url.split("://", 1)[0].strip().lower()
    return scheme if scheme in _VALID_SCHEMES else "vpn"


def rename_nodes(nodes: list[str], country_code: str) -> list[str]:
    """
    Strip toàn bộ label cũ và chuẩn hoá thành:
      {CC} | {scheme} | {index zero-padded}

    Ví dụ với country_code="JP":
      vmess://BASE64#Old Name  →  vmess://BASE64#JP | vmess | 01
      ss://...#🇸🇬 label       →  ss://...#SG | ss | 02

    Zero-padding: 2 chữ số (≤99 nodes), 3 chữ số (100-999 nodes).
    """
    pad_width = max(2, len(str(len(nodes))))   # 01, 02 … hoặc 001, 002 …

    renamed: list[str] = []
    for i, url in enumerate(nodes, 1):
        base   = _strip_fragment(url)
        scheme = _scheme_of(base)
        index  = str(i).zfill(pad_width)
        renamed.append(f"{base}#{country_code} | {scheme} | {index}")

    return renamed


# ═══════════════════════ Per-country pipeline ═════════════════════════
async def process_country(
    session: aiohttp.ClientSession,
    country: str,
    http_sem: asyncio.Semaphore,
    geoip_reader: Optional[maxminddb.Reader],
) -> None:
    flag = COUNTRY_FLAG[country]
    cc   = COUNTRY_CC[country]
    log.info("━━━ [%s] %s Bắt đầu ━━━", country.upper(), flag)

    # ── 1. Scrape ───────────────────────────────────────────────
    nodes = await scrape_country(session, country, http_sem)
    if not nodes:
        log.warning("[%s] Không scrape được node nào.", country.upper())
        return
    log.info("[%s] Scrape: %d nodes", country.upper(), len(nodes))

    # ── 2. Dedup ─────────────────────────────────────────────────
    nodes = deduplicate(nodes)
    log.info("[%s] Sau dedup: %d nodes", country.upper(), len(nodes))

    # ── 3. GeoIP filter ──────────────────────────────────────────
    if geoip_reader:
        before = len(nodes)
        nodes  = await geoip_filter(nodes, cc, geoip_reader)
        log.info("[%s] Sau GeoIP (%s): %d/%d nodes giữ lại",
                 country.upper(), cc, len(nodes), before)
    else:
        log.warning("[%s] GeoLite2 DB không có → bỏ qua GeoIP filter.", country.upper())

    if not nodes:
        log.warning("[%s] Không còn node sau GeoIP filter.", country.upper())
        return

    # ── 4. sing-box health check ─────────────────────────────────
    log.info("[%s] sing-box test %d nodes...", country.upper(), len(nodes))
    live = await health_check(nodes)
    log.info("[%s] ✔ %d/%d online | ✘ %d offline",
             country.upper(), len(live), len(nodes), len(nodes) - len(live))

    if not live:
        log.warning("[%s] Không có node online.", country.upper())
        return

    # ── 5. Rename ────────────────────────────────────────────────
    renamed = rename_nodes(live, cc)   # cc = "JP", "HK", "SG", "VN"

    # Preview 3 node đầu để verify format
    for preview in renamed[:3]:
        label = preview.split("#", 1)[-1] if "#" in preview else "(no label)"
        log.info("  [preview] %s", label)

    out = Path(f"{country}_sub.txt")
    out.write_text("\n".join(renamed), encoding="utf-8")
    log.info("[%s] Lưu %d nodes → %s", country.upper(), len(renamed), out)
    log.info("━━━ [%s] Xong ━━━\n", country.upper())


# ═══════════════════════ Main ═════════════════════════════════════════
async def main() -> None:
    log.info("┌────────────────────────────────────────────────────┐")
    log.info("│  Scrape → Dedup → GeoIP → sing-box → Rename       │")
    log.info("└────────────────────────────────────────────────────┘")

    # Load GeoLite2 reader (optional)
    geoip_reader: Optional[maxminddb.Reader] = None
    if Path(GEOIP_DB).exists():
        geoip_reader = maxminddb.open_database(GEOIP_DB)
        log.info("GeoLite2 DB loaded: %s", GEOIP_DB)
    else:
        log.warning("GeoLite2 DB không tìm thấy (%s) — GeoIP filter bị bỏ qua.", GEOIP_DB)

    http_sem  = asyncio.Semaphore(HTTP_CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=30, ssl=False, ttl_dns_cache=300)

    try:
        async with aiohttp.ClientSession(
            connector=connector,
            cookie_jar=aiohttp.CookieJar(unsafe=True),
        ) as session:
            for country in COUNTRIES:
                await process_country(session, country, http_sem, geoip_reader)
    finally:
        if geoip_reader:
            geoip_reader.close()

    log.info("┌────────────────────────────────────────────────────┐")
    log.info("│                    HOÀN TẤT                       │")
    log.info("└────────────────────────────────────────────────────┘")


if __name__ == "__main__":
    asyncio.run(main())
