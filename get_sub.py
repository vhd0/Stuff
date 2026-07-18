"""
get_sub.py — Multi-source VPN node collector
══════════════════════════════════════════════════════════════════════════════
Sources  : v2nodes.com (scrape) · EbraSha · Epodonios · OpenProxyList (GitHub)
Pipeline : Collect → Dedup → GeoIP country filter → sing-box validate
           → empirical health test (connectivity + latency + geo-probe)
           → keep top FINAL_NODE_COUNT by latency → Rename → Save
Label    : "JP | 06"  (CC + index, no protocol — clients display it already)

──────────────────────────────────────────────────────────────────────────────
DESIGN PHILOSOPHY — test, don't guess
──────────────────────────────────────────────────────────────────────────────
Earlier versions rejected nodes using static heuristics: ASN blocklists
(Cloudflare/AWS/GCP/...) and community CIDR lists (X4BNet "known VPN
networks"). This over-blocked in practice — entire popular datacenters got
excluded wholesale even though many individual IPs inside them are
perfectly clean and unblocked. A static list can only ever say "this ASN
is often abused", never "this specific IP is currently blocked" — and the
latter is what actually matters.

New approach: GeoIP now does ONLY country matching (is this IP geolocated
in the target country?). ALL quality judgment moves to the TEST phase,
where we can observe the node's REAL behavior:

  1. Connectivity — does traffic through this node reach the internet at
     all? Latency is measured, not just pass/fail.
  2. Empirical geo-probe (JP only, see GEO_PROBES) — routed through the
     node itself, checked against a real local geo-fenced service
     (radiko.jp) that actively detects and blocks foreign/VPN IPs. A node
     that passes is PROVEN clean for at least one real-world service —
     far stronger evidence than any static list, and immune to false
     positives on datacenters that aren't actually flagged.

Nodes that pass are ranked by latency (fastest first) and only the top
FINAL_NODE_COUNT survive into the output file. This directly targets what
matters — "give me the ~15 healthiest working nodes" — instead of
filtering down to whatever's left after a series of blunt guesses.

HK / SG / VN: no free, no-auth, simple-GET geo-fenced probe has been
verified yet (candidates like myTV SUPER/ViuTV require login flows), so
these rely on connectivity + latency ranking only. Add an entry to
GEO_PROBES to extend once a suitable service is found — no other code
changes needed, health_check() already handles "no probe configured"
gracefully.

──────────────────────────────────────────────────────────────────────────────
LARGE SOURCES (EbraSha/Epodonios dumps vary in size over time)
──────────────────────────────────────────────────────────────────────────────
Regardless of how large a source gets, the pipeline stays efficient:
  • Pre-dedup by (scheme, host, port) before classification.
  • GeoIP.resolve() fast-paths raw-IP hosts (no DNS call needed).
  • Classification runs in bounded chunks (gather_chunked), not one giant
    asyncio.gather — keeps memory predictable and logs progress.
  • TEST_POOL_SIZE caps how many GeoIP-passed candidates enter the (real
    network I/O) test phase per country, so total runtime stays bounded
    no matter how many raw nodes a source happens to list.
"""

import asyncio, base64, json, logging, os, re, socket
import subprocess, sys, tempfile, time
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

import aiohttp, maxminddb
from bs4 import BeautifulSoup


# ═══════════════════════════════ 1. CONFIG ════════════════════════════════

BASE_URL  = "https://www.v2nodes.com"
COUNTRIES = ["hk", "jp", "sg", "vn"]

CC       = {"hk": "HK",        "jp": "JP",   "sg": "SG",   "vn": "VN"}
CC_ALLOW = {"hk": {"HK","CN"}, "jp": {"JP"}, "sg": {"SG"}, "vn": {"VN"}}

# ── External node sources (GitHub-hosted; accessible from Azure/GH Actions
#    IPs, unlike the origin sites which are often Cloudflare-protected).
#    Order: raw.githubusercontent (authoritative) → jsDelivr CDN (fallback) ──
EBRASHA_RAW = ("https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list"
               "/refs/heads/main/V2Ray-Config-By-EbraSha.txt")
EBRASHA_CDN = ("https://cdn.jsdelivr.net/gh/ebrasha/free-v2ray-public-list"
               "@main/V2Ray-Config-By-EbraSha.txt")

EPO_RAW = ("https://raw.githubusercontent.com/Epodonios/v2ray-configs"
           "/refs/heads/main/All_Configs_Sub.txt")
EPO_CDN = "https://cdn.jsdelivr.net/gh/Epodonios/v2ray-configs@main/All_Configs_Sub.txt"

# roosterkid/openproxylist = official GitHub mirror of openproxylist.com
# (the .com domain itself is Cloudflare-protected and blocks Azure IPs)
OPL_RAW = "https://raw.githubusercontent.com/roosterkid/openproxylist/main/V2RAY_RAW.txt"
OPL_CDN = "https://cdn.jsdelivr.net/gh/roosterkid/openproxylist@main/V2RAY_RAW.txt"

# ── GeoIP ────────────────────────────────────────────────────────────────────
# Country lookup ONLY — no ASN/blocklist reasoning. See module docstring.
GEOIP_DB        = os.environ.get("GEOIP_DB", "GeoLite2-Country.mmdb")
DNS_CONCURRENCY = 150
DNS_TIMEOUT     = 5.0

# ── Chunked classification (any source size) ──────────────────────────────
CLASSIFY_CHUNK_SIZE = 4000

# ── Test-phase sizing ────────────────────────────────────────────────────────
# TEST_POOL_SIZE: how many GeoIP-passed candidates enter the real empirical
# test per country. This is now the ONLY thing standing between "raw source
# size" and "test phase runtime" — bounded regardless of how large sources
# get. Order favors curated/labeled sources (v2nodes/OPL collected first),
# but a large pool is fine now since quality judgment happens in the test,
# not before it.
TEST_POOL_SIZE   = int(os.environ.get("TEST_POOL_SIZE", "450"))
# FINAL_NODE_COUNT: after testing, keep only this many — the ones with the
# lowest latency among those that passed. This is the actual answer to
# "give me N healthy nodes", rather than "whatever happened to survive".
FINAL_NODE_COUNT = int(os.environ.get("FINAL_NODE_COUNT", "15"))

# ── Empirical geo-fence probes — the real quality signal, see docstring ────
# Each probe: GET `url` through the node's own proxy connection, decide
# pass/fail from the response body. `ok(body) -> bool`.
GEO_PROBES: dict[str, dict] = {
    "JP": {
        "url": "http://radiko.jp/area",
        "ok":  lambda body: bool(re.search(r'class="JP\d+"', body))
                             and '"OUT"' not in body,
    },
    # HK / SG / VN: not yet configured — see module docstring for why.
    # To add one: {"url": "...", "ok": lambda body: <pass/fail logic>}
}

# ── HTTP ─────────────────────────────────────────────────────────────────────
HTTP_SEM     = 10
HTTP_RETRY   = 3
CONN_TIMEOUT = 12
READ_TIMEOUT = 25

# ── sing-box ─────────────────────────────────────────────────────────────────
SINGBOX      = os.environ.get("SINGBOX_BIN", "/usr/local/bin/sing-box")
BATCH_SIZE   = 30
BASE_PORT    = 20000
SB_STARTUP   = 3.0
SB_VAL_SEM   = 8      # concurrent per-node config-validation processes
CONNECT_URLS = [
    "http://connectivitycheck.gstatic.com/generate_204",
    "http://www.msftconnecttest.com/connecttest.txt",
]
TEST_TIMEOUT = 10
TEST_SEM     = 30

# ── Regex / constants ─────────────────────────────────────────────────────────
_RE_TOTAL  = re.compile(r'\b\d+\s+of\s+(\d+)\b', re.I)
_RE_SRV    = re.compile(r'^/servers/(\d+)/$')
_RE_DCFG   = re.compile(r'data-config="([^"]+)"')
_RE_NODE   = re.compile(r'^(?:vmess|vless|trojan|ss|ssr)://.+', re.I | re.M)
_RE_FRAG   = re.compile(r'(?:#|%23).*$')
_RE_UUID   = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I
)
# OPL label: "🇸🇬[openproxylist.com] trojan-SG" or "vmess-HK 120ms HK [ISP]"
_RE_OPL_CC = re.compile(r'\[openproxylist\.com\]\s+\w+-([A-Z]{2})\b')
_SCHEMES   = {"vmess","vless","trojan","ss","ssr"}

_HDR = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ═══════════════════════════════ 2. HTTP HELPERS ═══════════════════════════

async def http_get(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    to = aiohttp.ClientTimeout(connect=CONN_TIMEOUT, total=READ_TIMEOUT)
    for n in range(HTTP_RETRY + 1):
        try:
            async with session.get(url, headers=_HDR, timeout=to) as r:
                r.raise_for_status()
                return await r.text()
        except Exception as e:
            if n == HTTP_RETRY:
                log.debug("GET %s fail: %s", url, e)
                return None
            await asyncio.sleep(2.0 ** n)
    return None


async def fetch_with_fallback(
    session:   aiohttp.ClientSession,
    primary:   str,
    fallback:  str,
    name:      str,
    validator = None,
) -> Optional[str]:
    """Fetch primary URL; if content fails validator, try fallback."""
    check = validator or (lambda t: bool(_RE_NODE.search(t)))
    text = await http_get(session, primary)
    if text and check(text):
        log.info("[%s] OK ← %s", name, primary.split("/")[2])
        return text
    log.info("[%s] primary miss → fallback...", name)
    text = await http_get(session, fallback)
    if text and check(text):
        log.info("[%s] OK ← %s", name, fallback.split("/")[2])
        return text
    log.warning("[%s] Fetch thất bại từ cả hai nguồn", name)
    return None


async def gather_chunked(
    items:      list,
    coro_fn,
    chunk_size: int = CLASSIFY_CHUNK_SIZE,
    label:      str = "",
) -> list:
    """
    Run coro_fn(item) for every item, in bounded chunks rather than one
    giant asyncio.gather — keeps peak memory predictable and gives progress
    feedback regardless of source size.
    """
    results: list = []
    total = len(items)
    for i in range(0, total, chunk_size):
        chunk = items[i:i + chunk_size]
        results.extend(await asyncio.gather(*[coro_fn(x) for x in chunk]))
        if label and total > chunk_size:
            done = min(i + chunk_size, total)
            log.info("  %s: %d/%d (%.0f%%)", label, done, total, 100 * done / total)
    return results


# ═══════════════════════════════ 3. GEOIP ══════════════════════════════════

class GeoIP:
    """Country lookup + cached DNS resolution. No quality/blocklist logic —
    see module docstring for why that moved entirely to the test phase."""

    def __init__(self) -> None:
        self._country: Optional[maxminddb.Reader] = None
        self._cache:   dict[str, Optional[str]]   = {}
        self._sem                                  = asyncio.Semaphore(DNS_CONCURRENCY)

        if Path(GEOIP_DB).exists():
            self._country = maxminddb.open_database(GEOIP_DB)
            log.info("GeoLite2-Country: %s", GEOIP_DB)
        else:
            log.warning("GeoLite2-Country không tìm thấy — GeoIP filter bị bỏ qua")

    @property
    def ok(self) -> bool:
        return self._country is not None

    def close(self) -> None:
        if self._country:
            self._country.close()

    async def resolve(self, host: str) -> Optional[str]:
        """Resolve hostname → IPv4, result cached per session (shared across sources).
        Raw-IP hosts short-circuit via socket.inet_pton — no network call."""
        if host in self._cache:
            return self._cache[host]
        for fam in (socket.AF_INET, socket.AF_INET6):
            try:
                socket.inet_pton(fam, host)
                self._cache[host] = host
                return host
            except OSError:
                pass
        async with self._sem:
            try:
                infos = await asyncio.wait_for(
                    asyncio.get_event_loop().getaddrinfo(
                        host, None, type=socket.SOCK_STREAM
                    ),
                    timeout=DNS_TIMEOUT,
                )
                ip = next((i[4][0] for i in infos if i[0] == socket.AF_INET), None)
                ip = ip or (infos[0][4][0] if infos else None)
            except Exception:
                ip = None
        self._cache[host] = ip
        return ip

    def country_of(self, ip: str) -> Optional[str]:
        if not self._country:
            return None
        try:
            return (self._country.get(ip) or {}).get("country", {}).get("iso_code")
        except Exception:
            return None

    async def cc_of_node(self, url: str) -> Optional[str]:
        """Return CC for classification (EbraSha/Epodonios have no CC in label)."""
        ep = parse_endpoint(url)
        if not ep:
            return None
        ip = await self.resolve(ep[1])
        if not ip:
            return None
        return self.country_of(ip)

    async def filter(self, nodes: list[str], allowed: set[str]) -> list[str]:
        """Keep node if DNS resolves AND GeoIP country ∈ allowed. That's it —
        no quality rejection here; the test phase judges quality empirically."""
        async def _keep(url: str) -> bool:
            ep = parse_endpoint(url)
            if not ep:
                return True
            ip = await self.resolve(ep[1])
            if ip is None:
                return False
            return self.country_of(ip) in allowed

        mask = await gather_chunked(nodes, _keep, label="GeoIP filter")
        return [n for n, keep in zip(nodes, mask) if keep]


# ═══════════════════════════════ 4. ENDPOINT PARSING ═══════════════════════

def _b64d(s: str) -> Optional[str]:
    try:
        return base64.b64decode(s + "=" * (-len(s) % 4)).decode("utf-8", errors="ignore")
    except Exception:
        return None


def parse_endpoint(url: str) -> Optional[tuple[str, str, int]]:
    """Return (scheme, host, port) — used for dedup key and GeoIP resolve."""
    try:
        clean  = url.split("#")[0].strip()
        scheme = clean.split("://")[0].lower()
        if scheme == "vmess":
            c = json.loads(_b64d(clean[8:]) or "{}")
            h, p = str(c.get("add","")).strip(), int(c.get("port",0))
            return (scheme, h, p) if h and p else None
        if scheme in ("vless","trojan"):
            pr = urlparse(clean)
            return (scheme, pr.hostname or "", pr.port or 0) if pr.hostname and pr.port else None
        if scheme == "ss":
            rest = clean[5:]
            if "@" in rest:
                hi = rest.rsplit("@",1)[1]
            else:
                d = _b64d(rest)
                if not d or "@" not in d: return None
                hi = d.rsplit("@",1)[1]
            hi = hi.split("?")[0].split("#")[0]
            if hi.startswith("["):
                h = hi[1:hi.index("]")]; p = int(hi[hi.index("]")+2:])
            else:
                h, p = hi.rsplit(":",1); p = int(p)
            return (scheme, h, p)
        if scheme == "ssr":
            d = _b64d(clean[6:])
            if d:
                pts = d.split(":")
                return (scheme, pts[0], int(pts[1]))
    except Exception:
        pass
    return None


# ═══════════════════════════════ 5. SOURCES ═════════════════════════════════
# Each fetch_* function returns node URLs, either as a flat list (v2nodes)
# or pre-bucketed by country {CC: [nodes]} (EbraSha/Epodonios/OPL).

# ── 5a. v2nodes.com (scrape, always country-accurate — no classification) ──

async def _node_from_server(
    session: aiohttp.ClientSession, sid: str, sem: asyncio.Semaphore
) -> Optional[str]:
    async with sem:
        html = await http_get(session, f"{BASE_URL}/servers/{sid}/")
    if not html:
        return None
    m = _RE_DCFG.search(html)
    if m:
        v = m.group(1).strip()
        if _RE_NODE.match(v):
            return v
    ta = BeautifulSoup(html, "lxml").find("textarea", {"id": "config"})
    if ta:
        v = (ta.get("data-config") or ta.get_text()).strip()
        if _RE_NODE.match(v):
            return v
    return None


async def scrape_v2nodes(
    session: aiohttp.ClientSession, country: str, sem: asyncio.Semaphore
) -> list[str]:
    async with sem:
        h1 = await http_get(session, f"{BASE_URL}/country/{country}/")
    if not h1:
        return []
    total = int(m.group(1)) if (m := _RE_TOTAL.search(h1)) else 1
    log.info("[v2nodes/%s] %d page(s)", country.upper(), total)

    soup = BeautifulSoup(h1, "lxml")
    ids  = list(dict.fromkeys(a["href"][9:-1] for a in soup.find_all("a", href=_RE_SRV)))
    if total > 1:
        extras = await asyncio.gather(*[
            http_get(session, f"{BASE_URL}/country/{country}/?page={p}")
            for p in range(2, total + 1)
        ])
        for h in extras:
            if h:
                ids.extend(a["href"][9:-1]
                           for a in BeautifulSoup(h,"lxml").find_all("a", href=_RE_SRV))
        ids = list(dict.fromkeys(ids))

    raw   = await asyncio.gather(*[_node_from_server(session, sid, sem) for sid in ids])
    nodes = [r for r in raw if r]
    log.info("[v2nodes/%s] %d nodes", country.upper(), len(nodes))
    return nodes


# ── 5b. EbraSha (no CC in label → full GeoIP classify) ──────────────────────

async def fetch_ebrasha(
    session: aiohttp.ClientSession, geoip: GeoIP
) -> dict[str, list[str]]:
    """
    Pre-dedup before classify keeps this efficient regardless of how large
    the source is on any given day (community dump — size varies over time).
    """
    text = await fetch_with_fallback(session, EBRASHA_RAW, EBRASHA_CDN, "EbraSha")
    if not text:
        return {}

    raw_nodes = [l.strip() for l in text.splitlines() if _RE_NODE.match(l.strip())]
    nodes     = deduplicate(raw_nodes)
    log.info("[EbraSha] %d raw → %d unique endpoints — GeoIP classify...",
             len(raw_nodes), len(nodes))

    t0  = time.monotonic()
    ccs = await gather_chunked(nodes, geoip.cc_of_node, label="EbraSha classify")
    log.info("[EbraSha] Classify hoàn tất trong %.1fs", time.monotonic() - t0)

    result: dict[str, list[str]] = {}
    dropped = 0
    for cc, node in zip(ccs, nodes):
        if cc is None: dropped += 1
        else:          result.setdefault(cc, []).append(node)

    log.info("[EbraSha] %d classified · %d dropped (DNS fail)",
             sum(len(v) for v in result.values()), dropped)
    return result


# ── 5c. Epodonios (no CC in label → full GeoIP classify) ────────────────────

async def fetch_epodonios(
    session: aiohttp.ClientSession, geoip: GeoIP
) -> dict[str, list[str]]:
    """Same handling as fetch_ebrasha — see its docstring."""
    text = await fetch_with_fallback(session, EPO_RAW, EPO_CDN, "Epodonios")
    if not text:
        return {}

    raw_nodes = [l.strip() for l in text.splitlines() if _RE_NODE.match(l.strip())]
    nodes     = deduplicate(raw_nodes)
    log.info("[Epodonios] %d raw → %d unique endpoints — GeoIP classify...",
             len(raw_nodes), len(nodes))

    t0  = time.monotonic()
    ccs = await gather_chunked(nodes, geoip.cc_of_node, label="Epodonios classify")
    log.info("[Epodonios] Classify hoàn tất trong %.1fs", time.monotonic() - t0)

    result: dict[str, list[str]] = {}
    dropped = 0
    for cc, node in zip(ccs, nodes):
        if cc is None: dropped += 1
        else:          result.setdefault(cc, []).append(node)

    log.info("[Epodonios] %d classified · %d dropped (DNS fail)",
             sum(len(v) for v in result.values()), dropped)
    return result


# ── 5d. OpenProxyList (CC embedded in label → no GeoIP needed here) ─────────

def _opl_cc(fragment: str) -> Optional[str]:
    m = _RE_OPL_CC.search(fragment)
    return m.group(1) if m else None


async def fetch_opl(session: aiohttp.ClientSession) -> dict[str, list[str]]:
    text = await fetch_with_fallback(session, OPL_RAW, OPL_CDN, "OPL")
    if not text:
        return {}

    nodes = [l.strip() for l in text.splitlines() if _RE_NODE.match(l.strip())]
    log.info("[OPL] %d nodes raw", len(nodes))

    result: dict[str, list[str]] = {}
    no_cc = 0
    for node in nodes:
        frag = node.split("#", 1)[1] if "#" in node else ""
        cc   = _opl_cc(frag)
        if cc: result.setdefault(cc, []).append(node)
        else:  no_cc += 1

    log.info("[OPL] %s · %d no-CC skipped",
             {k: len(v) for k, v in sorted(result.items())}, no_cc)
    return result


# ═══════════════════════════════ 6. DEDUPLICATION ══════════════════════════

def deduplicate(nodes: list[str]) -> list[str]:
    """Remove duplicates by (scheme, host, port). Preserves order (first wins)."""
    seen: set = set()
    out: list[str] = []
    dups = 0
    for url in nodes:
        ep  = parse_endpoint(url)
        key = ep if ep else url
        if key in seen:
            dups += 1
        else:
            seen.add(key)
            out.append(url)
    if dups:
        log.info("  Dedup: removed %d duplicates", dups)
    return out


# ═══════════════════════════════ 7. SING-BOX PARSERS ═══════════════════════
# Convert node URLs → sing-box outbound configs.

def _sni(raw: str, fallback: str = "") -> str:
    """Sanitize TLS SNI: must be plain hostname, no '/' '?' ' '."""
    for ch in ("/", "?", " "):
        raw = raw.split(ch)[0]
    return raw.strip() or fallback


def _ss(url: str, tag: str) -> Optional[dict]:
    try:
        rest = url.split("#")[0][5:]
        if "@" in rest:
            ui, hi = rest.rsplit("@", 1)
            d = _b64d(ui)
            if d and ":" in d:   method, pw = d.split(":", 1)
            elif ":" in ui:      method, pw = ui.split(":", 1)
            else: return None
        else:
            d = _b64d(rest)
            if not d or "@" not in d: return None
            mp, hi = d.rsplit("@", 1)
            method, pw = mp.split(":", 1)
        hi = hi.split("?")[0].split("#")[0]
        if hi.startswith("["):
            host = hi[1:hi.index("]")]; port = int(hi[hi.index("]")+2:])
        else:
            host, port = hi.rsplit(":", 1); port = int(port)
        return {"type":"shadowsocks","tag":tag,
                "server":host,"server_port":port,"method":method,"password":pw}
    except Exception:
        return None


def _vmess(url: str, tag: str) -> Optional[dict]:
    try:
        c    = json.loads(_b64d(url[8:].split("#")[0]) or "{}")
        host = str(c.get("add","")).strip()
        port = int(c.get("port", 0))
        uid  = c.get("id","")
        if not host or not port or not _RE_UUID.match(uid):
            return None
        ob: dict = {"type":"vmess","tag":tag,"server":host,"server_port":port,
                    "uuid":uid,
                    "security":c.get("scy", c.get("security","auto")),
                    "alter_id":int(c.get("aid",0))}
        net  = c.get("net","tcp")
        h    = c.get("host","")
        path = c.get("path","/") or "/"
        sni  = _sni(c.get("sni", h) or "", host)
        if net == "ws":
            ob["transport"] = {"type":"ws","path":path,
                               "headers":{"Host":h} if h else {}}
        elif net == "grpc":
            ob["transport"] = {"type":"grpc","service_name":path}
        elif net in ("h2","http"):
            ob["transport"] = {"type":"http","host":[h] if h else [],"path":path}
        if c.get("tls"):
            ob["tls"] = {"enabled":True,"server_name":sni,"insecure":True}
        return ob
    except Exception:
        return None


def _vless(url: str, tag: str) -> Optional[dict]:
    try:
        pr = urlparse(url)
        if not pr.hostname or not pr.port: return None
        uuid = pr.username or ""
        if not _RE_UUID.match(uuid): return None
        q = parse_qs(pr.query)
        def g(k): return q.get(k, [""])[0]
        net, sec = g("type") or "tcp", g("security")
        sni      = _sni(g("sni") or g("peer") or "", pr.hostname)
        host     = g("host")
        path     = g("path") or "/"
        ob: dict = {"type":"vless","tag":tag,
                    "server":pr.hostname,"server_port":pr.port,"uuid":uuid}
        if g("flow"): ob["flow"] = g("flow")
        if net == "ws":
            ob["transport"] = {"type":"ws","path":path,
                               "headers":{"Host":host} if host else {}}
        elif net == "grpc":
            ob["transport"] = {"type":"grpc","service_name":g("serviceName") or path}
        elif net in ("h2","http"):
            ob["transport"] = {"type":"http","host":[host] if host else [],"path":path}
        if sec in ("tls","reality","xtls"):
            tls: dict = {"enabled":True,"server_name":sni,"insecure":True}
            if sec == "reality":
                tls["reality"] = {"enabled":True,"public_key":g("pbk"),"short_id":g("sid")}
            if g("fp"):
                tls["utls"] = {"enabled":True,"fingerprint":g("fp")}
            ob["tls"] = tls
        return ob
    except Exception:
        return None


def _trojan(url: str, tag: str) -> Optional[dict]:
    try:
        pr = urlparse(url)
        if not pr.hostname or not pr.port: return None
        q = parse_qs(pr.query)
        def g(k): return q.get(k, [""])[0]
        net  = g("type") or "tcp"
        sni  = _sni(g("sni") or g("peer") or "", pr.hostname)
        host = g("host")
        path = g("path") or "/"
        ob: dict = {"type":"trojan","tag":tag,
                    "server":pr.hostname,"server_port":pr.port,
                    "password":pr.username or "",
                    "tls":{"enabled":True,"server_name":sni,"insecure":True}}
        if g("fp"):
            ob["tls"]["utls"] = {"enabled":True,"fingerprint":g("fp")}
        if net == "ws":
            ob["transport"] = {"type":"ws","path":path,
                               "headers":{"Host":host} if host else {}}
        elif net == "grpc":
            ob["transport"] = {"type":"grpc","service_name":g("serviceName") or path}
        return ob
    except Exception:
        return None


def to_singbox(url: str, tag: str) -> Optional[dict]:
    s = url.split("://")[0].lower()
    return (_vmess if s=="vmess" else _vless if s=="vless"
            else _trojan if s=="trojan" else _ss if s=="ss"
            else lambda *_: None)(url, tag)


# ═══════════════════════════════ 8. SING-BOX VALIDATE ══════════════════════

async def _sb_check_node(ob: dict, sem: asyncio.Semaphore) -> bool:
    """
    Run 'sing-box check' on a minimal config with just this outbound.
    Dry-run (no port binding). Catches invalid SNI, bad reality keys, etc.
    — config correctness, unrelated to the quality/health judgment below.
    A single bad config would otherwise crash the whole batch's test.
    """
    cfg = {
        "log":      {"disabled": True},
        "inbounds": [{"type":"http","tag":"chk","listen":"127.0.0.1","listen_port":29000}],
        "outbounds":[ob, {"type":"block","tag":"block"}],
        "route":    {"rules":[{"inbound":["chk"],"outbound":ob["tag"]}],"final":"block"},
    }
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    async with sem:
        try:
            json.dump(cfg, tmp); tmp.close()
            proc = await asyncio.create_subprocess_exec(
                SINGBOX, "check", "-c", tmp.name,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            try:
                await asyncio.wait_for(proc.wait(), timeout=5.0)
                return proc.returncode == 0
            except asyncio.TimeoutError:
                proc.kill(); return False
        except Exception:
            return False
        finally:
            try: os.unlink(tmp.name)
            except Exception: pass


# ═══════════════════════════════ 9. HEALTH TEST ═════════════════════════════
# This is now the PRIMARY quality gate (see module docstring). A node's
# fate is decided by its actual observed behavior, not by which ASN it's on.

async def _connect_test(port: int, sem: asyncio.Semaphore) -> Optional[float]:
    """
    Basic reachability + latency measurement. Returns elapsed ms for the
    first CONNECT_URLS target that responds successfully, or None if all
    fail — used both as a pass/fail gate and as the health ranking signal.
    """
    async with sem:
        to = aiohttp.ClientTimeout(total=TEST_TIMEOUT)
        for url in CONNECT_URLS:
            t0 = time.monotonic()
            try:
                async with aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(ssl=False)
                ) as s:
                    async with s.get(url, proxy=f"http://127.0.0.1:{port}",
                                     timeout=to, allow_redirects=True) as r:
                        if r.status in (200, 204):
                            return (time.monotonic() - t0) * 1000
            except Exception:
                continue
        return None


async def _geo_probe_test(port: int, cc: str, sem: asyncio.Semaphore) -> bool:
    """
    Empirical check against a real local geo-fenced service (GEO_PROBES).
    Returns True if no probe is configured for `cc` (nothing to disprove),
    or if the response indicates the node is accepted as a genuine local IP.
    """
    probe = GEO_PROBES.get(cc)
    if not probe:
        return True
    async with sem:
        to = aiohttp.ClientTimeout(total=TEST_TIMEOUT)
        try:
            async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=False)
            ) as s:
                async with s.get(probe["url"], proxy=f"http://127.0.0.1:{port}",
                                 timeout=to, allow_redirects=True) as r:
                    body = await r.text(errors="ignore")
                    return probe["ok"](body)
        except Exception:
            return False


async def _false() -> bool:
    """Placeholder coroutine — skips the geo-probe for already-dead nodes."""
    return False


async def _run_batch(
    batch: list[tuple[str, dict, int]], cc: str, sem: asyncio.Semaphore
) -> list[Optional[float]]:
    """
    Start sing-box for this batch, measure connectivity latency, then (only
    for nodes that responded) run the empirical geo-probe. Returns, per
    node in batch order: latency in ms if the node is healthy (passed
    connectivity AND, if configured, the geo-probe), else None.
    """
    cfg = {
        "log":      {"disabled": True},
        "inbounds": [{"type":"http","tag":f"in_{ob['tag']}",
                       "listen":"127.0.0.1","listen_port":port}
                     for _,ob,port in batch],
        "outbounds":[ob for _,ob,_ in batch] + [{"type":"block","tag":"block"}],
        "route":    {"rules":[{"inbound":[f"in_{ob['tag']}"],"outbound":ob["tag"]}
                               for _,ob,_ in batch],
                     "final":"block"},
    }
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    try:
        json.dump(cfg, tmp); tmp.close()

        proc = subprocess.Popen(
            [SINGBOX, "run", "-c", tmp.name],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )
        await asyncio.sleep(SB_STARTUP)

        if proc.poll() is not None:
            err = proc.stderr.read().decode(errors="ignore").strip()
            log.warning("sing-box crash rc=%d: %s", proc.returncode, err[:200])
            return [None] * len(batch)

        latencies = await asyncio.gather(*[_connect_test(p, sem) for _,_,p in batch])

        # Only geo-probe nodes that actually responded — saves requests
        probe_tasks = [
            _geo_probe_test(port, cc, sem) if lat is not None else _false()
            for (_,_,port), lat in zip(batch, latencies)
        ]
        probed = await asyncio.gather(*probe_tasks)

        proc.terminate()
        try:    proc.wait(timeout=3)
        except Exception: proc.kill()

        return [lat if (lat is not None and ok) else None
                for lat, ok in zip(latencies, probed)]
    finally:
        try: os.unlink(tmp.name)
        except Exception: pass


async def health_check(nodes: list[str], cc: str) -> list[str]:
    """
    1. Parse all nodes → sing-box outbound configs
    2. Per-node 'sing-box check' (concurrent, no network) — filters invalid
       configs before they can crash a shared batch
    3. Batch test: connectivity + latency + (if configured) geo-probe
    4. Rank survivors by latency, keep only the top FINAL_NODE_COUNT

    This is the primary quality decision in the whole pipeline — see the
    module docstring for why it replaced static ASN/CIDR blocklisting.
    """
    if not Path(SINGBOX).exists():
        log.warning("sing-box not found → skipping health check")
        return nodes[:FINAL_NODE_COUNT]

    candidates = [(url, ob, BASE_PORT + i)
                  for i, url in enumerate(nodes)
                  if (ob := to_singbox(url, f"n{i}")) is not None]
    parse_skip = len(nodes) - len(candidates)
    if parse_skip:
        log.info("  Parse skip: %d (ssr / unknown scheme)", parse_skip)

    val_sem    = asyncio.Semaphore(SB_VAL_SEM)
    valid_mask = await gather_chunked(
        candidates, lambda c: _sb_check_node(c[1], val_sem), label="sing-box validate"
    )
    valid     = [c for c, ok in zip(candidates, valid_mask) if ok]
    inv_count = len(candidates) - len(valid)
    if inv_count:
        log.info("  Config invalid (sing-box check): %d nodes removed", inv_count)
    log.info("  Validated: %d nodes ready for testing", len(valid))
    if not valid:
        return []

    has_probe = cc in GEO_PROBES
    log.info("  Testing: connectivity + latency%s",
             f" + geo-probe ({GEO_PROBES[cc]['url']})" if has_probe
             else " only (no geo-probe configured for this country)")

    test_sem = asyncio.Semaphore(TEST_SEM)
    batches  = [valid[i:i+BATCH_SIZE] for i in range(0, len(valid), BATCH_SIZE)]
    scored: list[tuple[str, float]] = []   # (node_url, latency_ms)

    for i, batch in enumerate(batches, 1):
        t0      = time.monotonic()
        results = await _run_batch(batch, cc, test_sem)
        healthy = [(url, lat) for (url,_,_), lat in zip(batch, results) if lat is not None]
        scored.extend(healthy)
        log.info("  Batch %d/%d: %d/%d healthy (%.1fs)",
                 i, len(batches), len(healthy), len(batch), time.monotonic()-t0)

    scored.sort(key=lambda x: x[1])
    top = scored[:FINAL_NODE_COUNT]

    if top:
        log.info("  Health test: %d/%d passed — keeping top %d by latency "
                 "(best %.0fms, worst kept %.0fms)",
                 len(scored), len(valid), len(top), top[0][1], top[-1][1])
    else:
        log.info("  Health test: 0/%d passed", len(valid))

    return [url for url, _ in top]


# ═══════════════════════════════ 10. RENAME ═════════════════════════════════

def rename_nodes(nodes: list[str], cc: str) -> list[str]:
    """
    Format: "JP | 06"  — CC + zero-padded index.
    Protocol omitted: Shadowrocket/clients show it on the sub-line already.
    vmess: update "ps" field inside base64 JSON (clients read ps, not #fragment).
    """
    pad = max(2, len(str(len(nodes))))

    def _label(i: int) -> str:
        return f"{cc} | {str(i).zfill(pad)}"

    def _do_vmess(base: str, label: str) -> str:
        try:
            c = json.loads(_b64d(base[8:]) or "{}")
            c["ps"] = label
            b64 = base64.b64encode(
                json.dumps(c, separators=(",",":"), ensure_ascii=False).encode()
            ).decode().rstrip("=")
            return f"vmess://{b64}#{label}"
        except Exception:
            return f"{base}#{label}"

    out = []
    for i, url in enumerate(nodes, 1):
        base   = _RE_FRAG.sub("", url).strip()
        label  = _label(i)
        scheme = base.split("://")[0].lower() if "://" in base else ""
        out.append(_do_vmess(base, label) if scheme == "vmess" else f"{base}#{label}")
    return out


# ═══════════════════════════════ 11. PIPELINE ═══════════════════════════════

async def process_country(
    session:  aiohttp.ClientSession,
    country:  str,
    http_sem: asyncio.Semaphore,
    geoip:    GeoIP,
    ebrasha:  dict[str, list[str]],
    epo:      dict[str, list[str]],
    opl:      dict[str, list[str]],
) -> None:
    cc        = CC[country]
    allowed   = CC_ALLOW[country]
    has_probe = cc in GEO_PROBES
    log.info("━━━ [%s] %s━━━", cc, "(empirical geo-probe available) " if has_probe else "")

    # 1. Collect from 4 sources (v2nodes first → dedup keeps its nodes on conflict)
    v2n     = await scrape_v2nodes(session, country, http_sem)
    ext_eba = [n for acc in allowed for n in ebrasha.get(acc, [])]
    ext_epo = [n for acc in allowed for n in epo.get(acc, [])]
    ext_opl = [n for acc in allowed for n in opl.get(acc, [])]
    nodes   = v2n + ext_eba + ext_epo + ext_opl
    log.info("[%s] Collected: %d total (v2nodes=%d ebrasha=%d epodonios=%d opl=%d)",
             cc, len(nodes), len(v2n), len(ext_eba), len(ext_epo), len(ext_opl))

    # 2. Dedup
    nodes = deduplicate(nodes)
    log.info("[%s] After dedup: %d", cc, len(nodes))

    # 3. GeoIP country filter ONLY — no quality rejection (see module docstring)
    if geoip.ok:
        before = len(nodes)
        nodes  = await geoip.filter(nodes, allowed)
        log.info("[%s] After GeoIP country filter {%s}: %d/%d",
                 cc, "/".join(sorted(allowed)), len(nodes), before)
    else:
        log.warning("[%s] No GeoIP DB — skipping filter", cc)
    if not nodes:
        log.warning("[%s] No nodes after GeoIP filter", cc)
        return

    # 4. Cap the pool entering the (real network I/O) test phase — bounds
    #    runtime regardless of source size, order favors v2nodes/OPL first
    if len(nodes) > TEST_POOL_SIZE:
        log.info("[%s] %d candidates > pool cap %d — testing first %d",
                 cc, len(nodes), TEST_POOL_SIZE, TEST_POOL_SIZE)
        nodes = nodes[:TEST_POOL_SIZE]

    # 5. Empirical health test — THE quality gate. Returns top FINAL_NODE_COUNT
    #    by latency among nodes that passed connectivity (+ geo-probe if any).
    log.info("[%s] sing-box: validating + testing %d candidates...", cc, len(nodes))
    live = await health_check(nodes, cc)
    if not live:
        log.warning("[%s] No healthy nodes found", cc)
        return

    # 6. Rename + save
    renamed = rename_nodes(live, cc)
    for r in renamed[:3]:
        log.info("  [preview] %s", r.split("#")[-1] if "#" in r else r)
    Path(f"{country}_sub.txt").write_text("\n".join(renamed), encoding="utf-8")
    log.info("[%s] Saved %d nodes → %s_sub.txt\n", cc, len(renamed), country)


# ═══════════════════════════════ 12. MAIN ═══════════════════════════════════

async def main() -> None:
    log.info("Sources : v2nodes · EbraSha · Epodonios · OpenProxyList (GitHub mirrors)")
    log.info("URL order: raw.githubusercontent (primary) → jsDelivr CDN (fallback)")
    log.info("Quality : empirical test only (connectivity+latency+geo-probe) — "
             "no static ASN/CIDR blocklist")
    log.info("Output  : top %d nodes/country by latency (pool cap %d) — geo-probe: %s",
             FINAL_NODE_COUNT, TEST_POOL_SIZE, ", ".join(GEO_PROBES) or "none configured")
    log.info("Label   : 'JP | 06'")

    geoip    = GeoIP()
    http_sem = asyncio.Semaphore(HTTP_SEM)
    conn     = aiohttp.TCPConnector(limit=30, ssl=False, ttl_dns_cache=300)

    try:
        async with aiohttp.ClientSession(
            connector=conn, cookie_jar=aiohttp.CookieJar(unsafe=True)
        ) as session:
            # Fetch all external sources in parallel before country loop
            ebrasha, epo, opl = await asyncio.gather(
                fetch_ebrasha(session, geoip),
                fetch_epodonios(session, geoip),
                fetch_opl(session),
            )
            log.info("EbraSha   : %s", {k: len(v) for k,v in sorted(ebrasha.items())})
            log.info("Epodonios : %s", {k: len(v) for k,v in sorted(epo.items())})
            log.info("OPL       : %s", {k: len(v) for k,v in sorted(opl.items())})

            for country in COUNTRIES:
                await process_country(session, country, http_sem, geoip, ebrasha, epo, opl)
    finally:
        geoip.close()


if __name__ == "__main__":
    asyncio.run(main())
