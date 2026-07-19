"""
get_sub.py — Multi-source VPN node collector
══════════════════════════════════════════════════════════════════════════════
Sources  : v2nodes.com (scrape) · EbraSha · Epodonios · OpenProxyList (GitHub)
Pipeline : Collect → Dedup → GeoIP country filter
           → Tier 1 SỐNG (alive)      : sing-box connectivity, must reach internet
           → Tier 2 NHẤT QUÁN (consistent): cross-check exit IP/country across
                                            independent "what-is-my-ip" services
           → Tier 3 SẠCH (clean)      : IPQualityScore Fraud Score, cached
           → Rename → Save
Label    : "JP | 06"  (CC + index, no protocol — clients display it already)
Output   : however many nodes clear ALL enabled tiers — no fixed quota. The
           count itself is information (a country yielding 3 vs 40 tells you
           something real); nothing is padded or capped to hit a target.

──────────────────────────────────────────────────────────────────────────────
DESIGN PHILOSOPHY
──────────────────────────────────────────────────────────────────────────────
Earlier versions rejected nodes via static heuristics — ASN blocklists,
community CIDR lists — which over-blocked: whole datacenters excluded even
though individual IPs inside them are often clean. A static list can only
say "this ASN is often abused", never "this specific IP is a problem right
now". A later version also imposed a fixed "keep top 15" output cap, which
throws away good nodes for no reason once enough of them pass.

Both are gone. Every decision now comes from live, per-IP evidence, checked
across three tiers that mirror how a real destination website actually
judges a visitor: can it connect, does it behave like a normal stable
client, and does it have a clean reputation right now.

  TIER 1 — SỐNG (alive): sing-box connectivity test. Cheapest tier, run
  first to eliminate dead nodes before spending effort on the rest.

  TIER 2 — NHẤT QUÁN (consistent): most websites make access decisions
  partly from IP *behavior*, and one behavioral tell is inconsistency —
  a shared/rotating proxy pool, a broken NAT layer, or a load-balancer
  fronting many backends will report DIFFERENT exit IPs/ASNs/locations
  depending on which service asks. A single dedicated VPS won't. Through
  the node's own proxy connection, we query 3 independent "what is my IP"
  services (ipinfo.io, ip-api.com, Cloudflare's own edge trace) in
  parallel and require them to agree on both the exit IP and the country.
  Disagreement is itself the signal — no blocklist needed, and it costs
  nothing (all 3 services are free, keyless, generous rate limits).

  TIER 3 — SẠCH (clean): IPQualityScore Fraud Score (0-100, "how likely is
  this IP fraudulent/abusive right now", continuously updated from IPQS's
  own honeypot/telemetry network — genuinely live evidence, not a fixed
  list). Free tier: 5,000 lookups/month (IPQS_KEY env/secret, optional —
  if unset this tier is skipped, fail-open, rest of the pipeline still
  runs). Reject only on fraud_score ≥ IPQS_FRAUD_THRESHOLD or an explicit
  recent_abuse flag.

  Budget optimization — persistent cache committed to the repo:
  fraud_score_cache.json stores {ip: {fraud_score, checked_at, ...}},
  loaded at the start of every run and saved back at the end (the workflow
  commits it alongside the *_sub.txt outputs). Since daily cron runs see
  heavily overlapping IP pools (the same handful of VPS providers get
  reused across sources), only genuinely NEW or TTL-expired IPs consume a
  lookup — the free 5,000/month budget stretches far further than 5,000
  raw queries once the cache warms up over a few days.

FAIL-OPEN THROUGHOUT: any tier that can't reach a verdict (no API key, rate
limit hit, network error, service down) does NOT reject the node — only an
explicit bad verdict does. A missing optional API key degrades the pipeline
to fewer tiers, never to zero output.

HK / SG / VN: Tier 2 applies universally (country-agnostic by design).
Tier 3 (IPQS) also applies universally once a key is configured. Neither
depends on finding a country-specific geo-fenced test service, so all four
countries get the same quality bar without needing bespoke per-country
probes.

──────────────────────────────────────────────────────────────────────────────
LARGE SOURCES (EbraSha/Epodonios dumps vary in size over time)
──────────────────────────────────────────────────────────────────────────────
  • Pre-dedup by (scheme, host, port) before classification.
  • GeoIP.resolve() fast-paths raw-IP hosts (no DNS call needed).
  • Classification runs in bounded chunks (gather_chunked), not one giant
    asyncio.gather — predictable memory, progress logging instead of a
    silent multi-minute gap.
  • TEST_POOL_SIZE bounds how many GeoIP-passed candidates enter the real
    network-I/O tiers per country — a runtime safety valve, not a quality
    filter (quality judgment happens inside the tiers themselves).
"""

import asyncio, base64, json, logging, os, re, socket
import subprocess, sys, tempfile, time
from datetime import datetime, timedelta, timezone
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
# Country lookup ONLY — no ASN/blocklist reasoning, see module docstring.
GEOIP_DB        = os.environ.get("GEOIP_DB", "GeoLite2-Country.mmdb")
DNS_CONCURRENCY = 150
DNS_TIMEOUT     = 5.0

# ── Chunked classification (any source size) ──────────────────────────────
CLASSIFY_CHUNK_SIZE = 4000

# ── Test-pool sizing — runtime safety valve, NOT a quality filter ─────────
TEST_POOL_SIZE = int(os.environ.get("TEST_POOL_SIZE", "450"))

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
SB_VAL_SEM   = 8
CONNECT_URLS = [
    "http://connectivitycheck.gstatic.com/generate_204",
    "http://www.msftconnecttest.com/connecttest.txt",
]
TEST_TIMEOUT = 10
TEST_SEM     = 30

# ── Tier 2 — consistency check (country-agnostic, free, keyless) ──────────
# Each entry: (name, url, parser). parser(status, body_or_json) -> Optional[(ip, cc)]
CONSISTENCY_PROVIDERS = ["ipinfo", "ip-api", "cf-trace"]
CONSISTENCY_MIN_AGREE = 2   # need at least this many providers to agree

# ── Tier 3 — IPQualityScore Fraud Score, with persistent cache ────────────
IPQS_KEY              = os.environ.get("IPQS_KEY", "")
IPQS_FRAUD_THRESHOLD  = int(os.environ.get("IPQS_FRAUD_THRESHOLD", "75"))
IPQS_MAX_CALLS_PER_RUN = int(os.environ.get("IPQS_MAX_CALLS_PER_RUN", "1500"))
IPQS_SEM              = 10

FRAUD_CACHE_FILE     = os.environ.get("FRAUD_CACHE_FILE", "fraud_score_cache.json")
FRAUD_CACHE_TTL_DAYS = int(os.environ.get("FRAUD_CACHE_TTL_DAYS", "30"))

# ── Empirical geo-fence probe (optional bonus signal, JP only for now) ────
# Not a required tier — see module docstring. Kept as extra corroboration
# when available; a node failing it is NOT auto-rejected, just noted.
GEO_PROBES: dict[str, dict] = {
    "JP": {
        "url": "http://radiko.jp/area",
        "ok":  lambda body: bool(re.search(r'class="JP\d+"', body))
                             and '"OUT"' not in body,
    },
}

# ── Regex / constants ─────────────────────────────────────────────────────────
_RE_TOTAL  = re.compile(r'\b\d+\s+of\s+(\d+)\b', re.I)
_RE_SRV    = re.compile(r'^/servers/(\d+)/$')
_RE_DCFG   = re.compile(r'data-config="([^"]+)"')
_RE_NODE   = re.compile(r'^(?:vmess|vless|trojan|ss|ssr)://.+', re.I | re.M)
_RE_FRAG   = re.compile(r'(?:#|%23).*$')
_RE_UUID   = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I
)
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
    items: list, coro_fn, chunk_size: int = CLASSIFY_CHUNK_SIZE, label: str = "",
) -> list:
    """Run coro_fn(item) in bounded chunks — predictable memory, progress logs."""
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
    quality judgment lives entirely in the 3-tier test phase (see docstring)."""

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
        """Resolve hostname → IPv4, cached per session. Raw-IP hosts short-circuit."""
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
        """Keep node if DNS resolves AND GeoIP country ∈ allowed. Nothing else."""
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


async def fetch_ebrasha(
    session: aiohttp.ClientSession, geoip: GeoIP
) -> dict[str, list[str]]:
    text = await fetch_with_fallback(session, EBRASHA_RAW, EBRASHA_CDN, "EbraSha")
    if not text:
        return {}

    raw_nodes = [l.strip() for l in text.splitlines() if _RE_NODE.match(l.strip())]
    nodes     = deduplicate(raw_nodes)
    log.info("[EbraSha] %d raw → %d unique endpoints — GeoIP classify...",
             len(raw_nodes), len(nodes))

    ccs = await gather_chunked(nodes, geoip.cc_of_node, label="EbraSha classify")

    result: dict[str, list[str]] = {}
    dropped = 0
    for cc, node in zip(ccs, nodes):
        if cc is None: dropped += 1
        else:          result.setdefault(cc, []).append(node)

    log.info("[EbraSha] %d classified · %d dropped (DNS fail)",
             sum(len(v) for v in result.values()), dropped)
    return result


async def fetch_epodonios(
    session: aiohttp.ClientSession, geoip: GeoIP
) -> dict[str, list[str]]:
    text = await fetch_with_fallback(session, EPO_RAW, EPO_CDN, "Epodonios")
    if not text:
        return {}

    raw_nodes = [l.strip() for l in text.splitlines() if _RE_NODE.match(l.strip())]
    nodes     = deduplicate(raw_nodes)
    log.info("[Epodonios] %d raw → %d unique endpoints — GeoIP classify...",
             len(raw_nodes), len(nodes))

    ccs = await gather_chunked(nodes, geoip.cc_of_node, label="Epodonios classify")

    result: dict[str, list[str]] = {}
    dropped = 0
    for cc, node in zip(ccs, nodes):
        if cc is None: dropped += 1
        else:          result.setdefault(cc, []).append(node)

    log.info("[Epodonios] %d classified · %d dropped (DNS fail)",
             sum(len(v) for v in result.values()), dropped)
    return result


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

def _sni(raw: str, fallback: str = "") -> str:
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
    """Dry-run 'sing-box check' — catches malformed configs before they can
    crash a shared batch. Config correctness only, unrelated to IP quality."""
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


# ═══════════════════════════════ 9. FRAUD SCORE CACHE (Tier 3 support) ═════

class FraudScoreCache:
    """
    Persistent IP → fraud-score cache, committed to the repo alongside the
    subscription outputs. See module docstring: this is what lets a 5,000/
    month free IPQS budget cover far more than 5,000 raw lookups — daily
    runs see heavily overlapping IP pools, so only new/expired IPs cost a
    real API call.
    """
    def __init__(self, path: str, ttl_days: int) -> None:
        self._path = Path(path)
        self._ttl  = timedelta(days=ttl_days)
        self._data: dict[str, dict] = {}
        self._hits = 0
        self._misses = 0
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                self._data = json.loads(self._path.read_text(encoding="utf-8"))
                log.info("FraudScoreCache: loaded %d entries từ %s", len(self._data), self._path)
            except Exception as e:
                log.warning("FraudScoreCache: không đọc được %s (%s) — bắt đầu rỗng", self._path, e)
                self._data = {}
        else:
            log.info("FraudScoreCache: %s chưa tồn tại — sẽ tạo mới", self._path)

    def get(self, ip: str) -> Optional[dict]:
        """Return cached record if present and not expired, else None."""
        rec = self._data.get(ip)
        if not rec:
            return None
        try:
            checked = datetime.fromisoformat(rec["checked_at"])
        except Exception:
            return None
        if datetime.now(timezone.utc) - checked > self._ttl:
            return None   # expired — treat as cache miss, will be re-queried
        self._hits += 1
        return rec

    def put(self, ip: str, fraud_score: int, recent_abuse: bool, proxy: bool, vpn: bool) -> None:
        self._data[ip] = {
            "fraud_score":  fraud_score,
            "recent_abuse": recent_abuse,
            "proxy":        proxy,
            "vpn":          vpn,
            "checked_at":   datetime.now(timezone.utc).isoformat(),
        }
        self._misses += 1

    def save(self) -> None:
        try:
            self._path.write_text(
                json.dumps(self._data, indent=1, sort_keys=True), encoding="utf-8"
            )
            log.info("FraudScoreCache: đã lưu %d entries → %s (hit=%d miss=%d)",
                     len(self._data), self._path, self._hits, self._misses)
        except Exception as e:
            log.warning("FraudScoreCache: lưu thất bại: %s", e)


class CallBudget:
    """Simple per-run call counter — asyncio is single-threaded/cooperative,
    so plain decrement is safe without a lock (no await inside take())."""
    def __init__(self, limit: int) -> None:
        self._left = limit
        self._limit = limit

    def take(self) -> bool:
        if self._left <= 0:
            return False
        self._left -= 1
        return True

    @property
    def used(self) -> int:
        return self._limit - self._left


async def check_fraud_score(
    session: aiohttp.ClientSession, ip: str, cache: FraudScoreCache, budget: CallBudget,
    sem: asyncio.Semaphore,
) -> Optional[dict]:
    """
    Return the fraud-score record for `ip` (from cache or a fresh IPQS
    lookup), or None if inconclusive (no key, budget exhausted, API error)
    — callers must treat None as "no evidence", NOT as "reject" (fail-open).
    """
    cached = cache.get(ip)
    if cached is not None:
        return cached

    if not IPQS_KEY or not budget.take():
        return None

    async with sem:
        try:
            to = aiohttp.ClientTimeout(total=8)
            url = f"https://ipqualityscore.com/api/json/ip/{IPQS_KEY}/{ip}"
            async with session.get(url, timeout=to) as r:
                if r.status != 200:
                    return None
                data = await r.json(content_type=None)
        except Exception:
            return None

    if not data.get("success", False):
        return None

    fraud_score  = int(data.get("fraud_score", 0) or 0)
    recent_abuse = bool(data.get("recent_abuse", False))
    proxy        = bool(data.get("proxy", False))
    vpn          = bool(data.get("vpn", False))

    cache.put(ip, fraud_score, recent_abuse, proxy, vpn)
    return cache.get(ip)  # re-fetch to get normalized/stored form (with checked_at)


# ═══════════════════════════════ 10. CONSISTENCY CHECK (Tier 2) ════════════
# Query several independent "what is my IP" services THROUGH the node's own
# proxy connection. A single dedicated VPS reports the same exit IP/country
# to everyone; a shared/rotating/misconfigured proxy often won't.

async def _probe_ipinfo(session: aiohttp.ClientSession, proxy: str) -> Optional[tuple[str, str]]:
    try:
        to = aiohttp.ClientTimeout(total=TEST_TIMEOUT)
        async with session.get("https://ipinfo.io/json", proxy=proxy, timeout=to) as r:
            d = await r.json(content_type=None)
            ip, cc = d.get("ip"), (d.get("country") or "").upper()
            return (ip, cc) if ip and cc else None
    except Exception:
        return None


async def _probe_ipapi(session: aiohttp.ClientSession, proxy: str) -> Optional[tuple[str, str]]:
    try:
        to = aiohttp.ClientTimeout(total=TEST_TIMEOUT)
        async with session.get("http://ip-api.com/json/", proxy=proxy, timeout=to) as r:
            d = await r.json(content_type=None)
            if d.get("status") != "success":
                return None
            ip, cc = d.get("query"), (d.get("countryCode") or "").upper()
            return (ip, cc) if ip and cc else None
    except Exception:
        return None


async def _probe_cf_trace(session: aiohttp.ClientSession, proxy: str) -> Optional[tuple[str, str]]:
    try:
        to = aiohttp.ClientTimeout(total=TEST_TIMEOUT)
        async with session.get("https://www.cloudflare.com/cdn-cgi/trace",
                               proxy=proxy, timeout=to) as r:
            text = await r.text()
        fields = dict(line.split("=", 1) for line in text.splitlines() if "=" in line)
        ip, cc = fields.get("ip"), (fields.get("loc") or "").upper()
        return (ip, cc) if ip and cc else None
    except Exception:
        return None


_PROBE_FUNCS = {"ipinfo": _probe_ipinfo, "ip-api": _probe_ipapi, "cf-trace": _probe_cf_trace}


async def consistency_check(port: int, target_cc: str, sem: asyncio.Semaphore) -> bool:
    """
    Tier 2 — NHẤT QUÁN. Query all CONSISTENCY_PROVIDERS through the node's
    proxy in parallel. Passes if at least CONSISTENCY_MIN_AGREE of them
    report BOTH the same exit IP and the same country, and that country
    matches target_cc (extra corroboration beyond MaxMind alone).
    """
    proxy_url = f"http://127.0.0.1:{port}"
    async with sem:
        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as s:
            results = await asyncio.gather(*[
                _PROBE_FUNCS[name](s, proxy_url) for name in CONSISTENCY_PROVIDERS
            ])

    successes = [r for r in results if r is not None]
    if len(successes) < CONSISTENCY_MIN_AGREE:
        return False   # not enough data to judge — treat as inconsistent (fail-closed
                        # here specifically, since Tier 2's whole point IS agreement;
                        # too few responses means we can't establish agreement at all)

    ips = [ip for ip, _ in successes]
    ccs = [cc for _, cc in successes]

    # Majority IP must match across all successful responses (single stable exit)
    same_ip = len(set(ips)) == 1
    # Majority country must match across responses AND equal our target
    same_cc = len(set(ccs)) == 1 and ccs[0] == target_cc

    return same_ip and same_cc


# ═══════════════════════════════ 11. TIER 1 — CONNECTIVITY ═════════════════

async def _connect_test(port: int, sem: asyncio.Semaphore) -> Optional[float]:
    """Tier 1 — SỐNG. Returns latency (ms) if reachable, else None."""
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
    """Optional bonus corroboration (JP only) — see module docstring."""
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


# ═══════════════════════════════ 12. HEALTH CHECK (Tiers 1+2 orchestration) ═

async def _run_batch(
    batch: list[tuple[str, dict, int]], cc: str, sem: asyncio.Semaphore
) -> list[Optional[float]]:
    """
    Starts sing-box for the batch, runs Tier 1 (connectivity+latency), then
    for survivors: Tier 2 (consistency) and the optional geo-probe. Returns,
    per node in batch order: latency (ms) if it cleared Tier 1 AND Tier 2
    AND the geo-probe (when configured), else None.
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

        # Tier 1 — alive + latency
        latencies = await asyncio.gather(*[_connect_test(p, sem) for _,_,p in batch])

        # Tier 2 (+ optional geo-probe) — only for survivors of Tier 1
        async def _tier2(port, lat):
            if lat is None:
                return False
            ok_consistent = await consistency_check(port, cc, sem)
            if not ok_consistent:
                return False
            return await _geo_probe_test(port, cc, sem)

        tier2_results = await asyncio.gather(*[
            _tier2(port, lat) for (_,_,port), lat in zip(batch, latencies)
        ])

        proc.terminate()
        try:    proc.wait(timeout=3)
        except Exception: proc.kill()

        return [lat if (lat is not None and ok) else None
                for lat, ok in zip(latencies, tier2_results)]
    finally:
        try: os.unlink(tmp.name)
        except Exception: pass


async def health_check(
    nodes: list[str], cc: str, session: aiohttp.ClientSession,
    fraud_cache: FraudScoreCache, fraud_budget: CallBudget, geoip: GeoIP,
) -> list[str]:
    """
    Runs all 3 tiers in order, funnel-style, logging the count surviving
    each stage. No fixed output size — returns whatever clears every
    enabled tier, sorted by latency (fastest first) for readability.
    """
    if not Path(SINGBOX).exists():
        log.warning("sing-box not found → skipping Tiers 1-2 entirely")
        return nodes

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
    valid = [c for c, ok in zip(candidates, valid_mask) if ok]
    log.info("[%s] Config valid: %d/%d", cc, len(valid), len(candidates))
    if not valid:
        return []

    # ── Tier 1 + Tier 2, batched (both need the live proxy connection) ────
    test_sem = asyncio.Semaphore(TEST_SEM)
    batches  = [valid[i:i+BATCH_SIZE] for i in range(0, len(valid), BATCH_SIZE)]
    scored: list[tuple[str, float]] = []

    for i, batch in enumerate(batches, 1):
        t0      = time.monotonic()
        results = await _run_batch(batch, cc, test_sem)
        healthy = [(url, lat) for (url,_,_), lat in zip(batch, results) if lat is not None]
        scored.extend(healthy)
        log.info("  Batch %d/%d: %d/%d sống+nhất quán (%.1fs)",
                 i, len(batches), len(healthy), len(batch), time.monotonic()-t0)

    scored.sort(key=lambda x: x[1])
    log.info("[%s] Tier 1+2 (sống + nhất quán): %d/%d nodes", cc, len(scored), len(valid))
    if not scored:
        return []

    # ── Tier 3 — SẠCH (Fraud Score via IPQS, cached) ───────────────────────
    if not IPQS_KEY:
        log.info("[%s] IPQS_KEY chưa cấu hình — bỏ qua Tier 3, dùng kết quả Tier 1+2", cc)
        return [url for url, _ in scored]

    fraud_sem = asyncio.Semaphore(IPQS_SEM)

    async def _tier3(item: tuple[str, float]) -> Optional[tuple[str, float]]:
        url, lat = item
        ep = parse_endpoint(url)
        if not ep:
            return item   # can't resolve → fail-open, keep
        ip = await geoip.resolve(ep[1])
        if not ip:
            return None   # DNS just failed now (rare) → drop, can't verify anything
        rec = await check_fraud_score(session, ip, fraud_cache, fraud_budget, fraud_sem)
        if rec is None:
            return item   # inconclusive (no key/budget/error) → fail-open, keep
        if rec["fraud_score"] >= IPQS_FRAUD_THRESHOLD or rec["recent_abuse"]:
            return None   # explicit bad evidence → reject
        return item

    tier3_results = await gather_chunked(scored, _tier3, label="Tier 3 fraud-score")
    clean = [r for r in tier3_results if r is not None]

    log.info("[%s] Tier 3 (sạch): %d/%d nodes qua Fraud Score "
             "(threshold=%d, budget dùng %d/%d)",
             cc, len(clean), len(scored), IPQS_FRAUD_THRESHOLD,
             fraud_budget.used, IPQS_MAX_CALLS_PER_RUN)

    return [url for url, _ in clean]


# ═══════════════════════════════ 13. RENAME ═════════════════════════════════

def rename_nodes(nodes: list[str], cc: str) -> list[str]:
    """Format: "JP | 06" — CC + zero-padded index. vmess: update "ps" field
    inside base64 JSON (clients read ps, not #fragment)."""
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


# ═══════════════════════════════ 14. PIPELINE ═══════════════════════════════

async def process_country(
    session:     aiohttp.ClientSession,
    country:     str,
    http_sem:    asyncio.Semaphore,
    geoip:       GeoIP,
    ebrasha:     dict[str, list[str]],
    epo:         dict[str, list[str]],
    opl:         dict[str, list[str]],
    fraud_cache: FraudScoreCache,
    fraud_budget: CallBudget,
) -> None:
    cc      = CC[country]
    allowed = CC_ALLOW[country]
    log.info("━━━ [%s] ━━━", cc)

    # 1. Collect from 4 sources
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

    # 3. GeoIP country filter ONLY
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

    # 4. Runtime safety cap — NOT a quality filter, see module docstring
    if len(nodes) > TEST_POOL_SIZE:
        log.info("[%s] %d candidates > pool cap %d — testing first %d",
                 cc, len(nodes), TEST_POOL_SIZE, TEST_POOL_SIZE)
        nodes = nodes[:TEST_POOL_SIZE]

    # 5. Three-tier empirical test — SỐNG → NHẤT QUÁN → SẠCH
    log.info("[%s] Testing %d candidates qua 3 tầng...", cc, len(nodes))
    live = await health_check(nodes, cc, session, fraud_cache, fraud_budget, geoip)
    if not live:
        log.warning("[%s] Không có node nào qua đủ 3 tầng", cc)
        return

    # 6. Rename + save
    renamed = rename_nodes(live, cc)
    for r in renamed[:3]:
        log.info("  [preview] %s", r.split("#")[-1] if "#" in r else r)
    Path(f"{country}_sub.txt").write_text("\n".join(renamed), encoding="utf-8")
    log.info("[%s] Saved %d nodes → %s_sub.txt\n", cc, len(renamed), country)


# ═══════════════════════════════ 15. MAIN ═══════════════════════════════════

async def main() -> None:
    log.info("Sources : v2nodes · EbraSha · Epodonios · OpenProxyList (GitHub mirrors)")
    log.info("URL order: raw.githubusercontent (primary) → jsDelivr CDN (fallback)")
    log.info("Tiers   : 1) Sống (connectivity)  2) Nhất quán (%s)  3) Sạch (%s)",
             "/".join(CONSISTENCY_PROVIDERS),
             f"IPQS fraud_score < {IPQS_FRAUD_THRESHOLD}" if IPQS_KEY else "SKIPPED, no IPQS_KEY")
    log.info("Output  : no fixed quota — whatever clears all enabled tiers")

    geoip        = GeoIP()
    http_sem     = asyncio.Semaphore(HTTP_SEM)
    conn         = aiohttp.TCPConnector(limit=30, ssl=False, ttl_dns_cache=300)
    fraud_cache  = FraudScoreCache(FRAUD_CACHE_FILE, FRAUD_CACHE_TTL_DAYS)
    fraud_budget = CallBudget(IPQS_MAX_CALLS_PER_RUN)

    try:
        async with aiohttp.ClientSession(
            connector=conn, cookie_jar=aiohttp.CookieJar(unsafe=True)
        ) as session:
            ebrasha, epo, opl = await asyncio.gather(
                fetch_ebrasha(session, geoip),
                fetch_epodonios(session, geoip),
                fetch_opl(session),
            )
            log.info("EbraSha   : %s", {k: len(v) for k,v in sorted(ebrasha.items())})
            log.info("Epodonios : %s", {k: len(v) for k,v in sorted(epo.items())})
            log.info("OPL       : %s", {k: len(v) for k,v in sorted(opl.items())})

            for country in COUNTRIES:
                await process_country(
                    session, country, http_sem, geoip,
                    ebrasha, epo, opl, fraud_cache, fraud_budget,
                )
    finally:
        geoip.close()
        fraud_cache.save()


if __name__ == "__main__":
    asyncio.run(main())
