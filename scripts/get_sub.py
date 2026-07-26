"""
get_sub.py -- Multi-source VPN node collector
==============================================================================
Sources : v2nodes.com (scrape) . EbraSha . Epodonios . OpenProxyList (GitHub)
Pipeline: Collect -> Dedup -> GeoIP country filter
          -> Phase 1 ONLINE      : sing-box connectivity + latency
          -> Phase 2 CONSISTENCY : cross-check exit IP/country across
                                    independent "what-is-my-ip" services
          -> Phase 3 GEO-BLOCK   : fetch a real geo-fenced destination site
                                    through the node's own proxy connection
          -> Rename -> Save
Label   : "JP | 06"  (country code + index; client apps already show the
          protocol on their own sub-line, so it's omitted here)
Output  : however many nodes clear the enabled phases -- no fixed quota.

==============================================================================
DESIGN PHILOSOPHY
==============================================================================
Static heuristics (ASN blocklists, community CIDR lists) over-block: whole
datacenters get excluded even though individual IPs inside them are often
clean. Every decision here instead comes from live, per-connection evidence,
checked across three phases of increasing cost and specificity.

  PHASE 1 -- ONLINE: cheapest phase, run first to eliminate dead nodes
  before spending any further effort. Does traffic through this node reach
  the internet at all? Measured via CONNECT_URLS, latency recorded both for
  ordering and as the fallback sort key (see Phase 3 fallback below).

  PHASE 2 -- CONSISTENCY: a real destination site partly judges visitors by
  IP *behavior*, and one behavioral tell is inconsistency -- a shared or
  rotating proxy pool, a broken NAT layer, or a load balancer fronting many
  backends will report DIFFERENT exit IPs/locations depending on which
  service asks; a single dedicated VPS won't. Through the node's own proxy
  connection we query three independent "what is my IP" services (ipinfo.io,
  ip-api.com, Cloudflare's own edge trace) in parallel and require them to
  agree on both the exit IP and the country. Disagreement is itself the
  signal -- free, keyless, no static list needed. Needs at least
  CONSISTENCY_MIN_AGREE successful responses to judge anything; a single
  flaky provider among three doesn't sink a node the other two agree on.

  PHASE 3 -- GEO-BLOCK TEST: the strongest possible evidence a node is
  genuinely useful for a target country isn't a cross-referenced guess --
  it's watching the node try to reach a real service that actively enforces
  regional restrictions and observing whether it gets through. Through the
  node's own proxy connection, we fetch one or more confirmed geo-fenced
  destination sites per country and check the response against a known
  block signature (see GEO_PROBES). Each country's probe list is tried in
  order; a probe is only skipped in favor of its fallback if the REQUEST
  itself fails (timeout/connection error) -- a response that loads, blocked
  or not, is treated as final.

  Endpoints, headers, and match logic are direct translations of the
  verified detector implementations from zhsama/clash-speedtest
  (backend/detectors/{radiko,abema,viu}.go) -- exact URLs and phrase
  matches, not guesses:
    JP -- mintj.com (plain 403) -> radiko.jp/area (cache-busted, checks
          for "OUT" vs "JAPAN" in body) -> api.abema.io JSON API (checks
          '"country":"JP"')
    HK -- viu.com (multi-phrase-group block/allow logic) -> myTV SUPER
          (fallback; sits behind bot-protection that can reject automated
          requests regardless of geolocation, kept only as a second layer)
    SG -- singpass.gov.sg (403 + body phrase) -> mewatch.sg (heuristic)
    VN -- vtvgo.vn (plain 403) -> fptplay.vn (heuristic)

  PHASE 3 FALLBACK: some geo-fenced sites sit behind bot-protection
  (Cloudflare challenge, etc.) that can reject ALL automated requests
  regardless of the visitor's real location -- indistinguishable from a
  true geo-block using only that one site's response. If Phase 3 rejects
  every single Phase-1+2 survivor for a country (zero passes where there
  was a non-empty candidate pool), that is treated as "Phase 3 probe
  unusable this run" rather than "every node is genuinely blocked" --
  the Phase 1+2 survivors are used as the country's output instead,
  sorted by latency, with a clear warning logged so this is never silent.
  This does NOT weaken Phase 3 when it's working: as soon as at least one
  node passes Phase 3 normally, only genuine Phase-3 passers are kept.

All three phases use the same code path for every country (different probe
URLs/thresholds only), so HK/SG/VN get the same quality bar as JP.
==============================================================================
LARGE SOURCES (EbraSha/Epodonios dumps vary in size over time)
==============================================================================
  - Pre-dedup by (scheme, host, port) before classification.
  - GeoIP.resolve() fast-paths raw-IP hosts (no DNS call needed).
  - Classification runs in bounded chunks (gather_chunked) -- predictable
    memory, progress logs instead of a silent multi-minute gap.
  - TEST_POOL_SIZE bounds how many GeoIP-passed candidates enter the real
    network-I/O phases per country -- a runtime safety valve, not a
    quality filter (quality judgment happens inside the phases themselves).
"""

import asyncio, base64, json, logging, os, re, socket
import subprocess, sys, tempfile, time
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import parse_qs, urlparse

import aiohttp, maxminddb
from bs4 import BeautifulSoup


# =============================== 1. CONFIG ==================================

BASE_URL  = "https://www.v2nodes.com"
COUNTRIES = ["hk", "jp", "sg", "vn"]

CC       = {"hk": "HK",        "jp": "JP",   "sg": "SG",   "vn": "VN"}
CC_ALLOW = {"hk": {"HK","CN"}, "jp": {"JP"}, "sg": {"SG"}, "vn": {"VN"}}

# -- External node sources (GitHub-hosted; accessible from Azure/GH Actions
#    IPs, unlike the origin sites which are often Cloudflare-protected).
#    Order: raw.githubusercontent (authoritative) -> jsDelivr CDN (fallback)
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

# -- GeoIP -- country lookup ONLY, no ASN/blocklist reasoning --------------
GEOIP_DB        = os.environ.get("GEOIP_DB", "GeoLite2-Country.mmdb")
DNS_CONCURRENCY = 150
DNS_TIMEOUT     = 5.0

# -- Chunked classification (any source size) ------------------------------
CLASSIFY_CHUNK_SIZE = 4000

# -- Test-pool sizing -- runtime safety valve, NOT a quality filter --------
TEST_POOL_SIZE = int(os.environ.get("TEST_POOL_SIZE", "450"))

# -- HTTP -------------------------------------------------------------------
HTTP_SEM     = 10
HTTP_RETRY   = 3
CONN_TIMEOUT = 12
READ_TIMEOUT = 25

# -- sing-box ----------------------------------------------------------------
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

# -- Phase 2 -- consistency check (country-agnostic, free, keyless) --------
CONSISTENCY_PROVIDERS = ["ipinfo", "ip-api", "cf-trace"]
CONSISTENCY_MIN_AGREE = 2   # need at least this many providers to agree

# -- Phase 3 -- geo-blocking test (empirical, real destination sites) ------
# Fetch the target site through the node's own proxy connection and check
# whether it responds as it would to a genuine local visitor.
#
# Each probe below is a direct Python translation of the verified detector
# logic from zhsama/clash-speedtest (backend/detectors/{radiko,abema,viu}.go),
# provided by the user -- exact endpoints, headers, and match phrases, not
# guesses. Per country, entries are tried in order; a probe is only skipped
# in favor of the next on a REQUEST failure (timeout/connection error).
#
# probe entry fields:
#   url      -- str, or zero-arg callable for URLs needing a fresh value
#               per request (e.g. radiko's cache-busting timestamp)
#   headers  -- dict merged over the default browser headers for this request
#   ok       -- (status:int, body:str) -> bool

GEO_BLOCK_PHRASES = (
    "not available in your region", "not available in your country",
    "content is not available", "geo-restricted", "geo restriction",
    "unavailable in your area", "isn't available in your location",
    "khu vực của bạn",      # VN: "...your region..."
    "không khả dụng",       # VN: "not available"
)


# -- JP: mintj.com (primary) --------------------------------------------------
def _status_403_blocked(status: int, body: str) -> bool:
    """JP (mintj.com) / VN (vtvgo.vn) -- confirmed: plain HTTP 403
    Forbidden for non-local IPs, 200 for genuine local ones."""
    return status != 403


# -- JP: radiko.jp/area (secondary) -- verified via radiko.go ----------------
def _radiko_ok(status: int, body: str) -> bool:
    """
    Verified from backend/detectors/radiko.go:
      - response containing "OUT" (blocked marker, e.g. class="OUT") -> blocked
      - response containing "JAPAN" (e.g. "TOKYO JAPAN") -> allowed
      - anything else -> treated as not-passing (unknown response)
    Checked in this order, matching the reference implementation exactly.
    """
    if "OUT" in body:
        return False
    if "JAPAN" in body:
        return True
    return False


def _radiko_url() -> str:
    """Cache-busting timestamp query param, same pattern as the Go client."""
    return f"https://radiko.jp/area?_={int(time.time() * 1000)}"


# -- JP: api.abema.io (tertiary) -- verified via abema.go, simple JSON API ---
def _abema_ok(status: int, body: str) -> bool:
    """Verified from backend/detectors/abema.go: success iff body contains
    the literal substring '"country":"JP"'."""
    return '"country":"JP"' in body


# -- HK: viu.com (primary) -- verified via viu.go, phrase-group logic --------
def _viu_ok(status: int, body: str) -> bool:
    """
    Verified from backend/detectors/viu.go. Go's switch/fallthrough groups
    translate to: check block-phrase groups first (any hit -> blocked),
    then success-phrase groups (any hit -> allowed), else unknown -> not-passing.
    """
    low = body.lower()
    if any(p in low for p in ("not available", "unavailable", "不可用")):
        return False
    if any(p in low for p in ("blocked", "restricted", "封鎖")):
        return False
    if any(p in low for p in ("geo-blocked", "location", "地區限制")):
        return False
    if any(p in low for p in ("market", "region", "country", "地區")):
        return True
    if any(p in low for p in ("viu", "drama", "劇集", "節目", "streaming")):
        return True
    return False


_VIU_HEADERS = {
    "Accept-Language": "en-US,en;q=0.9,zh-HK;q=0.8,zh;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}
_ABEMA_HEADERS = {"Accept": "application/json"}
_RADIKO_HEADERS = {"X-Requested-With": "XMLHttpRequest"}


# -- HK: myTV SUPER (fallback) -- unverified, kept as resilience layer -------
def _mytvsuper_ok(status: int, body: str) -> bool:
    """
    HK fallback -- myTV SUPER live channel page. CAVEAT: sits behind
    bot-protection that can reject automated requests regardless of
    geolocation. Kept only as a fallback behind the verified Viu probe;
    the Phase 3 fallback (see module docstring) is the ultimate safety
    net if both HK probes turn out unreliable in practice.
    """
    if status in (403, 451):
        return False
    if "此服務不適用於你目前所在地區" in body or "geo_block" in body.lower():
        return False
    return True


def _generic_geo_ok(status: int, body: str) -> bool:
    """Fallback heuristic for sites without a confirmed exact signature."""
    if status in (403, 451):
        return False
    if not body or len(body) < 500:
        return False
    low = body.lower()
    return not any(p.lower() in low for p in GEO_BLOCK_PHRASES)


def _singpass_ok(status: int, body: str) -> bool:
    """SG -- singpass.gov.sg. Confirmed: non-SG IPs get HTTP 403 with body
    "The request could not be satisfied." (CloudFront-style geo block)."""
    if status == 403:
        return False
    if "the request could not be satisfied" in body.lower():
        return False
    return True


# Per country: list of probe dicts tried in order. Only advances to the next
# on a REQUEST failure (timeout/connection error) -- a response that loads
# (blocked or not) is final, not retried against the alternate site.
GEO_PROBES: dict[str, list[dict]] = {
    "HK": [
        {"url": "https://www.viu.com/", "headers": _VIU_HEADERS, "ok": _viu_ok},
        {"url": "https://www.mytvsuper.com/tc/live/81/%E7%BF%A1%E7%BF%A0%E5%8F%B0/",
         "headers": {}, "ok": _mytvsuper_ok},
    ],
    "JP": [
        {"url": "https://mintj.com/",          "headers": {},               "ok": _status_403_blocked},
        {"url": _radiko_url,                    "headers": _RADIKO_HEADERS,  "ok": _radiko_ok},
        {"url": "https://api.abema.io/v1/ip/check?device=android",
         "headers": _ABEMA_HEADERS, "ok": _abema_ok},
    ],
    "SG": [
        {"url": "https://www.singpass.gov.sg/", "headers": {}, "ok": _singpass_ok},
        {"url": "https://www.mewatch.sg",       "headers": {}, "ok": _generic_geo_ok},
    ],
    "VN": [
        {"url": "https://vtvgo.vn",   "headers": {}, "ok": _status_403_blocked},
        {"url": "https://fptplay.vn", "headers": {}, "ok": _generic_geo_ok},
    ],
}

# -- Regex / constants -------------------------------------------------------
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


# =============================== 2. HTTP HELPERS ============================

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
    session: aiohttp.ClientSession, primary: str, fallback: str, name: str,
    validator=None,
) -> Optional[str]:
    """Fetch primary URL; if content fails validator, try fallback."""
    check = validator or (lambda t: bool(_RE_NODE.search(t)))
    text = await http_get(session, primary)
    if text and check(text):
        log.info("[%-10s] OK <- %s", name, primary.split("/")[2])
        return text
    log.info("[%-10s] primary miss -> fallback...", name)
    text = await http_get(session, fallback)
    if text and check(text):
        log.info("[%-10s] OK <- %s", name, fallback.split("/")[2])
        return text
    log.warning("[%-10s] Fetch failed from both sources", name)
    return None


async def gather_chunked(
    items: list, coro_fn, chunk_size: int = CLASSIFY_CHUNK_SIZE, label: str = "",
) -> list:
    """Run coro_fn(item) in bounded chunks -- predictable memory, progress logs."""
    results: list = []
    total = len(items)
    for i in range(0, total, chunk_size):
        chunk = items[i:i + chunk_size]
        results.extend(await asyncio.gather(*[coro_fn(x) for x in chunk]))
        if label and total > chunk_size:
            done = min(i + chunk_size, total)
            log.info("  %-22s %d/%d (%3.0f%%)", label, done, total, 100 * done / total)
    return results


# =============================== 3. GEOIP ====================================

class GeoIP:
    """Country lookup + cached DNS resolution. No quality/blocklist logic --
    quality judgment lives entirely in the 3-phase test (see docstring)."""

    def __init__(self) -> None:
        self._country: Optional[maxminddb.Reader] = None
        self._cache:   dict[str, Optional[str]]   = {}
        self._sem                                  = asyncio.Semaphore(DNS_CONCURRENCY)

        if Path(GEOIP_DB).exists():
            self._country = maxminddb.open_database(GEOIP_DB)
            log.info("GeoLite2-Country: %s", GEOIP_DB)
        else:
            log.warning("GeoLite2-Country not found -- GeoIP filter disabled")

    @property
    def ok(self) -> bool:
        return self._country is not None

    def close(self) -> None:
        if self._country:
            self._country.close()

    async def resolve(self, host: str) -> Optional[str]:
        """Resolve hostname -> IPv4, cached per session. Raw-IP hosts short-circuit."""
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
        """Keep node if DNS resolves AND GeoIP country is in `allowed`. Nothing else."""
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


# =============================== 4. ENDPOINT PARSING =========================

def _b64d(s: str) -> Optional[str]:
    try:
        return base64.b64decode(s + "=" * (-len(s) % 4)).decode("utf-8", errors="ignore")
    except Exception:
        return None


def parse_endpoint(url: str) -> Optional[tuple[str, str, int]]:
    """Return (scheme, host, port) -- used for dedup key and GeoIP resolve."""
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


# =============================== 5. SOURCES ==================================

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
    log.info("[v2nodes  ] %s: %d page(s) -> %d nodes", country.upper(), total, len(nodes))
    return nodes


async def fetch_ebrasha(session: aiohttp.ClientSession, geoip: GeoIP) -> dict[str, list[str]]:
    text = await fetch_with_fallback(session, EBRASHA_RAW, EBRASHA_CDN, "EbraSha")
    if not text:
        return {}

    raw_nodes = [l.strip() for l in text.splitlines() if _RE_NODE.match(l.strip())]
    nodes     = deduplicate(raw_nodes)
    log.info("[EbraSha  ] %d raw -> %d unique -- classifying...", len(raw_nodes), len(nodes))

    ccs = await gather_chunked(nodes, geoip.cc_of_node, label="EbraSha classify")

    result: dict[str, list[str]] = {}
    dropped = 0
    for cc, node in zip(ccs, nodes):
        if cc is None: dropped += 1
        else:          result.setdefault(cc, []).append(node)

    log.info("[EbraSha  ] %d classified, %d dropped (DNS fail)",
             sum(len(v) for v in result.values()), dropped)
    return result


async def fetch_epodonios(session: aiohttp.ClientSession, geoip: GeoIP) -> dict[str, list[str]]:
    text = await fetch_with_fallback(session, EPO_RAW, EPO_CDN, "Epodonios")
    if not text:
        return {}

    raw_nodes = [l.strip() for l in text.splitlines() if _RE_NODE.match(l.strip())]
    nodes     = deduplicate(raw_nodes)
    log.info("[Epodonios] %d raw -> %d unique -- classifying...", len(raw_nodes), len(nodes))

    ccs = await gather_chunked(nodes, geoip.cc_of_node, label="Epodonios classify")

    result: dict[str, list[str]] = {}
    dropped = 0
    for cc, node in zip(ccs, nodes):
        if cc is None: dropped += 1
        else:          result.setdefault(cc, []).append(node)

    log.info("[Epodonios] %d classified, %d dropped (DNS fail)",
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

    result: dict[str, list[str]] = {}
    no_cc = 0
    for node in nodes:
        frag = node.split("#", 1)[1] if "#" in node else ""
        cc   = _opl_cc(frag)
        if cc: result.setdefault(cc, []).append(node)
        else:  no_cc += 1

    log.info("[OPL      ] %d raw -> %s, %d no-CC skipped",
             len(nodes), {k: len(v) for k, v in sorted(result.items())}, no_cc)
    return result


# =============================== 6. DEDUPLICATION ============================

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


# =============================== 7. SING-BOX PARSERS =========================

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


# =============================== 8. SING-BOX VALIDATE ========================

async def _sb_check_node(ob: dict, sem: asyncio.Semaphore) -> bool:
    """Dry-run 'sing-box check' -- catches malformed configs before they can
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


# =============================== 9. PHASE 1 -- ONLINE ========================

async def phase1_online(port: int, sem: asyncio.Semaphore) -> Optional[float]:
    """Does traffic through this node reach the internet? Returns latency
    (ms) on the first successful CONNECT_URLS response, else None."""
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


# =============================== 10. PHASE 2 -- CONSISTENCY ==================
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


async def phase2_consistency(port: int, target_cc: str, sem: asyncio.Semaphore) -> bool:
    """
    PHASE 2. Query all CONSISTENCY_PROVIDERS through the node's proxy in
    parallel. Passes if at least CONSISTENCY_MIN_AGREE of them report BOTH
    the same exit IP and the same country, matching target_cc.
    """
    proxy_url = f"http://127.0.0.1:{port}"
    async with sem:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as s:
            results = await asyncio.gather(*[
                _PROBE_FUNCS[name](s, proxy_url) for name in CONSISTENCY_PROVIDERS
            ])

    successes = [r for r in results if r is not None]
    if len(successes) < CONSISTENCY_MIN_AGREE:
        return False

    ips = {ip for ip, _ in successes}
    ccs = {cc for _, cc in successes}
    return len(ips) == 1 and len(ccs) == 1 and next(iter(ccs)) == target_cc


# =============================== 11. PHASE 3 -- GEO-BLOCK TEST ===============

async def phase3_geo_block(port: int, cc: str, sem: asyncio.Semaphore) -> bool:
    """
    PHASE 3. Try each configured probe for `cc` in order; only advance to
    the next on a REQUEST failure (timeout/connection error). A country
    with no configured probe passes automatically. If every probe fails to
    respond at all, returns False (fail-closed) -- see health_check() for
    the run-level fallback that engages when this happens to ALL survivors.
    """
    probes = GEO_PROBES.get(cc)
    if not probes:
        return True

    proxy_url = f"http://127.0.0.1:{port}"
    to = aiohttp.ClientTimeout(total=TEST_TIMEOUT)

    async with sem:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as s:
            for probe in probes:
                url = probe["url"]() if callable(probe["url"]) else probe["url"]
                headers = {**_HDR, **probe.get("headers", {})}
                try:
                    async with s.get(url, headers=headers, proxy=proxy_url, timeout=to,
                                     allow_redirects=True) as r:
                        body = await r.text(errors="ignore")
                        return probe["ok"](r.status, body)
                except Exception:
                    continue   # this probe unreachable -- try the next one
    return False   # every configured probe failed to even respond


# =============================== 12. HEALTH CHECK (Phase 1+2+3) ==============

async def _run_batch(
    batch: list[tuple[str, dict, int]], cc: str, sem: asyncio.Semaphore,
) -> tuple[list[Optional[float]], list[Optional[float]]]:
    """
    Starts sing-box for the batch, runs Phase 1, then (for survivors)
    Phase 2, then (for THOSE survivors) Phase 3.
    Returns two parallel lists (batch order):
      p12_latencies -- latency if node cleared Phase 1+2, else None
                       (this is the Phase-3-fallback candidate pool)
      final_latencies -- latency if node cleared Phase 1+2+3, else None
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
            log.warning("sing-box crashed rc=%d: %s", proc.returncode, err[:200])
            empty = [None] * len(batch)
            return empty, empty

        # Phase 1
        p1 = await asyncio.gather(*[phase1_online(port, sem) for _,_,port in batch])

        # Phase 2 -- only for Phase 1 survivors
        async def _p2(port, lat):
            if lat is None:
                return False
            return await phase2_consistency(port, cc, sem)

        p2_ok = await asyncio.gather(*[
            _p2(port, lat) for (_,_,port), lat in zip(batch, p1)
        ])
        p12_latencies = [lat if (lat is not None and ok) else None
                          for lat, ok in zip(p1, p2_ok)]

        # Phase 3 -- only for Phase 1+2 survivors
        async def _p3(port, lat):
            if lat is None:
                return False
            return await phase3_geo_block(port, cc, sem)

        p3_ok = await asyncio.gather(*[
            _p3(port, lat) for (_,_,port), lat in zip(batch, p12_latencies)
        ])
        final_latencies = [lat if (lat is not None and ok) else None
                            for lat, ok in zip(p12_latencies, p3_ok)]

        proc.terminate()
        try:    proc.wait(timeout=3)
        except Exception: proc.kill()

        return p12_latencies, final_latencies
    finally:
        try: os.unlink(tmp.name)
        except Exception: pass


async def health_check(nodes: list[str], cc: str) -> list[str]:
    """
    Runs Phase 1 -> 2 -> 3, funnel-style, with per-phase counts logged.
    If Phase 3 rejects EVERY Phase-1+2 survivor for this country (and there
    was at least one), falls back to the Phase-1+2 result set instead of
    returning empty -- see module docstring "PHASE 3 FALLBACK". Either way,
    the returned list is sorted by latency (fastest first).
    """
    if not Path(SINGBOX).exists():
        log.warning("sing-box not found -- skipping health check")
        return nodes

    candidates = [(url, ob, BASE_PORT + i)
                  for i, url in enumerate(nodes)
                  if (ob := to_singbox(url, f"n{i}")) is not None]
    parse_skip = len(nodes) - len(candidates)
    if parse_skip:
        log.info("  Parse skip     : %d (ssr / unknown scheme)", parse_skip)

    val_sem    = asyncio.Semaphore(SB_VAL_SEM)
    valid_mask = await gather_chunked(
        candidates, lambda c: _sb_check_node(c[1], val_sem), label="sing-box validate"
    )
    valid = [c for c, ok in zip(candidates, valid_mask) if ok]
    log.info("  Config valid   : %d/%d", len(valid), len(candidates))
    if not valid:
        return []

    test_sem = asyncio.Semaphore(TEST_SEM)
    batches  = [valid[i:i+BATCH_SIZE] for i in range(0, len(valid), BATCH_SIZE)]
    scored_p12:   list[tuple[str, float]] = []
    scored_final: list[tuple[str, float]] = []

    for i, batch in enumerate(batches, 1):
        t0 = time.monotonic()
        p12_lat, final_lat = await _run_batch(batch, cc, test_sem)
        p12_ok   = [(url, lat) for (url,_,_), lat in zip(batch, p12_lat)   if lat is not None]
        final_ok = [(url, lat) for (url,_,_), lat in zip(batch, final_lat) if lat is not None]
        scored_p12.extend(p12_ok)
        scored_final.extend(final_ok)
        log.info("  Batch %2d/%-2d    : phase1+2=%2d/%2d  phase3=%2d/%2d  (%.1fs)",
                 i, len(batches), len(p12_ok), len(batch),
                 len(final_ok), len(p12_ok), time.monotonic()-t0)

    scored_p12.sort(key=lambda x: x[1])
    scored_final.sort(key=lambda x: x[1])
    log.info("  Phase 1+2 (online+consistent): %d/%d", len(scored_p12), len(valid))
    log.info("  Phase 3   (geo-block)        : %d/%d", len(scored_final), len(scored_p12))

    if not scored_final and scored_p12:
        log.warning("  Phase 3 probe rejected ALL %d Phase-1+2 survivors for %s -- "
                     "treating as a probe failure, not a genuine geo-block. "
                     "Falling back to Phase 1+2 results.", len(scored_p12), cc)
        return [url for url, _ in scored_p12]

    return [url for url, _ in scored_final]


# =============================== 13. RENAME ===================================

def rename_nodes(nodes: list[str], cc: str) -> list[str]:
    """Format: "JP | 06" -- CC + zero-padded index. vmess: update "ps" field
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


# =============================== 14. PIPELINE ================================

async def process_country(
    session:  aiohttp.ClientSession,
    country:  str,
    http_sem: asyncio.Semaphore,
    geoip:    GeoIP,
    ebrasha:  dict[str, list[str]],
    epo:      dict[str, list[str]],
    opl:      dict[str, list[str]],
) -> None:
    cc      = CC[country]
    allowed = CC_ALLOW[country]
    t_start = time.monotonic()
    log.info("+--- %s %s", cc, "-" * 58)

    # 1. Collect from 4 sources
    v2n     = await scrape_v2nodes(session, country, http_sem)
    ext_eba = [n for acc in allowed for n in ebrasha.get(acc, [])]
    ext_epo = [n for acc in allowed for n in epo.get(acc, [])]
    ext_opl = [n for acc in allowed for n in opl.get(acc, [])]
    nodes   = v2n + ext_eba + ext_epo + ext_opl
    log.info("| Collected     : %d  (v2nodes=%d ebrasha=%d epodonios=%d opl=%d)",
             len(nodes), len(v2n), len(ext_eba), len(ext_epo), len(ext_opl))

    # 2. Dedup
    nodes = deduplicate(nodes)
    log.info("| After dedup   : %d", len(nodes))

    # 3. GeoIP country filter ONLY
    if geoip.ok:
        before = len(nodes)
        nodes  = await geoip.filter(nodes, allowed)
        log.info("| GeoIP {%s}: %d/%d", "/".join(sorted(allowed)), len(nodes), before)
    else:
        log.warning("| No GeoIP DB -- skipping filter")
    if not nodes:
        log.warning("| No nodes after GeoIP filter")
        log.info("+%s\n", "-" * 63)
        return

    # 4. Runtime safety cap -- NOT a quality filter, see module docstring
    if len(nodes) > TEST_POOL_SIZE:
        log.info("| Pool cap      : %d -> testing first %d", len(nodes), TEST_POOL_SIZE)
        nodes = nodes[:TEST_POOL_SIZE]

    # 5. Three-phase empirical test -- ONLINE -> CONSISTENCY -> GEO-BLOCK
    log.info("| Testing %d candidates (Phase 1 online -> 2 consistency -> 3 geo-block)...",
             len(nodes))
    live = await health_check(nodes, cc)
    if not live:
        log.warning("| No nodes cleared the test phases")
        log.info("+%s\n", "-" * 63)
        return

    # 6. Rename + save
    renamed = rename_nodes(live, cc)
    Path(f"{country}_sub.txt").write_text("\n".join(renamed), encoding="utf-8")

    elapsed = time.monotonic() - t_start
    log.info("| Result        : %d healthy node(s) -> %s_sub.txt  (%.0fs)",
             len(renamed), country, elapsed)
    for r in renamed[:3]:
        log.info("|   - %s", r.split("#")[-1] if "#" in r else r)
    if len(renamed) > 3:
        log.info("|   - ... (+%d more)", len(renamed) - 3)
    log.info("+%s\n", "-" * 63)


# =============================== 15. MAIN =====================================

async def main() -> None:
    t0 = time.monotonic()
    log.info("=" * 68)
    log.info(" get_sub.py -- v2nodes / EbraSha / Epodonios / OPL")
    log.info(" Phase 1: ONLINE (connectivity)")
    log.info(" Phase 2: CONSISTENCY (%s)", "/".join(CONSISTENCY_PROVIDERS))
    log.info(" Phase 3: GEO-BLOCK TEST (real geo-fenced destination sites)")
    log.info(" Fallback: if Phase 3 rejects every Phase-1+2 survivor for a")
    log.info("           country, that country falls back to Phase 1+2 results")
    log.info(" Output : no fixed quota -- every node clearing the enabled phases")
    log.info("=" * 68)

    geoip    = GeoIP()
    http_sem = asyncio.Semaphore(HTTP_SEM)
    conn     = aiohttp.TCPConnector(limit=30, ssl=False, ttl_dns_cache=300)

    try:
        async with aiohttp.ClientSession(
            connector=conn, cookie_jar=aiohttp.CookieJar(unsafe=True)
        ) as session:
            ebrasha, epo, opl = await asyncio.gather(
                fetch_ebrasha(session, geoip),
                fetch_epodonios(session, geoip),
                fetch_opl(session),
            )
            log.info("External sources loaded:")
            log.info("  EbraSha   : %s", {k: len(v) for k,v in sorted(ebrasha.items())})
            log.info("  Epodonios : %s", {k: len(v) for k,v in sorted(epo.items())})
            log.info("  OPL       : %s", {k: len(v) for k,v in sorted(opl.items())})
            log.info("")

            for country in COUNTRIES:
                await process_country(session, country, http_sem, geoip, ebrasha, epo, opl)
    finally:
        geoip.close()

    log.info("=" * 68)
    log.info(" Done in %.0fs", time.monotonic() - t0)
    log.info("=" * 68)


if __name__ == "__main__":
    asyncio.run(main())
