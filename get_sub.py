"""
get_sub.py — Multi-source VPN node collector
══════════════════════════════════════════════════════════════════════════════
Sources  : v2nodes.com (scrape) · EbraSha · Epodonios · OpenProxyList (GitHub)
Pipeline : Collect → Dedup → GeoIP filter → sing-box validate → sing-box test
           → (empirical geo-probe, if available) → Rename → Save
Label    : "JP | 06"  (CC + index, no protocol — clients display it already)

──────────────────────────────────────────────────────────────────────────────
QUALITY FILTERING — two complementary layers
──────────────────────────────────────────────────────────────────────────────
Problem: a node's IP can be geolocated correctly (e.g. "JP") by GeoIP yet
still get blocked by real JP-only websites, because those sites use their
own IP-intelligence (not GeoIP) to flag datacenter/CDN/VPN addresses.

Layer 1 — Static heuristic (GeoIP.is_low_quality), applied to ALL countries:
  • Hyperscale-CDN ASN check (curated, ~30 entries: Cloudflare/AWS/GCP/
    Azure/...) — ALWAYS applied. Fronting through these causes genuine
    geolocation/attribution mismatches, a distinct & well-understood issue.
  • X4BNet output/vpn/ipv4.txt ("strictly just known VPN networks") — a
    community-maintained, auto-updated CIDR list. Applied ONLY to countries
    that lack an empirical probe (Layer 2), because it's a heuristic that
    can false-positive on legitimate datacenters the target sites don't
    actually block. We deliberately do NOT use X4BNet's broader
    "datacenter" list (input/datacenter/ASN.txt, output/datacenter/ipv4.txt)
    — that one covers "anything that is not an eyeball network", i.e. ALL
    VPS/hosting, which would reject virtually every self-hosted node.

Layer 2 — Empirical geo-probe (GEO_PROBES), applied per country if defined:
  Instead of guessing via static lists, verify DIRECTLY through the node's
  own proxy connection against a real geo-fenced local service. A node that
  passes is PROVEN to behave like a genuine local connection for at least
  one major real-world service — much stronger evidence than any blocklist,
  and immune to false positives on datacenters that aren't actually flagged.

  Currently configured:
    JP → radiko.jp/area (Japan's IP-simulcast radio; actively detects and
         blocks VPN/proxy/foreign IPs). Returns an area code like "JP13"
         for genuine Japan IPs, "OUT" for blocked ones. Free, no auth,
         single lightweight GET request.

  HK / SG / VN: no free, no-auth, simple-GET geo-fenced probe has been
  verified yet (candidates like myTV SUPER/ViuTV require complex login
  flows) — these countries currently rely on Layer 1 only. Add an entry
  to GEO_PROBES to extend once a suitable service is found; no other code
  changes needed — process_country() and health_check() already handle
  "probe not configured" gracefully.

──────────────────────────────────────────────────────────────────────────────
SCALING FOR LARGE SOURCES (EbraSha/Epodonios can list 100k+ raw lines)
──────────────────────────────────────────────────────────────────────────────
  1. Pre-dedup by (scheme, host, port) BEFORE classification — a source
     dump this size is heavily duplicated (same infra, many labels/UUIDs);
     typically removes >60% of raw lines before any network call is made.
  2. GeoIP.resolve() fast-paths raw-IP hosts (no DNS call, just a local
     socket.inet_pton check) — on a typical dump the large majority of
     unique hosts are literal IPs, so only a minority needs a real
     DNS round-trip. DNS_CONCURRENCY only gates that minority.
  3. Classification/filtering run through gather_chunked() instead of one
     giant asyncio.gather — bounds peak memory (predictable number of live
     Task objects at a time) and logs progress on steps that can otherwise
     run silently for minutes at this scale.
  4. MAX_TEST_CANDIDATES caps how many GeoIP-passed nodes enter sing-box
     testing per country. Testing is real network I/O (each batch spins
     up a live proxy and makes HTTP requests); without a cap, an
     unusually large source could make one country's test phase run for
     hours. Source order is preserved end-to-end, so the cap naturally
     favors the curated/labeled sources (v2nodes, OPL) over the raw dump.
"""

import asyncio, base64, bisect, ipaddress, json, logging, os, re, socket
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
# NOTE: this source has grown from a small curated list to a large-scale
# aggregator (~245k lines as of the last check, ~96k after endpoint dedup).
# See LARGE-SCALE HANDLING below — the same pre-dedup/chunk/cap strategy
# used for Epodonios now applies here too.

EPO_RAW = ("https://raw.githubusercontent.com/Epodonios/v2ray-configs"
           "/refs/heads/main/All_Configs_Sub.txt")
EPO_CDN = "https://cdn.jsdelivr.net/gh/Epodonios/v2ray-configs@main/All_Configs_Sub.txt"

# roosterkid/openproxylist = official GitHub mirror of openproxylist.com
# (the .com domain itself is Cloudflare-protected and blocks Azure IPs)
OPL_RAW = "https://raw.githubusercontent.com/roosterkid/openproxylist/main/V2RAY_RAW.txt"
OPL_CDN = "https://cdn.jsdelivr.net/gh/roosterkid/openproxylist@main/V2RAY_RAW.txt"

# X4BNet known-VPN-network CIDR list (see module docstring, Layer 1)
X4B_VPN_RAW = "https://raw.githubusercontent.com/X4BNet/lists_vpn/main/output/vpn/ipv4.txt"
X4B_VPN_CDN = "https://cdn.jsdelivr.net/gh/X4BNet/lists_vpn@main/output/vpn/ipv4.txt"

# ── GeoIP ────────────────────────────────────────────────────────────────────
GEOIP_DB        = os.environ.get("GEOIP_DB",     "GeoLite2-Country.mmdb")
GEOIP_ASN_DB    = os.environ.get("GEOIP_ASN_DB", "GeoLite2-ASN.mmdb")
# 150 concurrent DNS lookups is cheap — the fast-path for raw-IP hosts
# (~75% of a typical large source, see GeoIP.resolve) never touches this
# semaphore at all, so it only gates genuine hostname resolution.
DNS_CONCURRENCY = 150
DNS_TIMEOUT     = 5.0

# ── Large-source scaling (EbraSha/Epodonios can list 100k+ raw lines) ───────
# Classifying every node means resolving DNS for every unique host — done in
# bounded chunks (not one giant asyncio.gather) to keep memory predictable
# and give progress visibility on sources that take minutes to classify.
CLASSIFY_CHUNK_SIZE   = 4000
# Hard cap on how many GeoIP-passed candidates enter sing-box testing per
# country. Testing is real network I/O (each batch spins up a live proxy
# and makes HTTP requests) — without a cap, a source dump of 100k+ nodes
# could make one country's test phase run for hours. Ordering is preserved
# (v2nodes/OPL nodes are collected first, see process_country), so the cap
# naturally prioritizes the curated/labeled sources over the raw dump.
MAX_TEST_CANDIDATES  = 300

# Fallback IP-prefix detection when GeoLite2-ASN unavailable (legacy, coarse)
_CDN_PREFIXES = (

    "104.16.","104.17.","104.18.","104.19.","104.20.","104.21.",
    "104.22.","104.23.","104.24.","104.25.","104.26.","104.27.",
    "108.162.","141.101.","162.158.",
    "172.64.","172.65.","172.66.","172.67.",
    "173.245.","188.114.","190.93.","197.234.","198.41.",
    "151.101.","199.232.",
)

# Curated hyperscale-cloud / CDN ASN blocklist — always applied (Layer 1).
# Fronting through these causes genuine geolocation/attribution mismatches.
_ASN_NUMBERS_LOWQ: frozenset[int] = frozenset({
    13335, 209242,          # Cloudflare
    54113,                  # Fastly
    16509, 14618,           # Amazon AWS / EC2
    15169, 396982,          # Google / Google Cloud
    8075,   8068,           # Microsoft Azure
    37963, 45102,           # Alibaba Cloud
    132203, 59019,          # Tencent Cloud
    31898, 20473,           # Vultr / Choopa
    14061,                  # DigitalOcean
    16276,                  # OVH
    24940,                  # Hetzner
    63949,                  # Linode / Akamai Connected Cloud
    12876,                  # Scaleway
    51167,                  # Contabo
    9009,                   # M247
    36351, 62240, 33182,    # SoftLayer/IBM Cloud, Clouvider, Psychz
    203020,                 # QuadraNet
    46844,                  # Sharktech
})
_ASN_ORG_KEYWORDS_LOWQ = (
    "cloudflare", "fastly", "akamai",
    "amazon", "aws", "google", "microsoft", "azure",
    "alibaba", "tencent", "oracle cloud",
    "digitalocean", "vultr", "choopa", "ovh", "hetzner", "linode",
    "scaleway", "contabo", "leaseweb", "m247", "quadranet",
    "zenlayer", "psychz", "g-core", "gcore", "colocrossing",
    "hostroyale", "vdsina", "hivelocity", "cloudsigma", "upcloud",
    "kamatera", "bunnycdn", "stackpath", "cachefly", "limelight",
    "edgecast", "incapsula", "imperva", "sharktech", "servers.com",
)

# ── Empirical geo-fence probes (Layer 2) — see module docstring ─────────────
# Each probe: GET `url` through the node's own proxy, decide pass/fail from
# the response body. `ok(body) -> bool`.
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
    """
    Fetch primary URL; if content fails validator, try fallback.
    validator defaults to "_RE_NODE found somewhere" (node-list sources);
    pass a custom callable for other formats (e.g. CIDR blocklists).
    """
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
    giant asyncio.gather. Needed once a source scales into the tens/hundreds
    of thousands of lines (e.g. EbraSha's full dump): keeps peak memory
    predictable (bounded number of live Task objects at a time) and gives
    progress feedback on a step that can otherwise run silently for minutes.
    """
    results: list = []
    total = len(items)
    for i in range(0, total, chunk_size):
        chunk = items[i:i + chunk_size]
        results.extend(await asyncio.gather(*[coro_fn(x) for x in chunk]))
        if label:
            done = min(i + chunk_size, total)
            log.info("  %s: %d/%d (%.0f%%)", label, done, total, 100 * done / total)
    return results


# ═══════════════════════════════ 3. CIDR LOOKUP ════════════════════════════

class CIDRSet:
    """
    Efficient "IP ∈ any of N CIDR ranges" lookup — O(log n) per query.
    Ranges are merged (overlaps collapsed) after sorting, so a single
    bisect + boundary check is always correct.
    """
    __slots__ = ("_starts", "_ranges")

    def __init__(self, cidrs: list[str]) -> None:
        raw: list[tuple[int, int]] = []
        for c in cidrs:
            c = c.strip()
            if not c or c.startswith("#"):
                continue
            try:
                net = ipaddress.ip_network(c, strict=False)
                if net.version == 4:
                    raw.append((int(net.network_address), int(net.broadcast_address)))
            except ValueError:
                continue

        raw.sort()
        merged: list[tuple[int, int]] = []
        for start, end in raw:
            if merged and start <= merged[-1][1] + 1:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))

        self._ranges = merged
        self._starts = [r[0] for r in merged]

    def __len__(self) -> int:
        return len(self._ranges)

    def __contains__(self, ip: str) -> bool:
        try:
            ip_int = int(ipaddress.ip_address(ip))
        except ValueError:
            return False
        idx = bisect.bisect_right(self._starts, ip_int) - 1
        if idx < 0:
            return False
        start, end = self._ranges[idx]
        return start <= ip_int <= end


# ═══════════════════════════════ 4. GEOIP ══════════════════════════════════

class GeoIP:
    """
    Country lookup + quality filter (Layer 1) + cached DNS resolution.
    See module docstring for the full quality-filtering design rationale.
    """

    def __init__(self) -> None:
        self._country: Optional[maxminddb.Reader] = None
        self._asn:     Optional[maxminddb.Reader] = None
        self._cache:   dict[str, Optional[str]]   = {}
        self._sem                                  = asyncio.Semaphore(DNS_CONCURRENCY)
        self._vpn_ranges: Optional[CIDRSet]        = None  # X4BNet known-VPN networks

        if Path(GEOIP_DB).exists():
            self._country = maxminddb.open_database(GEOIP_DB)
            log.info("GeoLite2-Country: %s", GEOIP_DB)
        else:
            log.warning("GeoLite2-Country không tìm thấy — GeoIP filter bị bỏ qua")

        if Path(GEOIP_ASN_DB).exists():
            self._asn = maxminddb.open_database(GEOIP_ASN_DB)
            log.info("GeoLite2-ASN: %s", GEOIP_ASN_DB)
        else:
            log.warning("GeoLite2-ASN không tìm thấy — quality check dùng IP-prefix fallback")

    @property
    def ok(self) -> bool:
        return self._country is not None

    def close(self) -> None:
        if self._country: self._country.close()
        if self._asn:     self._asn.close()

    async def load_quality_lists(self, session: aiohttp.ClientSession) -> None:
        """Fetch X4BNet known-VPN CIDR list once. Network failure = graceful skip."""
        text = await fetch_with_fallback(
            session, X4B_VPN_RAW, X4B_VPN_CDN, "X4B-VPN",
            validator=lambda t: bool(re.search(r'\d+\.\d+\.\d+\.\d+/\d+', t)),
        )
        if text:
            self._vpn_ranges = CIDRSet([l for l in text.splitlines() if l.strip()])
            log.info("X4B-VPN: %d known-VPN ranges đã nạp", len(self._vpn_ranges))

    async def resolve(self, host: str) -> Optional[str]:
        """Resolve hostname → IPv4, result cached per session (shared across sources)."""
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

    def _is_hyperscale_cdn(self, ip: str) -> bool:
        """Curated ASN check — always applied, regardless of empirical probes."""
        if self._asn:
            try:
                rec = self._asn.get(ip) or {}
                num = rec.get("autonomous_system_number", 0)
                org = (rec.get("autonomous_system_organization") or "").lower()
                if num in _ASN_NUMBERS_LOWQ:
                    return True
                if any(kw in org for kw in _ASN_ORG_KEYWORDS_LOWQ):
                    return True
                return False
            except Exception:
                pass
        return ip.startswith(_CDN_PREFIXES)

    def is_low_quality(self, ip: str, *, use_vpn_heuristic: bool = True) -> bool:
        """
        True nếu IP nên bị loại.
          • Hyperscale-CDN ASN: LUÔN kiểm tra (Layer 1, phần cố định).
          • X4BNet known-VPN CIDR: chỉ áp dụng khi use_vpn_heuristic=True
            — set False cho các country ĐÃ có empirical probe (GEO_PROBES),
            vì probe thực nghiệm là bằng chứng đáng tin hơn heuristic tĩnh.
        """
        if self._is_hyperscale_cdn(ip):
            return True
        if use_vpn_heuristic and self._vpn_ranges and ip in self._vpn_ranges:
            return True
        return False

    def country_of(self, ip: str) -> Optional[str]:
        if not self._country:
            return None
        try:
            return (self._country.get(ip) or {}).get("country", {}).get("iso_code")
        except Exception:
            return None

    async def cc_of_node(self, url: str) -> Optional[str]:
        """
        Return CC để phân loại (dùng cho EbraSha/Epodonios — chưa biết
        country đích lúc này). Chỉ loại sớm qua hyperscale-CDN check
        (luôn đúng bất kể country); KHÔNG áp dụng X4B VPN heuristic ở đây
        — quyết định "quality" cuối cùng thuộc về filter() sau khi đã biết
        country đích và có thể chọn use_vpn_heuristic phù hợp.
        """
        ep = parse_endpoint(url)
        if not ep:
            return None
        ip = await self.resolve(ep[1])
        if not ip:
            return None
        if self._is_hyperscale_cdn(ip):
            return None
        return self.country_of(ip)

    async def filter(
        self, nodes: list[str], allowed: set[str], *, use_vpn_heuristic: bool = True
    ) -> list[str]:
        """Keep node if not low-quality AND GeoIP country ∈ allowed. Drop DNS-fail."""
        async def _keep(url: str) -> bool:
            ep = parse_endpoint(url)
            if not ep:
                return True
            ip = await self.resolve(ep[1])
            if ip is None:
                return False
            if self.is_low_quality(ip, use_vpn_heuristic=use_vpn_heuristic):
                return False
            return self.country_of(ip) in allowed

        mask = await gather_chunked(nodes, _keep)
        return [n for n, keep in zip(nodes, mask) if keep]


# ═══════════════════════════════ 5. ENDPOINT PARSING ═══════════════════════

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


# ═══════════════════════════════ 6. SOURCES ═════════════════════════════════
# Each fetch_* function returns node URLs, either as a flat list (v2nodes)
# or pre-bucketed by country {CC: [nodes]} (EbraSha/Epodonios/OPL).

# ── 6a. v2nodes.com (scrape, always country-accurate — no classification) ──

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


# ── 6b. EbraSha (no CC in label → full GeoIP classify) ──────────────────────

async def fetch_ebrasha(
    session: aiohttp.ClientSession, geoip: GeoIP
) -> dict[str, list[str]]:
    """
    EbraSha's full dump can list 100k+ raw lines (community aggregator,
    heavy duplication). Same large-scale handling as fetch_epodonios:
      • Pre-dedup by (scheme, host, port) BEFORE classify — typically
        removes >60% of raw lines on the full dump, so DNS/GeoIP work
        scales with unique endpoints, not raw line count.
      • Classify in bounded chunks (gather_chunked) instead of one giant
        gather — keeps memory predictable and logs progress on a step
        that can take minutes at this scale.
      • DNS cache in GeoIP is shared across ALL sources — repeated
        hostnames (common CDN edges, shared infra) cost one lookup total.
      • GeoIP.resolve() fast-paths raw-IP hosts (no network call at all);
        on a typical large dump the majority of unique hosts are literal
        IPs, so only a minority actually needs a DNS round-trip.
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

    log.info("[EbraSha] %d classified · %d dropped (DNS fail / hyperscale-CDN)",
             sum(len(v) for v in result.values()), dropped)
    return result


# ── 6c. Epodonios (~6000+ nodes, no CC in label → full GeoIP classify) ──────

async def fetch_epodonios(
    session: aiohttp.ClientSession, geoip: GeoIP
) -> dict[str, list[str]]:
    """Same large-scale handling as fetch_ebrasha — see its docstring."""
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

    log.info("[Epodonios] %d classified · %d dropped (DNS fail / hyperscale-CDN)",
             sum(len(v) for v in result.values()), dropped)
    return result


# ── 6d. OpenProxyList (CC embedded in label → no GeoIP needed here) ─────────

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


# ═══════════════════════════════ 7. DEDUPLICATION ══════════════════════════

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


# ═══════════════════════════════ 8. SING-BOX PARSERS ═══════════════════════
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


# ═══════════════════════════════ 9. SING-BOX VALIDATE ══════════════════════

async def _sb_check_node(ob: dict, sem: asyncio.Semaphore) -> bool:
    """
    Run 'sing-box check' on a minimal config with just this outbound.
    Dry-run (no port binding). Catches invalid SNI, bad reality keys, etc.
    BEFORE the node reaches a shared batch — a single bad config would
    otherwise crash the whole batch's connectivity test.
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


# ═══════════════════════════════ 10. SING-BOX TEST ═════════════════════════

async def _connect_test(port: int, sem: asyncio.Semaphore) -> bool:
    """Basic reachability: does traffic through this node reach the internet?"""
    async with sem:
        to = aiohttp.ClientTimeout(total=TEST_TIMEOUT)
        for url in CONNECT_URLS:
            try:
                async with aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(ssl=False)
                ) as s:
                    async with s.get(url, proxy=f"http://127.0.0.1:{port}",
                                     timeout=to, allow_redirects=True) as r:
                        if r.status in (200, 204): return True
            except Exception:
                continue
        return False


async def _geo_probe_test(port: int, cc: str, sem: asyncio.Semaphore) -> bool:
    """
    Empirical Layer-2 check (see module docstring). Returns True if no probe
    is configured for `cc` (nothing to disprove — don't penalize), or if the
    probe response indicates the node is accepted as a genuine local IP.
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


async def _run_batch(
    batch: list[tuple[str, dict, int]], cc: str, sem: asyncio.Semaphore
) -> list[bool]:
    """
    Start sing-box for this batch, run connectivity test, then (only for
    nodes that passed) the empirical geo-probe. A node must pass BOTH.
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
            return [False] * len(batch)

        connected = await asyncio.gather(*[_connect_test(p, sem) for _,_,p in batch])

        # Only geo-probe nodes that are actually reachable — saves requests
        probe_tasks = [
            _geo_probe_test(port, cc, sem) if alive else _false()
            for (_,_,port), alive in zip(batch, connected)
        ]
        probed = await asyncio.gather(*probe_tasks)

        proc.terminate()
        try:    proc.wait(timeout=3)
        except Exception: proc.kill()

        return [c and p for c, p in zip(connected, probed)]
    finally:
        try: os.unlink(tmp.name)
        except Exception: pass


async def _false() -> bool:
    """Trivial coroutine returning False — placeholder for skipped geo-probes."""
    return False


async def health_check(nodes: list[str], cc: str) -> list[str]:
    """
    1. Parse all nodes → sing-box outbound configs
    2. Per-node 'sing-box check' (concurrent, no network) — filters invalid
       configs before they can crash a shared batch
    3. Batch test: connectivity + (if configured) empirical geo-probe
    """
    if not Path(SINGBOX).exists():
        log.warning("sing-box not found → skipping health check")
        return nodes

    candidates = [(url, ob, BASE_PORT + i)
                  for i, url in enumerate(nodes)
                  if (ob := to_singbox(url, f"n{i}")) is not None]
    parse_skip = len(nodes) - len(candidates)
    if parse_skip:
        log.info("  Parse skip: %d (ssr / unknown scheme)", parse_skip)

    val_sem    = asyncio.Semaphore(SB_VAL_SEM)
    valid_mask = await asyncio.gather(*[
        _sb_check_node(ob, val_sem) for _, ob, _ in candidates
    ])
    valid     = [(url, ob, port) for (url, ob, port), ok in zip(candidates, valid_mask) if ok]
    inv_count = len(candidates) - len(valid)
    if inv_count:
        log.info("  Config invalid (sing-box check): %d nodes removed", inv_count)
    log.info("  Validated: %d nodes ready for testing", len(valid))
    if not valid:
        return []

    has_probe = cc in GEO_PROBES
    log.info("  Testing: connectivity%s", " + geo-probe (" + GEO_PROBES[cc]["url"] + ")"
             if has_probe else " only (no geo-probe configured for " + cc + ")")

    test_sem = asyncio.Semaphore(TEST_SEM)
    batches  = [valid[i:i+BATCH_SIZE] for i in range(0, len(valid), BATCH_SIZE)]
    online: list[str] = []
    for i, batch in enumerate(batches, 1):
        t0      = time.monotonic()
        results = await _run_batch(batch, cc, test_sem)
        ok      = sum(results)
        log.info("  Batch %d/%d: %d/%d online (%.1fs)",
                 i, len(batches), ok, len(batch), time.monotonic()-t0)
        online.extend(url for (url,_,_), alive in zip(batch, results) if alive)

    return online


# ═══════════════════════════════ 11. RENAME ═════════════════════════════════

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


# ═══════════════════════════════ 12. PIPELINE ═══════════════════════════════

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

    # 3. GeoIP + quality filter — skip the X4B VPN heuristic if we have an
    #    empirical probe for this country (Layer 2 will verify directly)
    if geoip.ok:
        before = len(nodes)
        nodes  = await geoip.filter(nodes, allowed, use_vpn_heuristic=not has_probe)
        log.info("[%s] After GeoIP+quality {%s}: %d/%d",
                 cc, "/".join(sorted(allowed)), len(nodes), before)
    else:
        log.warning("[%s] No GeoIP DB — skipping filter", cc)
    if not nodes:
        log.warning("[%s] No nodes after GeoIP filter", cc)
        return

    # 3.5. Cap candidates entering sing-box test — bounds worst-case test
    #      time regardless of how large the raw sources are. Order is
    #      preserved from step 1 (v2nodes/OPL first), so capping naturally
    #      favors the curated/labeled sources over the raw EbraSha/Epodonios
    #      dump while still including some of it.
    if len(nodes) > MAX_TEST_CANDIDATES:
        log.info("[%s] %d candidates > cap %d — testing first %d only",
                 cc, len(nodes), MAX_TEST_CANDIDATES, MAX_TEST_CANDIDATES)
        nodes = nodes[:MAX_TEST_CANDIDATES]

    # 4. sing-box validate + test (+ empirical geo-probe if configured)
    log.info("[%s] sing-box: validating + testing %d nodes...", cc, len(nodes))
    live = await health_check(nodes, cc)
    log.info("[%s] ✔ %d/%d online | ✘ %d rejected",
             cc, len(live), len(nodes), len(nodes)-len(live))
    if not live:
        log.warning("[%s] No online nodes", cc)
        return

    # 5. Rename + save
    renamed = rename_nodes(live, cc)
    for r in renamed[:3]:
        log.info("  [preview] %s", r.split("#")[-1] if "#" in r else r)
    Path(f"{country}_sub.txt").write_text("\n".join(renamed), encoding="utf-8")
    log.info("[%s] Saved %d nodes → %s_sub.txt\n", cc, len(renamed), country)


# ═══════════════════════════════ 13. MAIN ═══════════════════════════════════

async def main() -> None:
    log.info("Sources : v2nodes · EbraSha · Epodonios · OpenProxyList (GitHub mirrors)")
    log.info("URL order: raw.githubusercontent (primary) → jsDelivr CDN (fallback)")
    log.info("Quality : hyperscale-CDN ASN (always) + X4BNet known-VPN CIDR (no-probe "
             "countries) + empirical geo-probe (%s)", ", ".join(GEO_PROBES) or "none configured")
    log.info("Label   : 'JP | 06'")
    log.info("Scaling : DNS concurrency=%d · classify chunk=%d · test cap/country=%d",
              DNS_CONCURRENCY, CLASSIFY_CHUNK_SIZE, MAX_TEST_CANDIDATES)

    geoip    = GeoIP()
    http_sem = asyncio.Semaphore(HTTP_SEM)
    conn     = aiohttp.TCPConnector(limit=30, ssl=False, ttl_dns_cache=300)

    try:
        async with aiohttp.ClientSession(
            connector=conn, cookie_jar=aiohttp.CookieJar(unsafe=True)
        ) as session:
            # Load quality blocklists first — needed by cc_of_node() below
            await geoip.load_quality_lists(session)

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
