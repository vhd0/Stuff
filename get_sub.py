"""
get_sub.py — Multi-source VPN node collector
Sources  : v2nodes.com (scrape) · EbraSha · Epodonios · OpenProxyList (GitHub)
Pipeline : Collect → Dedup → GeoIP (country + ASN quality) → Validate → sing-box → Rename
Label    : "JP | 06"  (CC + index, no protocol — clients display it already)

Quality filter (GeoIP.is_low_quality):
  Nodes whose resolved IP belongs to a CDN (Cloudflare/Fastly/Akamai) or
  hyperscale cloud (AWS/GCP/Azure/Alibaba/...) are DROPPED even when GeoIP
  reports the correct target country. Reason: many geo-restricted sites use
  IP-intelligence databases (not GeoIP) that flag these ASNs as datacenter/
  proxy regardless of accurate geolocation — so a node can show "JP" and
  still fail to reach JP-only content. This is a stricter policy than simply
  matching country: it trades node quantity for nodes that behave like a
  genuine residential/local connection.
"""

import asyncio, base64, json, logging, os, re, socket, subprocess
import sys, tempfile, time
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

import aiohttp, maxminddb
from bs4 import BeautifulSoup

# ─────────────────────────── Config ──────────────────────────────────────────
BASE_URL  = "https://www.v2nodes.com"
COUNTRIES = ["hk", "jp", "sg", "vn"]

CC       = {"hk": "HK",        "jp": "JP",   "sg": "SG",   "vn": "VN"}
CC_ALLOW = {"hk": {"HK","CN"}, "jp": {"JP"}, "sg": {"SG"}, "vn": {"VN"}}

# ── External raw sources ─────────────────────────────────────────────────────
# Priority: raw.githubusercontent (authoritative) → jsDelivr CDN (cache fallback)
# Both are accessible from GitHub Actions Azure IPs (no Cloudflare block)

EBRASHA_RAW = (
    "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list"
    "/refs/heads/main/V2Ray-Config-By-EbraSha.txt"
)
EBRASHA_CDN = (
    "https://cdn.jsdelivr.net/gh/ebrasha/free-v2ray-public-list"
    "@main/V2Ray-Config-By-EbraSha.txt"
)

# roosterkid/openproxylist = official GitHub mirror of openproxylist.com
# (openproxylist.com itself is Cloudflare-protected → blocks Azure IPs)
OPL_RAW = "https://raw.githubusercontent.com/roosterkid/openproxylist/main/V2RAY_RAW.txt"
OPL_CDN = "https://cdn.jsdelivr.net/gh/roosterkid/openproxylist@main/V2RAY_RAW.txt"

# Epodonios: large aggregator (~6000+ nodes), no CC in label → full GeoIP classify
EPO_RAW = (
    "https://raw.githubusercontent.com/Epodonios/v2ray-configs"
    "/refs/heads/main/All_Configs_Sub.txt"
)
EPO_CDN = "https://cdn.jsdelivr.net/gh/Epodonios/v2ray-configs@main/All_Configs_Sub.txt"

# ── GeoIP ────────────────────────────────────────────────────────────────────
GEOIP_DB        = os.environ.get("GEOIP_DB",     "GeoLite2-Country.mmdb")
GEOIP_ASN_DB    = os.environ.get("GEOIP_ASN_DB", "GeoLite2-ASN.mmdb")
DNS_CONCURRENCY = 80
DNS_TIMEOUT     = 5.0

# Fallback IP-prefix detection when ASN DB unavailable (legacy, less accurate)
_CDN_PREFIXES = (
    "104.16.","104.17.","104.18.","104.19.","104.20.","104.21.",
    "104.22.","104.23.","104.24.","104.25.","104.26.","104.27.",
    "108.162.","141.101.","162.158.",
    "172.64.","172.65.","172.66.","172.67.",
    "173.245.","188.114.","190.93.","197.234.","198.41.",
    "151.101.","199.232.",
)

# ── ASN quality filter ─────────────────────────────────────────────────────
# Root cause of "JP IP nhưng bị chặn": nhiều node dùng CDN (Cloudflare/Fastly)
# hoặc hyperscale cloud (AWS/GCP/Azure...) làm địa chỉ kết nối. GeoIP đôi khi
# vẫn báo đúng "JP" cho các IP này (đặc biệt AWS ap-northeast-1, GCP
# asia-northeast1), nhưng các website Nhật Bản dùng IP-intelligence riêng
# (không phải GeoIP) để nhận diện đây là datacenter/proxy — không phải kết
# nối dân dụng thật — nên vẫn chặn dù geolocation đúng quốc gia.
#
# Giải pháp: loại các node có ASN thuộc nhóm "hosting/CDN phổ biến bị
# flag" NGAY CẢ KHI GeoIP báo đúng country. Trước đây code cũ tự động GIỮ
# mọi IP CDN bất kể target country — đây chính là bug gây lẫn node kém
# chất lượng vào kết quả.
_ASN_NUMBERS_LOWQ: frozenset[int] = frozenset({
    13335, 209242,                    # Cloudflare
    54113,                            # Fastly
    16509, 14618,                     # Amazon AWS / EC2
    15169, 396982,                    # Google / Google Cloud
    8075,   8068,                     # Microsoft Azure
    37963, 45102,                     # Alibaba Cloud
    132203, 59019,                    # Tencent Cloud
    31898, 20473,                     # Vultr / Choopa
    14061,                            # DigitalOcean
    16276,                            # OVH
    24940,                            # Hetzner
    63949,                            # Linode / Akamai Connected Cloud
    12876,                            # Scaleway
    51167,                            # Contabo
    9009,                             # M247
    36351, 62240, 33182,              # SoftLayer/IBM Cloud, Clouvider, Psychz
    62563,                            # Global Layer / QuadraNet-linked
    203020,                           # QuadraNet
    35916,                            # MULTACOM/others frequently proxy-flagged
    46844,                            # Sharktech
    174,                              # Cogent (bulk hosting transit, often flagged)
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
SB_VAL_SEM   = 8      # concurrent per-node validation processes
TEST_URLS    = [
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


# ─────────────────────────── HTTP ────────────────────────────────────────────
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
    session: aiohttp.ClientSession,
    primary: str,
    fallback: str,
    name: str,
) -> Optional[str]:
    """Fetch primary URL; if no valid node data, try fallback."""
    text = await http_get(session, primary)
    if text and _RE_NODE.search(text):
        log.info("[%s] OK ← %s", name, primary.split("/")[2])
        return text
    log.info("[%s] primary miss → fallback...", name)
    text = await http_get(session, fallback)
    if text and _RE_NODE.search(text):
        log.info("[%s] OK ← %s", name, fallback.split("/")[2])
        return text
    log.warning("[%s] Fetch thất bại từ cả hai nguồn", name)
    return None


# ─────────────────────────── GeoIP ───────────────────────────────────────────
class GeoIP:
    """
    Country lookup + ASN-based quality filter + cached DNS resolution.

    Two-stage check per node:
      1. Country match  — GeoLite2-Country: is the IP geolocated in the target CC?
      2. Quality check   — GeoLite2-ASN: is the IP a CDN/hyperscale-cloud address?
                            These get DROPPED even if country matches, because
                            IP-intelligence services (used by geo-restricted JP
                            sites, etc.) flag them as datacenter/proxy regardless
                            of correct geolocation.
    """

    def __init__(self) -> None:
        self._country: Optional[maxminddb.Reader] = None
        self._asn:     Optional[maxminddb.Reader] = None
        self._cache:   dict[str, Optional[str]]   = {}
        self._sem                                  = asyncio.Semaphore(DNS_CONCURRENCY)

        if Path(GEOIP_DB).exists():
            self._country = maxminddb.open_database(GEOIP_DB)
            log.info("GeoLite2-Country: %s", GEOIP_DB)
        else:
            log.warning("GeoLite2-Country không tìm thấy — GeoIP filter bị bỏ qua")

        if Path(GEOIP_ASN_DB).exists():
            self._asn = maxminddb.open_database(GEOIP_ASN_DB)
            log.info("GeoLite2-ASN: %s (quality filter bật)", GEOIP_ASN_DB)
        else:
            log.warning("GeoLite2-ASN không tìm thấy — dùng IP-prefix fallback cho CDN")

    @property
    def ok(self) -> bool:
        return self._country is not None

    def close(self) -> None:
        if self._country: self._country.close()
        if self._asn:     self._asn.close()

    async def resolve(self, host: str) -> Optional[str]:
        """Resolve hostname → IPv4, result cached per session."""
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

    def is_low_quality(self, ip: str) -> bool:
        """
        True nếu IP thuộc CDN/hyperscale-cloud thường bị IP-intelligence
        services flag là proxy/datacenter, bất kể GeoIP country đúng hay sai.
        """
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
        # Fallback nếu không có ASN DB: dùng IP-prefix cũ (chỉ bắt Cloudflare)
        return ip.startswith(_CDN_PREFIXES)

    def country_of(self, ip: str) -> Optional[str]:
        if not self._country:
            return None
        try:
            return (self._country.get(ip) or {}).get("country", {}).get("iso_code")
        except Exception:
            return None

    async def cc_of_node(self, url: str) -> Optional[str]:
        """
        Return CC nếu hợp lệ để phân loại (dùng cho EbraSha/Epodonios).
        Trả về None nếu: DNS fail, HOẶC IP là CDN/hyperscale-cloud
        (loại sớm — không đáng tin để gán country).
        """
        ep = parse_endpoint(url)
        if not ep:
            return None
        ip = await self.resolve(ep[1])
        if not ip:
            return None
        if self.is_low_quality(ip):
            return None
        return self.country_of(ip)

    async def filter(self, nodes: list[str], allowed: set[str]) -> list[str]:
        """
        Keep node CHỈ KHI:
          • DNS resolve thành công
          • IP KHÔNG thuộc CDN/hyperscale-cloud (is_low_quality == False)
          • GeoIP country ∈ allowed

        Khác biệt quan trọng so với bản cũ: không còn tự động giữ mọi
        CDN IP — đây là nguồn gốc của node "báo JP nhưng bị chặn".
        """
        async def _keep(url: str) -> bool:
            ep = parse_endpoint(url)
            if not ep:
                return True
            ip = await self.resolve(ep[1])
            if ip is None:
                return False
            if self.is_low_quality(ip):
                return False
            return self.country_of(ip) in allowed

        mask = await asyncio.gather(*[_keep(n) for n in nodes])
        return [n for n, ok in zip(nodes, mask) if ok]


# ─────────────────────────── Endpoint parsing ────────────────────────────────
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
            hi   = rest.rsplit("@",1)[1] if "@" in rest else (
                (_b64d(rest) or "").rsplit("@",1)[1] if "@" in (_b64d(rest) or "") else None
            )
            if not hi:
                return None
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


# ─────────────────────────── Source 1: v2nodes ───────────────────────────────
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
    ids  = list(dict.fromkeys(
        a["href"][9:-1]
        for a in soup.find_all("a", href=_RE_SRV)
    ))
    if total > 1:
        extras = await asyncio.gather(*[
            http_get(session, f"{BASE_URL}/country/{country}/?page={p}")
            for p in range(2, total + 1)
        ])
        for h in extras:
            if h:
                ids.extend(
                    a["href"][9:-1]
                    for a in BeautifulSoup(h,"lxml").find_all("a", href=_RE_SRV)
                )
        ids = list(dict.fromkeys(ids))

    raw   = await asyncio.gather(*[_node_from_server(session, sid, sem) for sid in ids])
    nodes = [r for r in raw if r]
    log.info("[v2nodes/%s] %d nodes", country.upper(), len(nodes))
    return nodes


# ─────────────────────────── Source 2: EbraSha ───────────────────────────────
async def fetch_ebrasha(
    session: aiohttp.ClientSession, geoip: GeoIP
) -> dict[str, list[str]]:
    """
    Fetch EbraSha raw list → GeoIP classify each node → return {CC: [nodes]}.
    Order: raw.githubusercontent (primary) → jsDelivr CDN (fallback).
    Nodes on CDN/hyperscale-cloud ASN are dropped here (low-quality — see
    GeoIP.is_low_quality), not just uncertain-country.
    """
    text = await fetch_with_fallback(session, EBRASHA_RAW, EBRASHA_CDN, "EbraSha")
    if not text:
        return {}

    nodes = [l.strip() for l in text.splitlines() if _RE_NODE.match(l.strip())]
    log.info("[EbraSha] %d nodes — GeoIP classify...", len(nodes))

    ccs    = await asyncio.gather(*[geoip.cc_of_node(n) for n in nodes])
    result: dict[str, list[str]] = {}
    dropped = dns_n = 0
    for cc, node in zip(ccs, nodes):
        if cc is None:
            dropped += 1   # DNS fail hoặc low-quality ASN (CDN/cloud)
        else:
            result.setdefault(cc, []).append(node)

    log.info("[EbraSha] %d classified · %d dropped (DNS fail / CDN-cloud ASN)",
             sum(len(v) for v in result.values()), dropped)
    return result


async def fetch_epodonios(
    session: aiohttp.ClientSession, geoip: GeoIP
) -> dict[str, list[str]]:
    """
    Fetch Epodonios aggregator (~6000+ nodes) → GeoIP classify → {CC: [nodes]}.
    Order: raw.githubusercontent (primary) → jsDelivr CDN (fallback).

    Optimization for large scale:
      1. _RE_NODE already filters to supported schemes only
         (vmess/vless/trojan/ss/ssr — hy2/hysteria2/socks auto-excluded,
         since Epodonios includes those but sing-box test doesn't support them)
      2. Pre-dedup by (scheme, host, port) BEFORE classify — Epodonios has many
         literal duplicates (same infra reused across entries with different
         UUID/labels). Classifying once per unique endpoint instead of once
         per line cuts DNS/GeoIP work substantially on a 6000-line source.
      3. DNS resolution is still cached in GeoIP (shared across all sources),
         so repeated hostnames (e.g. common CDN edges) cost one lookup total.
      4. Nodes on CDN/hyperscale-cloud ASN dropped (low-quality — see
         GeoIP.is_low_quality), same rule as EbraSha.
    """
    text = await fetch_with_fallback(session, EPO_RAW, EPO_CDN, "Epodonios")
    if not text:
        return {}

    raw_nodes = [l.strip() for l in text.splitlines() if _RE_NODE.match(l.strip())]
    nodes     = deduplicate(raw_nodes)
    log.info("[Epodonios] %d raw → %d unique endpoints — GeoIP classify...",
             len(raw_nodes), len(nodes))

    ccs    = await asyncio.gather(*[geoip.cc_of_node(n) for n in nodes])
    result: dict[str, list[str]] = {}
    dropped = 0
    for cc, node in zip(ccs, nodes):
        if cc is None:
            dropped += 1   # DNS fail hoặc low-quality ASN (CDN/cloud)
        else:
            result.setdefault(cc, []).append(node)

    log.info("[Epodonios] %d classified · %d dropped (DNS fail / CDN-cloud ASN)",
             sum(len(v) for v in result.values()), dropped)
    return result


# ─────────────────────────── Source 4: OpenProxyList ─────────────────────────
def _opl_cc(fragment: str) -> Optional[str]:
    """Extract CC from OPL label: '🇸🇬[openproxylist.com] trojan-SG' → 'SG'."""
    m = _RE_OPL_CC.search(fragment)
    return m.group(1) if m else None


async def fetch_opl(session: aiohttp.ClientSession) -> dict[str, list[str]]:
    """
    Fetch OpenProxyList from roosterkid/openproxylist GitHub mirror.
    Order: raw.githubusercontent (primary) → jsDelivr CDN (fallback).
    openproxylist.com direct URL is Cloudflare-protected and blocks Azure IPs.
    """
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


# ─────────────────────────── Deduplication ───────────────────────────────────
def deduplicate(nodes: list[str]) -> list[str]:
    """Remove duplicates by (scheme, host, port). Preserves order (v2nodes first)."""
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


# ─────────────────────────── sing-box parsers ────────────────────────────────
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
        if not _RE_UUID.match(uuid): return None   # reject non-UUID usernames
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


# ─────────────────────────── sing-box validation ─────────────────────────────
async def _sb_check_node(ob: dict, sem: asyncio.Semaphore) -> bool:
    """
    Run 'sing-box check' on a minimal config containing just this outbound.
    Port 29000 is fine for check — it never binds (dry-run only).
    Catches invalid SNI, bad reality keys, unsupported fields, etc.
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
            except: pass


# ─────────────────────────── sing-box test ───────────────────────────────────
async def _proxy_test(port: int, sem: asyncio.Semaphore) -> bool:
    async with sem:
        to = aiohttp.ClientTimeout(total=TEST_TIMEOUT)
        for url in TEST_URLS:
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


async def _run_batch(
    batch: list[tuple[str, dict, int]], sem: asyncio.Semaphore
) -> list[bool]:
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

        results = await asyncio.gather(*[_proxy_test(p, sem) for _,_,p in batch])
        proc.terminate()
        try:    proc.wait(timeout=3)
        except: proc.kill()
        return list(results)
    finally:
        try: os.unlink(tmp.name)
        except: pass


async def health_check(nodes: list[str]) -> list[str]:
    """
    1. Parse all nodes → sing-box outbound configs
    2. Per-node 'sing-box check' (concurrent, no network) → filter invalid
       Ensures nodes from ALL sources/locations pass validation before batching
       (invalid SNI, bad UUID, malformed keys → caught here, don't kill batch)
    3. Batch HTTP proxy test → keep online nodes
    """
    if not Path(SINGBOX).exists():
        log.warning("sing-box not found → skipping health check")
        return nodes

    # Step 1: parse
    candidates = [(url, ob, BASE_PORT + i)
                  for i, url in enumerate(nodes)
                  if (ob := to_singbox(url, f"n{i}")) is not None]
    parse_skip = len(nodes) - len(candidates)
    if parse_skip:
        log.info("  Parse skip: %d (ssr / unknown scheme)", parse_skip)

    # Step 2: per-node config validation (catches bad configs before they kill a batch)
    val_sem = asyncio.Semaphore(SB_VAL_SEM)
    valid_mask = await asyncio.gather(*[
        _sb_check_node(ob, val_sem) for _, ob, _ in candidates
    ])
    valid     = [(url, ob, port) for (url, ob, port), ok in zip(candidates, valid_mask) if ok]
    inv_count = len(candidates) - len(valid)
    if inv_count:
        log.info("  Config invalid (sing-box check): %d nodes removed", inv_count)
    log.info("  Validated: %d nodes ready for proxy test", len(valid))

    if not valid:
        return []

    # Step 3: batch proxy test
    test_sem = asyncio.Semaphore(TEST_SEM)
    batches  = [valid[i:i+BATCH_SIZE] for i in range(0, len(valid), BATCH_SIZE)]
    online: list[str] = []
    for i, batch in enumerate(batches, 1):
        t0      = time.monotonic()
        results = await _run_batch(batch, test_sem)
        ok      = sum(results)
        log.info("  Batch %d/%d: %d/%d online (%.1fs)",
                 i, len(batches), ok, len(batch), time.monotonic()-t0)
        online.extend(url for (url,_,_), alive in zip(batch, results) if alive)

    return online


# ─────────────────────────── Rename ──────────────────────────────────────────
def rename_nodes(nodes: list[str], cc: str) -> list[str]:
    """
    Format: "JP | 06"  — CC + zero-padded index.
    Protocol omitted: Shadowrocket/clients show it on the sub-line.
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


# ─────────────────────────── Pipeline ────────────────────────────────────────
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
    log.info("━━━ [%s] ━━━", cc)

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

    # 3. GeoIP filter
    if geoip.ok:
        before = len(nodes)
        nodes  = await geoip.filter(nodes, allowed)
        log.info("[%s] After GeoIP {%s}: %d/%d", cc, "/".join(sorted(allowed)), len(nodes), before)
    else:
        log.warning("[%s] No GeoIP DB — skipping filter", cc)
    if not nodes:
        log.warning("[%s] No nodes after GeoIP filter", cc)
        return

    # 4. sing-box validate + test
    log.info("[%s] sing-box: validating + testing %d nodes...", cc, len(nodes))
    live = await health_check(nodes)
    log.info("[%s] ✔ %d/%d online | ✘ %d offline",
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


# ─────────────────────────── Main ────────────────────────────────────────────
async def main() -> None:
    log.info("Sources : v2nodes · EbraSha · Epodonios · OpenProxyList (GitHub mirrors)")
    log.info("URL order: raw.githubusercontent (primary) → jsDelivr CDN (fallback)")
    log.info("Quality : GeoIP country match + ASN filter (drop CDN/hyperscale-cloud)")
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
