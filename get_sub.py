"""
get_sub.py
Multi-source VPN node collector: v2nodes (scrape) + EbraSha + OpenProxyList
Pipeline: Collect → Dedup → GeoIP filter → sing-box test → Rename
"""

import asyncio, base64, json, logging, os, re, socket, subprocess
import sys, tempfile, time
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

import aiohttp, maxminddb
from bs4 import BeautifulSoup

# ──────────────────────────── Config ─────────────────────────────────────────
BASE_URL  = "https://www.v2nodes.com"
COUNTRIES = ["hk", "jp", "sg", "vn"]

# ISO codes per country (allowed includes GeoLite2 mis-classifications)
CC        = {"hk": "HK",        "jp": "JP",   "sg": "SG",   "vn": "VN"}
CC_ALLOW  = {"hk": {"HK","CN"}, "jp": {"JP"}, "sg": {"SG"}, "vn": {"VN"}}

# External raw sources — GitHub hosted, no Cloudflare block on Azure IPs
EBRASHA_URL = (
    "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list"
    "/refs/heads/main/V2Ray-Config-By-EbraSha.txt"
)

# OpenProxyList mirror: github.com/roosterkid/openproxylist (official GitHub repo)
# jsDelivr = CDN cache layer (faster, fallback); raw.githubusercontent = update frequently
OPL_CDN = "https://cdn.jsdelivr.net/gh/roosterkid/openproxylist@main/V2RAY_RAW.txt"
OPL_RAW = "https://raw.githubusercontent.com/roosterkid/openproxylist/main/V2RAY_RAW.txt"

# GeoIP
GEOIP_DB        = os.environ.get("GEOIP_DB", "GeoLite2-Country.mmdb")
DNS_CONCURRENCY = 80
DNS_TIMEOUT     = 5.0

# Known CDN IP prefixes (Cloudflare / Fastly / Akamai)
_CDN = (
    "104.16.","104.17.","104.18.","104.19.","104.20.","104.21.",
    "104.22.","104.23.","104.24.","104.25.","104.26.","104.27.",
    "108.162.","141.101.","162.158.",
    "172.64.","172.65.","172.66.","172.67.",
    "173.245.","188.114.","190.93.","197.234.","198.41.",
    "151.101.","199.232.",
)

# HTTP
HTTP_SEM       = 10
HTTP_RETRY     = 3
CONN_TIMEOUT   = 12
READ_TIMEOUT   = 25

# sing-box
SINGBOX        = os.environ.get("SINGBOX_BIN", "/usr/local/bin/sing-box")
BATCH_SIZE     = 30
BASE_PORT      = 20000
SB_STARTUP     = 3.0
TEST_URLS      = [
    "http://connectivitycheck.gstatic.com/generate_204",
    "http://www.msftconnecttest.com/connecttest.txt",
]
TEST_TIMEOUT   = 10
TEST_SEM       = 30

_RE_TOTAL  = re.compile(r'\b\d+\s+of\s+(\d+)\b', re.I)
_RE_SRV    = re.compile(r'^/servers/(\d+)/$')
_RE_DCFG   = re.compile(r'data-config="([^"]+)"')
_RE_NODE   = re.compile(r'^(?:vmess|vless|trojan|ss|ssr)://.+', re.I)
_RE_FRAG   = re.compile(r'(?:#|%23).*$')
# OPL label format: "🇯🇵[openproxylist.com] vmess-JP" or "trojan-SG 123ms ..."
_RE_OPL_CC = re.compile(r'\[openproxylist\.com\]\s+\w+-([A-Z]{2})\b')
_SCHEMES   = {"vmess","vless","trojan","ss","ssr"}

_HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ──────────────────────────── HTTP ───────────────────────────────────────────
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


# ──────────────────────────── GeoIP ──────────────────────────────────────────
class GeoIP:
    """
    GeoLite2-Country lookup + CDN detection (IP prefix) + DNS cache.
    DNS cache: hostname → IP, deduplicates across all concurrent lookups.
    """
    def __init__(self) -> None:
        self._reader: Optional[maxminddb.Reader] = None
        self._cache:  dict[str, Optional[str]]   = {}
        self._sem                                 = asyncio.Semaphore(DNS_CONCURRENCY)

        if Path(GEOIP_DB).exists():
            self._reader = maxminddb.open_database(GEOIP_DB)
            log.info("GeoLite2-Country: %s", GEOIP_DB)
        else:
            log.warning("GeoLite2-Country không tìm thấy — GeoIP filter bị bỏ qua")

    @property
    def ok(self) -> bool:
        return self._reader is not None

    def close(self) -> None:
        if self._reader:
            self._reader.close()

    async def resolve(self, host: str) -> Optional[str]:
        """Resolve hostname → IPv4, kết quả được cache."""
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
                loop  = asyncio.get_event_loop()
                infos = await asyncio.wait_for(
                    loop.getaddrinfo(host, None, type=socket.SOCK_STREAM),
                    timeout=DNS_TIMEOUT,
                )
                ip = next((i[4][0] for i in infos if i[0] == socket.AF_INET), None)
                ip = ip or (infos[0][4][0] if infos else None)
            except Exception:
                ip = None
        self._cache[host] = ip
        return ip

    @staticmethod
    def is_cdn(ip: str) -> bool:
        return ip.startswith(_CDN)

    def country_of(self, ip: str) -> Optional[str]:
        if not self._reader:
            return None
        try:
            rec = self._reader.get(ip) or {}
            return rec.get("country", {}).get("iso_code")
        except Exception:
            return None

    async def cc_of_node(self, url: str) -> Optional[str]:
        """
        Trả về CC của node, "CDN" nếu là CDN IP, None nếu DNS fail.
        Dùng để classify node từ EbraSha (không có CC label).
        """
        ep = parse_endpoint(url)
        if not ep:
            return None
        ip = await self.resolve(ep[1])
        if not ip:
            return None
        if self.is_cdn(ip):
            return "CDN"
        return self.country_of(ip)

    async def filter(self, nodes: list[str], allowed: set[str]) -> list[str]:
        """
        Giữ node nếu:
          • GeoIP ∈ allowed  (country khớp)
          • HOẶC IP là CDN   (có thể là valid node cho country này)
        Loại bỏ:
          • DNS fail
          • GeoIP sai country VÀ không phải CDN
        """
        async def _keep(url: str) -> bool:
            ep = parse_endpoint(url)
            if not ep:
                return True           # không parse được → giữ
            ip = await self.resolve(ep[1])
            if ip is None:
                return False          # DNS fail → loại
            if self.is_cdn(ip):
                return True
            return self.country_of(ip) in allowed

        mask = await asyncio.gather(*[_keep(n) for n in nodes])
        return [n for n, ok in zip(nodes, mask) if ok]


# ──────────────────────────── Endpoint parsing ───────────────────────────────
def parse_endpoint(url: str) -> Optional[tuple[str, str, int]]:
    """Trả về (scheme, host, port) — dùng cho dedup key và GeoIP resolve."""
    def _b64d(s: str) -> Optional[str]:
        try:
            return base64.b64decode(s + "=" * (-len(s) % 4)).decode("utf-8", errors="ignore")
        except Exception:
            return None
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
                h, p = hi[1:hi.index("]")], int(hi[hi.index("]")+2:])
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


# ──────────────────────────── Source 1: v2nodes ──────────────────────────────
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

    ids = list(dict.fromkeys(
        sid for a in BeautifulSoup(h1,"lxml").find_all("a", href=_RE_SRV)
        if (sid := _RE_SRV.match(a["href"]).group(1))
    ))
    if total > 1:
        pages = await asyncio.gather(*[
            http_get(session, f"{BASE_URL}/country/{country}/?page={p}")
            for p in range(2, total + 1)
        ])
        for h in pages:
            if h:
                ids.extend(
                    _RE_SRV.match(a["href"]).group(1)
                    for a in BeautifulSoup(h,"lxml").find_all("a", href=_RE_SRV)
                )
        ids = list(dict.fromkeys(ids))

    raw   = await asyncio.gather(*[_node_from_server(session, sid, sem) for sid in ids])
    nodes = [r for r in raw if r]
    log.info("[v2nodes/%s] %d nodes", country.upper(), len(nodes))
    return nodes


# ──────────────────────────── Source 2: EbraSha ──────────────────────────────
async def fetch_ebrasha(
    session: aiohttp.ClientSession, geoip: GeoIP
) -> dict[str, list[str]]:
    """
    Fetch raw list → GeoIP classify mỗi node → trả về {CC: [nodes]}.
    CDN nodes bị bỏ qua (không xác định được country).
    """
    text = await http_get(session, EBRASHA_URL)
    if not text:
        log.warning("[EbraSha] Fetch thất bại")
        return {}

    nodes = [l.strip() for l in text.splitlines() if _RE_NODE.match(l.strip())]
    log.info("[EbraSha] %d nodes raw — GeoIP classify...", len(nodes))

    ccs    = await asyncio.gather(*[geoip.cc_of_node(n) for n in nodes])
    result: dict[str, list[str]] = {}
    cdn_n  = dns_n = 0
    for cc, node in zip(ccs, nodes):
        if cc is None:   dns_n += 1
        elif cc == "CDN": cdn_n += 1
        else:            result.setdefault(cc, []).append(node)

    log.info("[EbraSha] Phân loại: %d nodes / %d CDN bỏ qua / %d DNS fail",
             sum(len(v) for v in result.values()), cdn_n, dns_n)
    return result


# ──────────────────────────── Source 3: OpenProxyList ────────────────────────
def _opl_cc(fragment: str) -> Optional[str]:
    """
    Trích CC từ OPL label trong V2RAY_RAW.txt.
    Format: '🇯🇵[openproxylist.com] vmess-JP' hoặc 'trojan-SG 123ms SG [ISP]'
    """
    m = _RE_OPL_CC.search(fragment)
    return m.group(1) if m else None


async def fetch_opl(session: aiohttp.ClientSession) -> dict[str, list[str]]:
    """
    Fetch V2RAY_RAW.txt từ GitHub mirror của OpenProxyList.
    Primary: jsDelivr CDN (cached, faster)
    Fallback: raw.githubusercontent.com (direct)

    Tại sao dùng GitHub thay openproxylist.com trực tiếp:
      openproxylist.com → Cloudflare-protected → Azure/GitHub IPs bị block
      roosterkid/openproxylist → GitHub repo chính thức → luôn accessible
      jsDelivr → CDN cache của GitHub, không qua Cloudflare protection
    """
    text = await http_get(session, OPL_RAW)
    if not text or not _RE_NODE.search(text):
        log.info("[OPL] RAW Github miss → fallback jsDelivr")
        text = await http_get(session, OPL_CDN)
    if not text or not _RE_NODE.search(text):
        log.warning("[OPL] Fetch thất bại từ cả hai nguồn")
        return {}

    nodes  = [l.strip() for l in text.splitlines() if _RE_NODE.match(l.strip())]
    log.info("[OPL] %d nodes raw", len(nodes))

    result: dict[str, list[str]] = {}
    no_cc = 0
    for node in nodes:
        frag = node.split("#", 1)[1] if "#" in node else ""
        cc   = _opl_cc(frag)
        if cc:
            result.setdefault(cc, []).append(node)
        else:
            no_cc += 1

    log.info("[OPL] Map: %s | %d no-CC bỏ qua",
             {k: len(v) for k, v in sorted(result.items())}, no_cc)
    return result


# ──────────────────────────── Deduplication ──────────────────────────────────
def deduplicate(nodes: list[str]) -> list[str]:
    """Loại trùng theo (scheme, host, port). Giữ thứ tự, ưu tiên v2nodes."""
    seen: set[tuple] = set()
    out:  list[str]  = []
    dups = 0
    for url in nodes:
        ep = parse_endpoint(url)
        key = ep if ep else url   # fallback: dùng url nguyên nếu không parse được
        if key in seen:
            dups += 1
        else:
            seen.add(key)
            out.append(url)
    if dups:
        log.info("  Dedup: loại %d trùng", dups)
    return out


# ──────────────────────────── sing-box parsers ───────────────────────────────
def _b64d(s: str) -> Optional[str]:
    try:
        return base64.b64decode(s + "=" * (-len(s) % 4)).decode("utf-8", errors="ignore")
    except Exception:
        return None


def _sni(raw: str, fallback: str = "") -> str:
    """Sanitize SNI — phải là hostname thuần, không có '/' '?' ' '."""
    for ch in ("/","?"," "):
        raw = raw.split(ch)[0]
    return raw.strip() or fallback


def _ss(url: str, tag: str) -> Optional[dict]:
    try:
        rest = url.split("#")[0][5:]
        if "@" in rest:
            ui, hi = rest.rsplit("@",1)
            d = _b64d(ui)
            if d and ":" in d: method, pw = d.split(":",1)
            elif ":" in ui:    method, pw = ui.split(":",1)
            else: return None
        else:
            d = _b64d(rest)
            if not d or "@" not in d: return None
            mp, hi = d.rsplit("@",1)
            method, pw = mp.split(":",1)
        hi = hi.split("?")[0].split("#")[0]
        if hi.startswith("["):
            host = hi[1:hi.index("]")]; port = int(hi[hi.index("]")+2:])
        else:
            host, port = hi.rsplit(":",1); port = int(port)
        return {"type":"shadowsocks","tag":tag,"server":host,"server_port":port,
                "method":method,"password":pw}
    except Exception:
        return None


def _vmess(url: str, tag: str) -> Optional[dict]:
    try:
        c = json.loads(_b64d(url[8:].split("#")[0]) or "{}")
        h, p = str(c.get("add","")).strip(), int(c.get("port",0))
        if not h or not p: return None
        ob: dict = {"type":"vmess","tag":tag,"server":h,"server_port":p,
                    "uuid":c.get("id",""),
                    "security":c.get("scy",c.get("security","auto")),
                    "alter_id":int(c.get("aid",0))}
        net  = c.get("net","tcp")
        host = c.get("host","")
        path = c.get("path","/") or "/"
        if net == "ws":
            ob["transport"] = {"type":"ws","path":path,
                               "headers":{"Host":host} if host else {}}
        elif net == "grpc":
            ob["transport"] = {"type":"grpc","service_name":path}
        elif net in ("h2","http"):
            ob["transport"] = {"type":"http","host":[host] if host else [],"path":path}
        if c.get("tls"):
            ob["tls"] = {"enabled":True,
                         "server_name":_sni(c.get("sni",host) or "",h),
                         "insecure":True}
        return ob
    except Exception:
        return None


def _vless(url: str, tag: str) -> Optional[dict]:
    try:
        pr = urlparse(url)
        if not pr.hostname or not pr.port or not pr.username: return None
        q = parse_qs(pr.query)
        def g(k): return q.get(k,[""])[0]
        net, sec = g("type") or "tcp", g("security")
        sni      = _sni(g("sni") or g("peer") or "", pr.hostname)
        host, path = g("host"), g("path") or "/"
        ob: dict = {"type":"vless","tag":tag,
                    "server":pr.hostname,"server_port":pr.port,"uuid":pr.username}
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
        def g(k): return q.get(k,[""])[0]
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
            else _trojan if s=="trojan" else _ss if s=="ss" else lambda *_: None)(url, tag)


# ──────────────────────────── sing-box test ──────────────────────────────────
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
                        if r.status in (200,204): return True
            except Exception:
                continue
        return False


async def _run_batch(
    batch: list[tuple[str, dict, int]], sem: asyncio.Semaphore
) -> list[bool]:
    cfg = {
        "log": {"disabled": True},
        "inbounds":  [{"type":"http","tag":f"in_{ob['tag']}",
                        "listen":"127.0.0.1","listen_port":port}
                      for _,ob,port in batch],
        "outbounds": [ob for _,ob,_ in batch] + [{"type":"block","tag":"block"}],
        "route":     {"rules":[{"inbound":[f"in_{ob['tag']}"],"outbound":ob["tag"]}
                               for _,ob,_ in batch],
                      "final":"block"},
    }
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    try:
        json.dump(cfg, tmp); tmp.close()

        # Validate config — phát hiện invalid SNI / bad params trước khi start
        chk = subprocess.run([SINGBOX, "check", "-c", tmp.name],
                             capture_output=True, text=True, timeout=10)
        if chk.returncode:
            log.warning("sing-box config invalid: %s", chk.stderr.strip()[:200])
            return [False] * len(batch)

        proc = subprocess.Popen([SINGBOX, "run", "-c", tmp.name],
                                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
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
    if not Path(SINGBOX).exists():
        log.warning("sing-box không tìm thấy → giữ %d nodes không check", len(nodes))
        return nodes

    parsed = [(url, ob, BASE_PORT+i)
              for i, url in enumerate(nodes)
              if (ob := to_singbox(url, f"n{i}")) is not None]
    skipped = len(nodes) - len(parsed)
    if skipped: log.info("  sing-box: bỏ qua %d nodes (ssr/parse fail)", skipped)
    if not parsed: return []

    sem     = asyncio.Semaphore(TEST_SEM)
    batches = [parsed[i:i+BATCH_SIZE] for i in range(0, len(parsed), BATCH_SIZE)]
    online: list[str] = []
    for i, batch in enumerate(batches, 1):
        t0      = time.monotonic()
        results = await _run_batch(batch, sem)
        ok      = sum(results)
        log.info("  Batch %d/%d: %d/%d online (%.1fs)",
                 i, len(batches), ok, len(batch), time.monotonic()-t0)
        online.extend(url for (url,_,_), alive in zip(batch, results) if alive)
    return online


# ──────────────────────────── Rename ─────────────────────────────────────────
def rename_nodes(nodes: list[str], cc: str) -> list[str]:
    """
    Format: 'JP | 06'  — country code + zero-padded index.
    Protocol omitted: Shadowrocket/clients already display it on the sub-line.
    vmess: cập nhật field 'ps' trong base64 JSON (clients đọc ps, không đọc #).
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


# ──────────────────────────── Pipeline ───────────────────────────────────────
async def process_country(
    session:    aiohttp.ClientSession,
    country:    str,
    http_sem:   asyncio.Semaphore,
    geoip:      GeoIP,
    ebrasha:    dict[str, list[str]],
    opl:        dict[str, list[str]],
) -> None:
    cc      = CC[country]
    allowed = CC_ALLOW[country]
    log.info("━━━ [%s] Bắt đầu ━━━", cc)

    # 1. Collect từ 3 nguồn: v2nodes (scrape) + EbraSha + OPL (pre-classified)
    v2n     = await scrape_v2nodes(session, country, http_sem)
    ext_eba = [n for acc in allowed for n in ebrasha.get(acc, [])]
    ext_opl = [n for acc in allowed for n in opl.get(acc, [])]
    nodes   = v2n + ext_eba + ext_opl
    log.info("[%s] Tổng: %d (v2nodes=%d ebrasha=%d opl=%d)",
             cc, len(nodes), len(v2n), len(ext_eba), len(ext_opl))

    # 2. Dedup
    nodes = deduplicate(nodes)
    log.info("[%s] Sau dedup: %d", cc, len(nodes))

    # 3. GeoIP filter
    if geoip.ok:
        before = len(nodes)
        nodes  = await geoip.filter(nodes, allowed)
        log.info("[%s] Sau GeoIP: %d/%d", cc, len(nodes), before)
    else:
        log.warning("[%s] GeoIP không có — bỏ qua filter", cc)
    if not nodes:
        log.warning("[%s] Không còn node sau filter", cc)
        return

    # 4. sing-box health check
    log.info("[%s] sing-box test %d nodes...", cc, len(nodes))
    live = await health_check(nodes)
    log.info("[%s] ✔ %d/%d online | ✘ %d offline",
             cc, len(live), len(nodes), len(nodes)-len(live))
    if not live:
        log.warning("[%s] Không có node online", cc)
        return

    # 5. Rename + save
    renamed = rename_nodes(live, cc)
    for r in renamed[:3]:
        log.info("  [preview] %s", r.split("#")[-1] if "#" in r else r)
    Path(f"{country}_sub.txt").write_text("\n".join(renamed), encoding="utf-8")
    log.info("[%s] Lưu %d nodes → %s_sub.txt", cc, len(renamed), country)
    log.info("━━━ [%s] Xong ━━━\n", cc)


# ──────────────────────────── Main ───────────────────────────────────────────
async def main() -> None:
    log.info("Sources: v2nodes (scrape) + EbraSha + OPL (GitHub mirror via jsDelivr)")
    log.info("Label:   'JP | 06'  — CC + index, no protocol")

    geoip    = GeoIP()
    http_sem = asyncio.Semaphore(HTTP_SEM)
    connector = aiohttp.TCPConnector(limit=30, ssl=False, ttl_dns_cache=300)

    try:
        async with aiohttp.ClientSession(
            connector=connector, cookie_jar=aiohttp.CookieJar(unsafe=True)
        ) as session:
            # Fetch external sources once (parallel), then loop countries
            ebrasha, opl = await asyncio.gather(
                fetch_ebrasha(session, geoip),
                fetch_opl(session),
            )
            log.info("EbraSha: %s", {k: len(v) for k, v in sorted(ebrasha.items())})
            log.info("OPL:     %s", {k: len(v) for k, v in sorted(opl.items())})

            for country in COUNTRIES:
                await process_country(session, country, http_sem, geoip, ebrasha, opl)
    finally:
        geoip.close()


if __name__ == "__main__":
    asyncio.run(main())
