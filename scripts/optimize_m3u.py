import re, json, os, random, time, urllib.request, urllib.error
from collections import OrderedDict

# ====================================================================
# 1. CAU HINH CO BAN
# ====================================================================
M3U_SOURCES = [
    # Cac nguon cu: KHONG tin group-title co san trong nguon (neu co), tiep
    # tuc tu phan loai qua determine_group() nhu truoc gio (giu hanh vi cu).
    {"url": "https://raw.githubusercontent.com/DinhLap96/ListTivi/refs/heads/main/ListTiVi/dltivi_v2.ndl",
     "trust_group_title": False},
    {"url": "https://raw.githubusercontent.com/iptv-org/iptv/refs/heads/master/streams/vn.m3u",
     "trust_group_title": False},
    # Nguon moi: co san rat nhieu nhom da dang (the thao quoc te, TVB, VTVcab,
    # HTVC, cac kenh su kien, radio...). TIN TUONG group-title goc cua nguon
    # nay thay vi ep vao 6 nhom cu, vi phan loai theo tu khoa VTV/HTV/SCTV se
    # khong dung cho cac kenh quoc te/nuoc ngoai trong nguon nay.
    # exclude_group_keys: loai BO HOAN TOAN cac kenh thuoc nhom Live Event,
    # Radio, cac nuoc (Han Quoc/Trung Quoc/Thai Lan/Campuchia/UK/Israel), va
    # cac nhom TRUNG voi nhom da co san tu nguon khac (VTV/HTV/HTVC/VTVCab/
    # ON/Dia Phuong/Kenh thiet yeu) - CHI ap dung rieng cho nguon vmttv nay,
    # khong anh huong cac nguon khac.
    {"url": "https://raw.githubusercontent.com/vuminhthanh12/vuminhthanh12/refs/heads/main/vmttv",
     "trust_group_title": True,
     "exclude_group_keys": None},  # duoc gan gia tri thuc te ngay ben duoi
]

# Danh sach TU KHOA nhom can loai bo rieng cho nguon vmttv (viet tu nhien,
# se duoc chuan hoa ve "key" khong dau/khong ky tu dac biet de so khop chinh
# xac voi group-title thuc te, du group-title co emoji/dau gach dung kem).
_VMTTV_EXCLUDE_TERMS = [
    "LIVE EVENTS",                          # su kien truc tiep
    "Radio", "UK Radio",                    # cac kenh radio
    "Israel", "Han Quoc", "Trung Quoc", "Thai Lan", "Campuchia", "In The Box", "TVB",  # kenh nuoc ngoai
    "VTV", "VTVCab", "HTV", "HTVC", "Dia Phuong", "Thiet yeu", "ON",  # nhom trung
]

# Nhom "goc" uu tien hien thi truoc, mo phong dung cach chia nhom cua cac
# app OTT thuc te (FPT Play, TV360, VieON...): VTV va VTVCab la 2 THUONG
# HIEU RIENG BIET (VTVCab la kenh cap con cua VTV, chuong trinh khac han),
# tuong tu HTV va HTVC. KHONG gop chung, du cung "ho" VTV/HTV. Cac nhom MOI
# phat hien tu nguon co trust_group_title=True se duoc TU DONG noi vao ngay
# sau nhom goc (theo thu tu xuat hien lan dau), va "Dia Phuong" luon o CUOI
# CUNG vi day la nhom "gom-tat-ca" mac dinh.
BASE_GROUP_ORDER = ["VTV", "VTVCab", "HTV", "HTVC", "SCTV", "Kenh Dac Biet", "VOV", "ON"]
CATCH_ALL_GROUP = "Dia Phuong"

# Gop CHINH XAC (khong dau, khong phan biet hoa-thuong, da bo ky tu khong
# phai chu/so) - KHONG dung tien to/startswith nua, vi lam "VTVcab" bi nuot
# nham vao "VTV". Chi gop khi ten nhom TRUNG KHOP hoan toan voi 1 trong cac
# bien the duoi day; nhom nao khong khop se GIU NGUYEN ten goc cua nguon.
GROUP_TITLE_EXACT_ALIASES = {
    "vtv": "VTV",
    "vtvcab": "VTVCab",
    "htv": "HTV",
    "htvc": "HTVC",
    "sctv": "SCTV",
    "diaphuong": CATCH_ALL_GROUP,
}

# Loc theo NHOM (group-title) cho cac nguon TIN TUONG group-title cua ho
# (trust_group_title=True) - tranh dinh oan ten phim/kenh co chua tu tieng
# Anh trung ngau nhien (vd phim "Betting With Ghost" khong phai kenh ca do).
BLOCKED_GROUP_KEYWORDS = ("bet",)

# Loc theo TEN KENH/tvg-id cho cac nguon KHONG co group-title dang tin cay
# (trust_group_title=False) - vi cac nguon nay khong the phan loai theo
# nhom, nen van phai loc truc tiep tren ten kenh nhu truoc (vd "VSBet").
BLOCKED_NAME_KEYWORDS = ("bet",)

# vnepg_backup.json: NGUON CHUAN de chuan hoa TEN/NHOM/TVG-ID cua kenh, va la
# fallback cuoi cung cho logo. File nay duoc TU DONG CAP NHAT moi lan chay
# workflow bang cach goi truc tiep API cua vnepg.site thong qua danh sach
# proxy VN free (gom tu nhieu nguon), vi vnepg.site chan IP ngoai VN bang
# Cloudflare firewall nen GitHub Actions khong the fetch truc tiep.
# Neu TAT CA proxy deu that bai, GIU NGUYEN noi dung file da commit lan
# truoc (fallback), khong lam gian doan pipeline.
VNEPG_BACKUP_FILE = "vnepg_backup.json"
VNEPG_API_URL = "https://vnepg.site/api/channels"
MAX_PROXY_TRIES = 40       # so proxy toi da thu truoc khi bo cuoc
SOURCE_FETCH_TIMEOUT = 15  # giay, cho lay danh sach proxy tu 1 nguon
PROXY_REQUEST_TIMEOUT = 8  # giay, cho moi lan thu goi API qua proxy

IPTV_ORG_CHANNELS_API = "https://iptv-org.github.io/api/channels.json"
IPTV_ORG_LOGOS_API = "https://iptv-org.github.io/api/logos.json"

TRUSTED_DOMAINS = ("vnepg.site", "cdn.jsdelivr.net")

BLOCKED_LOGO_DOMAINS = ("wikia.nocookie.net", "thainguyentv.vn")

SAFE_LOGO_EXTS = (".png", ".jpg", ".jpeg")

IPTV_ORG_ID_HINTS = {
    "ANTV": "AnNinhTV.vn", "QPVN": "QPVN.vn", "Vietnam Today": "VietnamToday.vn",
    "HTV Key": "HTVKey.vn", "HTV Thể thao": "HTVSports.vn",
}

ID_ALIASES = {"vntoday": "vietnamtoday", "thuathienhuetv.vn": "hue"}

DISPLAY_NAME_OVERRIDE = {}

MANUAL_ID_OVERRIDE = {
    "th lam dong 2": "dongthap2",
}

# ====================================================================
# 2. TU DIEN CHUAN HOA TEN KENH
# ====================================================================
CHANNEL_MAPPING = {
    "VTV1": ["vtv1", "vtv1hd"], "VTV2": ["vtv2", "vtv2hd"], "VTV3": ["vtv3", "vtv3hd"],
    "VTV4": ["vtv4", "vtv4hd"], "VTV5": ["vtv5", "vtv5hd"], "VTV6": ["vtv6", "vtv6hd"],
    "VTV7": ["vtv7", "vtv7hd"], "VTV8": ["vtv8", "vtv8hd"], "VTV9": ["vtv9", "vtv9hd"],
    "VTV10": ["vtv10", "vtv10hd", "vtvcantho", "vtv can tho"],
    "VTV5 Tay Nam Bo": ["vtv5 tay nam bo", "vtv5tnb"],
    "VTV5 Tay Nguyen": ["vtv5 tay nguyen", "vtv5tn"],
    "HTV1": ["htv1"], "HTV2 - Vie Channel": ["htv2", "vie channel"], "HTV3": ["htv3"],
    "HTV4": ["htv4"], "HTV7": ["htv7", "htv7hd"], "HTV9": ["htv9", "htv9hd"],
    "THVL1": ["thvl1", "vinh long 1"], "THVL2": ["thvl2", "vinh long 2"],
    "THVL3": ["thvl3", "vinh long 3"], "THVL4": ["thvl4", "vinh long 4"],
    "ANTV": ["antv", "an ninh tv"], "QPVN": ["qpvn", "quoc phong viet nam"],
    "Hà Nội 1": ["ha noi 1", "hanoitv1"], "Hà Nội 2": ["ha noi 2", "hanoitv2"],
    "Lâm Đồng 1": ["lam dong", "lam dong tv"],
    "Huế": ["thua thien hue"],
}

# ====================================================================
# 3. HAM TIEN ICH
# ====================================================================
def remove_accents(s):
    s = s.lower()
    s = re.sub(r'[aàáạảãâầấậẩẫăằắặẳẵ]', 'a', s)
    s = re.sub(r'[eèéẹẻẽêềếệểễ]', 'e', s)
    s = re.sub(r'[iìíịỉĩ]', 'i', s)
    s = re.sub(r'[oòóọỏõôồốộổỗơờớợởỡ]', 'o', s)
    s = re.sub(r'[uùúụủũưừứựửữ]', 'u', s)
    s = re.sub(r'[yỳýỵỷỹ]', 'y', s)
    s = re.sub(r'[đ]', 'd', s)
    return re.sub(r'\s+', ' ', s).strip()

def collapse(s):
    return s.replace(" ", "")

def group_match_key(s):
    """Chuan hoa ten nhom ve 'key' chi gom chu/so, khong dau, khong phan
    biet hoa-thuong, da bo emoji/dau gach/ky tu dac biet - dung de so khop
    CHINH XAC ten nhom bat ke cach trinh bay khac nhau giua cac nguon."""
    return re.sub(r'[^a-z0-9]', '', remove_accents(s or ""))

# Gan gia tri thuc te cho exclude_group_keys cua nguon vmttv (dat sau khi da
# co ham group_match_key va _VMTTV_EXCLUDE_TERMS).
M3U_SOURCES[2]["exclude_group_keys"] = {group_match_key(t) for t in _VMTTV_EXCLUDE_TERMS}

def get_tvg_id(extinf_line):
    m = re.search(r'tvg-id="([^"]*)"', extinf_line, re.IGNORECASE)
    return m.group(1).strip().lower() if m else ""

def get_source_group_title(extinf_line):
    """Lay group-title co san trong dong EXTINF cua nguon (neu co)."""
    m = re.search(r'group-title="([^"]*)"', extinf_line, re.IGNORECASE)
    return m.group(1).strip() if m else ""

def normalize_group_title(raw_group):
    """Gop nhom CHINH XAC theo GROUP_TITLE_EXACT_ALIASES (vd 'VTVcab' ->
    'VTVCab' la thuong hieu rieng, KHONG gop vao 'VTV'). Giu nguyen ten goc
    neu khong trung khop hoan toan bien the nao (nhom doc lap/hoan toan
    moi)."""
    key_clean = re.sub(r'[^a-z0-9]', '', remove_accents(raw_group))
    return GROUP_TITLE_EXACT_ALIASES.get(key_clean, raw_group)

def clean_raw_name(raw_name):
    clean = re.sub(r'\(.*?\)|\[.*?\]', '', raw_name)
    clean = re.sub(r'\b(hd|sd|4k|1080p|720p|fhd)\b', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'(?i)^\s*TH\s+', '', clean)
    clean = re.sub(r'(?i)\bTV(\d*)\b', r'\1', clean)
    return " ".join(clean.split()).strip()

def resolve_vnepg_entry(raw_name, tvg_id, vnepg_id_map, vnepg_name_map):
    raw_key = remove_accents(" ".join(raw_name.split()))
    override_id = MANUAL_ID_OVERRIDE.get(raw_key)
    if override_id and override_id in vnepg_id_map:
        return vnepg_id_map[override_id], vnepg_id_map[override_id]["name"]

    clean = clean_raw_name(raw_name)
    key = remove_accents(clean)

    tvg_id_base = tvg_id.split("@")[0] if tvg_id else tvg_id
    tvg_id_base = ID_ALIASES.get(tvg_id_base, tvg_id_base)
    if tvg_id_base and tvg_id_base in vnepg_id_map:
        return vnepg_id_map[tvg_id_base], vnepg_id_map[tvg_id_base]["name"]

    if key in vnepg_name_map:
        return vnepg_name_map[key], vnepg_name_map[key]["name"]

    key_collapsed = collapse(key)
    if key_collapsed in vnepg_name_map:
        return vnepg_name_map[key_collapsed], vnepg_name_map[key_collapsed]["name"]

    return None, clean

def resolve_channel(raw_name, tvg_id, vnepg_id_map, vnepg_name_map):
    entry, resolved = resolve_vnepg_entry(raw_name, tvg_id, vnepg_id_map, vnepg_name_map)

    if entry is None:
        clean_no_mark = remove_accents(resolved)
        for canonical, variants in CHANNEL_MAPPING.items():
            variants_no_mark = [remove_accents(v) for v in variants]
            if clean_no_mark == remove_accents(canonical) or clean_no_mark in variants_no_mark:
                resolved = canonical
                again_key = remove_accents(canonical)
                entry = vnepg_name_map.get(again_key) or vnepg_name_map.get(collapse(again_key))
                break

    canonical_id = entry["id"] if (entry and entry.get("id")) else collapse(remove_accents(resolved)) or "unknown"
    display_name = DISPLAY_NAME_OVERRIDE.get(resolved, resolved)
    return canonical_id, display_name

def is_blocked_channel(raw_name, tvg_id):
    """Danh cho nguon KHONG co group-title dang tin cay: chan theo ten kenh/
    tvg-id (khong dau, khong phan biet hoa-thuong, khong can ranh gioi tu,
    de bat duoc ca dang dinh lien nhau kieu "VSBet")."""
    haystack = remove_accents(raw_name) + " " + (tvg_id or "")
    return any(kw in haystack for kw in BLOCKED_NAME_KEYWORDS)

def is_blocked_group(group_name):
    """Danh cho nguon CO group-title dang tin cay: chan theo TEN NHOM thay vi
    ten tung kenh/phim, tranh dinh oan cac ten phim/kenh co chua tu tieng
    Anh trung ngau nhien (vd phim "Betting With Ghost" khong phai kenh ca
    do)."""
    haystack = remove_accents(group_name or "")
    return any(kw in haystack for kw in BLOCKED_GROUP_KEYWORDS)

def determine_group(canonical_name, tvg_id, canonical_id):
    tvg_id = ID_ALIASES.get(tvg_id, tvg_id)
    gid = canonical_id or tvg_id or ""
    if gid.startswith("vtv") or gid == "vietnamtoday": return "VTV"
    if gid.startswith("htv"): return "HTV"
    if gid.startswith("sctv"): return "SCTV"
    if gid in ("antvhd", "qpvnhd"): return "Kenh Dac Biet"
    if gid.startswith("vov") or gid.startswith("voh"): return "VOV"
    # Nhom "ON" (ON Kids, ON Life, ON Movies, ON Vie Drama...): id dang
    # "onkids"/"onlife"/"onmovies"/"onviedrama" bat dau bang "on" + chu cai.
    if re.match(r'^on[a-z]', gid): return "ON"

    name_lower = remove_accents(canonical_name)
    if any(x in name_lower for x in ["vtv", "vietnam today"]): return "VTV"
    if "htv" in name_lower: return "HTV"
    if "sctv" in name_lower: return "SCTV"
    if any(x in name_lower for x in ["antv", "an ninh", "qpvn", "quoc phong", "quoc hoi"]): return "Kenh Dac Biet"
    if any(x in name_lower for x in ["vov", "voh", "zing"]): return "VOV"
    # Ten kenh dang "ON <ten>" (vd "On Kids", "On Life", "On Movies",
    # "On Vie Drama") - kiem tra tu dau rieng biet de tranh nham voi cac
    # ten dia phuong khac (khong co ten nao trong CHANNEL_MAPPING/Dia
    # Phuong bat dau bang tu "on" rieng biet).
    if re.match(r'^on\s', name_lower): return "ON"
    return "Dia Phuong"

# ====================================================================
# 4. CAP NHAT vnepg_backup.json TU API TRUC TIEP QUA NHIEU NGUON PROXY VN
# ====================================================================
def _dedup_proxies(proxy_list):
    seen = set()
    out = []
    for p in proxy_list:
        key = p.split("://")[-1]  # so trung theo ip:port, bo qua giao thuc
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out

def fetch_from_proxyscrape():
    """Nguon 1: ProxyScrape API (http/socks4/socks5, VN)."""
    out = []
    try:
        url = ("https://api.proxyscrape.com/v4/free-proxy-list/get"
               "?request=display_proxies&proxy_format=protocolipport&format=text&country=vn")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        content = urllib.request.urlopen(req, timeout=SOURCE_FETCH_TIMEOUT).read().decode("utf-8")
        out = [l.strip() for l in content.splitlines() if l.strip()]
        print(f"  [ProxyScrape] lay duoc {len(out)} proxy.")
    except Exception as e:
        print(f"  [ProxyScrape] loi: {e}")
    return out

def fetch_from_geonode():
    """Nguon 2: Geonode free proxy API, loc theo country=VN."""
    out = []
    try:
        url = ("https://proxylist.geonode.com/api/proxy-list"
               "?country=VN&protocols=http,https,socks4,socks5"
               "&limit=100&page=1&sort_by=lastChecked&sort_type=desc")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        content = urllib.request.urlopen(req, timeout=SOURCE_FETCH_TIMEOUT).read().decode("utf-8")
        data = json.loads(content)
        for item in data.get("data", []):
            ip = item.get("ip")
            port = item.get("port")
            protocols = item.get("protocols") or ["http"]
            if ip and port:
                proto = protocols[0]
                out.append(f"{proto}://{ip}:{port}")
        print(f"  [Geonode] lay duoc {len(out)} proxy.")
    except Exception as e:
        print(f"  [Geonode] loi: {e}")
    return out

def fetch_from_proxylist_download():
    """Nguon 3: proxy-list.download API, loc theo country=VN, nhieu giao thuc.
    Co delay giua cac lan goi de tranh bi rate-limit (429), va dung som neu
    da bi rate-limit (goi them cung mot domain chi ton thoi gian vo ich)."""
    out = []
    for proto in ("http", "https", "socks4", "socks5"):
        try:
            url = f"https://www.proxy-list.download/api/v1/get?type={proto}&country=VN"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            content = urllib.request.urlopen(req, timeout=SOURCE_FETCH_TIMEOUT).read().decode("utf-8")
            lines = [l.strip() for l in content.splitlines() if l.strip() and ":" in l]
            scheme = "http" if proto in ("http", "https") else proto
            out.extend([f"{scheme}://{l}" for l in lines])
        except urllib.error.HTTPError as e:
            print(f"  [proxy-list.download:{proto}] loi: {e}")
            if e.code == 429:
                print("  [proxy-list.download] bi rate-limit, dung som nguon nay.")
                break
        except Exception as e:
            print(f"  [proxy-list.download:{proto}] loi: {e}")
        time.sleep(2)  # gian cach giua cac request de giam rui ro bi rate-limit
    print(f"  [proxy-list.download] lay duoc {len(out)} proxy.")
    return out

def fetch_vn_proxies():
    """Gom proxy VN tu nhieu nguon free, loai trung, xao tron, gioi han so
    luong thu de khong lam workflow chay qua lau."""
    print("Dang gom proxy VN tu nhieu nguon...")
    all_proxies = []
    all_proxies.extend(fetch_from_proxyscrape())
    all_proxies.extend(fetch_from_geonode())
    all_proxies.extend(fetch_from_proxylist_download())

    all_proxies = _dedup_proxies(all_proxies)
    random.shuffle(all_proxies)
    print(f"Tong cong {len(all_proxies)} proxy VN doc nhat (thu toi da {MAX_PROXY_TRIES}).")
    return all_proxies[:MAX_PROXY_TRIES]

def fetch_channels_via_proxy(proxy_url):
    """Goi API vnepg.site/api/channels thong qua 1 proxy VN cu the."""
    proxy_handler = urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
    opener = urllib.request.build_opener(proxy_handler)
    req = urllib.request.Request(VNEPG_API_URL, headers={"User-Agent": "Mozilla/5.0"})
    with opener.open(req, timeout=PROXY_REQUEST_TIMEOUT) as resp:
        data = resp.read().decode("utf-8")
    return json.loads(data)

def normalize_vnepg_api_response(raw):
    """Chuan hoa JSON tra ve tu API sang dinh dang {"channels":[{"id","name",
    "logo"}...]}. Thu doan cac ten truong pho bien; neu vnepg_backup.json sau
    khi build thieu du lieu, kiem tra JSON that va sua lai danh sach key."""
    items = raw
    if isinstance(raw, dict):
        items = None
        for key in ("channels", "data", "items", "results"):
            if isinstance(raw.get(key), list):
                items = raw[key]
                break
        if items is None:
            items = []

    channels = []
    for it in items:
        if not isinstance(it, dict):
            continue
        cid = str(it.get("id") or it.get("channel_id") or it.get("slug") or "").strip().lower()
        name = str(it.get("name") or it.get("title") or it.get("display_name") or "").strip()
        logo = str(it.get("logo") or it.get("icon") or it.get("logo_url") or it.get("image") or "").strip()
        if name:
            channels.append({"id": cid, "name": name, "logo": logo})
    return {"channels": channels}

def update_vnepg_backup():
    """Thu fetch du lieu moi tu API qua danh sach proxy VN gom tu nhieu
    nguon. Neu thanh cong (co it nhat 1 kenh hop le), GHI DE vnepg_backup.json.
    Neu that bai voi TAT CA proxy da thu, GIU NGUYEN file hien co (fallback
    = ban da commit lan chay truoc)."""
    print("=== Cap nhat vnepg_backup.json tu API (qua proxy VN) ===")
    proxies = fetch_vn_proxies()

    if not proxies:
        print("Khong lay duoc proxy tu nguon nao. Giu nguyen vnepg_backup.json cu.")
        return False

    for i, proxy in enumerate(proxies, 1):
        try:
            print(f"  [{i}/{len(proxies)}] Thu proxy: {proxy}")
            raw = fetch_channels_via_proxy(proxy)
            normalized = normalize_vnepg_api_response(raw)
            if not normalized["channels"]:
                print("    -> Du lieu rong/khong khop schema, bo qua.")
                continue
            with open(VNEPG_BACKUP_FILE, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            print(f"    -> THANH CONG! Da cap nhat {len(normalized['channels'])} kenh.")
            return True
        except Exception as e:
            print(f"    -> Loi: {e}")
            continue

    print("Khong co proxy VN nao hoat dong sau khi thu het. "
          "Giu nguyen vnepg_backup.json cu lam fallback.")
    return False

# ====================================================================
# 5. NAP VNEPG_BACKUP.JSON (sau khi da thu cap nhat o buoc 4)
# ====================================================================
def load_vnepg_backup():
    id_map, name_map = {}, {}
    if not os.path.exists(VNEPG_BACKUP_FILE):
        print(f"Khong tim thay {VNEPG_BACKUP_FILE}, bo qua.")
        return id_map, name_map
    try:
        with open(VNEPG_BACKUP_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        for ch in data.get("channels", []):
            cid = str(ch.get("id", "")).strip().lower()
            name = str(ch.get("name", "")).strip()
            logo = str(ch.get("logo", "")).strip()
            if not name:
                continue
            entry = {"id": cid, "name": name, "logo": logo}
            if cid:
                id_map[cid] = entry
            key = remove_accents(name)
            name_map[key] = entry
            name_map.setdefault(collapse(key), entry)
        print(f"Da nap {len(id_map)} kenh tu vnepg_backup.json.")
    except Exception as e:
        print(f"Loi doc {VNEPG_BACKUP_FILE}: {e}")
    return id_map, name_map

# ====================================================================
# 6. NAP DU LIEU IPTV-ORG (channels.json + logos.json)
# ====================================================================
def http_get_json(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    content = urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8")
    return json.loads(content)

def load_iptvorg_data():
    name_to_id = {}
    id_to_logo = {}
    try:
        channels = http_get_json(IPTV_ORG_CHANNELS_API, timeout=30)
        for ch in channels:
            cid = ch.get("id")
            if not cid:
                continue
            names = [ch.get("name", "")] + list(ch.get("alt_names") or [])
            for n in names:
                n = remove_accents(str(n))
                if n and n not in name_to_id:
                    name_to_id[n] = cid
        print(f"Da nap {len(channels)} kenh tu channels.json cua iptv-org.")
    except Exception as e:
        print(f"Loi nap channels.json: {e}")

    try:
        logos = http_get_json(IPTV_ORG_LOGOS_API, timeout=30)
        for item in logos:
            cid = item.get("channel")
            url = item.get("url")
            if cid and url and cid not in id_to_logo:
                id_to_logo[cid] = url
        print(f"Da nap {len(id_to_logo)} logo tu logos.json cua iptv-org.")
    except Exception as e:
        print(f"Loi nap logos.json: {e}")

    return name_to_id, id_to_logo

# ====================================================================
# 7. VALIDATE URL
# ====================================================================
_validate_cache = {}

def validate_url(url, timeout=4):
    if not url:
        return False
    if url in _validate_cache:
        return _validate_cache[url]
    if any(d in url for d in TRUSTED_DOMAINS):
        _validate_cache[url] = True
        return True
    ok = False
    try:
        req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ok = 200 <= resp.status < 400
    except Exception:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Range": "bytes=0-1024"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                ok = 200 <= resp.status < 400
        except Exception:
            ok = False
    _validate_cache[url] = ok
    return ok

def is_safe_format(url):
    path = url.split("?")[0].split("#")[0].lower()
    return path.endswith(SAFE_LOGO_EXTS)

# ====================================================================
# 8. CHON LOGO KENH
# ====================================================================
def pick_logo(canonical_name, canonical_id, tvg_id, vnepg_id_map, vnepg_name_map,
              iptvorg_name_to_id, iptvorg_id_to_logo, raw_logo):
    tvg_id = ID_ALIASES.get(tvg_id, tvg_id)
    ordered_candidates = []

    if tvg_id and "@" in tvg_id:
        base_id = tvg_id.split("@")[0]
        for cid in iptvorg_id_to_logo:
            if cid.lower() == base_id:
                ordered_candidates.append(iptvorg_id_to_logo[cid])
                break
    cid = iptvorg_name_to_id.get(remove_accents(canonical_name))
    if cid and cid in iptvorg_id_to_logo:
        ordered_candidates.append(iptvorg_id_to_logo[cid])
    hinted_id = IPTV_ORG_ID_HINTS.get(canonical_name)
    if hinted_id and hinted_id in iptvorg_id_to_logo:
        ordered_candidates.append(iptvorg_id_to_logo[hinted_id])
    guess_id = re.sub(r'[^A-Za-z0-9]', '', canonical_name) + ".vn"
    if guess_id in iptvorg_id_to_logo:
        ordered_candidates.append(iptvorg_id_to_logo[guess_id])

    if raw_logo:
        ordered_candidates.append(raw_logo)

    if canonical_id and canonical_id in vnepg_id_map and vnepg_id_map[canonical_id]["logo"]:
        ordered_candidates.append(vnepg_id_map[canonical_id]["logo"])

    ordered_candidates = [c for c in ordered_candidates
                           if not any(d in c for d in BLOCKED_LOGO_DOMAINS)]

    validated = [c for c in ordered_candidates if validate_url(c)]
    if not validated:
        return ""
    safe = [c for c in validated if is_safe_format(c)]
    return safe[0] if safe else validated[0]

# ====================================================================
# 9. GROUP-LOGO
# ====================================================================
STATIC_GROUP_LOGOS = {
    "VTV": "https://upload.wikimedia.org/wikipedia/commons/2/22/VTV_2013.png",
    # Logo chinh thuc VTVcab (2023), lay qua Special:FilePath de khong can do
    # chinh xac hash thu muc luu tru cua Wikimedia.
    "VTVCab": "https://commons.wikimedia.org/wiki/Special:FilePath/VTVcab_logo_2023_(2).svg",
    "HTV": "https://upload.wikimedia.org/wikipedia/commons/7/74/HTV_Logo.png",
    # CHUA tim duoc logo HTVC dang tin cay (CC/PD) tren Wikimedia Commons -
    # de trong, script se tu bo qua group-logo cho nhom nay. Neu ban co URL
    # logo HTVC chinh thuc, them vao day.
    "SCTV": "https://upload.wikimedia.org/wikipedia/commons/d/d3/SCTV_logo_%28Vietnam%29.svg",
    "Kenh Dac Biet": "https://upload.wikimedia.org/wikipedia/commons/a/a3/Emblem_of_Vietnam.svg",
    "VOV": "https://upload.wikimedia.org/wikipedia/commons/d/dd/Logo_VOV.svg",
    # CHUA tim duoc logo thuong hieu "ON" chinh thuc tren Wikimedia Commons de
    # dam bao ban quyen/do tin cay nhu cac nhom khac. Tam dung logo cua kenh
    # "ON Kids" (da co san trong nguon du lieu, tu epg.io.vn) lam dai dien
    # chung cho nhom. Neu ban co URL logo "ON" chinh thuc, thay the o day.
    "ON": "https://epg.io.vn/logos/29.png",
    "Dia Phuong": "https://upload.wikimedia.org/wikipedia/commons/2/21/Flag_of_Vietnam.svg",
}

# ====================================================================
# 10. LOGIC CHINH
# ====================================================================
def main():
    update_vnepg_backup()

    print("Nap vnepg_backup.json...")
    vnepg_id_map, vnepg_name_map = load_vnepg_backup()

    print("Nap du lieu iptv-org (channels.json + logos.json)...")
    iptvorg_name_to_id, iptvorg_id_to_logo = load_iptvorg_data()

    channels_data = {}
    group_first_seen = []  # thu tu xuat hien lan dau cua tung nhom (giu on dinh)

    print("Phan tich cac nguon M3U...")
    for source in M3U_SOURCES:
        url = source["url"]
        trust_group_title = source.get("trust_group_title", False)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            content = urllib.request.urlopen(req, timeout=15).read().decode("utf-8-sig")
            lines = content.splitlines()

            current_extinf = ""
            current_raw_name = ""
            current_extra_tags = []  # vd #EXTVLCOPT:http-user-agent=..., #KODIPROP:...

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                if line.lower().startswith("#extinf:"):
                    current_extinf = line
                    match = re.search(r',([^,]*)$', line)
                    current_raw_name = match.group(1).strip() if match else line.split(',')[-1].strip()
                    current_extra_tags = []
                    continue

                # Cac tag phu tro can giu nguyen di kem URL (vd user-agent
                # rieng, drm key...) - KHONG duoc bo qua, neu khong stream se
                # khong phat duoc tren player.
                if line.lower().startswith("#extvlcopt:") or line.lower().startswith("#kodiprop:"):
                    current_extra_tags.append(line)
                    continue

                # Cac dong # khac khong ro dinh dang (comment la, vv.) - bo qua
                # nhung KHONG reset context dang xu ly.
                if line.startswith("#"):
                    continue

                if line.startswith("http") and current_extinf and current_raw_name:
                    tvg_id = get_tvg_id(current_extinf)
                    exclude_group_keys = source.get("exclude_group_keys")

                    # Nguon KHONG co group-title dang tin cay: chan som theo
                    # ten kenh/tvg-id (vd "VSBet"), truoc khi resolve.
                    if not trust_group_title and is_blocked_channel(current_raw_name, tvg_id):
                        print(f"  [BI CHAN - TEN KENH] Loai bo: {current_raw_name}")
                        current_extinf = ""
                        current_raw_name = ""
                        current_extra_tags = []
                        continue

                    # Loai bo SOM theo group-title GOC cua nguon (truoc khi
                    # chuan hoa/resolve), ap dung rieng cho nguon co khai bao
                    # exclude_group_keys (hien tai la nguon vmttv) - loai cac
                    # nhom Live Event/Radio/nuoc ngoai/nhom trung voi nguon khac.
                    if exclude_group_keys:
                        raw_group_key = group_match_key(get_source_group_title(current_extinf))
                        if raw_group_key in exclude_group_keys:
                            current_extinf = ""
                            current_raw_name = ""
                            current_extra_tags = []
                            continue

                    canonical_id, display_name = resolve_channel(current_raw_name, tvg_id, vnepg_id_map, vnepg_name_map)
                    dedup_key = remove_accents(display_name)

                    if trust_group_title:
                        source_group = get_source_group_title(current_extinf)
                        group = normalize_group_title(source_group) if source_group else determine_group(display_name, tvg_id, canonical_id)
                    else:
                        group = determine_group(display_name, tvg_id, canonical_id)

                    # Kiem tra lai theo group CUOI CUNG (bat ca truong hop kenh
                    # khong co group-title rieng, roi ve determine_group() ra
                    # dung 1 trong cac nhom bi loai, vd "ON"/"Dia Phuong").
                    if exclude_group_keys and group_match_key(group) in exclude_group_keys:
                        current_extinf = ""
                        current_raw_name = ""
                        current_extra_tags = []
                        continue

                    # Nguon CO group-title dang tin cay: chan theo TEN NHOM,
                    # tranh dinh oan ten phim/kenh (vd "Betting With Ghost").
                    if trust_group_title and is_blocked_group(group):
                        print(f"  [BI CHAN - NHOM] Loai bo '{current_raw_name}' (nhom: {group})")
                        current_extinf = ""
                        current_raw_name = ""
                        current_extra_tags = []
                        continue

                    if group not in group_first_seen:
                        group_first_seen.append(group)

                    raw_logo = ""
                    raw_match = re.search(r'tvg-logo="([^"]*)"', current_extinf, re.IGNORECASE)
                    if raw_match:
                        raw_logo = raw_match.group(1).strip()

                    final_logo = pick_logo(display_name, canonical_id, tvg_id, vnepg_id_map, vnepg_name_map,
                                            iptvorg_name_to_id, iptvorg_id_to_logo, raw_logo)

                    entry = {"url": line, "tags": list(current_extra_tags)}

                    if dedup_key not in channels_data:
                        channels_data[dedup_key] = {"name": display_name, "id": canonical_id,
                                                     "group": group, "logo": final_logo, "urls": [entry]}
                    else:
                        existing_urls = [e["url"] for e in channels_data[dedup_key]["urls"]]
                        if line not in existing_urls:
                            channels_data[dedup_key]["urls"].append(entry)
                        if not channels_data[dedup_key]["logo"] and final_logo:
                            channels_data[dedup_key]["logo"] = final_logo

                    current_extinf = ""
                    current_raw_name = ""
                    current_extra_tags = []

        except Exception as e:
            print(f"Loi khi xu ly {url}: {e}")

    print("Xuat ra listtivi.m3u...")

    # Thu tu nhom cuoi cung: nhom "goc" (BASE_GROUP_ORDER, chi lay nhom nao
    # thuc su co kenh) -> cac nhom MOI phat hien theo dung thu tu xuat hien
    # lan dau -> nhom "gom-tat-ca" (Dia Phuong) luon o cuoi.
    dynamic_extra_groups = [g for g in group_first_seen
                             if g not in BASE_GROUP_ORDER and g != CATCH_ALL_GROUP]
    final_group_order = (
        [g for g in BASE_GROUP_ORDER if g in group_first_seen]
        + dynamic_extra_groups
        + ([CATCH_ALL_GROUP] if CATCH_ALL_GROUP in group_first_seen else [])
    )

    final_ordered = OrderedDict()
    for g in final_group_order:
        sorted_keys = sorted(
            [k for k, v in channels_data.items() if v["group"] == g],
            key=lambda s: [int(t) if t.isdigit() else t for t in re.split(r'(\d+)', s)]
        )
        for k in sorted_keys:
            final_ordered[k] = channels_data[k]
    leftover = [k for k in channels_data if k not in final_ordered]
    for k in sorted(leftover):
        final_ordered[k] = channels_data[k]

    print(f"Cac nhom kenh trong ban build nay ({len(final_group_order)}): {final_group_order}")

    with open("listtivi.m3u", "w", encoding="utf-8") as f:
        f.write('#EXTM3U url-tvg="https://vnepg.site/epg.xml.gz"\n')
        for data in final_ordered.values():
            if not data["urls"]:
                continue

            id_attr = f' tvg-id="{data["id"]}"' if data["id"] else ""
            logo_attr = f' tvg-logo="{data["logo"]}"' if data["logo"] else ""
            group_logo_url = STATIC_GROUP_LOGOS.get(data["group"], "")
            group_logo_attr = f' group-logo="{group_logo_url}"' if group_logo_url else ""

            for idx, entry in enumerate(data["urls"]):
                suffix = f" [Du phong {idx}]" if idx > 0 else ""
                extinf = (f'#EXTINF:-1{id_attr}{logo_attr}{group_logo_attr} '
                          f'group-title="{data["group"]}",{data["name"]}{suffix}')
                f.write(f'{extinf}\n')
                for tag in entry["tags"]:
                    f.write(f'{tag}\n')
                f.write(f'{entry["url"]}\n')

    print("Build Success!")

if __name__ == "__main__":
    main()
