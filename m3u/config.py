"""
Cau hinh trung tam cho pipeline M3U Optimizer v2.
Tuan thu guidelines.txt muc 2 (INPUT SOURCES), 3 (EPG), 13 (FILTERING),
15-16 (STREAM SELECTION / HEALTHCHECK).
"""

import os

# Thu muc goc cua package m3u/ (de tra cuu file cau hinh yaml, cache...)
M3U_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(M3U_DIR, "cache")
CHANNELS_YAML = os.path.join(M3U_DIR, "channels.yaml")
GROUPS_YAML = os.path.join(M3U_DIR, "groups.yaml")
BUILD_STATS_PATH = os.path.join(CACHE_DIR, "build_stats.json")
EPG_CACHE_PATH = os.path.join(CACHE_DIR, "epg.xml")

# Duong dan output cuoi cung (repo root, dung nhu quy uoc cu).
OUTPUT_M3U_PATH = "listtivi.m3u"

# ====================================================================
# 1. NGUON DAU VAO (muc 2 cua guidelines)
# Thu tu trong danh sach = SOURCE PRIORITY (index nho hon = uu tien cao hon)
# dung cho: chon logo, chon primary/backup stream (muc 15, 18).
# ====================================================================
SOURCES = [
    {
        "name": "vmttv",
        "url": "https://raw.githubusercontent.com/vuminhthanh12/vuminhthanh12/refs/heads/main/vmttv",
        "format": "m3u",
        "trust_group_title": True,
    },
    {
        "name": "dltivi",
        "url": "https://raw.githubusercontent.com/DinhLap96/ListTivi/refs/heads/main/ListTiVi/dltivi_v2.ndl",
        "format": "m3u",
        "trust_group_title": False,
    },
    {
        "name": "tinhlagi",
        "url": "https://tinhlagi.pro/tv.json",
        "format": "json",
        # CHUA xac minh duoc schema JSON thuc te cua nguon nay (endpoint tra
        # ve khong the kiem tra truc tiep tu moi truong build). Parser trong
        # m3u/parser.py doan cac ten truong pho bien (name/title, url/link/
        # stream, logo/icon, group/category) - neu sau khi chay build_stats
        # cho thay nguon nay dong gop 0 kenh, hay kiem tra JSON that va sua
        # lai parse_json_source() trong m3u/parser.py.
        "trust_group_title": True,
    },
    {
        "name": "iptv-org-vn",
        "url": "https://raw.githubusercontent.com/iptv-org/iptv/refs/heads/master/streams/vn.m3u",
        "format": "m3u",
        # iptv-org gan group-title="Vietnam" dong loat cho moi kenh trong
        # file nay -> khong co gia tri phan loai (khong the phan biet
        # VTV/HTV/the thao/tin tuc...), nen KHONG tin group-title cua nguon
        # nay, de OTT secondary classifier (muc 7) tu xu ly theo ten kenh.
        "trust_group_title": False,
    },
    {
        "name": "easport",
        "url": "https://livesport.s.gy/easport",
        "format": "m3u",
        # Nguon chuyen kenh the thao, thuong khong co group-title rieng biet
        # huu ich -> de OTT secondary classifier gan vao "The thao" mac dinh
        # khi khong nhan dang duoc chu de khac.
        "trust_group_title": False,
        "default_content_hint": "sports",
    },
]

# Endpoint nguoi lon DA BI VO HIEU HOA - KHONG duoc dua vao SOURCES o tren,
# va domain cua no cung nam trong BLOCKED_DOMAINS phia duoi de phong truong
# hop bi tham chieu gian tiep tu 1 nguon khac.
DISABLED_ADULT_SOURCE = "https://tv.vietanhtv.top/sex/"

# ====================================================================
# 2. EPG (muc 3)
# ====================================================================
EPG_URL = "https://lichphatsong.io.vn/epg.xml"

# ====================================================================
# 3. FILTERING - BAO THU (muc 13). Chi loai "high-confidence junk".
# KHONG dua tu khoa noi dung chung chung (movie/sport/news/event/live/
# channel) vao day.
# ====================================================================
BLOCKED_NAME_KEYWORDS = (
    # nguoi lon / khieu dam
    "porn", "xxx", "adult", "sex",
    # ca do / gambling
    "casino", "gambling", "bet", "ca do", "cado",
    # kenh test/demo/offline ro rang
    "test channel", "demo channel", "no signal", "off air", "offline",
    "placeholder", "khong co tin hieu",
)

BLOCKED_DOMAINS = (
    "vietanhtv.top",  # domain cua DISABLED_ADULT_SOURCE o tren
)

# ====================================================================
# 4. HEALTHCHECK (muc 16)
# ====================================================================
HEALTHCHECK_TIMEOUT = 8          # giay
HEALTHCHECK_RANGE_BYTES = "bytes=0-8191"
HEALTHCHECK_WORKERS = 32

# ====================================================================
# 5. STREAM SELECTION (muc 15)
# ====================================================================
MAX_STREAMS_PER_CHANNEL = 2  # 1 primary + 1 backup

# ====================================================================
# 6. QUALITY GATE (muc 20)
# ====================================================================
MIN_CHANNEL_COUNT = 50            # nguong toi thieu de chap nhan ban build
SPECIAL_GROUP = "⭐ Kênh đặc biệt"
SPECIAL_GROUP_WHITELIST = {"antv", "qpvn"}  # canonical_id duy nhat duoc phep

# Danh sach thu tu nhom cuoi cung (muc 8) - KHONG co "Quoc te".
FINAL_GROUP_ORDER = [
    "📺 VTV",
    "📺 HTV",
    "📡 VTVCab",
    "⭐ Kênh đặc biệt",
    "📡 HTVC",
    "📡 SCTV",
    "🏠 Địa phương",
    "📰 Tin tức",
    "🎬 Phim",
    "🎭 Giải trí",
    "🎵 Âm nhạc",
    "⚽ Thể thao",
    "👶 Thiếu nhi",
    "🎓 Giáo dục & Khám phá",
    "📻 Radio",
    "🎟️ Sự kiện",
    "📦 Khác",
]
OTHER_GROUP = "📦 Khác"
