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
        # DA XAC MINH: endpoint nay dat ten ".json" nhung THUC TE tra ve noi
        # dung M3U/EXTM3U (khong phai JSON). parser.parse_source() tu nhan
        # dien dinh dang thuc te tu noi dung nen "format" o day chi la gia
        # tri fallback, khong con anh huong ket qua parse.
        "format": "m3u",
        # trust_group_title=True: cac nhom RO RANG theo genre (VTV/HTV/
        # SCTV/VTVcab/Dia Phuong) van duoc tin tuong binh thuong. Rieng cac
        # nhom "bundle" mo ho nhu "⭐ KÊNH YÊU THÍCH", "🌐 Quốc Tế VIP",
        # "📦 In The Box", "🎬 Rạp Phim" (gom lan cac kenh khac genre voi
        # nhau) KHONG duoc dua vao groups.yaml/group_alias, nen tu dong roi
        # qua OTT content classifier de phan loai dung theo TEN KENH thay vi
        # theo bundle mo ho cua nguon.
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

# LUU Y: guidelines.txt (ban dau) liet ke "https://tv.vietanhtv.top/sex/"
# la "endpoint nguoi lon da bi vo hieu hoa". Nguoi dung xac nhan LAI day
# CHI LA MOT DUONG DAN/TEN MIEN chua danh sach m3u thong thuong, KHONG
# phai noi dung nguoi lon - nen KHONG con bi chan theo domain nua (da xoa
# khoi BLOCKED_DOMAINS ben duoi). Neu ban muon dua link nay vao lam 1
# NGUON THUC SU (them vao SOURCES o tren), hay cung cap URL M3U/JSON cu
# the (vd "https://tv.vietanhtv.top/sex/tv.m3u" hay tuong tu) de bo sung.

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
    # nguoi lon / khieu dam (loc theo TEN KENH, khong lien quan domain
    # vietanhtv.top da giai thich o tren)
    "porn", "xxx", "adult",
    # ca do / gambling
    "casino", "gambling", "bet", "ca do", "cado",
    # kenh test/demo/offline ro rang
    "test channel", "demo channel", "no signal", "off air", "offline",
    "placeholder", "khong co tin hieu",
)

# Domain THAT SU can chan (rong - hien chua co domain nao duoc xac nhan la
# noi dung xau; vietanhtv.top DA DUOC BO KHOI danh sach nay theo xac nhan
# cua nguoi dung o tren).
BLOCKED_DOMAINS = ()

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
