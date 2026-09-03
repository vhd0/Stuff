"""
Cau hinh trung tam cho pipeline M3U Optimizer v3.

v3 THAY DOI LON so voi v2 (xem DESIGN_PHILOSOPHY.md o repo root de biet ly
do day du):
  - BO HAN healthcheck (qua cham, khong thuc te vi nhieu CDN VN chan/tra
    ket qua sai khi bi goi tu IP GitHub Actions - xem muc "TAI SAO BO
    HEALTHCHECK" trong DESIGN_PHILOSOPHY.md).
  - BO co che primary/backup VA bo luon danh sach nhieu URL thay the
    hien thi trung ten: MOI KENH CHI GIU DUNG 1 URL (uu tien cao nhat) de
    tranh 1 kenh bi hien thi lap lai nhieu lan trong playlist.
  - Them lai nguon vietanhtv (xac nhan la nguon m3u hop le, KHONG phai noi
    dung nguoi lon - ten duong dan "/sex/" chi la ngau nhien).
  - Fetch nguon "gia lap OTT app that" (header + retry) de tranh timeout
    (vd nguon EaSport).
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
# 1. NGUON DAU VAO
# Thu tu trong danh sach = SOURCE PRIORITY (index nho hon = uu tien cao
# hon) dung cho: chon logo (m3u/logo.py), va chon 1 URL duy nhat/kenh
# (m3u/config.py -> STREAMS_PER_CHANNEL).
# ====================================================================
SOURCES = [
    {
        "name": "vmttv",
        "url": "https://raw.githubusercontent.com/vuminhthanh12/vuminhthanh12/refs/heads/main/vmttv",
        "format": "m3u",
        "trust_group_title": True,
    },
    {
        "name": "vietanhtv",
        "url": "https://tv.vietanhtv.top/sex/",
        # DA XAC MINH truc tiep: day la 1 aggregator M3U HOP LE (VTV/HTV/
        # SCTV/ANTV/QPVN/dia phuong/radio day du + 1 khoi "Socolive" o
        # cuoi file - xem BLOCKED_STREAM_BRAND_GROUPS ben duoi). Duong dan
        # "/sex/" trong URL CHI LA TEN NGAU NHIEN cua tac gia, KHONG phai
        # dau hieu noi dung nguoi lon - da xac nhan qua noi dung thuc te.
        # File nay co 2 khoi #EXTM3U noi lien nhau (aggregator ghep nhieu
        # nguon con) - parser van xu ly dung vi dong "#EXTM3U" du thua
        # khong khop bat ky nhanh xu ly nao nen bi bo qua an toan.
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
        "trust_group_title": True,
    },
    {
        "name": "iptv-org-vn",
        "url": "https://raw.githubusercontent.com/iptv-org/iptv/refs/heads/master/streams/vn.m3u",
        "format": "m3u",
        # iptv-org gan group-title="Vietnam" dong loat cho moi kenh trong
        # file nay -> khong co gia tri phan loai, nen KHONG tin group-title
        # cua nguon nay, de OTT secondary classifier tu xu ly theo ten kenh.
        "trust_group_title": False,
    },
    {
        "name": "easport",
        "url": "https://livesport.s.gy/easport",
        "format": "m3u",
        "trust_group_title": False,
        "default_content_hint": "sports",
    },
]

# ====================================================================
# 2. EPG
# ====================================================================
EPG_URL = "https://lichphatsong.io.vn/epg.xml"

# ====================================================================
# 3. FILTERING - BAO THU. Chi loai "high-confidence junk". KHONG dua tu
# khoa noi dung chung chung (movie/sport/news/event/live/channel) vao day.
# ====================================================================
BLOCKED_NAME_KEYWORDS = (
    # nguoi lon / khieu dam (loc theo TEN KENH)
    "porn", "xxx", "adult",
    # ca do / gambling (tu khoa CHUNG, ap dung moi noi)
    "casino", "gambling", "bet", "ca do", "cado",
    # kenh test/demo/offline ro rang
    "test channel", "demo channel", "no signal", "off air", "offline",
    "placeholder", "khong co tin hieu",
)

# Domain THAT SU can chan - hien rong (vietanhtv.top DA duoc xac nhan hop
# le, khong con trong danh sach nay).
BLOCKED_DOMAINS = ()

# Ten NHOM (group-title) la THUONG HIEU WEB LAU BONG DA/CA DO da biet cu
# the tai VN (khac voi BLOCKED_NAME_KEYWORDS o tren la tu khoa CHUNG). Cac
# trang nay thuong dung "BLV <biet danh>" lam hau to ten de phan biet hang
# tram luong phat cho CUNG 1 tran dau, va gan lien voi quang cao ca do.
# CHI chan theo dung TEN THUONG HIEU cu the (khong phai tu chung chung nhu
# "the thao"/"live"), giu dung tinh than "bao thu" cua bo loc: khong đoán,
# chi loai khi co bang chung ro rang. Da phat hien "Socolive" trong nguon
# vietanhtv (xem SOURCES o tren). Neu ban KHONG muon chan nhom nay, xoa
# dong tuong ung khoi tuple ben duoi.
BLOCKED_STREAM_BRAND_GROUPS = (
    "socolive",
)

# ====================================================================
# 4. FETCH NGUON - GIA LAP 1 TRINH OTT APP THAT (khong con healthcheck,
# nhung van can fetch NOI DUNG DANH SACH KENH cho on dinh, tranh timeout
# nhu truong hop EaSport truoc day).
# ====================================================================
FETCH_TIMEOUT = 25          # giay - dai hon truoc (8s cu qua ngan cho 1 so
                             # nguon phan hoi cham, gay timeout gia).
FETCH_RETRIES = 3           # so lan thu lai neu that bai
FETCH_RETRY_BACKOFF = 3     # giay, tang dan giua cac lan retry
# User-Agent gia lap app Android that (Dalvik la runtime cua may ao Android)
# - da quan sat thay HAU HET cac CDN IPTV VN (mytvnet, fptplay, vtvprime,
# vtvdigital...) yeu cau dung UA kieu app di dong thay vi trinh duyet
# thong thuong, neu khong se bi tu choi/cham/timeout.
FETCH_USER_AGENT = "Dalvik/2.1.0 (Linux; U; Android 11; SM-G975F Build/RP1A.200720.012)"

# ====================================================================
# 5. STREAM - MOI KENH CHI GIU DUNG 1 URL (khong con danh sach thay the
# nhieu dong trung ten - da gay hien thi lap lai kenh nhieu lan trong
# playlist). Dedup URL trung nhau, sap theo (source priority, quality)
# roi CHI LAY 1 BAN GHI DAU TIEN. Khong con khai niem primary/backup (xem
# DESIGN_PHILOSOPHY.md) VA khong con nhieu dong thay the nua.
# ====================================================================
STREAMS_PER_CHANNEL = 1

# ====================================================================
# 6. QUALITY GATE
# ====================================================================
MIN_CHANNEL_COUNT = 50            # nguong toi thieu de chap nhan ban build
SPECIAL_GROUP = "⭐ Kênh đặc biệt"
SPECIAL_GROUP_WHITELIST = {"antv", "qpvn"}  # canonical_id duy nhat duoc phep

# Danh sach thu tu nhom cuoi cung - KHONG co "Quoc te".
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
