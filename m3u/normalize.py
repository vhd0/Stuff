"""
Chuan hoa ten kenh (muc 4 CHANNEL NAME NORMALIZATION) va ten nhom.
"""

import re

# Nhan/annotation ky thuat can loai bo khoi TEN HIEN THI (muc 4). Ap dung
# TRUOC khi bo dau, nen viet ca dang co dau/khong dau, hoa/thuong.
_TECH_LABEL_PATTERNS = [
    r'\bHD\b', r'\bFHD\b', r'\bUHD\b', r'\b4K\b', r'\b8K\b', r'\bSD\b',
    r'\b720P\b', r'\b1080P\b', r'\b1440P\b', r'\b2160P\b',
    r'\b50FPS\b', r'\b60FPS\b', r'\bHEVC\b', r'\bH\.?265\b', r'\bH\.?264\b',
]

# Chu thich nguon can loai bo hoan toan (ca ngoac).
_SOURCE_ANNOTATION_PATTERNS = [
    r'\[\s*Geo[\s\-]?Blocked\s*\]', r'\(\s*Geo[\s\-]?Blocked\s*\)',
    r'\[\s*Not\s*24/7\s*\]',
    r'\[\s*Backup\s*\]',
    r'\[\s*TEST\s*\]',
]

_TECH_RE = re.compile('|'.join(_TECH_LABEL_PATTERNS), re.IGNORECASE)
_ANNOTATION_RE = re.compile('|'.join(_SOURCE_ANNOTATION_PATTERNS), re.IGNORECASE)


def clean_display_name(raw_name):
    """Loai bo nhan ky thuat + chu thich nguon khoi ten kenh, KHONG dong
    cham vao tu ngu mang y nghia dinh danh kenh (muc 4).

    Vi du: 'VTV7 HD [Geo-blocked]' -> 'VTV7'
           'VTV5 HD 50FPS' -> 'VTV5'
    """
    name = raw_name
    name = _ANNOTATION_RE.sub('', name)
    name = _TECH_RE.sub('', name)
    # Don khoang trang/dau ngoac rong con sot lai sau khi xoa nhan.
    name = re.sub(r'\(\s*\)|\[\s*\]', '', name)
    name = re.sub(r'\s+', ' ', name).strip(" -_,")
    return name.strip()


def remove_accents(s):
    s = (s or "").lower()
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


# Hau to chat luong/domain hay bi cac nguon gan khac nhau vao CUNG 1
# tvg-id thuc te, khien 2 bien the cua CUNG 1 kenh bi coi la khac nhau neu
# so sanh tvg-id THO (vi du: "vtv1hd" vs "vtv1.vn@hd" deu la VTV1, nhung
# khac nhau hoan toan neu khong chuan hoa).
_TVG_ID_QUALITY_SUFFIX_RE = re.compile(r'(hd|fhd|uhd|4k|sd|1080p|720p)$')
_TVG_ID_DOMAIN_SUFFIX_RE = re.compile(r'\.(vn|com|net|org|tv)$')


def normalize_tvg_id(tvg_id):
    """Chuan hoa tvg-id ve 1 dang DUY NHAT bat ke nguon dinh dang khac
    nhau nhu the nao, dung lam khoa gop kenh (muc 5 CHANNEL IDENTITY).

    Vi du: "vtv1hd" va "vtv1.vn@hd" deu chuan hoa ve "vtv1":
      "vtv1.vn@hd" -> bo phan sau "@" -> "vtv1.vn"
                    -> bo hau to domain ".vn" -> "vtv1"
      "vtv1hd"      -> khong co "@"/domain -> bo hau to chat luong "hd" -> "vtv1"
    """
    if not tvg_id:
        return ""
    t = tvg_id.strip().lower()
    t = t.split("@")[0]  # bo phan chat luong sau dau "@" (vd "@hd", "@sd")
    t = _TVG_ID_DOMAIN_SUFFIX_RE.sub('', t)  # bo hau to domain (".vn", ".com"...)
    t = _TVG_ID_QUALITY_SUFFIX_RE.sub('', t)  # bo hau to chat luong dinh lien ("hd", "fhd"...)
    return t


def identity_key(clean_name, tvg_id="", tvg_name=""):
    """Khoa dinh danh dung de gop cac bien the ve CUNG 1 kenh (muc 5), khi
    chua tra duoc qua channels.yaml aliases. Uu tien tvg-id da CHUAN HOA
    (xem normalize_tvg_id), sau do tvg-name, cuoi cung moi fallback ve ten
    da chuan hoa (da bo nhan ky thuat) + collapse khoang trang.

    tvg-id LUON duoc chuan hoa truoc khi dung lam khoa, vi cac nguon khac
    nhau dat tvg-id theo quy uoc khac nhau cho CUNG 1 kenh (vd "vtv1hd" vs
    "vtv1.vn@hd" - neu khong chuan hoa se bi coi la 2 kenh khac nhau)."""
    norm_tvg_id = normalize_tvg_id(tvg_id)
    if norm_tvg_id:
        return norm_tvg_id
    if tvg_name:
        return collapse(remove_accents(tvg_name))
    return collapse(remove_accents(clean_name)) or "unknown"


def group_match_key(s):
    """Chuan hoa ten nhom (group-title) ve key chi gom chu/so, khong dau,
    khong phan biet hoa-thuong, da bo emoji/dau gach - dung de so khop
    CHINH XAC voi group_alias trong groups.yaml."""
    return re.sub(r'[^a-z0-9]', '', remove_accents(s or ""))
