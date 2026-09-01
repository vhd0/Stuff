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


def identity_key(clean_name, tvg_id="", tvg_name=""):
    """Khoa dinh danh dung de gop cac bien the ve CUNG 1 kenh (muc 5), khi
    chua tra duoc qua channels.yaml aliases. Uu tien tvg-id/tvg-name (it bi
    bien dang boi hau to chat luong hon ten hien thi), fallback ve ten da
    chuan hoa (da bo nhan ky thuat) + collapse khoang trang."""
    for candidate in (tvg_id, tvg_name):
        if candidate:
            return collapse(remove_accents(candidate))
    return collapse(remove_accents(clean_name)) or "unknown"


def group_match_key(s):
    """Chuan hoa ten nhom (group-title) ve key chi gom chu/so, khong dau,
    khong phan biet hoa-thuong, da bo emoji/dau gach - dung de so khop
    CHINH XAC voi group_alias trong groups.yaml."""
    return re.sub(r'[^a-z0-9]', '', remove_accents(s or ""))
