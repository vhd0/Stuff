"""
Parser cho tung dinh dang nguon: M3U/NDL (muc 2: VMTTV, DLTivi, IPTV-org,
EaSport) va JSON (muc 2: TinhLaGi).

Moi entry tra ve la 1 dict tho (CHUA qua chuan hoa ten/nhom):
  raw_name, tvg_id, tvg_name, group_raw, logo, url, tags, is_kodiprop
"tags" la danh sach cac dong #EXTVLCOPT/#KODIPROP di kem URL (muc "STREAM
HEALTH"), phai duoc GIU NGUYEN de phat duoc tren player (user-agent rieng,
DRM key...).
"""

import json
import re


def _get_attr(extinf_line, attr):
    m = re.search(rf'{attr}="([^"]*)"', extinf_line, re.IGNORECASE)
    return m.group(1).strip() if m else ""


def parse_m3u_source(text):
    entries = []
    lines = text.splitlines()

    current_extinf = ""
    current_raw_name = ""
    current_extra_tags = []
    is_kodiprop = False

    for line in lines:
        line = line.strip()
        if not line:
            continue

        low = line.lower()

        if low.startswith("#extinf:"):
            current_extinf = line
            m = re.search(r',([^,]*)$', line)
            current_raw_name = m.group(1).strip() if m else line.split(',')[-1].strip()
            current_extra_tags = []
            is_kodiprop = False
            continue

        if low.startswith("#extvlcopt:"):
            current_extra_tags.append(line)
            continue

        if low.startswith("#kodiprop:"):
            current_extra_tags.append(line)
            is_kodiprop = True
            continue

        if line.startswith("#"):
            # Comment/tag khong ro dinh dang - bo qua, KHONG reset context.
            continue

        if line.startswith("http") and current_extinf and current_raw_name:
            entries.append({
                "raw_name": current_raw_name,
                "tvg_id": _get_attr(current_extinf, "tvg-id").lower(),
                "tvg_name": _get_attr(current_extinf, "tvg-name"),
                "group_raw": _get_attr(current_extinf, "group-title"),
                "logo": _get_attr(current_extinf, "tvg-logo"),
                "url": line,
                "tags": list(current_extra_tags),
                "is_kodiprop": is_kodiprop,
            })
            current_extinf = ""
            current_raw_name = ""
            current_extra_tags = []
            is_kodiprop = False

    return entries


def parse_json_source(text):
    """Parser 'best-effort' cho nguon TinhLaGi (tv.json). Schema thuc te
    CHUA duoc xac minh (endpoint khong the kiem tra truc tiep tu moi
    truong build nay) - ham nay doan cac ten truong pho bien. Neu
    build_stats.json cho thay nguon nay dong gop 0 kenh, hay kiem tra JSON
    that va cap nhat danh sach ten truong duoi day."""
    try:
        data = json.loads(text)
    except Exception:
        return []

    items = data
    if isinstance(data, dict):
        items = None
        for key in ("channels", "data", "items", "list", "tv"):
            if isinstance(data.get(key), list):
                items = data[key]
                break
        if items is None:
            items = []

    entries = []
    for it in items:
        if not isinstance(it, dict):
            continue
        name = str(it.get("name") or it.get("title") or it.get("channel") or "").strip()
        url = str(it.get("url") or it.get("link") or it.get("stream_url")
                   or it.get("stream") or it.get("source") or "").strip()
        if not name or not url:
            continue
        entries.append({
            "raw_name": name,
            "tvg_id": str(it.get("tvg_id") or it.get("id") or "").strip().lower(),
            "tvg_name": str(it.get("tvg_name") or "").strip(),
            "group_raw": str(it.get("group") or it.get("category") or it.get("genre") or "").strip(),
            "logo": str(it.get("logo") or it.get("icon") or it.get("thumbnail") or "").strip(),
            "url": url,
            "tags": [],
            "is_kodiprop": False,
        })
    return entries


def parse_source(source_format, text):
    if source_format == "json":
        return parse_json_source(text)
    return parse_m3u_source(text)
