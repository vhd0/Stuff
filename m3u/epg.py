"""
EPG: chi dung https://lichphatsong.io.vn/epg.xml (muc 3). Validate XML
truoc khi thay cache; neu fetch/validate loi thi GIU NGUYEN cache cu, va
LOI EPG KHONG DUOC LAM MAT cac kenh hop le khac trong playlist.
"""

import os
import urllib.request
import xml.etree.ElementTree as ET

from . import config


def fetch_and_validate_epg():
    """Tra ve dict trang thai EPG de ghi vao build_stats.json. KHONG BAO
    GIO raise - loi EPG la non-fatal doi voi toan bo pipeline."""
    try:
        req = urllib.request.Request(config.EPG_URL, headers={"User-Agent": "Mozilla/5.0"})
        content = urllib.request.urlopen(req, timeout=20).read()
        ET.fromstring(content)  # chi validate cau truc XML, khong parse sau

        os.makedirs(config.CACHE_DIR, exist_ok=True)
        with open(config.EPG_CACHE_PATH, "wb") as f:
            f.write(content)

        return {"status": "ok", "url": config.EPG_URL}

    except Exception as e:
        has_cache = os.path.exists(config.EPG_CACHE_PATH)
        print(f"[epg] Fetch/validate that bai ({e}). "
              f"{'Giu nguyen cache cu.' if has_cache else 'Chua co cache truoc do.'}")
        return {
            "status": "stale_cache" if has_cache else "failed",
            "url": config.EPG_URL,
            "error": str(e),
        }
