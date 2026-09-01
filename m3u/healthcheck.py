"""
Healthcheck song song cho danh sach URL DUY NHAT (muc 16 HEALTHCHECK).
KODIPROP: KHONG healthcheck, danh dau "skipped" nhung VAN eligible (muc 17).
"""

import re
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor

from . import config


def _extract_user_agent(tags):
    for t in tags:
        m = re.search(r'http-user-agent\s*=\s*(.+)$', t, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return "Mozilla/5.0"


def _check_one(url, user_agent):
    headers = {
        "User-Agent": user_agent,
        "Range": config.HEALTHCHECK_RANGE_BYTES,
    }
    try:
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=config.HEALTHCHECK_TIMEOUT) as resp:
            return 200 <= resp.status < 400
    except urllib.error.HTTPError as e:
        # 416 = Range Not Satisfiable: tai nguyen TON TAI nhung qua nho hon
        # range yeu cau - van coi la healthy. Cac ma 2xx-3xx khac da duoc
        # xu ly o nhanh binh thuong ben tren.
        if e.code == 416:
            return True
        return False
    except Exception:
        return False


def healthcheck_candidates(unique_candidates):
    """unique_candidates: dict {url: {"tags": [...], "is_kodiprop": bool}}.
    Tra ve dict {url: "healthy" | "dead" | "skipped"}. Moi URL duy nhat chi
    duoc healthcheck 1 LAN du xuat hien o nhieu nhom/kenh (muc 11, 16)."""
    results = {}
    to_check = []

    for url, meta in unique_candidates.items():
        if meta.get("is_kodiprop"):
            results[url] = "skipped"
        else:
            to_check.append((url, _extract_user_agent(meta.get("tags", []))))

    def worker(item):
        url, ua = item
        ok = _check_one(url, ua)
        return url, ("healthy" if ok else "dead")

    if to_check:
        with ThreadPoolExecutor(max_workers=config.HEALTHCHECK_WORKERS) as ex:
            for url, status in ex.map(worker, to_check):
                results[url] = status

    return results
