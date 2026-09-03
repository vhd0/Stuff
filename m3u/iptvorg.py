"""
Fallback logo tu IPTV-org (muc 18: 'IPTV-org logo la fallback ONLY').
Khong dung du lieu nay de dat ten/nhom - CHI dung cho logo khi khong nguon
nao khac cung cap.
"""

import json
import urllib.request

IPTV_ORG_LOGOS_API = "https://iptv-org.github.io/api/logos.json"


def load_iptvorg_logo_fallback(timeout=20):
    """Tra ve dict {tvg_id_lower: logo_url}. Loi mang KHONG duoc lam sap
    build - tra ve dict rong neu that bai."""
    try:
        req = urllib.request.Request(IPTV_ORG_LOGOS_API, headers={"User-Agent": "Mozilla/5.0"})
        content = urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8")
        data = json.loads(content)
        out = {}
        for item in data:
            cid = item.get("channel")
            url = item.get("url")
            if cid and url:
                out.setdefault(cid.lower(), url)
        return out
    except Exception as e:
        print(f"[iptvorg] Khong lay duoc logos.json (fallback rong): {e}")
        return {}
