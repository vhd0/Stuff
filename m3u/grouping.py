"""
Phan giai nhom CHINH cho 1 kenh (muc 6 GROUPING PRINCIPLE, muc 7 OTT
REFERENCE MODEL). Nhom "dac biet" (ANTV/QPVN) va extra_groups duoc ap dung
o tang pipeline (main.py), KHONG nam trong module nay - de dam bao bat
buoc chi 2 canonical_id duoc phep vao SPECIAL_GROUP (muc 9).
"""

import yaml

from . import config
from .normalize import remove_accents, group_match_key


class GroupResolver:
    def __init__(self, path=config.GROUPS_YAML):
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        self.group_alias = data.get("group_alias", {}) or {}
        self.content_keywords = data.get("content_keywords", {}) or {}
        self.default_content_hint_map = data.get("default_content_hint_map", {}) or {}

    def _tvg_id_brand_group(self, tvg_id):
        """Fallback theo TIEN TO tvg-id (dang tin cay hon group-title cua
        mot so nguon hay GOM CHUNG nhieu thuong hieu vao 1 group-title, vd
        TinhLaGi dat chung "HTV & HTVC" cho ca kenh HTV lan HTVC). Quy uoc
        dat ten tvg-id trong thuc te (htv1, htvcplushd, vtv1hd, sctv1hd...)
        du de phan biet thuong hieu chinh xac hon group-title trong truong
        hop nay."""
        tid = (tvg_id or "").lower()
        if tid.startswith("htvc"):
            return "📡 HTVC"
        if tid.startswith("htv"):
            return "📺 HTV"
        if tid.startswith("vtvcab"):
            return "📡 VTVCab"
        if tid.startswith("vtv"):
            return "📺 VTV"
        if tid.startswith("sctv"):
            return "📡 SCTV"
        return None

    def resolve_primary_group(self, source_group_raw, trust_group_title,
                               clean_name, tvg_id="", default_content_hint=None):
        """Tra ve TEN NHOM CHINH (1 chuoi, khong phai list) cho 1 kenh.

        Uu tien (muc 6):
          1. Neu nguon duoc tin tuong VA co group-title khop group_alias
             -> dung luon canonical group do.
          2. Fallback theo TIEN TO tvg-id (xem _tvg_id_brand_group) - xu ly
             truong hop group-title cua nguon GOM CHUNG nhieu thuong hieu
             (vd "HTV & HTVC") ma group_alias khong the tach duoc.
          3. OTT content classifier theo tu khoa trong TEN KENH (muc 7).
          4. default_content_hint cua nguon (vd EaSport -> the thao).
          5. Cuoi cung -> "Khac" (muc 12, luon la last resort).
        """
        if trust_group_title and source_group_raw:
            key = group_match_key(source_group_raw)
            if key in self.group_alias:
                return self.group_alias[key]

        brand_group = self._tvg_id_brand_group(tvg_id)
        if brand_group:
            return brand_group

        name_lower = remove_accents(clean_name) + " " + remove_accents(tvg_id)
        for group, keywords in self.content_keywords.items():
            for kw in keywords:
                if kw in name_lower:
                    return group

        if default_content_hint and default_content_hint in self.default_content_hint_map:
            return self.default_content_hint_map[default_content_hint]

        return config.OTHER_GROUP
