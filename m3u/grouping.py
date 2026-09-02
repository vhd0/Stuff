"""
Phan giai nhom CHINH cho 1 kenh (muc 6 GROUPING PRINCIPLE, muc 7 OTT
REFERENCE MODEL, muc 10 LOCAL GROUP). Nhom "dac biet" (ANTV/QPVN) va
extra_groups duoc ap dung o tang pipeline (main.py), KHONG nam trong module
nay - de dam bao bat buoc chi 2 canonical_id duoc phep vao SPECIAL_GROUP
(muc 9).
"""

import yaml

from . import config
from .normalize import remove_accents, group_match_key

LOCAL_GROUP = "🏠 Địa phương"


class GroupResolver:
    def __init__(self, path=config.GROUPS_YAML):
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        self.group_alias = data.get("group_alias", {}) or {}
        self.content_keywords = data.get("content_keywords", {}) or {}
        self.default_content_hint_map = data.get("default_content_hint_map", {}) or {}
        self.local_province_keywords = data.get("local_province_keywords", []) or []
        self.local_province_ids = data.get("local_province_ids", []) or []

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

    def _content_classify(self, name_lower):
        """Tra ve nhom khop dau tien theo tu khoa OTT (muc 7), hoac None
        neu khong khop gi ca. Tach rieng de tai su dung lam GUARD chong
        nham quoc te vao Dia phuong (xem resolve_primary_group)."""
        for group, keywords in self.content_keywords.items():
            for kw in keywords:
                if kw in name_lower:
                    return group
        return None

    def _is_local_province(self, name_lower, tvg_id):
        """Nhan dien kenh dia phuong DOC LAP voi group-title nguon (muc 10),
        dua theo danh sach 63 tinh/thanh + id kenh dia phuong da biet. Dung
        de BAT DUNG kenh dia phuong that ke ca khi nguon khong tin
        group-title hoac dat sai ten nhom (tranh roi vao "Khac")."""
        tid = (tvg_id or "").lower()
        for pid in self.local_province_ids:
            if tid.startswith(pid):
                return True
        for kw in self.local_province_keywords:
            if kw in name_lower:
                return True
        return False

    def resolve_primary_group(self, source_group_raw, trust_group_title,
                               clean_name, tvg_id="", default_content_hint=None):
        """Tra ve TEN NHOM CHINH (1 chuoi, khong phai list) cho 1 kenh.

        Uu tien (muc 6, 10):
          1. Neu nguon duoc tin tuong VA co group-title khop group_alias ->
             dung canonical group do - TRU TRUONG HOP ket qua la "Dia
             phuong" nhung ten kenh khop ro rang voi 1 thuong hieu quoc te
             da biet (vd nguon gan nham CNN/Cartoon Network vao nhom Dia
             phuong) - khi do UU TIEN phan loai theo noi dung de bao ve
             tinh dung dan cua nhom Dia phuong (muc 10: "Do NOT put all
             unrecognized foreign... into local").
          2. Fallback theo TIEN TO tvg-id (xem _tvg_id_brand_group).
          3. Nhan dien DIA PHUONG doc lap (xem _is_local_province) - bat
             dung kenh tinh/thanh that ke ca khi nguon khong tin
             group-title (tranh kenh dia phuong bi roi vao Khac).
          4. OTT content classifier theo tu khoa trong TEN KENH (muc 7).
          5. default_content_hint cua nguon (vd EaSport -> the thao).
          6. Cuoi cung -> "Khac" (muc 12, luon la last resort).
        """
        name_lower = remove_accents(clean_name) + " " + remove_accents(tvg_id)

        if trust_group_title and source_group_raw:
            key = group_match_key(source_group_raw)
            if key in self.group_alias:
                candidate = self.group_alias[key]
                if candidate == LOCAL_GROUP:
                    override = self._content_classify(name_lower)
                    if override and override != LOCAL_GROUP:
                        return override
                return candidate

        brand_group = self._tvg_id_brand_group(tvg_id)
        if brand_group:
            return brand_group

        if self._is_local_province(name_lower, tvg_id):
            return LOCAL_GROUP

        classified = self._content_classify(name_lower)
        if classified:
            return classified

        if default_content_hint and default_content_hint in self.default_content_hint_map:
            return self.default_content_hint_map[default_content_hint]

        return config.OTHER_GROUP
