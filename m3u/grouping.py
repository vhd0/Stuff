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

    def resolve_primary_group(self, source_group_raw, trust_group_title,
                               clean_name, tvg_id="", default_content_hint=None):
        """Tra ve TEN NHOM CHINH (1 chuoi, khong phai list) cho 1 kenh.

        Uu tien (muc 6):
          1. Neu nguon duoc tin tuong VA co group-title khop group_alias
             -> dung luon canonical group do.
          2. Neu khong (khong tin tuong / khong co group-title / group-title
             khong khop, vd cac nhom quoc gia hay "Quoc Te" chung chung) ->
             OTT content classifier theo tu khoa trong TEN KENH (muc 7).
          3. Neu van khong khop -> default_content_hint cua nguon (vd
             EaSport -> the thao).
          4. Cuoi cung -> "Khac" (muc 12, luon la last resort).
        """
        if trust_group_title and source_group_raw:
            key = group_match_key(source_group_raw)
            if key in self.group_alias:
                return self.group_alias[key]

        name_lower = remove_accents(clean_name) + " " + remove_accents(tvg_id)
        for group, keywords in self.content_keywords.items():
            for kw in keywords:
                if kw in name_lower:
                    return group

        if default_content_hint and default_content_hint in self.default_content_hint_map:
            return self.default_content_hint_map[default_content_hint]

        return config.OTHER_GROUP
