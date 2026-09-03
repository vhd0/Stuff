"""
Phan giai nhom CHINH cho 1 kenh. Nhom "dac biet" (ANTV/QPVN) va extra_groups
duoc ap dung o tang pipeline (scripts/optimize_m3u.py), KHONG nam trong
module nay - de dam bao bat buoc chi 2 canonical_id duoc phep vao
SPECIAL_GROUP.

v3: Sieu GUARD chong nham kenh QUOC TE vao nhom DIA PHUONG (bao cao thuc
te: CNN/Cartoon Network/CCTV4/kenh tieng Nga bi 1 so nguon gan nham
group-title "Địa Phương"). Nguyen tac: chi tin group-title = Dia phuong
NEU co bang chung XAC NHAN (ten tinh/thanh, hoac tvg-id tinh/thanh biet
truoc, hoac co dau tieng Viet + khong co dau hieu quoc te nao). Neu KHONG
xac nhan duoc VA phat hien chu Cyrillic (chac chan khong phai kenh VN) ->
KHONG BAO GIO giu la Dia phuong.
"""

import re

import yaml

from . import config
from .normalize import remove_accents, group_match_key

LOCAL_GROUP = "🏠 Địa phương"

_CYRILLIC_RE = re.compile(r'[\u0400-\u04FF]')
_VIETNAMESE_DIACRITIC_RE = re.compile(
    r'[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡ'
    r'ùúụủũưừứựửữỳýỵỷỹđ]', re.IGNORECASE
)


class GroupResolver:
    def __init__(self, path=config.GROUPS_YAML):
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        self.group_alias = data.get("group_alias", {}) or {}
        self.content_keywords = data.get("content_keywords", {}) or {}
        self.default_content_hint_map = data.get("default_content_hint_map", {}) or {}
        self.local_province_keywords = data.get("local_province_keywords", []) or []
        self.local_province_ids = data.get("local_province_ids", []) or []
        self.local_extra_keywords = data.get("local_extra_keywords", []) or []

    def _tvg_id_brand_group(self, tvg_id):
        """Fallback theo TIEN TO tvg-id (dang tin cay hon group-title cua
        mot so nguon hay GOM CHUNG nhieu thuong hieu vao 1 group-title, vd
        TinhLaGi dat chung "HTV & HTVC" cho ca kenh HTV lan HTVC)."""
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
        """Tra ve nhom khop dau tien theo tu khoa OTT, hoac None neu khong
        khop gi ca. Tach rieng de tai su dung lam GUARD chong nham quoc te
        vao Dia phuong."""
        for group, keywords in self.content_keywords.items():
            for kw in keywords:
                if kw in name_lower:
                    return group
        return None

    def _is_local_province(self, name_lower, tvg_id):
        """Nhan dien kenh dia phuong DOC LAP voi group-title nguon, dua
        theo danh sach 63 tinh/thanh + id kenh dia phuong da biet."""
        tid = (tvg_id or "").lower()
        for pid in self.local_province_ids:
            if tid.startswith(pid):
                return True
        for kw in self.local_province_keywords:
            if kw in name_lower:
                return True
        for kw in self.local_extra_keywords:
            if kw in name_lower:
                return True
        return False

    def _confirm_local(self, clean_name, name_lower, tvg_id):
        """GUARD: xac nhan 1 kenh CO THUC SU la dia phuong VN hay khong,
        dung khi group-title nguon noi la Dia phuong nhung can kiem chung
        lai (khong tin mu). Tra ve True/False.

        Thu tu kiem tra:
          1. Khop danh sach tinh/thanh/id da biet -> XAC NHAN dia phuong.
          2. Co chu Cyrillic (kenh Nga/Trung A...) -> CHAC CHAN KHONG phai
             dia phuong VN, tu choi ngay.
          3. Khong co dau tieng Viet nao trong ten GOC (truoc khi bo dau)
             VA khong khop content classifier nao -> nghi ngo la kenh nuoc
             ngoai khong xac dinh duoc the loai, TU CHOI (an toan hon la
             nhan lieu).
          4. Con lai (co dau tieng Viet, khong khop content classifier
             quoc te nao) -> CHAP NHAN la dia phuong (kenh VN chua kip liet
             ke ten tinh, vd bien the ten dai chua co trong danh sach).
        """
        if self._is_local_province(name_lower, tvg_id):
            return True
        if _CYRILLIC_RE.search(clean_name):
            return False
        if _VIETNAMESE_DIACRITIC_RE.search(clean_name):
            return True
        return False

    def resolve_primary_group(self, source_group_raw, trust_group_title,
                               clean_name, tvg_id="", default_content_hint=None):
        """Tra ve TEN NHOM CHINH (1 chuoi, khong phai list) cho 1 kenh.

        Uu tien:
          1. Neu nguon duoc tin tuong VA co group-title khop group_alias ->
             dung canonical group do - TRU KHI ket qua la "Dia phuong" ma
             KHONG qua duoc _confirm_local() (xem ham do) - khi ay se roi
             qua content classifier / Khac thay vi giu nham la dia phuong.
          2. Fallback theo TIEN TO tvg-id (xem _tvg_id_brand_group).
          3. Nhan dien DIA PHUONG doc lap (_is_local_province) - bat dung
             kenh tinh/thanh that ke ca khi nguon khong tin group-title.
          4. OTT content classifier theo tu khoa trong TEN KENH.
          5. default_content_hint cua nguon (vd EaSport -> the thao).
          6. Cuoi cung -> "Khac" (luon la last resort).
        """
        name_lower = remove_accents(clean_name) + " " + remove_accents(tvg_id)

        if trust_group_title and source_group_raw:
            key = group_match_key(source_group_raw)
            if key in self.group_alias:
                candidate = self.group_alias[key]
                if candidate == LOCAL_GROUP:
                    if self._confirm_local(clean_name, name_lower, tvg_id):
                        return LOCAL_GROUP
                    override = self._content_classify(name_lower)
                    return override or config.OTHER_GROUP
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
