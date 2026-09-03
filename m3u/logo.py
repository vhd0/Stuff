"""
Chon logo cho 1 kenh canonical (muc 18 LOGO).
Uu tien: logo tu nguon dau vao (theo dung SOURCE PRIORITY) -> IPTV-org
fallback cuoi cung. KHONG BAO GIO ghi de logo nguon uu tien cao hon bang
IPTV-org.
"""


def choose_logo(candidates_by_source_priority, iptvorg_logo="", ):
    """candidates_by_source_priority: list cac logo tho theo dung thu tu
    SOURCE PRIORITY (candidates[0] la nguon uu tien cao nhat). Tra ve logo
    dau tien khac rong trong danh sach; neu khong co cai nao -> dung
    iptvorg_logo (co the la chuoi rong neu khong co)."""
    for logo in candidates_by_source_priority:
        if logo:
            return logo
    return iptvorg_logo or ""
