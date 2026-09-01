"""
Quality gate (muc 20): khong bao gio thay playlist dang tot bang 1 ban
build ro rang bi hong.
"""

from . import config


def validate_output(output_text, channel_count, groups_used, special_group_channel_ids):
    """Tra ve (ok: bool, reasons: list[str]). ok=False nghia la KHONG duoc
    ghi de listtivi.m3u hien tai."""
    reasons = []

    if "#EXTM3U" not in output_text:
        reasons.append("Thieu dong #EXTM3U")

    if "#EXTINF" not in output_text:
        reasons.append("Khong co dong #EXTINF nao (playlist rong)")

    if channel_count < config.MIN_CHANNEL_COUNT:
        reasons.append(
            f"So kenh ({channel_count}) thap hon nguong toi thieu "
            f"({config.MIN_CHANNEL_COUNT}) - co the do loi nguon/mang"
        )

    invalid_special = special_group_channel_ids - config.SPECIAL_GROUP_WHITELIST
    if invalid_special:
        reasons.append(
            f"Nhom '{config.SPECIAL_GROUP}' chua kenh khong duoc phep: {sorted(invalid_special)}"
        )

    for g in groups_used:
        gl = g.lower()
        if "quốc tế" in g.lower() or "quoc te" in gl:
            reasons.append(f"Phat hien nhom 'Quoc te' khong duoc phep ton tai: '{g}'")

    for domain in config.BLOCKED_DOMAINS:
        if domain in output_text:
            reasons.append(f"Phat hien domain bi chan lot vao output: {domain}")

    return (len(reasons) == 0), reasons
