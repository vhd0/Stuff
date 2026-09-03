#!/usr/bin/env python3
"""
M3U Optimizer v3 - entry point. Chay tu repo root:

    python3 scripts/optimize_m3u.py

Xem DESIGN_PHILOSOPHY.md o repo root de biet day du triet ly thiet ke va
ly do cac quyet dinh quan trong (dac biet: TAI SAO BO HEALTHCHECK, TAI SAO
BO PRIMARY/BACKUP).

Thu tu xu ly:

    FETCH (gia lap OTT app that, retry)
      -> HARD FILTER (nguoi lon/ca do/info-card/VOD-tap le)
      -> CHANNEL IDENTITY / NAME NORMALIZATION
      -> GROUPING (source group-title trusted -> guard dia phuong ->
         tvg-id brand fallback -> OTT content classifier)
      -> DEDUP STREAM (khong healthcheck, khong primary/backup)
      -> LOGO (source priority, iptv-org fallback)
      -> GHI OUTPUT + QUALITY GATE + build_stats.json
"""

import os
import re
import sys
import time
import urllib.request
import urllib.error

# Cho phep `import m3u` khi script duoc chay tu repo root bang duong dan
# tuong doi "scripts/optimize_m3u.py" (repo root chua nam san trong sys.path).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from m3u import config
from m3u.channel_registry import ChannelRegistry
from m3u.grouping import GroupResolver
from m3u.parser import parse_source
from m3u.normalize import clean_display_name, remove_accents, identity_key, group_match_key
from m3u.logo import choose_logo
from m3u.iptvorg import load_iptvorg_logo_fallback
from m3u.epg import fetch_and_validate_epg
from m3u.quality_gate import validate_output
from m3u.stats import write_build_stats


def fetch_source_text(url):
    """Gia lap 1 trinh OTT app THAT (User-Agent Android + retry + timeout
    dai hon) thay vi 1 request trinh duyet don gian - tranh timeout/bi tu
    choi nhu truong hop nguon EaSport truoc day. Retry voi backoff tang
    dan neu that bai (mang chap chon, CDN cham...)."""
    last_error = None
    for attempt in range(1, config.FETCH_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": config.FETCH_USER_AGENT,
                "Accept": "*/*",
                "Connection": "keep-alive",
            })
            return urllib.request.urlopen(req, timeout=config.FETCH_TIMEOUT) \
                .read().decode("utf-8-sig", errors="replace")
        except Exception as e:
            last_error = e
            if attempt < config.FETCH_RETRIES:
                print(f"    (lan {attempt}/{config.FETCH_RETRIES} that bai: {e}, "
                      f"thu lai sau {config.FETCH_RETRY_BACKOFF}s...)")
                time.sleep(config.FETCH_RETRY_BACKOFF)
    raise last_error


def is_blocked_entry(clean_name, tvg_id, url):
    haystack = remove_accents(clean_name) + " " + remove_accents(tvg_id)
    if any(kw in haystack for kw in config.BLOCKED_NAME_KEYWORDS):
        return True
    if any(domain in url for domain in config.BLOCKED_DOMAINS):
        return True
    return False


def is_blocked_stream_brand_group(group_raw):
    """Chan theo TEN THUONG HIEU web lau bong da/ca do CU THE da biet (vd
    "Socolive"), KHAC voi is_blocked_entry() la chan theo tu khoa CHUNG.
    Day la danh sach thuong hieu RIENG BIET, khong phai tu chung chung nen
    khong vi pham nguyen tac loc bao thu (khong loai "sport"/"live" chung
    chung, chi loai dung 1 thuong hieu cu the da xac minh)."""
    key = group_match_key(group_raw)
    return any(brand in key for brand in config.BLOCKED_STREAM_BRAND_GROUPS)


# Dau hieu VOD/phim bo-tap le RO RANG (co tag cu the). Khop theo TU DA
# CHUAN HOA (khong dau).
_VOD_EPISODE_RE = re.compile(
    r'\btap\s*\d+\b'          # "Tap 12", "tap12"
    r'|\bphan\s*\d+\b'        # "Phan 2" (season/part)
    r'|\bepisode\s*\d+\b'
    r'|\bep\s*\d+\b'
    r'|\bss\d+\b'             # "SS1", "SS2"
    r'|\(\s*(19|20)\d{2}\s*\)'  # nam phat hanh trong ngoac, vd "(2019)"
    r'|\bvietsub\b'
    r'|\bthuyet minh\b'
    r'|\bfull\s*(bo|series)\b'
)

# Dau hieu "day la 1 CHANNEL" (bundle/thuong hieu da biet, hau to kenh...).
_CHANNEL_LIKE_RE = re.compile(
    r'\b(kenh|channel|tv|box|cine|360|htvc|sctv|vtv|htv|rap chieu|'
    r'phim tong hop|onsports|oncine)\b'
)


def is_vod_episode_entry(raw_name, tvg_id="", group_raw=""):
    """True neu muc trong nhieu kha nang la 1 TAP/BO/TUA PHIM VOD rieng le
    hon la 1 channel phim tuyen tinh. Chi loai bo phan nay, KHONG dong den
    cac channel phim that su (HBO, Cinemax, ON Cine, SCTV Phim Tong Hop,
    360 Phim Viet...).

    2 dieu kien (bat ky dieu kien nao dung deu bi loai):
      1. Co tag ro rang (Tap/Phan/Episode/nam phat hanh/Vietsub/Thuyet
         minh) - AP DUNG CHO MOI NGUON.
      2. Group-title GOC cua nguon goi y day la 1 bucket PHIM (vd "Rạp
         Phim") VA muc nay KHONG co tvg-id VA ten KHONG khop dau hieu "la 1
         channel" nao. CHI ap dung khi group goi y "phim", tranh bat nham
         kenh dia phuong khong co tvg-id (vd "Sơn La", "Cần Thơ 1").
    """
    name_l = remove_accents(raw_name)
    if _VOD_EPISODE_RE.search(name_l):
        return True

    group_l = remove_accents(group_raw)
    is_movie_bucket = "phim" in group_l
    if is_movie_bucket and not tvg_id and not _CHANNEL_LIKE_RE.search(name_l) \
            and len(name_l.split()) >= 2:
        return True

    return False


# Cac muc "info/tu quang cao" cua chinh nha cung cap nguon (vd TinhLaGi co
# cac "kenh" nhu "Địa Chỉ IP Của Bạn", "Cập Nhật"... tro thang toi 1 FILE
# ANH TINH thay vi 1 stream that.
_IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".svg")


def is_non_stream_url(url):
    path = url.split("?")[0].split("#")[0].lower()
    return path.endswith(_IMAGE_EXTENSIONS)


def extract_quality_score(raw_name, url):
    """Diem chat luong dung lam TIE-BREAKER phu khi sap xep thu tu URL
    thay the (source priority quan trong hon resolution theo mac dinh)."""
    haystack = f"{raw_name} {url}".lower()
    best = 0
    for token, score in (("2160", 2160), ("4k", 2160), ("1440", 1440),
                         ("1080", 1080), ("720", 720), ("576", 576), ("480", 480)):
        if token in haystack:
            best = max(best, score)
    return best


def main():
    registry = ChannelRegistry()
    resolver = GroupResolver()

    canonical_channels = {}   # canonical_id -> {id, name, tvg_id, groups:set, candidates:[]}
    source_item_counts = {}
    source_errors = {}
    filtered_counts = {}      # ly do -> so luong

    print("=== Dang tai va phan tich cac nguon ===")
    for idx, source in enumerate(config.SOURCES):
        name = source["name"]
        print(f"[{name}] dang tai ({source['url']})...")
        try:
            text = fetch_source_text(source["url"])
            raw_entries = parse_source(source["format"], text)
        except Exception as e:
            print(f"[{name}] LOI TAI/PARSE sau {config.FETCH_RETRIES} lan thu: {e}")
            source_errors[name] = str(e)
            source_item_counts[name] = 0
            continue

        source_item_counts[name] = len(raw_entries)
        print(f"[{name}] {len(raw_entries)} muc thu duoc.")

        for entry in raw_entries:
            clean_name = clean_display_name(entry["raw_name"])
            if not clean_name:
                continue

            if is_blocked_entry(clean_name, entry["tvg_id"], entry["url"]):
                filtered_counts["blocked_keyword_or_domain"] = (
                    filtered_counts.get("blocked_keyword_or_domain", 0) + 1
                )
                continue

            if is_blocked_stream_brand_group(entry["group_raw"]):
                filtered_counts["blocked_stream_brand_group"] = (
                    filtered_counts.get("blocked_stream_brand_group", 0) + 1
                )
                continue

            if is_non_stream_url(entry["url"]):
                filtered_counts["non_stream_url_filtered"] = (
                    filtered_counts.get("non_stream_url_filtered", 0) + 1
                )
                continue

            if is_vod_episode_entry(entry["raw_name"], entry["tvg_id"], entry["group_raw"]):
                filtered_counts["vod_episode_filtered"] = (
                    filtered_counts.get("vod_episode_filtered", 0) + 1
                )
                continue

            # --- CHANNEL IDENTITY ---
            reg_id, reg_name, extra_groups = registry.resolve(
                clean_name, entry["tvg_id"], entry["tvg_name"]
            )
            if reg_id:
                canonical_id, canonical_name = reg_id, reg_name
            else:
                canonical_id = identity_key(clean_name, entry["tvg_id"], entry["tvg_name"])
                canonical_name = clean_name
                extra_groups = []

            # --- GROUPING ---
            if registry.is_special_whitelisted(canonical_id):
                primary_group = config.SPECIAL_GROUP
            else:
                primary_group = resolver.resolve_primary_group(
                    entry["group_raw"], source["trust_group_title"],
                    clean_name, entry["tvg_id"],
                    default_content_hint=source.get("default_content_hint"),
                )

            groups_for_entry = {primary_group}
            for g in extra_groups:
                # An toan tuyet doi: KHONG channel nao khac ANTV/QPVN duoc
                # phep lot vao SPECIAL_GROUP, du channels.yaml co khai bao sai.
                if g == config.SPECIAL_GROUP and canonical_id not in config.SPECIAL_GROUP_WHITELIST:
                    continue
                groups_for_entry.add(g)

            channel = canonical_channels.setdefault(canonical_id, {
                "id": canonical_id,
                "name": canonical_name,
                "tvg_id": "",
                "groups": set(),
                "candidates": [],
            })
            if not channel["tvg_id"] and entry["tvg_id"]:
                channel["tvg_id"] = entry["tvg_id"]
            channel["groups"] |= groups_for_entry

            channel["candidates"].append({
                "url": entry["url"],
                "tags": entry["tags"],
                "logo": entry["logo"],
                "source_priority": idx,
                "source_name": name,
                "quality_score": extract_quality_score(entry["raw_name"], entry["url"]),
            })

    print(f"=== Tong {len(canonical_channels)} kenh canonical ===")

    # --- LOGO FALLBACK (source priority truoc, iptv-org fallback cuoi) ---
    iptvorg_logos = load_iptvorg_logo_fallback()

    # --- DEDUP STREAM (KHONG healthcheck, KHONG primary/backup - xem
    # DESIGN_PHILOSOPHY.md). Chi loai URL trung nhau, sap theo source
    # priority + quality lam tie-breaker, giu toi da ALT_STREAM_SOFT_CAP
    # ban thay the de chong phinh to bat thuong. ---
    khac_channel_list = []
    total_alt_streams = 0

    for cid, channel in canonical_channels.items():
        seen_urls = set()
        deduped = []
        for c in channel["candidates"]:
            if c["url"] in seen_urls:
                continue
            seen_urls.add(c["url"])
            deduped.append(c)

        deduped.sort(key=lambda c: (c["source_priority"], -c["quality_score"]))
        channel["streams"] = deduped[: config.ALT_STREAM_SOFT_CAP]
        total_alt_streams += len(channel["streams"])

        logo_candidates = [c["logo"] for c in
                            sorted(channel["candidates"], key=lambda c: c["source_priority"])]
        channel["logo"] = choose_logo(logo_candidates, iptvorg_logos.get(channel["tvg_id"], ""))

        if config.OTHER_GROUP in channel["groups"] and len(channel["groups"]) == 1:
            khac_channel_list.append({
                "id": cid, "name": channel["name"],
                "sources": sorted({c["source_name"] for c in channel["candidates"]}),
            })

    # --- GHI OUTPUT - ghi ra buffer truoc, chi flush neu qua duoc quality
    # gate. ---
    group_to_channels = {g: [] for g in config.FINAL_GROUP_ORDER}
    special_ids_used = set()

    for cid, channel in canonical_channels.items():
        if not channel.get("streams"):
            continue
        for g in channel["groups"]:
            if g not in group_to_channels:
                group_to_channels.setdefault(config.OTHER_GROUP, [])
                group_to_channels[config.OTHER_GROUP].append(channel)
                continue
            group_to_channels[g].append(channel)
            if g == config.SPECIAL_GROUP:
                special_ids_used.add(cid)

    epg_status = fetch_and_validate_epg()

    lines = [f'#EXTM3U url-tvg="{config.EPG_URL}"']
    total_channel_entries = 0

    for group in config.FINAL_GROUP_ORDER:
        channels_in_group = group_to_channels.get(group, [])
        channels_in_group.sort(key=lambda ch: [
            int(t) if t.isdigit() else t for t in re.split(r'(\d+)', ch["name"])
        ])
        for channel in channels_in_group:
            id_attr = f' tvg-id="{channel["tvg_id"]}"' if channel["tvg_id"] else ""
            logo_attr = f' tvg-logo="{channel["logo"]}"' if channel.get("logo") else ""
            # KHONG con nhan "[Dự phòng]" - moi stream duoc ghi voi CUNG 1
            # ten kenh (quy uoc M3U pho bien: player tu chuyen doi giua cac
            # ban ghi trung ten khi 1 nguon bi loi).
            for stream in channel["streams"]:
                lines.append(
                    f'#EXTINF:-1{id_attr}{logo_attr} group-title="{group}",{channel["name"]}'
                )
                for tag in stream["tags"]:
                    lines.append(tag)
                lines.append(stream["url"])
                total_channel_entries += 1

    output_text = "\n".join(lines) + "\n"

    ok, gate_reasons = validate_output(
        output_text,
        channel_count=sum(1 for c in canonical_channels.values() if c.get("streams")),
        groups_used=set(group_to_channels.keys()),
        special_group_channel_ids=special_ids_used,
    )

    if ok:
        with open(config.OUTPUT_M3U_PATH, "w", encoding="utf-8") as f:
            f.write(output_text)
        print(f"Build THANH CONG - da ghi {config.OUTPUT_M3U_PATH} "
              f"({total_channel_entries} dong EXTINF).")
    else:
        print("!!! QUALITY GATE THAT BAI - KHONG ghi de listtivi.m3u hien co !!!")
        for r in gate_reasons:
            print(f"  - {r}")

    stats = {
        "gate_passed": ok,
        "gate_reasons": gate_reasons,
        "source_item_counts": source_item_counts,
        "source_errors": source_errors,
        "filtered_counts": filtered_counts,
        "canonical_channel_count": len(canonical_channels),
        "channel_count_with_stream": sum(
            1 for c in canonical_channels.values() if c.get("streams")
        ),
        "total_alt_streams": total_alt_streams,
        "channels_per_group": {g: len(v) for g, v in group_to_channels.items()},
        "khac_channel_list": khac_channel_list,
        "epg_status": epg_status,
    }
    write_build_stats(stats)
    print(f"Da ghi {config.BUILD_STATS_PATH}")

    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
