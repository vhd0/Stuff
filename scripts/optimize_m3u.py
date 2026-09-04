#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IPTV M3U Optimizer
==================

Mục tiêu:
- Canonical mapping độc lập với iptv-org.
- Canonicalize TRƯỚC khi dedupe.
- Một canonical channel chỉ xuất hiện 1 lần.
- Một channel chỉ giữ 1 stream URL.
- Giữ nguyên playback directives:
    #KODIPROP
    #EXTVLCOPT
    #EXTHTTP
    #EXT-X-*
    và các dòng metadata liên quan.
- Gom group về taxonomy thống nhất.
- Loại radio/FM/VOV toàn cục.
- Loại UPDATE HH:MM... bằng regex.
- Không health-check stream.
- Logo từ nguồn ưu tiên trước; logo fallback nếu được cung cấp.
- EPG được gom từ các nguồn.
- Không phụ thuộc iptv-org để xác định identity.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML is required.")
    print("Install with: pip install pyyaml")
    sys.exit(1)


# ============================================================
# CONFIG
# ============================================================

DEFAULT_MAPPING = "m3u/canonical_channels.yml"

SOURCE_PRIORITY = {
    "vmttv": 500,
    "vietanhtv": 400,
    "dltivi": 300,
    "iptv-org": 200,
    "easport": 100,
    "unknown": 0,
}


FINAL_GROUPS = {
    "VTV": "📺 VTV",
    "HTV": "📺 HTV",
    "SCTV": "📺 SCTV",
    "Thiet yeu": "📡 Thiết yếu",
    "Dia phuong": "🏠 Địa phương",
    "VTVCab": "📺 VTVCab",
    "HTVC": "📺 HTVC",
    "The thao": "🏆 Thể thao",
    "Phim": "🎬 Phim",
    "Thieu nhi": "👧 Thiếu nhi",
    "Am nhac": "🎵 Âm nhạc",
    "Tin tuc": "📰 Tin tức",
    "Quoc te": "🌍 Quốc tế",
    "Khac": "📦 Khác",
}


# ============================================================
# EXCLUSION RULES
# ============================================================

GLOBAL_RADIO_RE = re.compile(
    r"""
    \b
    (
        radio
        | fm
        | vov
        | vov1
        | vov2
        | vov3
        | vov4
        | vov5
        | vov6
        | vovgt
        | vovgiao
        | vovgiaothong
    )
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)

UPDATE_GROUP_RE = re.compile(
    r"^\s*update\s+\d{1,2}:\d{2}\b.*$",
    re.IGNORECASE,
)

VM_TTV_EXCLUDED_GROUPS = {
    "live events",
    "radio",
    "uk radio",
    "israel",
    "hàn quốc",
    "han quoc",
    "trung quốc",
    "trung quoc",
    "thái lan",
    "thai lan",
    "cola tv",
    "pháo hoa tv",
    "phao hoa tv",
}

VIETANHTV_EXCLUDED_GROUPS = {
    "update",
    "dự phòng",
    "du phong",
    "fpt",
    "sự kiện 360",
    "su kien 360",
    "rạp phim",
    "rap phim",
    "radio",
    "socolive",
}

DLTIVI_EXCLUDED_GROUPS = {
    "vov",
}

EASPORT_EXCLUDED_GROUPS = {
    "info",
}

EXCLUDED_CHANNEL_NAMES = {
    "vsbet",
}


# ============================================================
# NORMALIZATION
# ============================================================

VIETNAMESE_REPLACEMENTS = {
    "đ": "d",
    "Đ": "D",
}


def strip_accents(value: str) -> str:
    value = value.translate(str.maketrans(VIETNAMESE_REPLACEMENTS))

    value = unicodedata.normalize("NFD", value)

    return "".join(
        ch
        for ch in value
        if unicodedata.category(ch) != "Mn"
    )


def normalize_text(value: str) -> str:
    """
    Chuẩn hóa:
        VTV1.vn@HD
        VTV 1 HD
        vtv1-hd
        VTV1_HD

    thành dạng tương đương để matching.
    """

    if not value:
        return ""

    value = unicodedata.normalize("NFKC", str(value))

    value = value.strip().lower()

    value = strip_accents(value)

    # Các separator phổ biến
    value = re.sub(r"[@._:/\\|+]+", " ", value)

    value = re.sub(r"[-]+", " ", value)

    # HTML entities / dấu thừa
    value = value.replace("&amp;", " and ")

    # Chỉ giữ chữ/số/khoảng trắng
    value = re.sub(r"[^a-z0-9\s]", " ", value)

    # Gom khoảng trắng
    value = re.sub(r"\s+", " ", value)

    return value.strip()


def compact(value: str) -> str:
    """
    VTV1.vn@HD -> vtv1hd
    VTV 1 HD   -> vtv1hd
    """

    return re.sub(
        r"[^a-z0-9]",
        "",
        normalize_text(value),
    )


def clean_display_name(value: str) -> str:
    """
    Chỉ dùng để chuẩn hóa nhẹ tên hiển thị.
    Không phá metadata gốc nếu không cần.
    """

    if not value:
        return ""

    value = re.sub(r"\s+", " ", value.strip())

    return value


# ============================================================
# M3U PARSER
# ============================================================

ATTR_RE = re.compile(
    r'([A-Za-z0-9_-]+)="([^"]*)"'
)


def parse_extinf(line: str) -> Tuple[Dict[str, str], str]:
    """
    Parse:

    #EXTINF:-1 tvg-id="..." tvg-name="..." group-title="...",Name

    Trả về:
        attrs, title
    """

    if not line.startswith("#EXTINF:"):
        return {}, ""

    try:
        metadata, title = line.split(",", 1)
    except ValueError:
        metadata = line
        title = ""

    attrs = {
        key.lower(): value
        for key, value in ATTR_RE.findall(metadata)
    }

    return attrs, title.strip()


def replace_extinf_attr(
    line: str,
    attr_name: str,
    value: str,
) -> str:

    pattern = re.compile(
        rf'({re.escape(attr_name)}=")[^"]*(")',
        re.IGNORECASE,
    )

    if pattern.search(line):
        return pattern.sub(
            lambda m: f'{m.group(1)}{value}{m.group(2)}',
            line,
            count=1,
        )

    return line


def get_url_from_block(lines: List[str]) -> Optional[str]:
    """
    URL thường là dòng cuối của block.
    Bỏ qua comment/directive.
    """

    for line in reversed(lines):
        line = line.strip()

        if not line:
            continue

        if line.startswith("#"):
            continue

        return line

    return None


@dataclass
class M3UEntry:
    source: str
    extinf: str
    attrs: Dict[str, str]
    title: str
    url: str
    extra_lines: List[str] = field(default_factory=list)

    canonical_id: Optional[str] = None
    canonical_score: int = 0

    source_group: str = ""
    final_group: str = "Khac"

    logo: str = ""
    epg_id: str = ""

    index: int = 0

    @property
    def tvg_id(self) -> str:
        return self.attrs.get("tvg-id", "")

    @property
    def tvg_name(self) -> str:
        return self.attrs.get("tvg-name", "")

    @property
    def group_title(self) -> str:
        return self.attrs.get("group-title", "")

    @property
    def display_name(self) -> str:
        return self.title or self.tvg_name or self.tvg_id


def parse_m3u(
    text: str,
    source: str,
) -> Tuple[List[M3UEntry], List[str]]:

    lines = text.splitlines()

    entries: List[M3UEntry] = []
    epg_urls: List[str] = []

    current_extinf: Optional[str] = None
    current_attrs: Dict[str, str] = {}
    current_title = ""
    current_extra: List[str] = []
    entry_index = 0

    for raw in lines:

        line = raw.rstrip("\r\n")

        if not line.strip():
            continue

        stripped = line.strip()

        # ----------------------------------------------------
        # Header / EPG
        # ----------------------------------------------------

        if stripped.startswith("#EXTM3U"):
            for key, value in ATTR_RE.findall(stripped):
                if key.lower() == "url-tvg" and value:
                    epg_urls.extend(
                        x.strip()
                        for x in value.split(",")
                        if x.strip()
                    )

            continue

        # ----------------------------------------------------
        # EXTINF
        # ----------------------------------------------------

        if stripped.startswith("#EXTINF:"):

            # Flush malformed previous block
            if current_extinf is not None:

                old_url = get_url_from_block(current_extra)

                if old_url:
                    entries.append(
                        M3UEntry(
                            source=source,
                            extinf=current_extinf,
                            attrs=current_attrs,
                            title=current_title,
                            url=old_url,
                            extra_lines=current_extra,
                            index=entry_index,
                        )
                    )
                    entry_index += 1

            current_extinf = stripped
            current_attrs, current_title = parse_extinf(stripped)
            current_extra = []

            continue

        # ----------------------------------------------------
        # Entry body
        # ----------------------------------------------------

        if current_extinf is not None:
            current_extra.append(line)

    # Flush final
    if current_extinf is not None:

        url = get_url_from_block(current_extra)

        if url:
            entries.append(
                M3UEntry(
                    source=source,
                    extinf=current_extinf,
                    attrs=current_attrs,
                    title=current_title,
                    url=url,
                    extra_lines=current_extra,
                    index=entry_index,
                )
            )

    return entries, epg_urls


# ============================================================
# CANONICAL MAPPING
# ============================================================

class CanonicalResolver:

    def __init__(self, mapping: Dict):
        self.mapping = mapping or {}

        self.exact: Dict[str, str] = {}
        self.compact_index: Dict[str, str] = {}

        self.ambiguous_exact = set()
        self.ambiguous_compact = set()

        self._build_index()

    def _register(
        self,
        index: Dict[str, str],
        ambiguous: set,
        key: str,
        canonical_id: str,
    ):

        if not key:
            return

        old = index.get(key)

        if old is None:
            index[key] = canonical_id
            return

        if old != canonical_id:
            ambiguous.add(key)

    def _build_index(self):

        for canonical_id, data in self.mapping.items():

            if not isinstance(data, dict):
                continue

            aliases = set()

            aliases.add(canonical_id)

            name = data.get("name")

            if name:
                aliases.add(str(name))

            for alias in data.get("aliases", []) or []:
                if alias:
                    aliases.add(str(alias))

            for alias in aliases:

                normalized = normalize_text(alias)
                compacted = compact(alias)

                self._register(
                    self.exact,
                    self.ambiguous_exact,
                    normalized,
                    canonical_id,
                )

                self._register(
                    self.compact_index,
                    self.ambiguous_compact,
                    compacted,
                    canonical_id,
                )

    def resolve(
        self,
        tvg_id: str = "",
        tvg_name: str = "",
        title: str = "",
    ) -> Tuple[Optional[str], int]:

        candidates = [
            tvg_id,
            tvg_name,
            title,
        ]

        # ====================================================
        # PASS 1
        # Exact normalized alias
        # ====================================================

        for value in candidates:

            key = normalize_text(value)

            if not key:
                continue

            if key in self.ambiguous_exact:
                continue

            canonical = self.exact.get(key)

            if canonical:
                return canonical, 100

        # ====================================================
        # PASS 2
        # Compact alias
        # ====================================================

        for value in candidates:

            key = compact(value)

            if not key:
                continue

            if key in self.ambiguous_compact:
                continue

            canonical = self.compact_index.get(key)

            if canonical:
                return canonical, 95

        # ====================================================
        # PASS 3
        # Controlled network pattern
        # ====================================================

        for value in candidates:

            key = compact(value)

            if not key:
                continue

            match = re.fullmatch(
                r"(vtv|htv|sctv)(\d{1,2})(?:hd)?",
                key,
            )

            if match:

                candidate = (
                    match.group(1)
                    + match.group(2)
                )

                if candidate in self.mapping:
                    return candidate, 90

        return None, 0


# ============================================================
# LOCAL CANONICAL ID
# ============================================================

def make_local_id(
    name: str,
    group: str = "",
) -> str:

    identity = (
        compact(name)
        + "|"
        + compact(group)
    )

    digest = hashlib.sha1(
        identity.encode("utf-8")
    ).hexdigest()[:12]

    return f"local_{digest}"


# ============================================================
# FILTERING
# ============================================================

def is_radio_like(entry: M3UEntry) -> bool:

    values = [
        entry.tvg_id,
        entry.tvg_name,
        entry.title,
        entry.group_title,
    ]

    combined = " ".join(
        x for x in values if x
    )

    return bool(
        GLOBAL_RADIO_RE.search(combined)
    )


def group_is_excluded(
    source: str,
    group: str,
) -> bool:

    normalized = normalize_text(group)

    if not normalized:
        return False

    # UPDATE 12:30..., UPDATE 8:00...
    if UPDATE_GROUP_RE.match(group):
        return True

    if source == "vmttv":
        return normalized in {
            normalize_text(x)
            for x in VM_TTV_EXCLUDED_GROUPS
        }

    if source == "vietanhtv":
        return normalized in {
            normalize_text(x)
            for x in VIETANHTV_EXCLUDED_GROUPS
        }

    if source == "dltivi":
        return normalized in {
            normalize_text(x)
            for x in DLTIVI_EXCLUDED_GROUPS
        }

    if source == "easport":
        return normalized in {
            normalize_text(x)
            for x in EASPORT_EXCLUDED_GROUPS
        }

    return False


def is_excluded(entry: M3UEntry) -> bool:

    if not entry.url:
        return True

    if group_is_excluded(
        entry.source,
        entry.group_title,
    ):
        return True

    if is_radio_like(entry):
        return True

    for value in (
        entry.tvg_id,
        entry.tvg_name,
        entry.title,
    ):

        if compact(value) in {
            compact(x)
            for x in EXCLUDED_CHANNEL_NAMES
        }:
            return True

    return False


# ============================================================
# GROUP CLASSIFICATION
# ============================================================

def classify_group(
    entry: M3UEntry,
    mapping_data: Optional[Dict],
) -> str:

    """
    Ưu tiên canonical mapping.

    Nếu mapping không có:
        phân loại từ tên/group.

    Internal group name KHÔNG có emoji.
    """

    if mapping_data:

        mapped_group = mapping_data.get("group")

        if mapped_group:
            return str(mapped_group)

    text = normalize_text(
        " ".join(
            [
                entry.group_title,
                entry.title,
                entry.tvg_name,
            ]
        )
    )

    compact_text = compact(text)

    # --------------------------------------------------------
    # Network groups
    # --------------------------------------------------------

    if re.search(r"\bvtv\b", text):
        return "VTV"

    if re.search(r"\bhtv\b", text):
        return "HTV"

    if re.search(r"\bsctv\b", text):
        return "SCTV"

    if "vtvcab" in compact_text:
        return "VTVCab"

    if "htvc" in compact_text:
        return "HTVC"

    # --------------------------------------------------------
    # Essential
    # --------------------------------------------------------

    if any(
        keyword in compact_text
        for keyword in (
            "qpvn",
            "quocphong",
            "antv",
            "anninh",
        )
    ):
        return "Thiet yeu"

    # --------------------------------------------------------
    # Local
    # --------------------------------------------------------

    if any(
        keyword in compact_text
        for keyword in (
            "hanoitv",
            "hanoitelevision",
            "tinh",
            "dia phuong",
            "local",
        )
    ):
        return "Dia phuong"

    # --------------------------------------------------------
    # Sports
    # --------------------------------------------------------

    if any(
        keyword in compact_text
        for keyword in (
            "thethao",
            "sport",
            "sports",
            "bongda",
            "football",
            "soccer",
            "tennis",
            "basketball",
            "racing",
        )
    ):
        return "The thao"

    # --------------------------------------------------------
    # Movies
    # --------------------------------------------------------

    if any(
        keyword in compact_text
        for keyword in (
            "phim",
            "movie",
            "movies",
            "cinema",
            "film",
            "filmhd",
        )
    ):
        return "Phim"

    # --------------------------------------------------------
    # Kids
    # --------------------------------------------------------

    if any(
        keyword in compact_text
        for keyword in (
            "thieunhi",
            "kids",
            "kid",
            "cartoon",
            "children",
        )
    ):
        return "Thieu nhi"

    # --------------------------------------------------------
    # Music
    # --------------------------------------------------------

    if any(
        keyword in compact_text
        for keyword in (
            "amnhac",
            "music",
            "mtv",
        )
    ):
        return "Am nhac"

    # --------------------------------------------------------
    # News
    # --------------------------------------------------------

    if any(
        keyword in compact_text
        for keyword in (
            "tintuc",
            "news",
            "newsroom",
        )
    ):
        return "Tin tuc"

    # --------------------------------------------------------
    # International
    # --------------------------------------------------------

    if any(
        keyword in compact_text
        for keyword in (
            "quocte",
            "international",
            "internationaltv",
            "world",
        )
    ):
        return "Quoc te"

    return "Khac"


# ============================================================
# CANONICALIZE
# ============================================================

def canonicalize_entry(
    entry: M3UEntry,
    resolver: CanonicalResolver,
) -> M3UEntry:

    canonical_id, score = resolver.resolve(
        tvg_id=entry.tvg_id,
        tvg_name=entry.tvg_name,
        title=entry.title,
    )

    if canonical_id is None:

        canonical_id = make_local_id(
            entry.display_name,
            entry.group_title,
        )

        score = 0

    entry.canonical_id = canonical_id
    entry.canonical_score = score

    return entry


# ============================================================
# WINNER SELECTION
# ============================================================

def winner_score(entry: M3UEntry) -> int:

    score = 0

    # Canonical match là yếu tố mạnh nhất
    score += entry.canonical_score * 100

    # Source priority
    score += SOURCE_PRIORITY.get(
        entry.source,
        SOURCE_PRIORITY["unknown"],
    )

    # Metadata bonuses
    if entry.tvg_id:
        score += 15

    if entry.tvg_name:
        score += 15

    if entry.logo:
        score += 20

    if entry.epg_id:
        score += 10

    # Ưu tiên URL có vẻ hoàn chỉnh
    if entry.url.startswith(("http://", "https://")):
        score += 10

    return score


def deduplicate(
    entries: Iterable[M3UEntry],
) -> Tuple[List[M3UEntry], List[Tuple[M3UEntry, M3UEntry]]]:

    winners: Dict[str, M3UEntry] = {}

    dropped: List[Tuple[M3UEntry, M3UEntry]] = []

    for entry in entries:

        if not entry.canonical_id:
            continue

        cid = entry.canonical_id

        old = winners.get(cid)

        if old is None:

            winners[cid] = entry

            continue

        if winner_score(entry) > winner_score(old):

            winners[cid] = entry
            dropped.append((entry, old))

        else:

            dropped.append((old, entry))

    return list(winners.values()), dropped


# ============================================================
# LOGO
# ============================================================

def apply_logo(
    entry: M3UEntry,
    mapping_data: Optional[Dict],
):

    """
    Thứ tự:
        1. logo trong source
        2. logo trong canonical mapping
    """

    source_logo = (
        entry.attrs.get("tvg-logo")
        or entry.attrs.get("logo")
        or ""
    ).strip()

    mapping_logo = ""

    if mapping_data:
        mapping_logo = str(
            mapping_data.get("logo", "")
        ).strip()

    logo = source_logo or mapping_logo

    if logo:
        entry.logo = logo

        entry.extinf = replace_extinf_attr(
            entry.extinf,
            "tvg-logo",
            logo,
        )


# ============================================================
# EPG
# ============================================================

def apply_epg(
    entry: M3UEntry,
    mapping_data: Optional[Dict],
):

    if not mapping_data:
        return

    epg_id = mapping_data.get("epg_id")

    if not epg_id:
        return

    epg_id = str(epg_id).strip()

    if not epg_id:
        return

    entry.epg_id = epg_id

    entry.extinf = replace_extinf_attr(
        entry.extinf,
        "tvg-id",
        epg_id,
    )


# ============================================================
# OUTPUT EXTINF
# ============================================================

def build_output_extinf(
    entry: M3UEntry,
) -> str:

    line = entry.extinf

    # --------------------------------------------------------
    # Canonical tvg-id
    # --------------------------------------------------------

    if entry.canonical_id:

        # Không dùng local ID để thay EPG tvg-id nếu
        # entry đã có một tvg-id hữu ích.
        #
        # Identity nội bộ vẫn là canonical_id.
        # tvg-id trên output giữ giá trị EPG/source.
        pass

    # --------------------------------------------------------
    # Final group
    # --------------------------------------------------------

    final_group = FINAL_GROUPS.get(
        entry.final_group,
        FINAL_GROUPS["Khac"],
    )

    line = replace_extinf_attr(
        line,
        "group-title",
        final_group,
    )

    # --------------------------------------------------------
    # Logo
    # --------------------------------------------------------

    if entry.logo:

        line = replace_extinf_attr(
            line,
            "tvg-logo",
            entry.logo,
        )

    return line


# ============================================================
# SORTING
# ============================================================

def natural_tokens(value: str):

    value = normalize_text(value)

    parts = re.split(
        r"(\d+)",
        value,
    )

    result = []

    for part in parts:

        if part.isdigit():
            result.append((0, int(part)))
        else:
            result.append((1, part))

    return result


def sort_key(entry: M3UEntry):

    group_order = [
        "VTV",
        "HTV",
        "SCTV",
        "Thiet yeu",
        "Dia phuong",
        "VTVCab",
        "HTVC",
        "The thao",
        "Phim",
        "Thieu nhi",
        "Am nhac",
        "Tin tuc",
        "Quoc te",
        "Khac",
    ]

    try:
        group_index = group_order.index(
            entry.final_group
        )
    except ValueError:
        group_index = len(group_order)

    return (
        group_index,
        natural_tokens(entry.display_name),
        normalize_text(entry.canonical_id or ""),
    )


# ============================================================
# M3U WRITER
# ============================================================

def render_m3u(
    entries: List[M3UEntry],
    epg_urls: List[str],
) -> str:

    unique_epg = []

    seen_epg = set()

    for url in epg_urls:

        url = url.strip()

        if not url:
            continue

        if url in seen_epg:
            continue

        seen_epg.add(url)
        unique_epg.append(url)

    if unique_epg:

        header = (
            "#EXTM3U "
            + "url-tvg=\""
            + ",".join(unique_epg)
            + "\""
        )

    else:
        header = "#EXTM3U"

    output = [header]

    for entry in sorted(
        entries,
        key=sort_key,
    ):

        output.append(
            build_output_extinf(entry)
        )

        # ----------------------------------------------------
        # Preserve directives / metadata
        # ----------------------------------------------------

        for line in entry.extra_lines:

            stripped = line.strip()

            if not stripped:
                continue

            # URL đã được ghi riêng
            if stripped == entry.url.strip():
                continue

            # Giữ nguyên toàn bộ directive
            if stripped.startswith("#"):
                output.append(line)

        output.append(entry.url)

    return "\n".join(output) + "\n"


# ============================================================
# LOAD MAPPING
# ============================================================

def load_mapping(
    path: Path,
) -> Dict:

    if not path.exists():

        print(
            f"WARNING: canonical mapping not found: {path}"
        )

        return {}

    with path.open(
        "r",
        encoding="utf-8",
    ) as f:

        data = yaml.safe_load(f) or {}

    channels = data.get(
        "channels",
        data,
    )

    if not isinstance(channels, dict):
        raise ValueError(
            "canonical mapping must contain "
            "'channels:' mapping"
        )

    return channels


# ============================================================
# PROCESS
# ============================================================

def process(
    source_files: List[Tuple[str, Path]],
    mapping_path: Path,
) -> Tuple[List[M3UEntry], List[str]]:

    mapping = load_mapping(mapping_path)

    resolver = CanonicalResolver(mapping)

    all_entries: List[M3UEntry] = []
    all_epg: List[str] = []

    stats = Counter()

    # --------------------------------------------------------
    # Parse sources
    # --------------------------------------------------------

    for source, path in source_files:

        if not path.exists():

            print(
                f"WARNING: source file not found: {path}"
            )

            continue

        print(
            f"[LOAD] {source}: {path}"
        )

        text = path.read_text(
            encoding="utf-8",
            errors="replace",
        )

        entries, epg_urls = parse_m3u(
            text,
            source,
        )

        all_epg.extend(epg_urls)

        print(
            f"       entries={len(entries)} "
            f"epg={len(epg_urls)}"
        )

        for entry in entries:

            stats[f"{source}:parsed"] += 1

            # ------------------------------------------------
            # Filtering BEFORE canonicalization
            # ------------------------------------------------

            if is_excluded(entry):

                stats[f"{source}:excluded"] += 1

                continue

            # ------------------------------------------------
            # Canonicalization
            # ------------------------------------------------

            canonicalize_entry(
                entry,
                resolver,
            )

            mapping_data = mapping.get(
                entry.canonical_id,
                {},
            )

            if not isinstance(
                mapping_data,
                dict,
            ):
                mapping_data = {}

            # ------------------------------------------------
            # Logo / EPG
            # ------------------------------------------------

            apply_logo(
                entry,
                mapping_data,
            )

            apply_epg(
                entry,
                mapping_data,
            )

            # ------------------------------------------------
            # Group classification
            # ------------------------------------------------

            entry.source_group = (
                entry.group_title
            )

            entry.final_group = classify_group(
                entry,
                mapping_data,
            )

            all_entries.append(entry)

            stats[f"{source}:accepted"] += 1

    print()
    print(
        f"[TOTAL] accepted before dedupe: "
        f"{len(all_entries)}"
    )

    # --------------------------------------------------------
    # CRITICAL:
    # canonicalize completed BEFORE this point.
    # --------------------------------------------------------

    final_entries, dropped = deduplicate(
        all_entries
    )

    print(
        f"[TOTAL] after canonical dedupe: "
        f"{len(final_entries)}"
    )

    print(
        f"[TOTAL] duplicates removed: "
        f"{len(dropped)}"
    )

    # --------------------------------------------------------
    # Group stats
    # --------------------------------------------------------

    group_counts = Counter(
        entry.final_group
        for entry in final_entries
    )

    print()
    print("[GROUPS]")

    for group, count in group_counts.most_common():

        label = FINAL_GROUPS.get(
            group,
            group,
        )

        print(
            f"  {label}: {count}"
        )

    # --------------------------------------------------------
    # Duplicate safety check
    # --------------------------------------------------------

    canonical_ids = [
        entry.canonical_id
        for entry in final_entries
        if entry.canonical_id
    ]

    duplicate_ids = [
        cid
        for cid, count in Counter(
            canonical_ids
        ).items()
        if count > 1
    ]

    if duplicate_ids:

        print()
        print(
            "ERROR: canonical duplicate detected:"
        )

        for cid in duplicate_ids:
            print(
                f"  {cid}"
            )

        raise RuntimeError(
            "Canonical dedupe invariant failed."
        )

    # --------------------------------------------------------
    # URL duplicate check
    # --------------------------------------------------------

    urls = [
        entry.url.strip()
        for entry in final_entries
        if entry.url.strip()
    ]

    duplicate_urls = [
        url
        for url, count in Counter(urls).items()
        if count > 1
    ]

    if duplicate_urls:

        print()
        print(
            "WARNING: duplicate URLs detected:"
        )

        for url in duplicate_urls[:20]:
            print(
                f"  {url}"
            )

        # Không fail chỉ vì hai canonical channel
        # dùng chung URL. Tuy nhiên trong output hiện tại
        # mỗi canonical channel vẫn chỉ có một URL.

    return final_entries, all_epg


# ============================================================
# CLI
# ============================================================

def parse_source_argument(
    value: str,
) -> Tuple[str, Path]:

    """
    source=path
    """

    if "=" not in value:

        raise argparse.ArgumentTypeError(
            "Source must use source=path"
        )

    source, path = value.split(
        "=",
        1,
    )

    source = source.strip().lower()
    path = path.strip()

    if not source or not path:

        raise argparse.ArgumentTypeError(
            "Invalid source=path"
        )

    return source, Path(path)


def main() -> int:

    parser = argparse.ArgumentParser(
        description=(
            "Optimize and canonicalize IPTV M3U "
            "without depending on iptv-org identity."
        )
    )

    parser.add_argument(
        "--mapping",
        default=DEFAULT_MAPPING,
        help=(
            "Canonical mapping YAML "
            "(default: m3u/canonical_channels.yml)"
        ),
    )

    parser.add_argument(
        "--source",
        action="append",
        required=True,
        type=parse_source_argument,
        help=(
            "Input source in source=path format. "
            "Can be repeated."
        ),
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Output M3U path",
    )

    args = parser.parse_args()

    mapping_path = Path(
        args.mapping
    )

    output_path = Path(
        args.output
    )

    print("=" * 70)
    print(" IPTV M3U OPTIMIZER")
    print("=" * 70)
    print(
        "Canonical mapping:",
        mapping_path,
    )
    print(
        "Output:",
        output_path,
    )
    print()

    try:

        entries, epg_urls = process(
            source_files=args.source,
            mapping_path=mapping_path,
        )

        # ----------------------------------------------------
        # Safety
        # ----------------------------------------------------

        if not entries:

            raise RuntimeError(
                "Build produced ZERO channels. "
                "Output file will not be written."
            )

        # ----------------------------------------------------
        # Render
        # ----------------------------------------------------

        output = render_m3u(
            entries,
            epg_urls,
        )

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        # Atomic-ish write
        temp_path = output_path.with_suffix(
            output_path.suffix + ".tmp"
        )

        temp_path.write_text(
            output,
            encoding="utf-8",
            newline="\n",
        )

        temp_path.replace(
            output_path
        )

        print()
        print("=" * 70)
        print(
            f"SUCCESS: {len(entries)} channels"
        )
        print(
            f"Written: {output_path}"
        )
        print("=" * 70)

        return 0

    except Exception as exc:

        print()
        print("=" * 70)
        print("BUILD FAILED")
        print("=" * 70)
        print(
            f"{type(exc).__name__}: {exc}"
        )

        # Tuyệt đối không xóa fallback cũ.
        # Output temp nếu có sẽ bị xóa.
        temp_path = output_path.with_suffix(
            output_path.suffix + ".tmp"
        )

        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass

        return 1


if __name__ == "__main__":
    sys.exit(main())
