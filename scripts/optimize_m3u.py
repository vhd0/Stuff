#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
IPTV M3U Optimizer

Pipeline:
    remote sources
        -> fetch
        -> parse
        -> filter
        -> canonical identity
        -> deduplicate
        -> priority group lock
        -> event classifier
        -> content classifier
        -> render

Priority groups are HARD LOCKED:
    VTV
    HTV
    SCTV
    VTVCab
    HTVC
    Thiết yếu
    Địa phương

Important:
    canonicalize BEFORE dedupe.
"""


from __future__ import annotations

import argparse
import hashlib
import re
import sys
import time
import unicodedata

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests
import yaml


# ============================================================
# CONFIG
# ============================================================

DEFAULT_OUTPUT = Path("m3u/listtivi.m3u")
DEFAULT_MAPPING = Path("m3u/canonical_channels.yml")

SOURCE_URLS = {
    "vmttv": (
        "https://raw.githubusercontent.com/"
        "vuminhthanh12/vuminhthanh12/refs/heads/main/vmttv"
    ),
    "vietanhtv": "https://tv.vietanhtv.top/sex/",
    "dltivi": (
        "https://raw.githubusercontent.com/"
        "DinhLap96/ListTivi/refs/heads/main/ListTiVi/dltivi_v2.ndl"
    ),
    "iptv-org": (
        "https://raw.githubusercontent.com/"
        "iptv-org/iptv/refs/heads/master/streams/vn.m3u"
    ),
    "easport": "https://livesport.s.gy/easport",
}


SOURCE_PRIORITY = {
    "vmttv": 500,
    "vietanhtv": 400,
    "dltivi": 300,
    "iptv-org": 200,
    "easport": 100,
}


DALVIK_UA = "Dalvik/2.1.0 (Linux; U; Android 10; K)"

# EaSport:
# Không dùng browser UA làm phương án ưu tiên.
# Ưu tiên UA thường gặp ở Android OTT / IPTV / media player.
EASPORT_UA_CANDIDATES = (
    "okhttp/4.9.3",
    "okhttp/4.12.0",
    "ExoPlayerLib/2.18.1 (Linux;Android 12) ExoPlayerLib/2.18.1",
    "Dalvik/2.1.0 (Linux; U; Android 10; K)",
)


COMMON_HEADERS = {
    "Accept": "*/*",
    "Connection": "keep-alive",
    "Accept-Encoding": "gzip, deflate",
}


# ============================================================
# FINAL GROUPS
# ============================================================

FINAL_GROUPS = {
    "VTV": "📺 VTV",
    "HTV": "📺 HTV",
    "SCTV": "📺 SCTV",
    "VTVCAB": "📡 VTVCab",
    "HTVC": "📡 HTVC",
    "THIET_YEU": "⭐ Thiết yếu",
    "DIA_PHUONG": "🏙️ Địa phương",

    "SU_KIEN": "🎟️ Sự kiện",
    "THE_THAO": "🏆 Thể thao",
    "PHIM": "🎬 Phim",
    "THIEU_NHI": "👧 Thiếu nhi",
    "AM_NHAC": "🎵 Âm nhạc",
    "TIN_TUC": "📰 Tin tức",
    "QUOC_TE": "🌍 Quốc tế",
    "KHAC": "📦 Khác",
}


PRIORITY_GROUPS = {
    "VTV",
    "HTV",
    "SCTV",
    "VTVCAB",
    "HTVC",
    "THIET_YEU",
    "DIA_PHUONG",
}


# ============================================================
# FILTER RULES
# ============================================================

UPDATE_GROUP_RE = re.compile(
    r"^\s*update\s+\d{1,2}:\d{2}\b.*$",
    re.IGNORECASE,
)


RADIO_RE = re.compile(
    r"""
    (
        \bradio\b
        |\bfm\b
        |\bam\b
        |\bvov\b
        |\bvov[0-9a-z]*\b
        |phat\s*thanh
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)


VMTTV_EXCLUDED_GROUPS = {
    "live events",
    "radio",
    "uk radio",
    "israel",
    "hàn quốc",
    "trung quốc",
    "thái lan",
    "cola tv",
    "cola tv sv2",
    "pháo hoa tv",
}


VIETANHTV_EXCLUDED_GROUPS = {
    "update",
    "dự phòng",
    "fpt",
    "sự kiện 360",
    "rạp phim",
    "radio",
    "socolive",
}


DLTIVI_EXCLUDED_GROUPS = {
    "vov",
}


EASPORT_EXCLUDED_GROUPS = {
    "info",
}


# ============================================================
# TEXT NORMALIZATION
# ============================================================

def strip_accents(value: str) -> str:
    """
    Normalize Vietnamese accents.

    Đ/đ must be replaced BEFORE NFD because they are not
    decomposed by unicodedata.normalize().
    """

    if not value:
        return ""

    value = value.replace("Đ", "D").replace("đ", "d")

    value = unicodedata.normalize(
        "NFD",
        value,
    )

    return "".join(
        c
        for c in value
        if unicodedata.category(c) != "Mn"
    )


def normalize_text(value: str) -> str:
    """
    Matching normalization.

    Quality tokens such as HD, SD, 720p, 1080p and .vn
    are removed.
    """

    if not value:
        return ""

    value = str(value).strip()

    value = value.replace("_", " ")
    value = value.replace("-", " ")
    value = value.replace(".", " ")
    value = value.replace("@", " ")
    value = value.replace("/", " ")
    value = value.replace("|", " ")

    value = strip_accents(value)
    value = value.lower()

    value = re.sub(
        r"\s+",
        " ",
        value,
    )

    value = re.sub(
        r"\b("
        r"uhd|fhd|fullhd|hd|sd|vn|"
        r"2k|4k|8k|"
        r"240p|360p|480p|576p|720p|"
        r"1080p|1440p|2160p"
        r")\b",
        " ",
        value,
        flags=re.IGNORECASE,
    )

    return re.sub(
        r"\s+",
        " ",
        value,
    ).strip()


def compact(value: str) -> str:
    return re.sub(
        r"[^a-z0-9]+",
        "",
        normalize_text(value),
    )


# ============================================================
# LOCAL PROVINCE IDENTITY
# ============================================================

LOCAL_PROVINCE_SLUGS = (
    "angiang",
    "bacgiang",
    "backan",
    "baclieu",
    "bacninh",
    "bariavungtau",
    "bentre",
    "binhdinh",
    "binhduong",
    "binhphuoc",
    "binhthuan",
    "camau",
    "cantho",
    "caobang",
    "danang",
    "daklak",
    "daknong",
    "dienbien",
    "dongnai",
    "dongthap",
    "gialai",
    "hagiang",
    "hanam",
    "hanoi",
    "hatinh",
    "haiduong",
    "haiphong",
    "hoabinh",
    "hungyen",
    "khanhhoa",
    "kiengiang",
    "kontum",
    "laichau",
    "lamdong",
    "langson",
    "laocai",
    "namdinh",
    "nghean",
    "ninhbinh",
    "ninhthuan",
    "phutho",
    "phuyen",
    "quangbinh",
    "quangnam",
    "quangngai",
    "quangninh",
    "quangtri",
    "soctrang",
    "sonla",
    "tayninh",
    "thaibinh",
    "thainguyen",
    "thanhhoa",
    "tiengiang",
    "travinh",
    "tuyenquang",
    "vinhlong",
    "vinhphuc",
    "yenbai",
    "hue",
)


_PROVINCE_SLUG_RE = re.compile(
    "|".join(
        sorted(
            LOCAL_PROVINCE_SLUGS,
            key=len,
            reverse=True,
        )
    )
)


# Acronyms thường dùng cho đài địa phương.
# Chỉ dùng khi tvg-id đã xác định được tỉnh,
# tránh false-positive kiểu "NgheAnTV".
LOCAL_STATION_PREFIXES = (
    "atv",
    "bctv",
    "bgiangtv",
    "btv",
    "dantoc",
    "dhtv",
    "dltv",
    "drt",
    "dtv",
    "hanoi",
    "htv",
    "hue",
    "ktv",
    "lbtv",
    "laocaitv",
    "ntv",
    "ptth",
    "qbtv",
    "qntv",
    "qptv",
    "qtv",
    "stp",
    "tn1",
    "tn",
    "tntv",
    "thbt",
    "thdt",
    "tth",
    "tvtv",
    "ybtv",
)


def _find_province(value: str) -> Optional[str]:
    """
    Find province slug in ONE value.

    This intentionally does not merge all fields first.
    """

    key = compact(value)

    if not key:
        return None

    match = _PROVINCE_SLUG_RE.search(key)

    if match:
        return match.group(0)

    return None


def _explicit_local_number(
    value: str,
    province: str,
) -> Optional[int]:
    """
    Detect an explicit local channel number.

    Examples:
        thainguyen1
        thainguyen2
        TN1
        TN 1
        ATV1
        AnGiangTV1

    Do NOT scan arbitrary digits.
    """

    if not value:
        return None

    normalized = normalize_text(value)
    key = compact(value)

    if not key:
        return None

    # --------------------------------------------------------
    # Province slug + number
    # --------------------------------------------------------

    match = re.fullmatch(
        rf"{re.escape(province)}(\d{{1,2}})",
        key,
    )

    if match:
        return int(match.group(1))

    # --------------------------------------------------------
    # Province + TV + number
    #
    # angiangtv1
    # angiangtv2
    # --------------------------------------------------------

    match = re.fullmatch(
        rf"{re.escape(province)}tv(\d{{1,2}})",
        key,
    )

    if match:
        return int(match.group(1))

    # --------------------------------------------------------
    # Explicit station acronym + number.
    #
    # Only if the number appears directly after a known
    # local station prefix.
    # --------------------------------------------------------

    for prefix in LOCAL_STATION_PREFIXES:

        match = re.fullmatch(
            rf"{re.escape(prefix)}(\d{{1,2}})",
            key,
        )

        if match:
            return int(match.group(1))

    # --------------------------------------------------------
    # Name forms:
    #
    # TN1 HD
    # TN 1 HD
    # ATV1 - Báo...
    # An Giang TV1
    # --------------------------------------------------------

    patterns = (
        r"\b(?:tn|atv|btv|drt|dtv|ntv|ptth|qbtv|"
        r"qntv|qptv|qtv|tntv|thbt|thdt|tth|"
        r"angiangtv|hue tv)\s*"
        r"(\d{1,2})\b",

        rf"\b{re.escape(province)}\s*"
        r"(?:tv\s*)?(\d{1,2})\b",
    )

    for pattern in patterns:

        match = re.search(
            pattern,
            normalized,
            flags=re.IGNORECASE,
        )

        if match:
            return int(match.group(1))

    return None


def detect_local_identity(
    tvg_id: str = "",
    tvg_name: str = "",
    original_name: str = "",
) -> Optional[str]:
    """
    Return stable local identity:

        thainguyen1
        thainguyen2
        angiang1
        danang1

    Important rules:

    1. Province is determined from tvg-id first.
    2. Explicit number is searched in tvg-id/name.
    3. If tvg-id is exactly province slug, primary channel = 1.
    4. URL is NEVER inspected.
    """

    values = (
        tvg_id,
        tvg_name,
        original_name,
    )

    province = None

    # --------------------------------------------------------
    # Province from tvg-id has highest confidence.
    # --------------------------------------------------------

    if tvg_id:

        tvg_key = compact(tvg_id)

        if tvg_key in LOCAL_PROVINCE_SLUGS:
            province = tvg_key

    # --------------------------------------------------------
    # Otherwise find province in fields in priority order.
    # --------------------------------------------------------

    if not province:

        for value in values:

            found = _find_province(value)

            if found:
                province = found
                break

    if not province:
        return None

    # --------------------------------------------------------
    # Explicit number.
    # --------------------------------------------------------

    for value in values:

        number = _explicit_local_number(
            value,
            province,
        )

        if number is not None:

            if 1 <= number <= 99:

                return (
                    f"{province}{number}"
                )

    # --------------------------------------------------------
    # Very important:
    #
    # tvg-id exactly equals province slug means this is normally
    # the primary/main local channel.
    #
    # Example:
    #   tvg-id="thainguyen"
    #   TN - Báo và PTTH Thái Nguyên
    #
    # -> thainguyen1
    # --------------------------------------------------------

    if tvg_id:

        tvg_key = compact(tvg_id)

        if tvg_key == province:
            return f"{province}1"

    return None


# Backward-compatible helper.
# Kept so external code importing this function will not break.
def detect_province_number(
    texts: Iterable[str],
) -> Optional[str]:

    values = [
        str(x)
        for x in texts
        if x
    ]

    if not values:
        return None

    # Do not combine arbitrary fields and scan random digits.
    # Try each value independently.
    for value in values:

        identity = detect_local_identity(
            tvg_id=value,
            tvg_name="",
            original_name="",
        )

        if identity:
            return identity

    return None


def clean_display_name(
    name: str,
) -> str:

    if not name:
        return ""

    return re.sub(
        r"\s+",
        " ",
        name.strip(),
    )


# ============================================================
# M3U ENTRY
# ============================================================

@dataclass
class M3UEntry:

    source: str
    extinf: str
    url: str

    extra_lines: List[str] = field(
        default_factory=list
    )

    duration: str = "-1"
    original_name: str = ""

    tvg_id: str = ""
    tvg_name: str = ""
    tvg_logo: str = ""
    group_title: str = ""

    canonical_id: str = ""
    canonical_score: int = 0
    canonical_reason: str = ""

    canonical_name: str = ""
    canonical_group: str = ""
    epg_id: str = ""

    source_score: int = 0

    @property
    def identity_text(self) -> str:

        return " ".join(
            [
                self.tvg_id,
                self.tvg_name,
                self.original_name,
            ]
        ).strip()


# ============================================================
# EXTINF
# ============================================================

ATTR_RE = re.compile(
    r'([A-Za-z0-9_-]+)="([^"]*)"'
)


def parse_extinf(
    line: str,
) -> Dict[str, str]:

    return {
        m.group(1).lower(): m.group(2)
        for m in ATTR_RE.finditer(line)
    }


def parse_display_name(
    line: str,
) -> str:

    if "," not in line:
        return ""

    return line.split(
        ",",
        1,
    )[1].strip()


def parse_m3u(
    text: str,
    source: str,
) -> List[M3UEntry]:

    entries: List[M3UEntry] = []

    current_extinf: Optional[str] = None
    current_extra: List[str] = []

    for raw_line in text.splitlines():

        line = raw_line.strip()

        if not line:
            continue

        if line.startswith("#EXTINF:"):

            current_extinf = line
            current_extra = []

            continue

        if current_extinf is None:
            continue

        if not line.startswith("#"):

            attrs = parse_extinf(
                current_extinf
            )

            comma_pos = (
                current_extinf.find(",")
            )

            duration = "-1"

            if comma_pos >= 0:

                duration_part = (
                    current_extinf[
                        len("#EXTINF:"):comma_pos
                    ]
                )

                duration = (
                    duration_part.strip()
                )

            entries.append(
                M3UEntry(
                    source=source,
                    extinf=current_extinf,
                    url=line,
                    extra_lines=list(
                        current_extra
                    ),
                    duration=duration,
                    original_name=(
                        parse_display_name(
                            current_extinf
                        )
                    ),
                    tvg_id=attrs.get(
                        "tvg-id",
                        "",
                    ),
                    tvg_name=attrs.get(
                        "tvg-name",
                        "",
                    ),
                    tvg_logo=attrs.get(
                        "tvg-logo",
                        "",
                    ),
                    group_title=attrs.get(
                        "group-title",
                        "",
                    ),
                    source_score=(
                        SOURCE_PRIORITY.get(
                            source,
                            0,
                        )
                    ),
                )
            )

            current_extinf = None
            current_extra = []

            continue

        current_extra.append(line)

    return entries


# ============================================================
# CANONICAL RESOLVER
# ============================================================

class CanonicalResolver:

    def __init__(
        self,
        mapping_path: Path,
    ):

        self.mapping_path = mapping_path

        self.channels: Dict[str, dict] = {}

        self.alias_exact: Dict[
            str,
            str,
        ] = {}

        self.alias_compact: Dict[
            str,
            str,
        ] = {}

        self.ambiguous_exact = set()
        self.ambiguous_compact = set()

        self.vtvcab_number_index: Dict[
            int,
            str,
        ] = {}

        self.province_number_index: Dict[
            str,
            str,
        ] = {}

        self.local_auto_index: Dict[
            str,
            str,
        ] = {}

        self.load()

    # --------------------------------------------------------
    # LOAD YAML
    # --------------------------------------------------------

    def load(self) -> None:

        if not self.mapping_path.exists():

            raise FileNotFoundError(
                "Canonical mapping not found: "
                f"{self.mapping_path}"
            )

        with self.mapping_path.open(
            "r",
            encoding="utf-8",
        ) as f:

            data = yaml.safe_load(f) or {}

        if "channels" in data:
            data = data["channels"]

        if not isinstance(data, dict):

            raise ValueError(
                "canonical_channels.yml must contain "
                "a 'channels' mapping."
            )

        for raw_canonical_id, raw in data.items():

            canonical_id = str(
                raw_canonical_id
            ).strip()

            if not canonical_id:
                continue

            if not isinstance(raw, dict):
                raw = {}

            self.channels[
                canonical_id
            ] = raw

            aliases = raw.get(
                "aliases",
                [],
            )

            if isinstance(
                aliases,
                str,
            ):
                aliases = [aliases]

            aliases = list(aliases)

            aliases.append(
                canonical_id
            )

            if raw.get("name"):

                aliases.append(
                    str(
                        raw["name"]
                    )
                )

            provider_names = raw.get(
                "provider_names",
                [],
            )

            if isinstance(
                provider_names,
                str,
            ):
                provider_names = [
                    provider_names
                ]

            aliases.extend(
                str(x)
                for x in provider_names
            )

            for alias in aliases:

                self._register_alias(
                    str(alias),
                    canonical_id,
                )

            number = raw.get(
                "vtvcab_number"
            )

            if number is not None:

                try:

                    self.vtvcab_number_index[
                        int(number)
                    ] = canonical_id

                except (
                    TypeError,
                    ValueError,
                ):
                    pass

    # --------------------------------------------------------
    # REGISTER ALIAS
    # --------------------------------------------------------

    def _register_alias(
        self,
        alias: str,
        canonical_id: str,
    ) -> None:

        if not alias:
            return

        exact = normalize_text(alias)
        short = compact(alias)

        # ----------------------------------------------------
        # NEW LOCAL INDEX
        #
        # This is the critical part.
        #
        # If YAML contains:
        #
        #   thainguyen
        #   TN1
        #   TN - Báo...
        #
        # all can produce:
        #
        #   thainguyen1
        #
        # and point to the same canonical_id.
        # ----------------------------------------------------

        local_key = detect_local_identity(
            tvg_id=alias,
            tvg_name=alias,
            original_name=alias,
        )

        if local_key:

            old_local = (
                self.province_number_index.get(
                    local_key
                )
            )

            if (
                old_local is None
                or old_local == canonical_id
            ):

                self.province_number_index[
                    local_key
                ] = canonical_id

        # ----------------------------------------------------
        # Exact alias.
        # ----------------------------------------------------

        if exact:

            old = self.alias_exact.get(
                exact
            )

            if (
                old is not None
                and old != canonical_id
            ):

                self.ambiguous_exact.add(
                    exact
                )

                self.alias_exact.pop(
                    exact,
                    None,
                )

            elif (
                exact
                not in self.ambiguous_exact
            ):

                self.alias_exact[
                    exact
                ] = canonical_id

        # ----------------------------------------------------
        # Compact alias.
        # ----------------------------------------------------

        if short:

            old = self.alias_compact.get(
                short
            )

            if (
                old is not None
                and old != canonical_id
            ):

                self.ambiguous_compact.add(
                    short
                )

                self.alias_compact.pop(
                    short,
                    None,
                )

            elif (
                short
                not in self.ambiguous_compact
            ):

                self.alias_compact[
                    short
                ] = canonical_id

    # --------------------------------------------------------
    # VTVCAB NUMBER
    # --------------------------------------------------------

    @staticmethod
    def detect_vtvcab_number(
        value: str,
    ) -> Optional[int]:

        if not value:
            return None

        normalized = normalize_text(
            value
        )

        compact_value = compact(
            value
        )

        patterns = (
            r"\bvtv\s*cab\s*(\d+)\b",
            r"\bvtvcab\s*(\d+)\b",
        )

        for pattern in patterns:

            match = re.search(
                pattern,
                normalized,
                flags=re.IGNORECASE,
            )

            if match:
                return int(
                    match.group(1)
                )

        match = re.fullmatch(
            r"vtvcab(\d+)",
            compact_value,
        )

        if match:

            return int(
                match.group(1)
            )

        return None

    # --------------------------------------------------------
    # KNOWN FAMILY
    # --------------------------------------------------------

    def detect_known_family(
        self,
        values: Iterable[str],
    ) -> Optional[str]:

        for value in values:

            key = compact(value)

            if not key:
                continue

            match = re.fullmatch(
                r"vtv(\d+)(?:hd)?",
                key,
            )

            if match:

                candidate = (
                    f"vtv{match.group(1)}"
                )

                if candidate in self.channels:
                    return candidate

            match = re.fullmatch(
                r"htv(\d+)(?:hd)?",
                key,
            )

            if match:

                candidate = (
                    f"htv{match.group(1)}"
                )

                if candidate in self.channels:
                    return candidate

            match = re.fullmatch(
                r"sctv(\d+)(?:hd)?",
                key,
            )

            if match:

                candidate = (
                    f"sctv{match.group(1)}"
                )

                if candidate in self.channels:
                    return candidate

        return None

    # --------------------------------------------------------
    # RESOLVE
    # --------------------------------------------------------

    def resolve(
        self,
        entry: M3UEntry,
    ) -> Tuple[
        Optional[str],
        int,
        str,
    ]:

        candidates = (
            (
                "tvg-id",
                entry.tvg_id,
                100,
            ),
            (
                "name",
                entry.original_name,
                90,
            ),
            (
                "tvg-name",
                entry.tvg_name,
                90,
            ),
            (
                "group-title",
                entry.group_title,
                80,
            ),
        )

        # ----------------------------------------------------
        # 1. Exact alias.
        # ----------------------------------------------------

        for (
            reason,
            value,
            score,
        ) in candidates:

            key = normalize_text(value)

            if (
                not key
                or key in self.ambiguous_exact
            ):
                continue

            canonical_id = (
                self.alias_exact.get(key)
            )

            if canonical_id:

                return (
                    canonical_id,
                    score,
                    f"{reason}:exact",
                )

        # ----------------------------------------------------
        # 2. Compact alias.
        # ----------------------------------------------------

        for (
            reason,
            value,
            score,
        ) in candidates:

            key = compact(value)

            if (
                not key
                or key in self.ambiguous_compact
            ):
                continue

            canonical_id = (
                self.alias_compact.get(key)
            )

            if canonical_id:

                return (
                    canonical_id,
                    score - 5,
                    f"{reason}:compact",
                )

        # ----------------------------------------------------
        # 3. VTVCab number.
        # ----------------------------------------------------

        for (
            reason,
            value,
            _score,
        ) in candidates:

            number = (
                self.detect_vtvcab_number(
                    value
                )
            )

            if number is None:
                continue

            canonical_id = (
                self.vtvcab_number_index.get(
                    number
                )
            )

            if canonical_id:

                return (
                    canonical_id,
                    95,
                    (
                        f"{reason}:"
                        f"vtvcab_number:"
                        f"{number}"
                    ),
                )

        # ----------------------------------------------------
        # 4. Known family.
        # ----------------------------------------------------

        family = (
            self.detect_known_family(
                value
                for _, value, _ in candidates
            )
        )

        if family:

            return (
                family,
                70,
                "known_family",
            )

        # ----------------------------------------------------
        # 5. LOCAL IDENTITY
        #
        # This now runs on fields independently.
        #
        # Example:
        #
        # tvg-id="thainguyen"
        # name="TN1 HD | TH Thái Nguyên"
        #
        # -> thainguyen1
        #
        # tvg-id="thainguyen"
        # name="TN - Báo và PTTH Thái Nguyên"
        #
        # -> thainguyen1
        # ----------------------------------------------------

        local_key = detect_local_identity(
            tvg_id=entry.tvg_id,
            tvg_name=entry.tvg_name,
            original_name=entry.original_name,
        )

        if local_key:

            existing = (
                self.province_number_index.get(
                    local_key
                )
            )

            if existing:

                return (
                    existing,
                    75,
                    f"local_identity:{local_key}",
                )

            auto_id = (
                self.local_auto_index.get(
                    local_key
                )
            )

            if not auto_id:

                auto_id = (
                    f"local_auto_{local_key}"
                )

                self.local_auto_index[
                    local_key
                ] = auto_id

            return (
                auto_id,
                65,
                f"local_identity_auto:{local_key}",
            )

        return (
            None,
            0,
            "unknown",
        )

    # --------------------------------------------------------
    # GET
    # --------------------------------------------------------

    def get(
        self,
        canonical_id: str,
    ) -> dict:

        return self.channels.get(
            canonical_id,
            {},
        )


# ============================================================
# FILTER HELPERS
# ============================================================

def normalized_group(
    group: str,
) -> str:

    return normalize_text(group)


VMTTV_EXCLUDED = {
    normalized_group(x)
    for x in VMTTV_EXCLUDED_GROUPS
}


VIETANHTV_EXCLUDED = {
    normalized_group(x)
    for x in VIETANHTV_EXCLUDED_GROUPS
}


DLTIVI_EXCLUDED = {
    normalized_group(x)
    for x in DLTIVI_EXCLUDED_GROUPS
}


EASPORT_EXCLUDED = {
    normalized_group(x)
    for x in EASPORT_EXCLUDED_GROUPS
}


def is_update_group(
    group: str,
) -> bool:

    return bool(
        UPDATE_GROUP_RE.match(
            group or ""
        )
    )


def is_global_radio(
    entry: M3UEntry,
) -> bool:

    text = " ".join(
        [
            entry.group_title,
            entry.tvg_id,
            entry.tvg_name,
            entry.original_name,
        ]
    )

    return bool(
        RADIO_RE.search(text)
    )


def entry_filter_text(
    entry: M3UEntry,
) -> str:

    return normalize_text(
        " ".join(
            [
                entry.group_title,
                entry.tvg_id,
                entry.tvg_name,
                entry.original_name,
                entry.url,
            ]
        )
    )


def is_vmttv_blocked_content(
    entry: M3UEntry,
) -> Optional[str]:

    if entry.source != "vmttv":
        return None

    group = normalized_group(
        entry.group_title
    )

    text = entry_filter_text(
        entry
    )

    compact_text = compact(text)

    if group == "live events":
        return "LIVE EVENTS"

    if "liveevents" in compact_text:
        return "LIVE EVENTS"

    if group == "cola tv sv2":
        return "COLA TV SV2"

    if "colatvsv2" in compact_text:
        return "COLA TV SV2"

    return None


def group_is_excluded(
    entry: M3UEntry,
) -> bool:

    group = normalized_group(
        entry.group_title
    )

    if is_update_group(group):
        return True

    if is_global_radio(entry):
        return True

    if is_vmttv_blocked_content(entry):
        return True

    excluded = {
        "vmttv": VMTTV_EXCLUDED,
        "vietanhtv": VIETANHTV_EXCLUDED,
        "dltivi": DLTIVI_EXCLUDED,
        "easport": EASPORT_EXCLUDED,
    }.get(
        entry.source,
        set(),
    )

    return group in excluded


def is_vsbet(
    entry: M3UEntry,
) -> bool:

    text = " ".join(
        [
            entry.tvg_id,
            entry.tvg_name,
            entry.original_name,
        ]
    )

    return "vsbet" in compact(text)


GAMBLING_BRAND_NAME_RE = re.compile(
    r"^\s*blv\b",
    re.IGNORECASE,
)


GAMBLING_BRAND_DOMAINS = (
    "msdht.app",
    "phaohoa.live",
    "phaohoa1.live",
)


GAMBLING_BRAND_LOGO_MARKERS = (
    "colatv_logo",
    "phaohoa1.live",
)


def is_gambling_brand(
    entry: M3UEntry,
) -> bool:

    name = normalize_text(
        entry.original_name
        or entry.tvg_name
    )

    if GAMBLING_BRAND_NAME_RE.match(name):
        return True

    url_lower = (
        entry.url or ""
    ).lower()

    if any(
        domain in url_lower
        for domain in GAMBLING_BRAND_DOMAINS
    ):
        return True

    logo_lower = (
        entry.tvg_logo or ""
    ).lower()

    if any(
        marker in logo_lower
        for marker in GAMBLING_BRAND_LOGO_MARKERS
    ):
        return True

    return False


def should_remove(
    entry: M3UEntry,
) -> bool:

    return (
        group_is_excluded(entry)
        or is_vsbet(entry)
        or is_gambling_brand(entry)
    )


# ============================================================
# CANONICAL APPLY
# ============================================================

def apply_canonical(
    entry: M3UEntry,
    resolver: CanonicalResolver,
) -> None:

    (
        canonical_id,
        score,
        reason,
    ) = resolver.resolve(entry)

    if canonical_id:

        entry.canonical_id = canonical_id
        entry.canonical_score = score
        entry.canonical_reason = reason

        mapping = resolver.get(
            canonical_id
        )

        entry.canonical_name = str(
            mapping.get(
                "name",
                "",
            )
        ).strip()

        entry.canonical_group = str(
            mapping.get(
                "group",
                "",
            )
        ).strip()

        entry.epg_id = str(
            mapping.get(
                "epg_id",
                "",
            )
        ).strip()

        return

    # Unknown:
    # deterministic identity from channel metadata,
    # never URL-first.
    identity = compact(
        entry.tvg_id
        or entry.tvg_name
        or entry.original_name
    )

    if not identity:
        identity = compact(
            entry.url
        )

    digest = hashlib.sha1(
        identity.encode("utf-8")
    ).hexdigest()[:12]

    entry.canonical_id = (
        f"local-{digest}"
    )

    entry.canonical_score = 10
    entry.canonical_reason = (
        "local_identity"
    )


# ============================================================
# GROUP HELPERS
# ============================================================

def group_label(
    group_key: str,
) -> str:

    key = str(
        group_key or ""
    ).strip()

    if key in FINAL_GROUPS:
        return FINAL_GROUPS[key]

    if key in FINAL_GROUPS.values():
        return key

    return ""


def priority_group_from_mapping(
    mapping: dict,
) -> Optional[str]:

    group_key = str(
        mapping.get(
            "group",
            "",
        )
    ).strip()

    if group_key in PRIORITY_GROUPS:

        return FINAL_GROUPS[
            group_key
        ]

    return None


def mapping_provider(
    mapping: dict,
) -> str:

    return normalize_text(
        str(
            mapping.get(
                "provider",
                "",
            )
        )
    )


# ============================================================
# EVENT CLASSIFIER
# ============================================================

EVENT_PATTERNS = (
    "tv360 su kien",
    "tv360 event",
    "tv360 events",
    "fpt su kien",
    "fpt play su kien",
    "fpt event",
    "fpt events",
    "vtvprime",
)


def is_event_channel(
    entry: M3UEntry,
    mapping: dict,
) -> bool:

    if priority_group_from_mapping(mapping):
        return False

    text = normalize_text(
        " ".join(
            [
                entry.source,
                entry.group_title,
                entry.tvg_id,
                entry.tvg_name,
                entry.original_name,
                str(
                    mapping.get(
                        "name",
                        "",
                    )
                ),
                str(
                    mapping.get(
                        "provider",
                        "",
                    )
                ),
            ]
        )
    )

    compact_text = compact(text)

    mapped_group = str(
        mapping.get(
            "group",
            "",
        )
    ).strip()

    if mapped_group == "SU_KIEN":
        return True

    if "vtvprime" in compact_text:
        return True

    if (
        "tv360 su kien" in text
        or "tv360 event" in text
        or "tv360 events" in text
    ):
        return True

    if (
        "fpt su kien" in text
        or "fpt play su kien" in text
        or "fpt event" in text
        or "fpt events" in text
    ):
        return True

    if re.search(
        r"\bsu kien\b",
        text,
    ):
        return True

    if re.search(
        r"\bevent\b",
        text,
    ):
        return True

    if "home_event" in (
        entry.tvg_logo or ""
    ).lower():
        return True

    return False


# ============================================================
# GROUP RESOLUTION
# ============================================================

def classify_group(
    entry: M3UEntry,
    mapping: dict,
) -> str:

    # --------------------------------------------------------
    # 1. HARD LOCK.
    # --------------------------------------------------------

    locked = priority_group_from_mapping(
        mapping
    )

    if locked:
        return locked

    # --------------------------------------------------------
    # 2. Provider lock.
    # --------------------------------------------------------

    provider = mapping_provider(
        mapping
    )

    if provider == "vtvcab":
        return FINAL_GROUPS["VTVCAB"]

    if provider == "htvc":
        return FINAL_GROUPS["HTVC"]

    if provider == "sctv":
        return FINAL_GROUPS["SCTV"]

    if provider == "vtv":
        return FINAL_GROUPS["VTV"]

    if provider == "htv":
        return FINAL_GROUPS["HTV"]

    # --------------------------------------------------------
    # 3. LOCAL IDENTITY.
    #
    # This is intentionally before content classification.
    # --------------------------------------------------------

    local_identity = detect_local_identity(
        tvg_id=entry.tvg_id,
        tvg_name=entry.tvg_name,
        original_name=entry.original_name,
    )

    if local_identity:
        return FINAL_GROUPS["DIA_PHUONG"]

    # --------------------------------------------------------
    # 4. Text.
    # --------------------------------------------------------

    text = normalize_text(
        " ".join(
            [
                entry.tvg_id,
                entry.tvg_name,
                entry.original_name,
                entry.group_title,
            ]
        )
    )

    compact_text = compact(text)

    # --------------------------------------------------------
    # VTV
    # --------------------------------------------------------

    if re.search(
        r"\bvtv\s*\d+\b",
        text,
    ):
        return FINAL_GROUPS["VTV"]

    # --------------------------------------------------------
    # HTV
    # --------------------------------------------------------

    if re.search(
        r"\bhtv\s*\d+\b",
        text,
    ):
        return FINAL_GROUPS["HTV"]

    # --------------------------------------------------------
    # SCTV
    # --------------------------------------------------------

    if re.search(
        r"\bsctv\s*\d+\b",
        text,
    ):
        return FINAL_GROUPS["SCTV"]

    if re.search(
        r"\bsctv\b",
        text,
    ):
        return FINAL_GROUPS["SCTV"]

    # --------------------------------------------------------
    # HTVC
    # --------------------------------------------------------

    if "htvc" in compact_text:
        return FINAL_GROUPS["HTVC"]

    # --------------------------------------------------------
    # VTVCab / ON.
    # --------------------------------------------------------

    if "vtvcab" in compact_text:
        return FINAL_GROUPS["VTVCAB"]

    on_patterns = (
        "on football",
        "on sports+",
        "on sports plus",
        "on sports news",
        "on sports",
        "on golf",
        "on phim viet",
        "on movies",
        "on cine",
        "on vie giai tri",
        "on vie dramas",
        "on echannel",
        "on style",
        "on kids",
        "on bibi",
        "on info tv",
        "on o2tv",
        "on life",
        "on music",
        "on vfamily",
        "on trending",
        "on homeshopping",
    )

    if any(
        pattern in text
        for pattern in on_patterns
    ):
        return FINAL_GROUPS["VTVCAB"]

    # --------------------------------------------------------
    # THIẾT YẾU
    # --------------------------------------------------------

    essential_patterns = (
        r"\bqpvn\b",
        r"\bquoc phong\b",
        r"\bantv\b",
        r"\ban ninh\b",
    )

    if any(
        re.search(
            pattern,
            text,
        )
        for pattern in essential_patterns
    ):
        return FINAL_GROUPS["THIET_YEU"]

    # --------------------------------------------------------
    # FALLBACK LOCAL TEXT.
    #
    # Kept for cases where province identity cannot be built
    # but the text clearly says a local province.
    # --------------------------------------------------------

    local_keywords = (
        "hanoi tv",
        "ha noi tv",
        "hanoitv",
        "hai phong",
        "da nang",
        "can tho",
        "quang ninh",
        "hai duong",
        "bac ninh",
        "nam dinh",
        "thai nguyen",
        "nghe an",
        "ha tinh",
        "quang binh",
        "quang tri",
        "thua thien hue",
        "hue tv",
        "hue",
        "binh dinh",
        "khanh hoa",
        "dak lak",
        "lam dong",
        "dong nai",
        "binh duong",
        "ba ria",
        "vung tau",
        "tay ninh",
        "long an",
        "tien giang",
        "ben tre",
        "vinh long",
        "dong thap",
        "an giang",
        "kien giang",
        "ca mau",
        "soc trang",
        "bac lieu",
        "tra vinh",
        "binh thuan",
        "phu yen",
        "gia lai",
        "kon tum",
        "dak nong",
        "ninh thuan",
        "son la",
        "dien bien",
        "lai chau",
        "lao cai",
        "yen bai",
        "ha giang",
        "cao bang",
        "bac kan",
        "tuyen quang",
        "thai binh",
        "hung yen",
        "lang son",
        "ninh binh",
        "hoa binh",
        "vinh phuc",
        "bac giang",
        "quang ngai",
        "phu tho",
        "quang nam",
        "binh phuoc",
        "sai gon",
        "tp hcm",
        "ho chi minh",
        "dai ptth",
    )

    if any(
        keyword in text
        for keyword in local_keywords
    ):
        return FINAL_GROUPS["DIA_PHUONG"]

    # --------------------------------------------------------
    # EVENT
    # --------------------------------------------------------

    if is_event_channel(
        entry,
        mapping,
    ):
        return FINAL_GROUPS["SU_KIEN"]

    # --------------------------------------------------------
    # SPORTS
    # --------------------------------------------------------

    sports_keywords = (
        "the thao",
        "sport",
        "sports",
        "football",
        "soccer",
        "basketball",
        "tennis",
        "volleyball",
        "boxing",
        "wrestling",
        "golf",
        "racing",
        "motogp",
        "formula 1",
        "f1",
        "ufc",
        "nba",
        "nfl",
        "nhl",
        "mlb",
    )

    if any(
        keyword in text
        for keyword in sports_keywords
    ):
        return FINAL_GROUPS["THE_THAO"]

    # --------------------------------------------------------
    # MOVIES
    # --------------------------------------------------------

    movie_keywords = (
        "phim",
        "movie",
        "movies",
        "cinema",
        "film",
        "tvb",
        "in the box",
        "inthebox",
    )

    if any(
        keyword in text
        for keyword in movie_keywords
    ):
        return FINAL_GROUPS["PHIM"]

    # --------------------------------------------------------
    # KIDS
    # --------------------------------------------------------

    kids_keywords = (
        "thieu nhi",
        "kids",
        "kid",
        "children",
        "cartoon",
        "animation",
        "baby",
    )

    if any(
        keyword in text
        for keyword in kids_keywords
    ):
        return FINAL_GROUPS["THIEU_NHI"]

    # --------------------------------------------------------
    # MUSIC
    # --------------------------------------------------------

    music_keywords = (
        "am nhac",
        "music",
        "mtv",
        "karaoke",
    )

    if any(
        keyword in text
        for keyword in music_keywords
    ):
        return FINAL_GROUPS["AM_NHAC"]

    # --------------------------------------------------------
    # NEWS
    # --------------------------------------------------------

    news_keywords = (
        "tin tuc",
        "news",
        "newsasia",
        "bbc news",
        "cnn",
        "al jazeera",
        "bloomberg",
    )

    if any(
        keyword in text
        for keyword in news_keywords
    ):
        return FINAL_GROUPS["TIN_TUC"]

    # --------------------------------------------------------
    # INTERNATIONAL
    # --------------------------------------------------------

    international_keywords = (
        "quoc te",
        "international",
        "world",
        "korea",
        "japan",
        "china",
        "thai",
        "uk",
        "usa",
        "france",
        "germany",
        "italy",
        "spain",
        "portugal",
    )

    if any(
        keyword in text
        for keyword in international_keywords
    ):
        return FINAL_GROUPS["QUOC_TE"]

    return FINAL_GROUPS["KHAC"]


# ============================================================
# URL / SCORING
# ============================================================

def normalize_url(
    url: str,
) -> str:

    return re.sub(
        r"\s+",
        "",
        url.strip(),
    )


def url_key(
    url: str,
) -> str:

    return normalize_url(
        url
    ).lower()


def metadata_bonus(
    entry: M3UEntry,
) -> int:

    score = 0

    if entry.tvg_logo:
        score += 5

    if entry.tvg_id:
        score += 5

    if entry.tvg_name:
        score += 5

    if entry.group_title:
        score += 2

    if entry.extra_lines:
        score += 3

    return score


def winner_score(
    entry: M3UEntry,
) -> int:

    return (
        entry.canonical_score * 1000
        + entry.source_score
        + metadata_bonus(entry)
    )


# ============================================================
# DEDUPE
# ============================================================

def deduplicate(
    entries: List[M3UEntry],
) -> Tuple[
    List[M3UEntry],
    Dict[str, List[M3UEntry]],
]:

    grouped: Dict[
        str,
        List[M3UEntry],
    ] = defaultdict(list)

    for entry in entries:

        grouped[
            entry.canonical_id
        ].append(entry)

    winners: List[M3UEntry] = []

    for candidates in grouped.values():

        unique_by_url: Dict[
            str,
            M3UEntry,
        ] = {}

        for entry in candidates:

            key = url_key(
                entry.url
            )

            if not key:
                continue

            old = unique_by_url.get(
                key
            )

            if (
                old is None
                or winner_score(entry)
                > winner_score(old)
            ):

                unique_by_url[
                    key
                ] = entry

        candidates = list(
            unique_by_url.values()
        )

        if not candidates:
            continue

        candidates.sort(
            key=winner_score,
            reverse=True,
        )

        winners.append(
            candidates[0]
        )

    return (
        winners,
        grouped,
    )


# ============================================================
# EXTINF ATTRIBUTES
# ============================================================

def upsert_extinf_attr(
    line: str,
    attr: str,
    value: str,
) -> str:

    pattern = re.compile(
        rf'({re.escape(attr)}=")[^"]*(")',
        re.IGNORECASE,
    )

    if pattern.search(line):

        return pattern.sub(
            lambda m:
            m.group(1)
            + value
            + m.group(2),
            line,
            count=1,
        )

    comma = line.find(",")

    if comma < 0:
        return line

    prefix = line[:comma]
    suffix = line[comma:]

    return (
        prefix
        + f' {attr}="{value}"'
        + suffix
    )


# ============================================================
# OUTPUT METADATA
# ============================================================

def prepare_output_entry(
    entry: M3UEntry,
    resolver: CanonicalResolver,
) -> None:

    mapping = resolver.get(
        entry.canonical_id
    )

    display_name = clean_display_name(
        entry.canonical_name
        or entry.tvg_name
        or entry.original_name
        or entry.tvg_id
        or entry.canonical_id
    )

    final_group = classify_group(
        entry,
        mapping,
    )

    entry.canonical_group = final_group

    epg_id = (
        entry.epg_id
        or str(
            mapping.get(
                "epg_id",
                "",
            )
        ).strip()
        or entry.tvg_id
    )

    logo = (
        entry.tvg_logo
        or str(
            mapping.get(
                "logo",
                "",
            )
        ).strip()
    )

    line = entry.extinf

    line = upsert_extinf_attr(
        line,
        "tvg-id",
        str(epg_id or ""),
    )

    line = upsert_extinf_attr(
        line,
        "tvg-name",
        display_name,
    )

    line = upsert_extinf_attr(
        line,
        "group-title",
        final_group,
    )

    if logo:

        line = upsert_extinf_attr(
            line,
            "tvg-logo",
            logo,
        )

    if "," in line:

        prefix = line.split(
            ",",
            1,
        )[0]

        line = (
            prefix
            + ","
            + display_name
        )

    entry.extinf = line


# ============================================================
# FETCH
# ============================================================

class FetchError(RuntimeError):
    pass


def build_headers(
    source: str,
    user_agent: Optional[str] = None,
) -> Dict[str, str]:

    headers = dict(
        COMMON_HEADERS
    )

    headers["User-Agent"] = (
        user_agent or DALVIK_UA
    )

    if source == "easport":

        headers.update(
            {
                "Accept": (
                    "application/vnd.apple.mpegurl,"
                    "application/x-mpegURL,"
                    "audio/mpegurl,"
                    "*/*"
                ),
                "Referer": (
                    "https://livesport.s.gy/"
                ),
                "Origin": (
                    "https://livesport.s.gy"
                ),
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
        )

    return headers


def response_looks_like_m3u(
    text: str,
) -> bool:

    if not text:
        return False

    upper = text.upper()

    # Normal playlist.
    if "#EXTM3U" in upper:
        return True

    # Một số endpoint trả playlist thiếu dòng EXT-M3U
    # nhưng vẫn có EXTINF.
    if "#EXTINF" in upper:
        return True

    return False


def response_looks_like_html(
    text: str,
) -> bool:

    if not text:
        return False

    head = text[:4096].lower()

    html_markers = (
        "<!doctype html",
        "<html",
        "<head",
        "<body",
        "<script",
        "cloudflare",
        "access denied",
        "just a moment",
    )

    return any(
        marker in head
        for marker in html_markers
    )


def response_looks_like_mp4(
    content: bytes,
    content_type: str,
    text: str,
) -> bool:

    ctype = (
        content_type or ""
    ).lower()

    if "video/mp4" in ctype:
        return True

    if content.startswith(
        b"\x00\x00\x00"
    ) and b"ftyp" in content[:32]:
        return True

    text_head = (
        text[:100].lower()
        if text
        else ""
    )

    if "video/mp4" in text_head:
        return True

    return False


def fetch_source(
    session: requests.Session,
    source: str,
    url: str,
    retries: int = 3,
    timeout: Tuple[int, int] = (
        15,
        45,
    ),
) -> str:

    last_error: Optional[
        Exception
    ] = None

    if source == "easport":

        # Thử ít nhất toàn bộ UA candidates.
        retries = max(
            retries,
            len(EASPORT_UA_CANDIDATES),
        )

    for attempt in range(
        1,
        retries + 1,
    ):

        if source == "easport":

            ua = (
                EASPORT_UA_CANDIDATES[
                    (attempt - 1)
                    % len(
                        EASPORT_UA_CANDIDATES
                    )
                ]
            )

        else:

            ua = DALVIK_UA

        headers = build_headers(
            source,
            user_agent=ua,
        )

        # ----------------------------------------------------
        # Concise log only.
        # ----------------------------------------------------

        print(
            f"FETCH {source}"
        )

        try:

            response = session.get(
                url,
                headers=headers,
                timeout=timeout,
                allow_redirects=True,
            )

            response.raise_for_status()

            content = response.content

            if not content:

                raise FetchError(
                    "empty response"
                )

            try:

                text = content.decode(
                    "utf-8-sig"
                )

            except UnicodeDecodeError:

                text = content.decode(
                    "utf-8",
                    errors="replace",
                )

            content_type = (
                response.headers.get(
                    "Content-Type",
                    "",
                )
            )

            # ------------------------------------------------
            # Reject MP4 landing/media response.
            # ------------------------------------------------

            if response_looks_like_mp4(
                content,
                content_type,
                text,
            ):

                raise FetchError(
                    "server returned MP4 "
                    "instead of M3U"
                )

            # ------------------------------------------------
            # Reject HTML/landing page.
            # ------------------------------------------------

            if response_looks_like_html(text):

                raise FetchError(
                    "server returned HTML "
                    "instead of M3U"
                )

            # ------------------------------------------------
            # Validate playlist.
            # ------------------------------------------------

            if not response_looks_like_m3u(
                text
            ):

                raise FetchError(
                    "response is not M3U"
                )

            return text

        except Exception as exc:

            last_error = exc

            if attempt < retries:

                time.sleep(
                    min(
                        2 ** attempt,
                        6,
                    )
                )

    raise FetchError(
        f"Unable to fetch {source}: "
        f"{last_error}"
    )


# ============================================================
# RENDER
# ============================================================

def build_header() -> str:

    return (
        '#EXTM3U '
        'url-tvg='
        '"https://lichphatsong.io.vn/epg.xml"'
    )


def render_m3u(
    entries: List[M3UEntry],
) -> str:

    lines = [
        build_header()
    ]

    for entry in entries:

        lines.append(
            entry.extinf
        )

        for extra in entry.extra_lines:
            lines.append(extra)

        lines.append(
            entry.url
        )

    return (
        "\n".join(lines)
        + "\n"
    )


# ============================================================
# SORT
# ============================================================

GROUP_ORDER = {
    FINAL_GROUPS["VTV"]: 10,
    FINAL_GROUPS["HTV"]: 20,
    FINAL_GROUPS["SCTV"]: 30,
    FINAL_GROUPS["VTVCAB"]: 40,
    FINAL_GROUPS["HTVC"]: 50,
    FINAL_GROUPS["THIET_YEU"]: 60,
    FINAL_GROUPS["DIA_PHUONG"]: 70,

    FINAL_GROUPS["SU_KIEN"]: 80,
    FINAL_GROUPS["THE_THAO"]: 90,
    FINAL_GROUPS["PHIM"]: 100,
    FINAL_GROUPS["THIEU_NHI"]: 110,
    FINAL_GROUPS["AM_NHAC"]: 120,
    FINAL_GROUPS["TIN_TUC"]: 130,
    FINAL_GROUPS["QUOC_TE"]: 140,
    FINAL_GROUPS["KHAC"]: 900,
}


def natural_key(value: str):

    return [
        (
            int(part)
            if part.isdigit()
            else part.lower()
        )
        for part in re.split(
            r"(\d+)",
            value,
        )
    ]


def sort_entries(
    entries: List[M3UEntry],
) -> List[M3UEntry]:

    return sorted(
        entries,
        key=lambda e: (
            GROUP_ORDER.get(
                e.canonical_group,
                999,
            ),
            natural_key(
                e.canonical_name
                or e.tvg_name
                or e.original_name
            ),
            e.canonical_id,
        ),
    )


# ============================================================
# VALIDATION
# ============================================================

def validate_output(
    entries: List[M3UEntry],
) -> None:

    if not entries:

        raise RuntimeError(
            "Optimizer produced ZERO "
            "channels. Refusing to "
            "overwrite output."
        )

    ids = [
        e.canonical_id
        for e in entries
    ]

    duplicates = [
        item
        for item, count
        in Counter(ids).items()
        if count > 1
    ]

    if duplicates:

        raise RuntimeError(
            "Canonical deduplication "
            "failed. Duplicate IDs: "
            f"{duplicates[:20]}"
        )


# ============================================================
# CONCISE LOG HELPERS
# ============================================================

def display_name_for_log(
    entry: M3UEntry,
) -> str:

    return clean_display_name(
        entry.canonical_name
        or entry.original_name
        or entry.tvg_name
        or entry.tvg_id
        or entry.canonical_id
    )


def log_canonical(
    entry: M3UEntry,
) -> None:

    print(
        "CANONICAL "
        f"{display_name_for_log(entry)} "
        "-> "
        f"{entry.canonical_id}"
    )


def log_final(
    entry: M3UEntry,
) -> None:

    print(
        "FINAL "
        f"{entry.canonical_id} "
        "-> "
        f"{entry.canonical_group}"
    )


# ============================================================
# MAIN OPTIMIZE
# ============================================================

def optimize(
    mapping_path: Path,
    output_path: Path,
) -> None:

    resolver = CanonicalResolver(
        mapping_path
    )

    session = requests.Session()

    adapter = (
        requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=0,
        )
    )

    session.mount(
        "http://",
        adapter,
    )

    session.mount(
        "https://",
        adapter,
    )

    all_entries: List[
        M3UEntry
    ] = []

    # --------------------------------------------------------
    # FETCH
    # --------------------------------------------------------

    for source, url in SOURCE_URLS.items():

        try:

            text = fetch_source(
                session,
                source,
                url,
            )

            entries = parse_m3u(
                text,
                source,
            )

            kept: List[
                M3UEntry
            ] = []

            for entry in entries:

                if should_remove(entry):
                    continue

                kept.append(entry)

            all_entries.extend(
                kept
            )

        except Exception as exc:

            # Keep error short.
            print(
                f"FETCH {source} FAILED: "
                f"{exc}",
                file=sys.stderr,
            )

    if not all_entries:

        raise RuntimeError(
            "All remote sources failed "
            "or were empty."
        )

    # --------------------------------------------------------
    # CANONICALIZE BEFORE DEDUPE
    # --------------------------------------------------------

    for entry in all_entries:

        apply_canonical(
            entry,
            resolver,
        )

        log_canonical(entry)

    # --------------------------------------------------------
    # DEDUPE
    # --------------------------------------------------------

    final_entries, _grouped = (
        deduplicate(
            all_entries
        )
    )

    # --------------------------------------------------------
    # FINAL GROUP
    # --------------------------------------------------------

    for entry in final_entries:

        prepare_output_entry(
            entry,
            resolver,
        )

        log_final(entry)

    # --------------------------------------------------------
    # SORT
    # --------------------------------------------------------

    final_entries = sort_entries(
        final_entries
    )

    # --------------------------------------------------------
    # VALIDATE
    # --------------------------------------------------------

    validate_output(
        final_entries
    )

    # --------------------------------------------------------
    # RENDER
    # --------------------------------------------------------

    output_text = render_m3u(
        final_entries
    )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    tmp_path = output_path.with_suffix(
        output_path.suffix
        + ".tmp"
    )

    tmp_path.write_text(
        output_text,
        encoding="utf-8",
    )

    tmp_path.replace(
        output_path
    )


# ============================================================
# CLI
# ============================================================

def parse_args():

    parser = argparse.ArgumentParser(
        description=(
            "Fetch remote IPTV M3U "
            "sources, canonicalize, "
            "deduplicate and generate "
            "final playlist."
        )
    )

    parser.add_argument(
        "--mapping",
        default=str(
            DEFAULT_MAPPING
        ),
    )

    parser.add_argument(
        "--output",
        default=str(
            DEFAULT_OUTPUT
        ),
    )

    return parser.parse_args()


def main() -> int:

    args = parse_args()

    try:

        optimize(
            mapping_path=Path(
                args.mapping
            ),
            output_path=Path(
                args.output
            ),
        )

        return 0

    except KeyboardInterrupt:

        print(
            "FATAL Interrupted.",
            file=sys.stderr,
        )

        return 130

    except Exception as exc:

        print(
            f"FATAL {exc}",
            file=sys.stderr,
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
