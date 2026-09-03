from __future__ import annotations

import json
import os
import re
import sys
import time
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests
import yaml
from rapidfuzz import fuzz, process


# ============================================================
# CONFIG
# ============================================================

ROOT = Path(__file__).resolve().parents[1]

CONFIG_FILE = ROOT / "m3u" / "channel_aliases.yml"
CACHE_DIR = ROOT / ".cache" / "iptv"
OUTPUT_FILE = ROOT / "listtivi.m3u"
FALLBACK_FILE = ROOT / "m3u" / "listtivi.m3u"

META_CACHE_TTL = int(os.getenv("META_CACHE_TTL", "86400"))

MIN_CHANNELS = int(os.getenv("MIN_CHANNELS", "100"))
MIN_RATIO = float(os.getenv("MIN_RATIO", "0.70"))

TIMEOUT = (
    int(os.getenv("CONNECT_TIMEOUT", "15")),
    int(os.getenv("READ_TIMEOUT", "45")),
)

DALVIK_UA = (
    "Dalvik/2.1.0 "
    "(Linux; U; Android 13; SM-S918B Build/TP1A.220624.014)"
)

HEADERS = {
    "User-Agent": DALVIK_UA,
    "Accept": "*/*",
    "Connection": "keep-alive",
}

IPTV_ORG_CHANNELS = (
    "https://raw.githubusercontent.com/iptv-org/iptv/"
    "master/data/channels.json"
)

IPTV_ORG_LOGOS = (
    "https://raw.githubusercontent.com/iptv-org/iptv/"
    "master/data/logos.json"
)


# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass
class Entry:
    source: str
    extinf: str
    url: str
    metadata: list[str] = field(default_factory=list)

    tvg_id: str = ""
    tvg_name: str = ""
    tvg_logo: str = ""
    group: str = ""
    name: str = ""

    canonical_id: str = ""
    canonical_name: str = ""
    canonical_group: str = ""

    alias_score: int = 0
    mapping_method: str = "unknown"


# ============================================================
# HTTP
# ============================================================

session = requests.Session()
session.headers.update(HEADERS)


def fetch_text(url: str) -> str:
    last_error = None

    for attempt in range(3):
        try:
            response = session.get(url, timeout=TIMEOUT)
            response.raise_for_status()
            response.encoding = response.encoding or "utf-8"
            return response.text
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(2 ** attempt)

    raise RuntimeError(f"Cannot fetch {url}: {last_error}")


def cached_json(url: str, filename: str):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / filename

    if path.exists():
        age = time.time() - path.stat().st_mtime
        if age < META_CACHE_TTL:
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                pass

    data = fetch_text(url)
    parsed = json.loads(data)

    path.write_text(
        json.dumps(parsed, ensure_ascii=False),
        encoding="utf-8",
    )

    return parsed


# ============================================================
# NORMALIZATION
# ============================================================

def remove_accents(value: str) -> str:
    value = unicodedata.normalize("NFD", value)
    return "".join(
        ch for ch in value
        if unicodedata.category(ch) != "Mn"
    )


def normalize_text(value: str) -> str:
    if not value:
        return ""

    value = remove_accents(value.lower())

    value = value.replace("&", " and ")

    # Common Vietnamese spelling variants.
    value = value.replace("đ", "d")

    # HD / FHD / SD / 4K normally do not identify the channel.
    value = re.sub(
        r"\b(4k|8k|uhd|fhd|fullhd|hd|sd|1080p|720p|576p|480p)\b",
        " ",
        value,
        flags=re.I,
    )

    # Common separators.
    value = re.sub(r"[_./|:+\-]+", " ", value)

    value = re.sub(r"[^a-z0-9\s]", " ", value)

    value = re.sub(r"\s+", " ", value).strip()

    return value


def compact(value: str) -> str:
    return normalize_text(value).replace(" ", "")


def normalize_channel_name(value: str) -> str:
    value = normalize_text(value)

    replacements = {
        "truyen hinh": "",
        "television": "",
        "tv": "tv",
    }

    for old, new in replacements.items():
        value = value.replace(old, new)

    return re.sub(r"\s+", " ", value).strip()


# ============================================================
# M3U PARSER
# ============================================================

def extract_m3u_payload(text: str) -> str:
    pos = text.find("#EXTM3U")

    if pos >= 0:
        return text[pos:]

    pos = text.find("#EXTINF")

    if pos >= 0:
        return "#EXTM3U\n" + text[pos:]

    raise ValueError("Source does not contain M3U data")


def parse_extinf(line: str):
    attrs = {}

    for match in re.finditer(
        r'([\w-]+)="([^"]*)"',
        line,
    ):
        attrs[match.group(1).lower()] = match.group(2)

    if "," in line:
        name = line.split(",", 1)[1].strip()
    else:
        name = attrs.get("tvg-name", "").strip()

    group = attrs.get("group-title", "").strip()

    return attrs, name, group


def parse_m3u(text: str, source: str) -> tuple[str, list[Entry]]:
    text = extract_m3u_payload(text)

    lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip()
    ]

    epg_urls = []

    if lines:
        header = lines[0]

        match = re.search(
            r'url-tvg="([^"]+)"',
            header,
            flags=re.I,
        )

        if match:
            epg_urls.extend(
                x.strip()
                for x in match.group(1).split(",")
                if x.strip()
            )

    entries: list[Entry] = []

    i = 0

    while i < len(lines):
        line = lines[i]

        if not line.startswith("#EXTINF"):
            i += 1
            continue

        attrs, name, group = parse_extinf(line)

        metadata = []

        j = i + 1
        url = ""

        while j < len(lines):
            current = lines[j]

            if current.startswith("#EXTINF"):
                break

            if current.startswith("#"):
                # Preserve important stream/player metadata.
                if (
                    current.startswith("#KODIPROP")
                    or current.startswith("#EXTVLCOPT")
                    or current.startswith("#EXTHTTP")
                    or current.startswith("#EXT-X-")
                ):
                    metadata.append(current)

                j += 1
                continue

            url = current
            break

        if url:
            entries.append(
                Entry(
                    source=source,
                    extinf=line,
                    url=url,
                    metadata=metadata,
                    tvg_id=attrs.get("tvg-id", "").strip(),
                    tvg_name=attrs.get("tvg-name", "").strip(),
                    tvg_logo=attrs.get("tvg-logo", "").strip(),
                    group=group,
                    name=name,
                )
            )

        i = max(j, i + 1)

    return ",".join(dict.fromkeys(epg_urls)), entries


# ============================================================
# SOURCE CONFIG
# ============================================================

DEFAULT_SOURCES = {
    "vmttv": (
        "https://raw.githubusercontent.com/vuminhthanh12/"
        "vuminhthanh12/refs/heads/main/vmttv"
    ),
    "vietanhtv": "https://tv.vietanhtv.top/sex/",
    "dltivi": (
        "https://raw.githubusercontent.com/DinhLap96/ListTivi/"
        "refs/heads/main/ListTiVi/dltivi_v2.ndl"
    ),
    "iptv-org": (
        "https://raw.githubusercontent.com/iptv-org/iptv/"
        "refs/heads/master/streams/vn.m3u"
    ),
    "easport": "https://livesport.s.gy/easport",
}


# ============================================================
# IPTV-ORG DATABASE
# ============================================================

class IptvDatabase:
    def __init__(self, channels, logos):
        self.channels = channels
        self.logos = logos

        self.by_id = {}
        self.name_index = {}
        self.alias_index = {}

        self.logo_by_channel = {}

        self._build()

    def _build(self):
        for item in self.channels:
            channel_id = str(item.get("id", "")).strip()

            if not channel_id:
                continue

            self.by_id[channel_id] = item

            names = []

            name = item.get("name")
            if name:
                names.append(name)

            names.extend(item.get("alt_names") or [])

            for value in names:
                key = compact(str(value))

                if key:
                    self.name_index.setdefault(
                        key,
                        channel_id,
                    )

        for logo in self.logos:
            channel_id = str(
                logo.get("channel", "")
            ).strip()

            url = str(
                logo.get("url", "")
            ).strip()

            if not channel_id or not url:
                continue

            # Prefer in_use logo.
            if (
                channel_id not in self.logo_by_channel
                or logo.get("in_use") is True
            ):
                self.logo_by_channel[channel_id] = url


# ============================================================
# YAML MAPPING
# ============================================================

def load_config():
    if not CONFIG_FILE.exists():
        return {
            "source_priority": {
                "vmttv": 10,
                "vietanhtv": 20,
                "dltivi": 30,
                "iptv-org": 40,
                "easport": 50,
            },
            "groups": {},
            "channels": {},
            "exclude": {},
        }

    return yaml.safe_load(
        CONFIG_FILE.read_text(encoding="utf-8")
    ) or {}


# ============================================================
# EXCLUSIONS
# ============================================================

def is_excluded(entry: Entry, config: dict) -> bool:
    exclude = config.get("exclude", {})

    source_groups = exclude.get("groups", {})
    source_channels = exclude.get("channels", {})

    forbidden_groups = source_groups.get(
        entry.source,
        [],
    )

    forbidden_channels = source_channels.get(
        entry.source,
        [],
    )

    normalized_group = compact(entry.group)
    normalized_name = compact(entry.name)
    normalized_id = compact(entry.tvg_id)

    for item in forbidden_groups:
        if normalized_group == compact(item):
            return True

    for item in forbidden_channels:
        item_key = compact(item)

        if (
            normalized_name == item_key
            or normalized_id == item_key
        ):
            return True

    return False


# ============================================================
# GROUP MAPPING
# ============================================================

def map_group(group: str, config: dict) -> str:
    if not group:
        return "Khác"

    groups = config.get("groups", {})

    key = compact(group)

    for canonical, definition in groups.items():
        aliases = definition.get("aliases", [])
        aliases = list(aliases) + [canonical]

        for alias in aliases:
            if key == compact(alias):
                return canonical

    # Conservative fuzzy group mapping.
    candidates = {}

    for canonical, definition in groups.items():
        for alias in definition.get("aliases", []):
            candidates[normalize_text(alias)] = canonical

    if candidates:
        result = process.extractOne(
            normalize_text(group),
            candidates.keys(),
            scorer=fuzz.ratio,
        )

        if result:
            matched, score, _ = result

            if score >= 94:
                return candidates[matched]

    return group.strip()


# ============================================================
# CHANNEL MAPPING
# ============================================================

def build_yaml_indexes(config: dict):
    channels = config.get("channels", {})

    by_id = {}
    aliases = {}

    for canonical_id, definition in channels.items():
        canonical_id = str(canonical_id)

        by_id[canonical_id] = definition

        names = [
            canonical_id,
            definition.get("name", ""),
        ]

        names.extend(
            definition.get("aliases", [])
        )

        names.extend(
            definition.get("ids", [])
        )

        for name in names:
            if not name:
                continue

            aliases[compact(name)] = canonical_id

    return by_id, aliases


def iptv_org_channel_name(db: IptvDatabase, channel_id: str):
    item = db.by_id.get(channel_id)

    if not item:
        return ""

    return str(item.get("name", "")).strip()


def map_channel(
    entry: Entry,
    config: dict,
    db: IptvDatabase,
):
    yaml_channels, yaml_aliases = build_yaml_indexes(config)

    # --------------------------------------------------------
    # 1. Explicit YAML tvg-id / alias mapping
    # --------------------------------------------------------

    for candidate in [
        entry.tvg_id,
        entry.tvg_name,
        entry.name,
    ]:
        key = compact(candidate)

        if key and key in yaml_aliases:
            canonical_id = yaml_aliases[key]
            definition = yaml_channels[canonical_id]

            entry.canonical_id = canonical_id
            entry.canonical_name = (
                definition.get("name")
                or canonical_id
            )
            entry.mapping_method = "yaml-exact"
            entry.alias_score = 100

            return

    # --------------------------------------------------------
    # 2. Exact iptv-org channel ID
    # --------------------------------------------------------

    if entry.tvg_id in db.by_id:
        item = db.by_id[entry.tvg_id]

        entry.canonical_id = entry.tvg_id
        entry.canonical_name = str(
            item.get("name")
            or entry.name
        )

        entry.mapping_method = "iptv-org-id"
        entry.alias_score = 100

        return

    # --------------------------------------------------------
    # 3. Exact normalized name / alt_name
    # --------------------------------------------------------

    candidates = [
        entry.tvg_name,
        entry.name,
    ]

    for candidate in candidates:
        key = compact(candidate)

        if key in db.name_index:
            canonical_id = db.name_index[key]
            item = db.by_id.get(canonical_id, {})

            entry.canonical_id = canonical_id
            entry.canonical_name = str(
                item.get("name")
                or candidate
            )

            entry.mapping_method = "iptv-org-name"
            entry.alias_score = 95

            return

    # --------------------------------------------------------
    # 4. Fuzzy only for unknown channels
    # --------------------------------------------------------

    query = normalize_channel_name(
        entry.tvg_name or entry.name
    )

    if query:
        result = process.extractOne(
            query,
            db.name_index.keys(),
            scorer=fuzz.ratio,
        )

        if result:
            matched, score, _ = result

            # High threshold intentionally.
            if score >= 95:
                canonical_id = db.name_index[matched]
                item = db.by_id.get(canonical_id, {})

                entry.canonical_id = canonical_id
                entry.canonical_name = str(
                    item.get("name")
                    or entry.name
                )

                entry.mapping_method = "iptv-org-fuzzy"
                entry.alias_score = int(score)

                return

    # --------------------------------------------------------
    # 5. Unknown → deterministic local canonical ID
    # --------------------------------------------------------

    fallback = compact(
        entry.tvg_name
        or entry.name
    )

    entry.canonical_id = fallback or "unknown"
    entry.canonical_name = (
        entry.tvg_name
        or entry.name
        or fallback
        or "Unknown"
    )

    entry.mapping_method = "local"
    entry.alias_score = 50


# ============================================================
# LOGO
# ============================================================

def resolve_logo(
    entry: Entry,
    db: IptvDatabase,
) -> str:
    # Raw source always wins.
    if entry.tvg_logo:
        return entry.tvg_logo

    if entry.canonical_id in db.logo_by_channel:
        return db.logo_by_channel[
            entry.canonical_id
        ]

    return ""


# ============================================================
# URL VALIDATION — NO HEALTH CHECK
# ============================================================

def valid_stream_url(url: str) -> bool:
    try:
        parsed = urlparse(url)

        return (
            parsed.scheme in {"http", "https"}
            and bool(parsed.hostname)
        )

    except Exception:
        return False


# ============================================================
# SOURCE SCORE
# ============================================================

def source_score(
    entry: Entry,
    config: dict,
) -> int:
    priority = config.get(
        "source_priority",
        {},
    )

    # Higher source priority = higher score.
    score = 1000 - int(
        priority.get(entry.source, 999)
    )

    score += entry.alias_score

    # Prefer actual tvg-id over name-only match.
    if entry.tvg_id:
        score += 20

    # Prefer source entries with logos.
    if entry.tvg_logo:
        score += 5

    # Avoid obvious bad labels.
    lower = entry.name.lower()

    for bad in config.get(
        "avoid_labels",
        [],
    ):
        if bad.lower() in lower:
            score -= 500

    # Prefer HLS when candidates come from same source.
    if ".m3u8" in entry.url.lower():
        score += 3

    return score


# ============================================================
# DEDUPLICATION
# ============================================================

def deduplicate(
    entries: list[Entry],
    config: dict,
):
    buckets: dict[str, list[Entry]] = {}

    for entry in entries:
        key = entry.canonical_id

        if not key:
            key = compact(entry.name)

        buckets.setdefault(key, []).append(entry)

    winners = []

    for canonical_id, candidates in buckets.items():
        candidates = [
            x for x in candidates
            if valid_stream_url(x.url)
        ]

        if not candidates:
            continue

        # Remove duplicate URL candidates.
        unique = {}

        for entry in candidates:
            url_key = entry.url.strip()

            if url_key not in unique:
                unique[url_key] = entry

        candidates = list(unique.values())

        winner = max(
            candidates,
            key=lambda x: source_score(
                x,
                config,
            ),
        )

        winners.append(winner)

    return winners


# ============================================================
# EXTINF REBUILD
# ============================================================

def escape_attr(value: str) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace('"', "&quot;")
    )


def rebuild_extinf(
    entry: Entry,
    logo: str,
) -> str:
    attrs, _, _ = parse_extinf(entry.extinf)

    attrs["tvg-id"] = entry.canonical_id
    attrs["tvg-name"] = entry.canonical_name

    if logo:
        attrs["tvg-logo"] = logo
    else:
        attrs.pop("tvg-logo", None)

    attrs["group-title"] = entry.canonical_group

    ordered = [
        "tvg-id",
        "tvg-name",
        "tvg-logo",
        "group-title",
    ]

    parts = []

    for key in ordered:
        if key in attrs and attrs[key]:
            parts.append(
                f'{key}="{escape_attr(attrs[key])}"'
            )

    display_name = entry.canonical_name

    return (
        "#EXTINF:-1 "
        + " ".join(parts)
        + ","
        + display_name
    )


# ============================================================
# OUTPUT
# ============================================================

def write_playlist(
    path: Path,
    epg_urls: list[str],
    entries: list[Entry],
    db: IptvDatabase,
):
    path.parent.mkdir(parents=True, exist_ok=True)

    header = "#EXTM3U"

    if epg_urls:
        header += (
            ' url-tvg="'
            + ",".join(
                dict.fromkeys(epg_urls)
            )
            + '"'
        )

    lines = [header]

    for entry in entries:
        logo = resolve_logo(entry, db)

        lines.append(
            rebuild_extinf(
                entry,
                logo,
            )
        )

        lines.extend(entry.metadata)

        lines.append(entry.url)

    path.write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


# ============================================================
# SAFETY
# ============================================================

def count_channels(path: Path) -> int:
    if not path.exists():
        return 0

    return sum(
        1
        for line in path.read_text(
            encoding="utf-8",
            errors="ignore",
        ).splitlines()
        if line.startswith("#EXTINF")
    )


def validate_result(path: Path):
    count = count_channels(path)

    print(f"Generated channels: {count}")

    if count < MIN_CHANNELS:
        raise RuntimeError(
            f"Playlist too small: {count} < {MIN_CHANNELS}"
        )

    old_count = count_channels(FALLBACK_FILE)

    if old_count > 0:
        ratio = count / old_count

        print(
            f"Previous: {old_count}, "
            f"ratio: {ratio:.2%}"
        )

        if ratio < MIN_RATIO:
            raise RuntimeError(
                "Generated playlist dropped too much: "
                f"{ratio:.2%} < {MIN_RATIO:.2%}"
            )


# ============================================================
# MAIN
# ============================================================

def main():
    print("=== IPTV M3U Optimizer V3 ===")
    print("Health checks: DISABLED")
    print("Selection: source priority + mapping")
    print()

    config = load_config()

    print("Loading iptv-org metadata...")

    channels = cached_json(
        IPTV_ORG_CHANNELS,
        "channels.json",
    )

    logos = cached_json(
        IPTV_ORG_LOGOS,
        "logos.json",
    )

    db = IptvDatabase(
        channels,
        logos,
    )

    sources = config.get(
        "sources",
        DEFAULT_SOURCES,
    )

    all_entries = []
    epg_urls = []

    for source, url in sources.items():
        print(f"\nFetching: {source}")
        print(url)

        try:
            text = fetch_text(url)

            epg, entries = parse_m3u(
                text,
                source,
            )

            if epg:
                epg_urls.extend(
                    x.strip()
                    for x in epg.split(",")
                    if x.strip()
                )

            print(
                f"  Parsed: {len(entries)}"
            )

            before = len(entries)

            entries = [
                x
                for x in entries
                if not is_excluded(
                    x,
                    config,
                )
            ]

            print(
                f"  After exclusion: "
                f"{len(entries)} "
                f"(removed {before - len(entries)})"
            )

            for entry in entries:
                entry.canonical_group = map_group(
                    entry.group,
                    config,
                )

                map_channel(
                    entry,
                    config,
                    db,
                )

            all_entries.extend(entries)

        except Exception as exc:
            print(
                f"  WARNING: {source} failed: {exc}"
            )

    if not all_entries:
        raise RuntimeError(
            "No streams were parsed from any source."
        )

    print()
    print(
        f"Total candidates: {len(all_entries)}"
    )

    winners = deduplicate(
        all_entries,
        config,
    )

    # Sort by configured OTT-style group order.
    group_order = config.get(
        "group_order",
        [],
    )

    group_index = {
        group: index
        for index, group
        in enumerate(group_order)
    }

    winners.sort(
        key=lambda x: (
            group_index.get(
                x.canonical_group,
                999,
            ),
            normalize_text(
                x.canonical_name
            ),
        )
    )

    print(
        f"Final unique channels: "
        f"{len(winners)}"
    )

    epg_urls = list(
        dict.fromkeys(epg_urls)
    )

    write_playlist(
        OUTPUT_FILE,
        epg_urls,
        winners,
        db,
    )

    validate_result(
        OUTPUT_FILE
    )

    # Small summary for Actions log only.
    source_count = {}

    for entry in winners:
        source_count.setdefault(
            entry.source,
            0,
        )
        source_count[entry.source] += 1

    print()
    print("Selected streams by source:")

    for source, count in sorted(
        source_count.items(),
        key=lambda x: x[0],
    ):
        print(
            f"  {source}: {count}"
        )

    print()
    print(
        f"Output: {OUTPUT_FILE}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(
            f"\nERROR: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
