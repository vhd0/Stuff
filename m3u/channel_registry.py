"""
Nap channels.yaml va cung cap tra cuu canonical identity (muc 5) +
extra_groups (muc 11 MULTI-GROUP MEMBERSHIP).
"""

import yaml

from . import config
from .normalize import remove_accents, collapse, normalize_tvg_id


class ChannelRegistry:
    def __init__(self, path=config.CHANNELS_YAML):
        self.by_canonical_id = {}
        self.alias_to_canonical = {}
        self._load(path)

    def _load(self, path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except FileNotFoundError:
            data = {}

        for canonical_id, entry in data.items():
            canonical_id = str(canonical_id).strip().lower()
            name = entry.get("name", canonical_id)
            extra_groups = entry.get("extra_groups", []) or []
            aliases = entry.get("aliases", []) or []

            self.by_canonical_id[canonical_id] = {
                "id": canonical_id,
                "name": name,
                "extra_groups": extra_groups,
            }
            # Chinh canonical_id cung la 1 alias hop le.
            for alias in list(aliases) + [canonical_id, name]:
                key = collapse(remove_accents(alias))
                if key:
                    self.alias_to_canonical[key] = canonical_id

    def resolve(self, clean_name, tvg_id="", tvg_name=""):
        """Tra ve (canonical_id, canonical_name, extra_groups) neu khop
        entry da khai bao trong channels.yaml, nguoc lai tra ve
        (None, None, []).

        tvg_id duoc CHUAN HOA (normalize_tvg_id) truoc khi so khop, giong
        het cach identity_key() lam - dam bao 1 kenh khop channels.yaml
        theo dung 1 cach nhat quan bat ke nguon dat tvg-id kieu gi (vd
        "antv" khop ca "antvhd" lan "antv.vn@hd")."""
        candidates = (normalize_tvg_id(tvg_id), tvg_name, clean_name)
        for candidate in candidates:
            key = collapse(remove_accents(candidate))
            if key and key in self.alias_to_canonical:
                cid = self.alias_to_canonical[key]
                entry = self.by_canonical_id[cid]
                return entry["id"], entry["name"], entry["extra_groups"]
        return None, None, []

    def is_special_whitelisted(self, canonical_id):
        return canonical_id in config.SPECIAL_GROUP_WHITELIST
