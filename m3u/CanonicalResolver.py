class CanonicalResolver:

    def __init__(self, mapping):
        self.mapping = mapping

        self.exact = {}
        self.compact = {}

        self._build_index()

    def _build_index(self):

        for canonical_id, data in self.mapping.items():

            aliases = set()

            aliases.add(canonical_id)
            aliases.add(data["name"])

            for alias in data.get("aliases", []):
                aliases.add(alias)

            for alias in aliases:

                n = normalize_text(alias)

                if not n:
                    continue

                self.exact[n] = canonical_id

                c = compact(alias)

                if c:
                    self.compact[c] = canonical_id

    def resolve(self, tvg_id="", tvg_name="", title=""):

        candidates = [
            tvg_id,
            tvg_name,
            title,
        ]

        # --------------------------------
        # PASS 1: exact normalized
        # --------------------------------

        for value in candidates:

            n = normalize_text(value)

            if n in self.exact:
                return self.exact[n], 100

        # --------------------------------
        # PASS 2: compact
        # --------------------------------

        for value in candidates:

            c = compact(value)

            if c in self.compact:
                return self.compact[c], 95

        # --------------------------------
        # PASS 3: controlled family pattern
        # --------------------------------

        for value in candidates:

            c = compact(value)

            match = re.fullmatch(
                r"(vtv|htv|sctv)(\d{1,2})(?:hd)?",
                c
            )

            if match:

                candidate = (
                    f"{match.group(1)}"
                    f"{match.group(2)}"
                )

                if candidate in self.mapping:
                    return candidate, 90

        return None, 0
