import re
import unicodedata


def normalize_text(value: str) -> str:
    if not value:
        return ""

    value = unicodedata.normalize("NFKC", value)

    value = value.lower().strip()

    # bỏ accent
    value = unicodedata.normalize("NFD", value)
    value = "".join(
        c for c in value
        if unicodedata.category(c) != "Mn"
    )

    # @, _, -, /, ., : → space
    value = re.sub(r"[@_./:-]+", " ", value)

    # gom whitespace
    value = re.sub(r"\s+", " ", value)

    return value.strip()


def compact(value: str) -> str:
    return re.sub(
        r"[^a-z0-9]",
        "",
        normalize_text(value)
    )
