import re

PII_PATTERNS = {
    "email": re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I),
    "phone": re.compile(r"(?<!\d)(?:\+?966|0)?5\d{8}(?!\d)"),
    "card": re.compile(r"\b(?:\d[ -]*?){13,19}\b"),
}


def mask_pii(text: str) -> tuple[str, list[str]]:
    """Mask common PII before Silver/Vector storage and return detected types."""
    masked, detected = text, []
    for label, pattern in PII_PATTERNS.items():
        if pattern.search(masked):
            detected.append(label)
            masked = pattern.sub(f"[REDACTED_{label.upper()}]", masked)
    return masked, detected

