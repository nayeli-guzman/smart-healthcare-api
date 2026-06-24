import re
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sanitize_key(name: str) -> str:
    """
    Converts CSV column names into Glue/Athena-friendly snake_case names.
    Example:
    'Heart Rate (bpm)' -> 'heart_rate_bpm'
    """
    if name is None:
        name = "unknown_column"

    clean = name.strip().lower()
    clean = re.sub(r"[^a-z0-9_]+", "_", clean)
    clean = re.sub(r"_+", "_", clean)
    clean = clean.strip("_")

    if not clean:
        clean = "column"

    if clean[0].isdigit():
        clean = f"col_{clean}"

    return clean


def normalize_value(value: Any) -> Any:
    """
    Converts CSV string values into JSON-friendly values:
    - empty values -> None
    - integers -> int
    - decimals -> float
    - true/false -> bool
    """
    if value is None:
        return None

    if not isinstance(value, str):
        return value

    value = value.strip()

    if value.lower() in {"", "null", "none", "nan", "na", "n/a"}:
        return None

    if value.lower() == "true":
        return True

    if value.lower() == "false":
        return False

    if re.fullmatch(r"-?\d+", value):
        try:
            return int(value)
        except ValueError:
            return value

    if re.fullmatch(r"-?\d+\.\d+", value):
        try:
            return float(value)
        except ValueError:
            return value

    return value