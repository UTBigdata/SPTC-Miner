'Helpers for parsing directed OD event identifiers.'

from typing import Tuple

def split_od(od: str) -> Tuple[str, str]:
    """Split OD exactly as produced by code1; preserve Bus_/Subway_ prefixes."""
    parts = od.split(' -> ')
    if len(parts) != 2 or not all(p.strip() for p in parts):
        raise ValueError('Invalid OD string (expected "A -> B"): {!r}'.format(od))
    return parts[0], parts[1]
