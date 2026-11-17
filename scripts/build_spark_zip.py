#!/usr/bin/env python3
"""Create a --py-files bundle from the current source tree (not a stale binary)."""
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

root = Path(__file__).resolve().parents[1]
src = root / 'src'
out = root / 'dist' / 'sptc_miner_lib.zip'
out.parent.mkdir(exist_ok=True)
with ZipFile(out, 'w', compression=ZIP_DEFLATED) as archive:
    for p in sorted((src / 'sptc_miner').rglob('*.py')):
        archive.write(p, p.relative_to(src).as_posix())
print('Created:', out)
