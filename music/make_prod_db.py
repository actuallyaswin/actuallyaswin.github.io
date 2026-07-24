#!/usr/bin/env python3
"""Build master_prod.sqlite: a copy of master.sqlite with import-only
columns/indexes stripped that no frontend view ever reads. Run after
master.sqlite changes, before regenerating the .gz the SPA fetches.
"""
import shutil
import sqlite3
import sys
from pathlib import Path

SRC  = Path(__file__).parent / 'master.sqlite'
DEST = Path(__file__).parent / 'master_prod.sqlite'


def main():
    for p in (DEST, DEST.with_name(DEST.name + '-wal'), DEST.with_name(DEST.name + '-shm')):
        p.unlink(missing_ok=True)
    shutil.copyfile(SRC, DEST)

    conn = sqlite3.connect(DEST)
    conn.execute('DROP INDEX IF EXISTS listens_ts_src')
    conn.execute('ALTER TABLE listens DROP COLUMN raw_source_id')
    conn.commit()
    conn.execute('VACUUM')
    conn.close()

    src_size  = SRC.stat().st_size
    dest_size = DEST.stat().st_size
    pct = 100 * (1 - dest_size / src_size)
    print(f'master.sqlite      {src_size:,} bytes')
    print(f'master_prod.sqlite {dest_size:,} bytes  ({pct:.1f}% smaller)')


if __name__ == '__main__':
    sys.exit(main())
