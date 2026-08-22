#!/usr/bin/env python3
"""Build master_prod.sqlite: a copy of master.sqlite with import-only
columns/indexes stripped that no frontend view ever reads, plus any
local-only/secret rows removed. Run after master.sqlite changes, before
regenerating the .gz the SPA fetches.

The output is verified before it is declared good: this file is downloaded by
every visitor, and a truncated or corrupt copy surfaces in the browser only as
a cryptic sql.js "Extra bytes past the end" error.
"""
import shutil
import sqlite3
import sys
from pathlib import Path

SRC  = Path(__file__).parent / 'master.sqlite'
DEST = Path(__file__).parent / 'master_prod.sqlite'

# Tables whose row counts must match between source and output. Anything that is
# deliberately stripped (see _strip) is excluded from this check.
_ROW_COUNT_SKIP = {'settings'}


def _table_names(conn):
    return [
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]


def _row_counts(conn):
    return {t: conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            for t in _table_names(conn)}


def _strip(conn):
    """Remove import-only structures and anything that must not be public."""
    conn.execute('DROP INDEX IF EXISTS listens_ts_src')
    conn.execute('ALTER TABLE listens DROP COLUMN raw_source_id')

    # Enrichment-pipeline columns no frontend view reads.
    for col in ('bio', 'disambiguation', 'formed_year', 'disbanded_year', 'image_source',
                'is_supergroup', 'mb_attempted', 'spotify_followers', 'spotify_popularity'):
        conn.execute(f'ALTER TABLE artists DROP COLUMN {col}')
    for col in ('date_source', 'album_art_source', 'stat_tracks_heard', 'upc',
                'album_art_height', 'album_art_width', 'album_art_thumb_height', 'album_art_thumb_width',
                'spotify_popularity'):
        conn.execute(f'ALTER TABLE releases DROP COLUMN {col}')
    conn.execute('DROP INDEX IF EXISTS idx_tracks_canonical')
    for col in ('musical_key', 'beatport_genre', 'beatport_sub_genre', 'is_explicit',
                'track_variant_type', 'canonical_track_id', 'spotify_popularity'):
        conn.execute(f'ALTER TABLE tracks DROP COLUMN {col}')
    conn.execute('ALTER TABLE collection_items DROP COLUMN discogs_genres')
    conn.execute('ALTER TABLE listens DROP COLUMN skipped')

    # The admin editor is local-only; its PBKDF2 salt+hash must not ship inside
    # a file every visitor downloads.
    try:
        conn.execute("DELETE FROM settings WHERE key LIKE '%pin%'")
    except sqlite3.OperationalError:
        pass  # no settings table in this schema version


def _verify(src_counts, dest_conn):
    """Fail loudly rather than shipping a bad database."""
    problems = []

    integrity = dest_conn.execute('PRAGMA integrity_check').fetchone()[0]
    if integrity != 'ok':
        problems.append(f'integrity_check returned {integrity!r}')

    fk = dest_conn.execute('PRAGMA foreign_key_check').fetchall()
    if fk:
        # Reported, not fatal: there is a known backlog of orphaned rows, and
        # this script is not where that gets fixed. Visibility is the point.
        print(f'WARNING: {len(fk)} foreign key violations in output', file=sys.stderr)

    dest_counts = _row_counts(dest_conn)
    if set(src_counts) != set(dest_counts):
        missing = set(src_counts) - set(dest_counts)
        extra = set(dest_counts) - set(src_counts)
        problems.append(f'table set differs (missing={missing}, extra={extra})')

    for table, n in src_counts.items():
        if table in _ROW_COUNT_SKIP or table not in dest_counts:
            continue
        if dest_counts[table] != n:
            problems.append(
                f'{table}: {n} rows in source, {dest_counts[table]} in output'
            )

    leaked = dest_conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='settings'"
    ).fetchone()[0]
    if leaked:
        pins = dest_conn.execute(
            "SELECT COUNT(*) FROM settings WHERE key LIKE '%pin%'"
        ).fetchone()[0]
        if pins:
            problems.append(f'{pins} admin PIN row(s) still present')

    return problems


def main():
    if not SRC.exists():
        print(f'error: {SRC} does not exist', file=sys.stderr)
        return 1

    # shutil.copyfile copies only the main DB file, so any content still sitting
    # in the WAL would be silently dropped. Checkpoint first.
    src_conn = sqlite3.connect(SRC)
    try:
        src_conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    except sqlite3.OperationalError as e:
        print(f'warning: could not checkpoint WAL: {e}', file=sys.stderr)
    src_conn.close()

    for p in (DEST, DEST.with_name(DEST.name + '-wal'), DEST.with_name(DEST.name + '-shm')):
        p.unlink(missing_ok=True)
    shutil.copyfile(SRC, DEST)

    # Row counts come from DEST itself, before stripping — a byte-for-byte
    # copy of SRC at the moment shutil.copyfile ran. Querying SRC again here
    # would race a concurrent writer: it could commit between the copy and
    # that second read, making the "source" count reflect data newer than
    # what was actually copied and never comparable to DEST in the first place.
    dest_conn = sqlite3.connect(DEST)
    try:
        src_counts = _row_counts(dest_conn)
        _strip(dest_conn)
        dest_conn.commit()
        dest_conn.execute('VACUUM')

        problems = _verify(src_counts, dest_conn)
    finally:
        dest_conn.close()

    if problems:
        print('error: refusing to publish master_prod.sqlite:', file=sys.stderr)
        for p in problems:
            print(f'  - {p}', file=sys.stderr)
        DEST.unlink(missing_ok=True)
        return 1

    src_size  = SRC.stat().st_size
    dest_size = DEST.stat().st_size
    pct = 100 * (1 - dest_size / src_size)
    print(f'master.sqlite      {src_size:,} bytes')
    print(f'master_prod.sqlite {dest_size:,} bytes  ({pct:.1f}% smaller)')
    print('verified: integrity_check ok, row counts match, no secrets')
    return 0


if __name__ == '__main__':
    sys.exit(main())
