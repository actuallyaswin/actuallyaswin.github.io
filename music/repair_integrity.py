#!/usr/bin/env python3
"""Repair referential integrity in master.sqlite and add missing indexes.

Why this exists: `open_db` sets `PRAGMA foreign_keys=ON`, but two code paths
defeated it — `cmd_artist_merge` turned foreign keys off around a bulk
delete, and several delete paths removed parents whose children had no
`ON DELETE CASCADE`. The result was 259 orphaned rows that shipped to
production, because `PRAGMA integrity_check` only validates page structure, not
references.

Orphan handling is deliberately asymmetric:

  * Pure join/metadata rows for a deleted parent are DELETED — they carry no
    information once the parent is gone.
  * Rows that are themselves irreplaceable data (a `listens` row is a real
    historical event) have the dangling reference SET NULL instead, so no
    listening history is ever destroyed.

Idempotent: safe to run repeatedly. Run with --dry-run first.
"""
import argparse
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

DB = Path(__file__).parent / 'master.sqlite'

# (table, fk_column, parent_table, parent_key) → delete orphaned rows.
_DELETE_ORPHANS = [
    ('release_genres',          'release_id', 'releases', 'id'),
    ('release_genres',          'aoty_genre_id', 'genres', 'aoty_id'),
    ('release_artists',         'release_id', 'releases', 'id'),
    ('release_artists',         'artist_id',  'artists',  'id'),
    ('release_variants',        'canonical_id', 'releases', 'id'),
    ('release_variants',        'variant_id',   'releases', 'id'),
    ('release_soundtrack_meta', 'release_id', 'releases', 'id'),
    ('release_service_links',   'release_id', 'releases', 'id'),
    ('track_artists',           'track_id',   'tracks',   'id'),
    ('track_artists',           'artist_id',  'artists',  'id'),
    ('artist_members',          'group_artist_id',  'artists', 'id'),
    ('artist_members',          'member_artist_id', 'artists', 'id'),
]

# (table, fk_column, parent_table, parent_key) → NULL the reference, keep the row.
_NULL_ORPHANS = [
    # A listen is an irreplaceable historical event; never delete one because
    # its track was removed. It reverts to an unmatched listen.
    ('listens',  'track_id',           'tracks',   'id'),
    ('tracks',   'canonical_track_id', 'tracks',   'id'),
    ('tracks',   'release_id',         'releases', 'id'),
    ('releases', 'primary_artist_id',  'artists',  'id'),
]

# SQLite never auto-indexes the child side of a foreign key, so every column
# used for a join, a lookup, or a cascade needs one explicitly. Each of these
# was a confirmed full table scan.
_INDEXES = [
    ('idx_tracks_release_id',          'tracks',           '(release_id)'),
    ('idx_release_genres_genre',       'release_genres',   '(aoty_genre_id)'),
    ('idx_release_variants_variant',   'release_variants', '(variant_id)'),
    ('idx_release_artists_artist',     'release_artists',  '(artist_id)'),
    ('idx_listens_year',               'listens',          '(year)'),
]


def _columns(conn, table):
    return {r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')}


def _table_exists(conn, table):
    return bool(conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone())


def _orphan_count(conn, table, col, parent, pkey):
    return conn.execute(
        f'SELECT COUNT(*) FROM "{table}" c '
        f'WHERE c."{col}" IS NOT NULL '
        f'  AND NOT EXISTS (SELECT 1 FROM "{parent}" p WHERE p."{pkey}" = c."{col}")'
    ).fetchone()[0]


def _applicable(conn, specs):
    for table, col, parent, pkey in specs:
        if not _table_exists(conn, table) or not _table_exists(conn, parent):
            continue
        if col not in _columns(conn, table):
            continue
        yield table, col, parent, pkey


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--dry-run', action='store_true',
                    help='report what would change without writing')
    ap.add_argument('--db', type=Path, default=DB)
    args = ap.parse_args()

    if not args.db.exists():
        print(f'error: {args.db} does not exist', file=sys.stderr)
        return 1

    conn = sqlite3.connect(args.db)
    conn.execute('PRAGMA foreign_keys=OFF')  # we are the thing doing the fixing

    before = len(conn.execute('PRAGMA foreign_key_check').fetchall())
    print(f'foreign_key_check violations before: {before}')

    planned = []
    for table, col, parent, pkey in _applicable(conn, _DELETE_ORPHANS):
        n = _orphan_count(conn, table, col, parent, pkey)
        if n:
            planned.append(('delete', table, col, parent, pkey, n))
    for table, col, parent, pkey in _applicable(conn, _NULL_ORPHANS):
        n = _orphan_count(conn, table, col, parent, pkey)
        if n:
            planned.append(('null', table, col, parent, pkey, n))

    if not planned:
        print('no orphaned rows found')
    for action, table, col, parent, pkey, n in planned:
        verb = 'DELETE' if action == 'delete' else 'SET NULL'
        print(f'  {verb:8} {n:5} rows  {table}.{col} -> {parent}.{pkey}')

    if args.dry_run:
        missing = [name for name, t, c in _INDEXES
                   if _table_exists(conn, t)
                   and not conn.execute(
                       "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
                       (name,)).fetchone()]
        print(f'indexes to create: {missing or "none"}')
        conn.close()
        return 0

    if planned:
        stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        backup = args.db.with_name(f'{args.db.name}.bak-{stamp}')
        conn.close()
        shutil.copyfile(args.db, backup)
        print(f'backup written to {backup.name}')
        conn = sqlite3.connect(args.db)
        conn.execute('PRAGMA foreign_keys=OFF')

    try:
        conn.execute('BEGIN')
        for action, table, col, parent, pkey, _ in planned:
            cond = (f'"{col}" IS NOT NULL AND NOT EXISTS '
                    f'(SELECT 1 FROM "{parent}" p WHERE p."{pkey}" = "{table}"."{col}")')
            if action == 'delete':
                conn.execute(f'DELETE FROM "{table}" WHERE {cond}')
            else:
                conn.execute(f'UPDATE "{table}" SET "{col}" = NULL WHERE {cond}')

        for name, table, cols in _INDEXES:
            if _table_exists(conn, table):
                conn.execute(f'CREATE INDEX IF NOT EXISTS {name} ON "{table}" {cols}')
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    after = len(conn.execute('PRAGMA foreign_key_check').fetchall())
    integrity = conn.execute('PRAGMA integrity_check').fetchone()[0]
    conn.close()

    print(f'foreign_key_check violations after:  {after}')
    print(f'integrity_check: {integrity}')

    if after or integrity != 'ok':
        print('error: database is still not clean', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
