#!/usr/bin/env python3
"""
sync — Listening History Sync

Migrates scrobble history from parquet / old sqlite / Last.fm API
into master.sqlite, then interactively resolves unmatched listens
album-by-album.

Usage:
  sync fetch [--parquet FILE] [--sqlite FILE] [--no-live] [--full] [--since N]
             [--spotify [DIR]]
  sync match [--limit N] [--skipped] [--recent]
  sync status
"""

import argparse
import base64
import concurrent.futures
import json
import math
import os
import re
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from rich.console import Console
from rich.rule import Rule

from mdb_strings import (
    is_valid_mbid as _is_valid_mbid,
    normalize_text as _norm,
    ascii_key as _ascii_key,
    parse_track_title as _parse_track_title,
    detect_variant_type as _detect_variant_type,
    _PRIMARY_TYPES, _SECONDARY_TYPES, _EDITION_TYPES,
    extract_mbid as _extract_mbid,
    extract_spotify_id as _extract_spotify_id,
)
from mdb_apis import SpotifyRelease
from mdb_cli  import render_diff
from mdb_ops  import bulk_rematch, bulk_rematch_by_name, db_search_releases as _db_search_releases

console = Console(width=80, highlight=False)

_DIR       = os.path.dirname(os.path.abspath(__file__))
DB_PATH    = os.path.join(_DIR, 'master.sqlite')
OLD_SQLITE = os.path.join(_DIR, 'listening_history.sqlite')
MDB        = os.path.join(_DIR, 'mdb.py')
PYTHON     = sys.executable

_SP_HISTORY_DEFAULT = os.path.join(
    os.path.expanduser('~'), 'Downloads', 'Spotify Extended Streaming History'
)

LASTFM_API = 'https://ws.audioscrobbler.com/2.0/'
SP_TOKEN   = 'https://accounts.spotify.com/api/token'
SP_SEARCH  = 'https://api.spotify.com/v1/search'


def _prompt_choice(label, options, current=None, allow_hide=False):
    """Mirror of mdb.py _prompt_choice. Returns (value, quit, hide)."""
    default = current if current and current in options else options[0]
    console.print(f'\n  [bold]{label}[/bold]')
    cols, col_w = 3, 18
    for row_start in range(0, len(options), cols):
        row = options[row_start:row_start + cols]
        parts = []
        for j, opt in enumerate(row, row_start):
            marker = '*' if opt == default else ' '
            parts.append(f'[dim]{marker}[{j}][/dim] {opt:<{col_w}}')
        console.print('  ' + '  '.join(parts))
    hide_hint = '  \\[h]ide' if allow_hide else ''
    console.print(f'  [dim]Enter=keep ({default}){hide_hint}  q=quit:[/dim] ', end='')
    raw = input().strip().lower()
    if raw == 'q':
        return None, True, False
    if allow_hide and raw == 'h':
        return None, False, True
    if raw == '' or not raw.isdigit():
        return default, False, False
    idx = int(raw)
    if 0 <= idx < len(options):
        return options[idx], False, False
    return default, False, False


def _write_variant_links(conn, canonical, type_updates, edition_links, hide_ids):
    """Write accumulated type/variant/hide changes for a variant group."""
    for rid in hide_ids:
        conn.execute('UPDATE releases SET hidden = 1 WHERE id = ?', (rid,))
    for rid, (ptype, stype) in type_updates.items():
        conn.execute(
            'UPDATE releases SET type = ?, type_secondary = ? WHERE id = ?',
            (ptype, stype, rid),
        )
    for variant_id, edition_type, sort_order in edition_links:
        conn.execute('''
            INSERT OR REPLACE INTO release_variants
                (canonical_id, variant_id, variant_type, sort_order)
            VALUES (?, ?, ?, ?)
        ''', [canonical['id'], variant_id, edition_type, sort_order])
    conn.commit()


# ── Helpers ───────────────────────────────────────────────────────────────────


def _source_id(track_mbid, artist_name, track_name) -> str:
    """
    Stable identifier for deduplication and later re-matching.
    Prefer the track MBID (reliable); fall back to normalised artist|||track.
    """
    if _is_valid_mbid(track_mbid):
        return track_mbid
    return f"{_norm(artist_name)}|||{_norm(track_name)}"


def _load_env():
    env_path = os.path.join(_DIR, '.env')
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, _, v = line.partition('=')
                    os.environ.setdefault(k.strip(), v.strip())


# ── Database ──────────────────────────────────────────────────────────────────

def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA foreign_keys=ON')
    _migrate(conn)
    return conn


def _migrate(conn: sqlite3.Connection):
    """Apply any schema additions needed by sync.py."""
    # Unique index on (timestamp, raw_source_id) enables INSERT OR IGNORE dedup.
    # raw_source_id is always non-null (MBID or artist|||track fallback).
    conn.executescript('''
        CREATE UNIQUE INDEX IF NOT EXISTS listens_ts_src
            ON listens(timestamp, raw_source_id);
    ''')
    # Spotify-sourced columns (added later; safe to run every open).
    cols = {row[1] for row in conn.execute('PRAGMA table_info(listens)').fetchall()}
    if 'ms_played' not in cols:
        conn.execute('ALTER TABLE listens ADD COLUMN ms_played INTEGER')
    if 'skipped' not in cols:
        conn.execute('ALTER TABLE listens ADD COLUMN skipped INTEGER DEFAULT 0')
    conn.commit()


def dedup_spotify_lastfm(conn: sqlite3.Connection) -> int:
    """Remove Last.fm listens that duplicate a Spotify listen for the same track.

    Called after bulk_rematch (which assigns track_ids to Last.fm rows). At that
    point some Last.fm rows will share a track_id with a Spotify listen within
    ±120 seconds — the same physical play captured by both sources.

    We keep the Spotify listen (it has ms_played / skipped data) and delete the
    Last.fm one. Only deletes Last.fm rows that have track_id already set;
    unmatched Last.fm rows are left alone.

    Returns the number of deleted rows.
    """
    cur = conn.execute('''
        DELETE FROM listens
        WHERE source = 'lastfm'
          AND track_id IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM listens sp
              WHERE sp.source = 'spotify'
                AND sp.track_id = listens.track_id
                AND sp.timestamp BETWEEN listens.timestamp - 120
                                     AND listens.timestamp + 120
          )
    ''')
    conn.commit()
    return cur.rowcount


# ── Row normalisation ─────────────────────────────────────────────────────────

def _make_row(*, timestamp, year, month, track_name, artist_name, album_name,
              track_mbid=None) -> dict:
    return dict(
        timestamp=int(timestamp),
        year=int(year),
        month=int(month),
        raw_track_name=str(track_name or ''),
        raw_artist_name=str(artist_name or ''),
        raw_album_name=str(album_name or ''),
        raw_source_id=_source_id(track_mbid, artist_name, track_name),
        source='lastfm',
    )


# ── Data sources ──────────────────────────────────────────────────────────────

def _iter_parquet(path: str):
    try:
        import pandas as pd
    except ImportError:
        console.print('[red]pandas not installed — run: pip install pandas pyarrow[/red]')
        sys.exit(1)

    df = pd.read_parquet(path)
    # Replace NaN/None with Python None so isinstance checks work cleanly
    df = df.where(df.notna(), None)

    for r in df.to_dict('records'):
        ts = r['timestamp']
        if ts is None or (isinstance(ts, float) and math.isnan(ts)):
            continue
        yield _make_row(
            timestamp=int(ts),
            year=int(r['year']) if r['year'] is not None else 0,
            month=int(r['month']) if r['month'] is not None else 0,
            track_name=r.get('track_name') or '',
            artist_name=r.get('artist_name') or '',
            album_name=r.get('album_name') or '',
            track_mbid=r.get('track_mbid'),
        )


def _iter_old_sqlite(path: str):
    """
    Yield rows from the legacy listening_history.sqlite.
    Joins with its tracks table to recover track names.
    INSERT OR IGNORE on the destination handles overlap with parquet rows.
    """
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute('''
        SELECT l.track_mbid,
               l.timestamp,
               l.main_artist_name,
               l.album_name,
               l.year,
               l.month,
               t.track_name
        FROM   listens l
        LEFT JOIN tracks t ON t.track_mbid = l.track_mbid
    ''').fetchall()
    conn.close()

    for r in rows:
        ts = r['timestamp']
        if not ts:
            continue
        yield _make_row(
            timestamp=ts,
            year=r['year'] or 0,
            month=r['month'] or 0,
            track_name=r['track_name'] or '',
            artist_name=r['main_artist_name'] or '',
            album_name=r['album_name'] or '',
            track_mbid=r['track_mbid'],
        )


def _iter_lastfm_api(api_key: str, user: str, since: int = 0):
    """
    Page through user.getRecentTracks (most-recent → oldest).
    Stops early if it reaches a page entirely before `since`.
    """
    page = 1
    total_pages = None

    while True:
        params = {
            'method': 'user.getrecenttracks',
            'user':    user,
            'api_key': api_key,
            'format':  'json',
            'limit':   '200',
            'page':    str(page),
        }
        if since:
            params['from'] = str(since)

        url = LASTFM_API + '?' + urllib.parse.urlencode(params)
        try:
            with urllib.request.urlopen(url, timeout=20) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            console.print(f'[red]Last.fm HTTP {e.code} on page {page}[/red]')
            break
        except Exception as e:
            console.print(f'[red]Last.fm error: {e}[/red]')
            break

        rt = data.get('recenttracks', {})
        if total_pages is None:
            attr = rt.get('@attr', {})
            total_pages = int(attr.get('totalPages', 1))
            total_tracks = int(attr.get('total', 0))
            console.print(f'  {total_tracks:,} scrobbles across {total_pages} pages')

        tracks = rt.get('track', [])
        if isinstance(tracks, dict):
            tracks = [tracks]  # single-track page comes as dict, not list

        for t in tracks:
            # Skip "now playing" entry (has @attr but no date)
            if t.get('@attr', {}).get('nowplaying'):
                continue
            date_info = t.get('date', {})
            if not date_info:
                continue
            ts = int(date_info.get('uts', 0))
            if not ts:
                continue
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            yield _make_row(
                timestamp=ts,
                year=dt.year,
                month=dt.month,
                track_name=t.get('name', ''),
                artist_name=(t.get('artist') or {}).get('#text', ''),
                album_name=(t.get('album') or {}).get('#text', ''),
                track_mbid=t.get('mbid') or None,
            )

        console.print(f'  page {page}/{total_pages}', end='\r')
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.25)


# ── Spotify helpers ───────────────────────────────────────────────────────────

def _sp_request(req, *, retries=2, delay=1.0):
    """Execute a urllib request with simple retry logic for transient failures."""
    last_exc = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read())
        except Exception as e:
            last_exc = e
            if attempt < retries:
                time.sleep(delay)
    raise last_exc


def _sp_token() -> str | None:
    _load_env()
    cid = os.environ.get('SPOTIFY_CLIENT_ID')
    sec = os.environ.get('SPOTIFY_CLIENT_SECRET')
    if not (cid and sec):
        return None
    auth = base64.b64encode(f'{cid}:{sec}'.encode()).decode()
    req = urllib.request.Request(
        SP_TOKEN,
        data=b'grant_type=client_credentials',
        headers={
            'Authorization':  f'Basic {auth}',
            'Content-Type':   'application/x-www-form-urlencoded',
        },
    )
    try:
        return _sp_request(req)['access_token']
    except Exception as e:
        console.print(f'  [dim yellow]Spotify auth failed: {e}[/dim yellow]')
        return None


def _sp_search_album(token: str, artist: str, album: str) -> list:
    q = urllib.parse.quote(f'album:{album} artist:{artist}')
    req = urllib.request.Request(
        f'{SP_SEARCH}?q={q}&type=album&limit=4',
        headers={'Authorization': f'Bearer {token}'},
    )
    try:
        data = _sp_request(req)
        return [
            SpotifyRelease.from_search_item(item)
            for item in data.get('albums', {}).get('items', [])
        ]
    except Exception as e:
        console.print(f'  [dim yellow]Spotify search failed: {e}[/dim yellow]')
        return []


def _print_db_result(i: int, row: dict) -> None:
    """Print one DB release candidate (mirrors _print_sp_result layout)."""
    vtype     = _detect_variant_type(row['title'])
    vtype_str = f'  [yellow]{vtype}[/yellow]' if vtype else ''
    t = (row.get('type') or '').capitalize()
    s = (row.get('type_secondary') or '').capitalize()
    mb_type  = f'{t} · {s}' if t and s else t or s or ''
    mb_str   = f'  [dim]{mb_type}[/dim]' if mb_type else ''
    date_str = f'  [dim]{row["release_date"]}[/dim]' if row.get('release_date') else ''
    console.print(
        f'  [bold]{i}.[/bold]  [bold]{row["title"]}[/bold]'
        f'{date_str}'
        f'  [dim]{row["artist_name"] or ""}[/dim]'
        f'{mb_str}'
        f'{vtype_str}'
    )
    console.print(f'       [dim green]db:{row["id"]}[/dim green]')


_AUTO_MATCH_THRESHOLD = 0.90


def _check_track_match_rate(
    conn: sqlite3.Connection, release_id: str, artist: str, album: str
) -> tuple[float, list[str]]:
    """Return (match_rate, unmatched_names) for a candidate DB release.

    match_rate = fraction of unique scrobbled track names (ETI-stripped) that
    appear in the release's tracklist.  unmatched_names lists the raw names
    that didn't match (capped at 5 for display).
    """
    scrobbled = conn.execute('''
        SELECT DISTINCT raw_track_name FROM listens
        WHERE  track_id IS NULL
          AND  raw_artist_name = ?
          AND  raw_album_name  = ?
    ''', [artist, album]).fetchall()
    scrobbled_names = [r[0] for r in scrobbled]
    if not scrobbled_names:
        return 0.0, []

    db_tracks = conn.execute('''
        SELECT title FROM tracks WHERE release_id = ? AND hidden = 0
    ''', [release_id]).fetchall()
    db_keys = {_ascii_key(_parse_track_title(r[0]).clean_title) for r in db_tracks}

    unmatched = []
    matched_count = 0
    for name in scrobbled_names:
        clean = _parse_track_title(name).clean_title
        if _ascii_key(clean) in db_keys:
            matched_count += 1
        else:
            unmatched.append(name)

    rate = matched_count / len(scrobbled_names)
    return rate, unmatched[:5]


# ── Shared insert helper ──────────────────────────────────────────────────────

_INSERT_SQL = '''
    INSERT OR IGNORE INTO listens
        (timestamp, year, month, raw_track_name, raw_artist_name,
         raw_album_name, raw_source_id, source)
    VALUES
        (:timestamp, :year, :month, :raw_track_name, :raw_artist_name,
         :raw_album_name, :raw_source_id, :source)
'''

_INSERT_SQL_SPOTIFY = '''
    INSERT OR IGNORE INTO listens
        (timestamp, year, month, raw_track_name, raw_artist_name,
         raw_album_name, raw_source_id, source, track_id, ms_played, skipped)
    VALUES
        (:timestamp, :year, :month, :raw_track_name, :raw_artist_name,
         :raw_album_name, :raw_source_id, :source, :track_id, :ms_played, :skipped)
'''


def _insert_rows(conn: sqlite3.Connection, rows: list) -> int:
    """Insert rows, returning the number actually inserted (ignoring duplicates)."""
    before = conn.total_changes
    conn.executemany(_INSERT_SQL, rows)
    conn.commit()
    return conn.total_changes - before


# ── spotify ────────────────────────────────────────────────────────────────────

def _iter_spotify_history(history_dir: str):
    """Yield qualifying play records from Spotify Extended Streaming History JSON files.

    Qualifying: track (not podcast/audiobook), not incognito, ms_played >= 30s.
    Files matched: Streaming_History_Audio_*.json
    """
    import glob
    for path in sorted(glob.glob(os.path.join(history_dir, 'Streaming_History_Audio_*.json'))):
        with open(path, encoding='utf-8') as f:
            data = json.load(f)
        for r in data:
            if r.get('episode_name') or r.get('audiobook_title'):
                continue
            if r.get('incognito_mode'):
                continue
            if not r.get('master_metadata_track_name'):
                continue
            if (r.get('ms_played') or 0) < 30_000:
                continue
            yield r


def cmd_fetch_spotify(conn: sqlite3.Connection, history_dir: str) -> None:
    """Import listens from Spotify Extended Streaming History into master.sqlite.

    For plays whose Spotify track ID is already in the tracks table:
      - Skip if a Last.fm scrobble for the same track exists within ±120s
        (avoids double-counting the same physical listen)
      - Otherwise insert with source='spotify', track_id pre-populated,
        ms_played and skipped populated from Spotify metadata

    After insertion, prints a ranked list of albums not yet imported so the
    caller knows what to feed into `mdb import` next.
    """
    import glob

    if not os.path.isdir(history_dir):
        console.print(f'[red]Spotify history directory not found: {history_dir}[/red]')
        console.print('[dim]Pass --spotify <DIR> to specify the location.[/dim]')
        return

    # Build spotify_track_id → ULID lookup from our catalog
    track_lookup = {
        row[0]: row[1]
        for row in conn.execute(
            'SELECT spotify_id, id FROM tracks WHERE spotify_id IS NOT NULL'
        ).fetchall()
    }

    total = 0
    matched = 0
    skipped_dup = 0
    batch: list = []
    inserted = 0
    unmatched: dict = {}  # (artist, album) → play count

    console.print(f'[bold]Spotify history:[/bold] {history_dir}')

    for r in _iter_spotify_history(history_dir):
        total += 1
        sp_id = r['spotify_track_uri'].split(':')[2]
        ts    = int(datetime.fromisoformat(r['ts'].replace('Z', '+00:00')).timestamp())
        dt    = datetime.fromtimestamp(ts, tz=timezone.utc)

        if sp_id not in track_lookup:
            key = (
                r['master_metadata_album_artist_name'],
                r['master_metadata_album_album_name'],
            )
            unmatched[key] = unmatched.get(key, 0) + 1
            continue

        track_id = track_lookup[sp_id]
        matched += 1

        # Skip if a Last.fm scrobble already covers this listen (±120s, same track)
        dup = conn.execute(
            "SELECT 1 FROM listens "
            "WHERE track_id = ? AND timestamp BETWEEN ? AND ? AND source = 'lastfm' LIMIT 1",
            (track_id, ts - 120, ts + 120),
        ).fetchone()
        if dup:
            skipped_dup += 1
            continue

        batch.append({
            'timestamp':      ts,
            'year':           dt.year,
            'month':          dt.month,
            'raw_track_name': r['master_metadata_track_name'],
            'raw_artist_name': r['master_metadata_album_artist_name'],
            'raw_album_name': r['master_metadata_album_album_name'],
            'raw_source_id':  r['spotify_track_uri'],
            'source':         'spotify',
            'track_id':       track_id,
            'ms_played':      r['ms_played'],
            'skipped':        1 if r.get('skipped') else 0,
        })

        if len(batch) >= 1_000:
            before = conn.total_changes
            conn.executemany(_INSERT_SQL_SPOTIFY, batch)
            conn.commit()
            inserted += conn.total_changes - before
            batch.clear()
            console.print(f'  {total:,} records scanned…', end='\r')

    if batch:
        before = conn.total_changes
        conn.executemany(_INSERT_SQL_SPOTIFY, batch)
        conn.commit()
        inserted += conn.total_changes - before

    console.print(f'  {total:,} qualifying plays scanned                   ')
    console.print(f'  {matched:,} with tracks already in catalog')
    console.print(f'  {skipped_dup:,} already covered by Last.fm scrobbles (skipped)')
    console.print(f'  [green]{inserted:,} new Spotify listens inserted[/green]')

    if unmatched:
        sorted_albums = sorted(unmatched.items(), key=lambda x: -x[1])
        console.print()
        console.print(
            f'[bold]{len(sorted_albums):,} albums not yet imported[/bold]'
            f'  [dim]({sum(unmatched.values()):,} plays missing)[/dim]'
            f'  — top candidates:'
        )
        for (artist, album), count in sorted_albums[:25]:
            console.print(f'  {count:4d}  {artist} — {album}')
        if len(sorted_albums) > 25:
            console.print(f'  [dim]… {len(sorted_albums) - 25:,} more albums[/dim]')
        console.print(
            '  [dim]Run [bold]mdb import <url>[/bold] then re-run '
            '[bold]sync fetch --spotify[/bold] to pick them up.[/dim]'
        )

def cmd_fetch(args):
    conn = open_db()

    def _drain(label: str, source, early_stop: bool = False):
        """Consume an iterator in batches, insert with progress display.

        If early_stop=True, halts as soon as a full batch yields zero new rows
        (used for live fetch when paginating into already-imported history).
        Returns total rows yielded from source.
        """
        batch = []
        n = 0
        for row in source:
            batch.append(row)
            n += 1
            if len(batch) >= 5_000:
                new = _insert_rows(conn, batch)
                batch = []
                console.print(f'  {n:,} rows…', end='\r')
                if early_stop and new == 0:
                    console.print(f'  [dim]reached already-imported scrobbles, stopping[/dim]      ')
                    return n
        if batch:
            _insert_rows(conn, batch)
        console.print(f'  [green]{n:,} rows loaded from {label}[/green]      ')
        return n

    # 1 — Parquet (legacy migration; skipped silently if file absent)
    parquet_path = args.parquet or os.path.join(
        os.path.expanduser('~'), 'Downloads', 'recenttracks.parquet'
    )
    if args.parquet or os.path.exists(parquet_path):
        console.print(f'[bold]Parquet:[/bold] {parquet_path}')
        _drain('parquet', _iter_parquet(parquet_path))

    # 2 — Old sqlite (legacy migration; skipped silently if file absent)
    old_path = args.sqlite or OLD_SQLITE
    if args.sqlite or os.path.exists(old_path):
        console.print(f'[bold]Old sqlite:[/bold] {old_path}')
        _drain('old sqlite', _iter_old_sqlite(old_path))

    # 3 — Live Last.fm API (default; skip with --no-live)
    if not args.no_live:
        _load_env()
        api_key = os.environ.get('LASTFM_API_KEY')
        user    = os.environ.get('LASTFM_USER')
        if not (api_key and user):
            console.print('[red]Set LASTFM_API_KEY and LASTFM_USER in music/.env[/red]')
        else:
            # Auto-calculate since from DB unless --full or --since explicitly given
            if args.full:
                since = 0
            elif args.since is not None:
                since = args.since
            else:
                row   = conn.execute(
                    "SELECT MAX(timestamp) FROM listens WHERE source = 'lastfm'"
                ).fetchone()
                since = row[0] or 0

            console.print(f'[bold]Last.fm API:[/bold] {user}')
            if since:
                console.print(
                    f'  incremental fetch since '
                    f'{datetime.fromtimestamp(since, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}'
                )
            _drain('Last.fm API', _iter_lastfm_api(api_key, user, since=since),
                   early_stop=not since)

    # 4 — Spotify Extended Streaming History (opt-in with --spotify)
    if args.spotify is not None:
        history_dir = args.spotify if args.spotify else _SP_HISTORY_DEFAULT
        console.print()
        cmd_fetch_spotify(conn, history_dir)

    # Auto-match whatever we can via track MBID
    console.print()
    console.print('[bold]Auto-matching by track MBID…[/bold]')
    matched = bulk_rematch(conn)
    console.print(f'  [green]{matched:,} listens matched to catalog tracks[/green]')

    # Remove Last.fm listens that are now duplicated by a Spotify listen (same
    # track_id, within ±120s). bulk_rematch must run first so track_ids are set.
    removed = dedup_spotify_lastfm(conn)
    if removed:
        console.print(f'  [dim]{removed:,} Last.fm listens removed (covered by Spotify)[/dim]')

    # Final summary
    total    = conn.execute('SELECT COUNT(*) FROM listens').fetchone()[0]
    resolved = conn.execute('SELECT COUNT(*) FROM listens WHERE track_id IS NOT NULL').fetchone()[0]
    console.print()
    pct = 100 * resolved / total if total else 0
    console.print(
        f'[bold]Done.[/bold]  {total:,} total listens  ·  '
        f'{resolved:,} matched ({pct:.1f}%)  ·  '
        f'{total - resolved:,} unresolved'
    )
    console.print(f'  Run [bold]sync match[/bold] to resolve unmatched albums interactively.')
    conn.close()


# ── match ─────────────────────────────────────────────────────────────────────

def _print_sp_result(i, r: 'SpotifyRelease') -> None:
    """Print one Spotify search candidate, mirroring mdb.py _print_member layout."""
    vtype     = _detect_variant_type(r.name)
    vtype_str = f'  [yellow]{vtype}[/yellow]' if vtype else ''
    console.print(
        f'  [bold]{i}.[/bold]  [bold]{r.name}[/bold]'
        f'  [dim]{r.year or "?"}[/dim]'
        f'  [dim]{r.artist}[/dim]'
        f'{vtype_str}'
    )
    console.print(f'       [dim cyan]sp:{r.id}[/dim cyan]')


def _print_release_card(i, m):
    """Print one release row. m: title, track_count, explicit_count,
    optionally release_date, type, type_secondary."""
    explicit_count = m.get('explicit_count') or 0
    total_tracks   = m.get('track_count') or 0
    if explicit_count == 0:
        expl_str = ''
    elif explicit_count == total_tracks:
        expl_str = '  [red]explicit[/red]'
    else:
        expl_str = f'  [red]{explicit_count}[/red][dim]/{total_tracks} explicit[/dim]'
    vtype     = _detect_variant_type(m['title'])
    vtype_str = f'  [cyan]{vtype}[/cyan]' if vtype else ''
    t = (m.get('type') or '').capitalize()
    s = (m.get('type_secondary') or '').capitalize()
    mb_type = f'{t} · {s}' if t and s else t or s or ''
    mb_str   = f'  [dim]{mb_type}[/dim]' if mb_type else ''
    date_str = f'  [dim]{m["release_date"]}[/dim]' if m.get('release_date') else ''
    console.print(
        f'  [bold]{i}.[/bold]  [bold]{m["title"]}[/bold]'
        f'{date_str}{mb_str}'
        f'  [dim]{total_tracks} tracks[/dim]'
        f'{expl_str}{vtype_str}'
    )


def cmd_match(args):
    conn   = open_db()
    token  = _sp_token()
    limit  = args.limit or 50
    include_deferred = getattr(args, 'skipped', False)
    sort_recent      = getattr(args, 'recent',  False)

    if not token:
        console.print('[yellow]No Spotify credentials found — auto-search disabled.[/yellow]')

    console.rule('[bold]SYNC MATCH[/bold]')

    # Resolve any MBID-based listens that became matchable since last run
    # (e.g. from a direct `mdb import` outside of sync match).
    newly_matched = bulk_rematch(conn)
    if newly_matched:
        console.print(f'  [green]✓ {newly_matched} listens auto-matched via MBID[/green]\n')

    # Thread pool for background prefetches (searches + full album data).
    # Lives for the duration of cmd_match, shared across batches.
    _pool = concurrent.futures.ThreadPoolExecutor(max_workers=5,
                                                  thread_name_prefix='prefetch')

    auto_matched_albums   = 0
    auto_matched_listens  = 0

    while True:
        # Top unresolved albums, excluding permanently-hidden (and deferred unless --skipped)
        method_filter = "('hide')" if include_deferred else "('hide', 'skip')"

        if sort_recent:
            # Walk the listen timeline and surface contiguous runs of the same album,
            # ordered by the most recent session first.  LAG() detects boundaries.
            rows = conn.execute(f'''
                SELECT   raw_album_name,
                         raw_artist_name,
                         COUNT(*)                      AS listen_count,
                         COUNT(DISTINCT raw_source_id) AS unique_tracks,
                         MAX(timestamp)                AS last_listened
                FROM (
                    SELECT timestamp, raw_artist_name, raw_album_name, raw_source_id,
                           SUM(boundary) OVER (ORDER BY timestamp) AS run_id
                    FROM (
                        SELECT timestamp, raw_artist_name, raw_album_name, raw_source_id,
                               CASE
                                   WHEN raw_album_name  != LAG(raw_album_name)  OVER (ORDER BY timestamp)
                                     OR raw_artist_name != LAG(raw_artist_name) OVER (ORDER BY timestamp)
                                     OR LAG(raw_album_name) OVER (ORDER BY timestamp) IS NULL
                                   THEN 1 ELSE 0 END AS boundary
                        FROM   listens
                        WHERE  track_id IS NULL
                          AND  NOT EXISTS (
                                   SELECT 1 FROM legacy_track_map ltm
                                   WHERE  ltm.lastfm_id = 'album|||'
                                              || lower(raw_artist_name)
                                              || '|||'
                                              || lower(raw_album_name)
                                     AND  ltm.match_method IN {method_filter}
                               )
                    )
                )
                GROUP BY run_id, raw_artist_name, raw_album_name
                ORDER BY last_listened DESC
                LIMIT    ?
            ''', [limit]).fetchall()
        else:
            rows = conn.execute(f'''
                SELECT   l.raw_album_name,
                         l.raw_artist_name,
                         COUNT(*)                      AS listen_count,
                         COUNT(DISTINCT l.raw_source_id) AS unique_tracks,
                         MAX(l.timestamp)              AS last_listened
                FROM     listens l
                WHERE    l.track_id IS NULL
                  AND    NOT EXISTS (
                             SELECT 1 FROM legacy_track_map ltm
                             WHERE  ltm.lastfm_id = 'album|||'
                                        || lower(l.raw_artist_name)
                                        || '|||'
                                        || lower(l.raw_album_name)
                               AND  ltm.match_method IN {method_filter}
                         )
                GROUP BY l.raw_album_name, l.raw_artist_name
                ORDER BY listen_count DESC
                LIMIT    ?
            ''', [limit]).fetchall()

        if not rows:
            console.print('[green]All listens resolved (or skipped)![/green]')
            break

        # Per-batch search cache: 0-based album index → Future[list[SpotifyRelease]]
        search_cache: dict = {}

        def _pre_search(idx: int) -> None:
            if token and 0 <= idx < len(rows) and idx not in search_cache:
                r = rows[idx]
                search_cache[idx] = _pool.submit(
                    _sp_search_album, token,
                    r['raw_artist_name'], r['raw_album_name'],
                )

        # Kick off the first two albums' searches immediately so the first
        # prompt is ready before the user has even read the header.
        _pre_search(0)
        _pre_search(1)

        total_unresolved = conn.execute(
            'SELECT COUNT(*) FROM listens WHERE track_id IS NULL'
        ).fetchone()[0]
        mode_label = 'most recent session' if sort_recent else 'play count'
        console.print(
            f'  [dim]{total_unresolved:,} unresolved listens — '
            f'showing top {len(rows)} albums by {mode_label}[/dim]\n'
        )

        for album_i, row in enumerate(rows, 1):
            album  = row['raw_album_name']
            artist = row['raw_artist_name']
            count  = row['listen_count']
            tracks = row['unique_tracks']

            # DB search happens first so auto-match can skip the interactive prompt.
            db_results = _db_search_releases(conn, artist, album)

            # ── Auto-match check ─────────────────────────────────────────────
            # Conditions: exactly 1 DB result, exact ascii_key title match, ≥90%
            # of scrobbled track names found in that release's tracklist.
            if (
                len(db_results) == 1
                and _ascii_key(db_results[0]['title']) == _ascii_key(album)
            ):
                release = db_results[0]
                rate, unmatched_names = _check_track_match_rate(conn, release['id'], artist, album)
                if rate >= _AUTO_MATCH_THRESHOLD:
                    matched = bulk_rematch_by_name(conn, [release['id']], artist, album)
                    if matched:
                        pct = int(rate * 100)
                        console.print(
                            f'  [dim green]✓ auto[/dim green]  '
                            f'[bold]{artist}[/bold]  [dim]—  {album}[/dim]  '
                            f'[dim]({matched} listens, {pct}% track match)[/dim]'
                        )
                        auto_matched_albums  += 1
                        auto_matched_listens += matched
                        _pre_search(album_i)
                        _pre_search(album_i + 1)
                        continue
                    # matched == 0 means bulk_rematch_by_name found nothing despite
                    # the track-rate check passing — fall through to manual prompt.
                else:
                    # Rate below threshold — show manual prompt with rate hint
                    pct = int(rate * 100)
                    unmatched_str = ', '.join(f'"{n}"' for n in unmatched_names)
                    console.print(
                        f'  [dim yellow]~ partial match[/dim yellow]  '
                        f'[bold]{artist}[/bold]  [dim]—  {album}[/dim]  '
                        f'[dim]({pct}% track match)[/dim]'
                    )
                    if unmatched_names:
                        console.print(f'  [dim yellow]  unmatched: {unmatched_str}[/dim yellow]')
            # ── End auto-match check ─────────────────────────────────────────

            console.rule(f'[dim]{album_i}/{len(rows)}[/dim]', style='dim')
            console.print(f'  [bold]{artist}[/bold]  [dim]—  {album}[/dim]')
            if sort_recent:
                last_dt = datetime.fromtimestamp(row['last_listened'], tz=timezone.utc).strftime('%Y-%m-%d')
                console.print(f'  [dim]{count:,} listen{"s" if count != 1 else ""} in session · last played {last_dt}[/dim]')
            else:
                console.print(f'  [dim]{count:,} listens · {tracks} unique tracks[/dim]')

            # Raw scrobble preview — shows exact Last.fm field values so mismatches
            # (artist name variants, feat. formatting, missing MBIDs) are visible.
            preview_lim = min(count if sort_recent else tracks, 8)
            ts_clause   = 'AND timestamp <= ?' if sort_recent else ''
            ts_params   = [row['last_listened']] if sort_recent else []
            preview = conn.execute(f'''
                SELECT raw_artist_name, raw_track_name, raw_source_id, COUNT(*) AS cnt
                FROM   listens
                WHERE  track_id IS NULL
                  AND  raw_artist_name = ?
                  AND  raw_album_name  = ?
                  {ts_clause}
                GROUP  BY raw_artist_name, raw_track_name
                ORDER  BY MAX(timestamp) DESC
                LIMIT  ?
            ''', [artist, album, *ts_params, preview_lim]).fetchall()
            for p in preview:
                src_tag = 'mbid' if _is_valid_mbid(p['raw_source_id']) else 'key'
                cnt_s   = f' ×{p["cnt"]}' if p['cnt'] > 1 else ''
                console.print(
                    f'  [dim]"{p["raw_artist_name"]}"  ·  '
                    f'"{p["raw_track_name"]}"  [{src_tag}]{cnt_s}[/dim]'
                )
            console.print()

            # DB results (already-imported releases that match this album)
            for idx, db_row in enumerate(db_results, 1):
                _print_db_result(idx, db_row)
            if db_results:
                console.print()

            # Auto-search Spotify (from prefetch cache if ready, else block)
            sp_offset = len(db_results)
            sp_results = []
            if token and artist and album:
                fut = search_cache.get(album_i - 1)
                if fut is not None:
                    try:
                        sp_results = fut.result()
                    except Exception as e:
                        console.print(f'  [dim yellow]Spotify search failed: {e}[/dim yellow]')
                else:
                    sp_results = _sp_search_album(token, artist, album)
                for idx, r in enumerate(sp_results, sp_offset + 1):
                    _print_sp_result(idx, r)
                if sp_results:
                    console.print()

            total_results = len(db_results) + len(sp_results)

            # While the user reads results and thinks, kick off background work:
            # 1. Pre-fetch full track data for every displayed Spotify result so that
            #    selecting any of them triggers no additional network wait.
            for r in sp_results:
                _pool.submit(r._ensure_full)
            # 2. Pre-search the next two albums in the queue.
            _pre_search(album_i)
            _pre_search(album_i + 1)

            # Build hint (escape brackets so Rich doesn't consume them as markup)
            if total_results == 1:
                if db_results:
                    hint = '  [dim]Enter=match  \\[s]kip  \\[h]ide  \\[q]uit:[/dim] '
                else:
                    hint = '  [dim]Enter=import  \\[s]kip  \\[h]ide  \\[q]uit:[/dim] '
            elif total_results > 1:
                diff_hint = '  \\[d]iff' if len(sp_results) >= 2 else ''
                hint = (
                    f'  [dim]\\[1-{total_results}] select  \\[1 2] multi'
                    f'{diff_hint}  \\[s]kip  \\[h]ide  \\[q]uit:[/dim] '
                )
            else:
                hint = '  [dim]URL / db:ULID  \\[s]kip  \\[h]ide  \\[q]uit:[/dim] '

            console.print(hint, end='')

            while True:
                try:
                    raw = input().strip()
                except (KeyboardInterrupt, EOFError):
                    console.print()
                    console.print('  [dim]Quit — progress saved.[/dim]')
                    conn.close()
                    return

                choice = raw.lower()

                if choice == 'q':
                    console.print('  [dim]Quit — progress saved.[/dim]')
                    conn.close()
                    return

                elif choice == 'h':
                    skip_key = f"album|||{_norm(artist)}|||{_norm(album)}"
                    conn.execute('''
                        INSERT OR REPLACE INTO legacy_track_map
                            (lastfm_id, track_id, match_method, confidence)
                        VALUES (?, NULL, 'hide', 1.0)
                    ''', [skip_key])
                    conn.commit()
                    console.print('  [dim]Hidden.[/dim]')
                    break

                elif choice == 's':
                    defer_key = f"album|||{_norm(artist)}|||{_norm(album)}"
                    conn.execute('''
                        INSERT OR REPLACE INTO legacy_track_map
                            (lastfm_id, track_id, match_method, confidence)
                        VALUES (?, NULL, 'skip', 1.0)
                    ''', [defer_key])
                    conn.commit()
                    console.print('  [dim]Skipped.[/dim]')
                    break

                elif raw.startswith('http') or re.search(r'\b(sp:|mb:|musicbrainz\.org|[0-9a-f]{8}-[0-9a-f]{4})', raw):
                    to_match, to_import = _parse_mixed_tokens(raw, db_results, sp_results)
                    if to_match is None:
                        console.print('  [yellow]Unrecognised input — paste URLs, sp:ID, UUIDs, or numbers[/yellow]')
                        console.print(hint, end='')
                        continue
                    if to_match and not to_import:
                        release_ids = [r['id'] for r in to_match]
                        matched = bulk_rematch_by_name(conn, release_ids, artist, album)
                        if matched:
                            console.print(f'  [green]✓ {matched} listens matched[/green]')
                        else:
                            console.print('  [yellow]No new matches — track names may differ[/yellow]')
                    elif to_import:
                        _do_multi_import(conn, to_import, raw_artist=artist, raw_album=album)
                        if to_match:
                            # Also rematch against already-imported DB releases
                            release_ids = [r['id'] for r in to_match]
                            bulk_rematch_by_name(conn, release_ids, artist, album)
                    break

                elif choice == 'd' and len(sp_results) >= 2:
                    render_diff(*sp_results, compact=True)
                    console.print(hint, end='')

                elif total_results:
                    # Enter with a single result → auto-select it
                    if choice == '' and total_results == 1:
                        indices_raw = '1'
                    else:
                        indices_raw = raw
                    to_match, to_import = _parse_mixed_tokens(indices_raw, db_results, sp_results)
                    if to_match is None:
                        console.print(
                            f'  [yellow]Enter 1–{total_results}, '
                            f'multiple like "1 2", URLs, or s/h/q[/yellow]'
                        )
                        console.print(hint, end='')
                        continue

                    if to_match and not to_import:
                        # All DB — just rematch
                        release_ids = [r['id'] for r in to_match]
                        matched = bulk_rematch_by_name(conn, release_ids, artist, album)
                        if matched:
                            console.print(f'  [green]✓ {matched} listens matched[/green]')
                        else:
                            console.print('  [yellow]No new matches — track names may differ[/yellow]')
                        break
                    elif to_import:
                        if len(to_import) > 1:
                            sp_only = [x for x in to_import if isinstance(x, SpotifyRelease)]
                            if len(sp_only) >= 2:
                                render_diff(*sp_only, compact=True)
                            console.print(
                                f'  [dim]Import all {len(to_import)}? '
                                f'\\[Y/n]:[/dim] ',
                                end='',
                            )
                            try:
                                confirm = input().strip().lower()
                            except (KeyboardInterrupt, EOFError):
                                console.print()
                                break
                            if confirm in ('n', 'no'):
                                console.print(hint, end='')
                                continue
                        _do_multi_import(conn, to_import, raw_artist=artist, raw_album=album)
                        if to_match:
                            release_ids = [r['id'] for r in to_match]
                            bulk_rematch_by_name(conn, release_ids, artist, album)
                        break
                    else:
                        console.print(hint, end='')
                        continue

                elif raw.lower().startswith('db:') or re.match(r'^[0-9A-Z]{26}$', raw):
                    ulid = raw[3:] if raw.lower().startswith('db:') else raw
                    rel = conn.execute(
                        'SELECT id, title FROM releases WHERE id = ?', [ulid]
                    ).fetchone()
                    if rel:
                        matched = bulk_rematch_by_name(conn, [rel['id']], artist, album)
                        if matched:
                            console.print(f'  [green]✓ {matched} listens matched[/green]')
                        else:
                            console.print('  [yellow]No new matches — track names may differ[/yellow]')
                        break
                    else:
                        console.print(f'  [red]Release {ulid!r} not found in DB[/red]')
                        console.print(hint, end='')

                else:
                    console.print('  [yellow]Paste a URL, or enter s / h / q[/yellow]')
                    console.print(hint, end='')

            console.print()

        # End of batch — offer to continue or stop
        if len(rows) < limit:
            break  # no more albums to show
        console.rule(style='dim')
        console.print('  [dim]Press Enter for next batch, or \\[q]uit:[/dim] ', end='')
        try:
            if input().strip().lower() == 'q':
                break
        except (KeyboardInterrupt, EOFError):
            break

    _pool.shutdown(wait=False)
    if auto_matched_albums:
        console.print(
            f'\n  [dim green]Auto-matched {auto_matched_albums} album'
            f'{"s" if auto_matched_albums != 1 else ""} '
            f'({auto_matched_listens:,} listens)[/dim green]'
        )
    conn.close()


def _parse_indices(s: str, max_n: int):
    """
    Parse single or multi-number input into a 0-based index list.
    Accepts: '1', '2', '1 2', '1,2,3', 'all'.
    Returns list[int] or None if input is invalid.
    """
    s = s.strip().lower()
    if s == 'all':
        return list(range(max_n))
    tokens = re.split(r'[\s,]+', s)
    try:
        indices = [int(t) - 1 for t in tokens if t]
        if indices and all(0 <= i < max_n for i in indices):
            return indices
    except ValueError:
        pass
    return None


def _do_import(conn: sqlite3.Connection, url: str,
               raw_artist: str = None, raw_album: str = None):
    """Import a single URL via mdb.py, then bulk-rematch."""
    _do_multi_import(conn, [SpotifyRelease(url)],
                     raw_artist=raw_artist, raw_album=raw_album)


def _parse_mixed_tokens(raw: str, db_results: list, sp_results: list) -> 'tuple[list, list]':
    """
    Parse a raw input string into (to_match, to_import) where:
      to_match  — list of DB release row dicts (already in DB, just need rematch)
      to_import — list of items to import: SpotifyRelease or {'mbid': ..., 'url': ...}

    Accepts any comma- or space-separated mix of:
      - Numbers       → index into db_results + sp_results (1-based)
      - Spotify URLs  → SpotifyRelease
      - sp:ID         → SpotifyRelease (shorthand shown in display)
      - MB URLs/UUIDs → MB dict
    Returns (None, None) if any token is unrecognisable.
    """
    sp_offset = len(db_results)
    tokens = [t.strip() for t in re.split(r'[\s,]+', raw.strip()) if t.strip()]
    to_match, to_import = [], []

    for token in tokens:
        # Numbered result
        if token.isdigit():
            idx = int(token) - 1
            if idx < sp_offset:
                db_row = db_results[idx]
                if db_row not in to_match:
                    to_match.append(db_row)
            elif idx < sp_offset + len(sp_results):
                sp_item = sp_results[idx - sp_offset]
                if sp_item not in to_import:
                    to_import.append(sp_item)
            else:
                return None, None
            continue

        # sp:ID shorthand
        sp_m = re.match(r'^sp:([A-Za-z0-9]+)$', token)
        if sp_m:
            to_import.append(SpotifyRelease(sp_m.group(1)))
            continue

        # mb:UUID shorthand
        mb_m = re.match(r'^mb:(.+)$', token)
        if mb_m:
            mbid = _extract_mbid(mb_m.group(1))
            if mbid:
                to_import.append({'mbid': mbid, 'url': f'https://musicbrainz.org/release/{mbid}'})
                continue
            return None, None

        # Spotify URL
        if 'spotify.com/album/' in token:
            to_import.append(SpotifyRelease(token))
            continue

        # MB URL or bare UUID
        mbid = _extract_mbid(token)
        if mbid:
            to_import.append({'mbid': mbid, 'url': f'https://musicbrainz.org/release/{mbid}'})
            continue

        return None, None  # unrecognised token

    return to_match, to_import


def _do_multi_import(conn: sqlite3.Connection, selected: list,
                     raw_artist: str = None, raw_album: str = None):
    """
    Import one or more Spotify or MusicBrainz albums via mdb.py.
    selected items are SpotifyRelease objects or {'mbid': ..., 'url': ...} dicts.
    After import, bulk-rematch all listens (MBID-based, then name-based if context given).
    If multiple albums imported, offer to link them as variants.
    """
    imported = []  # list of (item, release_row)

    for item in selected:
        is_mb  = isinstance(item, dict) and 'mbid' in item
        url    = item['url'] if is_mb else item.url
        console.print(f'  Importing [dim]{url}[/dim]')
        result = subprocess.run([PYTHON, MDB, 'import', url])
        if result.returncode == 0:
            if is_mb:
                rel = conn.execute(
                    'SELECT id, title FROM releases WHERE mbid = ?', [item['mbid']]
                ).fetchone()
            else:
                rel = conn.execute(
                    'SELECT id, title FROM releases WHERE spotify_id = ?', [item.id]
                ).fetchone()
            if rel:
                imported.append((item, rel))
        else:
            label = item['url'] if is_mb else (item.name or item.url)
            console.print(f'  [red]Import failed for {label}[/red]')

    if not imported:
        console.print('  [red]All imports failed[/red]')
        return

    matched = bulk_rematch(conn)
    if raw_artist and raw_album:
        release_ids = [rel['id'] for _, rel in imported]
        matched += bulk_rematch_by_name(conn, release_ids, raw_artist, raw_album)
    if matched:
        console.print(f'  [green]✓ {matched} listens now matched[/green]')
    else:
        console.print('  [yellow]Imported — no new matches yet '
                      '(run sync match again after tracks are enriched)[/yellow]')

    if len(imported) > 1:
        _prompt_variants(conn, imported)


def _prompt_variants(conn: sqlite3.Connection, imported: list):
    """
    After importing multiple editions of the same album, let the user
    designate a canonical release, then run three-stage type assignment
    for each member (mirrors mdb.py release variants flow).
    """
    console.print()
    console.rule('[dim]Link as variants?[/dim]', style='dim')

    # Fetch full release data so we can show date/type and run stage prompts
    full_releases = []
    for _item, rel in imported:
        row = conn.execute('''
            SELECT r.id, r.title, r.release_date, r.type, r.type_secondary,
                   COUNT(t.id)                                    AS track_count,
                   SUM(CASE WHEN t.is_explicit = 1 THEN 1 ELSE 0 END) AS explicit_count
            FROM   releases r
            LEFT JOIN tracks t ON t.release_id = r.id AND t.hidden = 0
            WHERE  r.id = ?
            GROUP  BY r.id
        ''', [rel['id']]).fetchone()
        if row:
            full_releases.append(dict(row))

    if not full_releases:
        return

    def _show_list():
        console.print('  [bold]Pick the canonical (standard) edition:[/bold]\n')
        for i, m in enumerate(full_releases, 1):
            _print_release_card(i, m)
        console.print()

    _show_list()
    console.print('  [dim]number / \\[s]kip / \\[q]uit / Spotify or MB URL:[/dim] ', end='')

    canonical = None
    while True:
        try:
            raw = input().strip()
        except (KeyboardInterrupt, EOFError):
            return

        if raw.lower() == 's':
            return

        if raw.lower() == 'q':
            console.print('  [dim]Quit — progress saved.[/dim]')
            sys.exit(0)

        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(full_releases):
                canonical = full_releases[idx]
                break

        elif 'spotify.com/album/' in raw:
            sp_id = _extract_spotify_id(raw)
            rel = conn.execute(
                'SELECT id, title FROM releases WHERE spotify_id = ?', [sp_id]
            ).fetchone()
            if not rel:
                console.print(f'  Importing [dim]{raw}[/dim]')
                result = subprocess.run([PYTHON, MDB, 'import', raw])
                if result.returncode != 0:
                    console.print('  [red]Import failed — try again[/red]')
                    console.print(
                        '  [dim]number / \\[s]kip / \\[q]uit / Spotify or MB URL:[/dim] ', end=''
                    )
                    continue
                rel = conn.execute(
                    'SELECT id, title FROM releases WHERE spotify_id = ?', [sp_id]
                ).fetchone()
            if rel:
                # Fetch full row and add to group if not already present
                if not any(m['id'] == rel['id'] for m in full_releases):
                    row = conn.execute('''
                        SELECT r.id, r.title, r.release_date, r.type, r.type_secondary,
                               COUNT(t.id)                                    AS track_count,
                               SUM(CASE WHEN t.is_explicit = 1 THEN 1 ELSE 0 END) AS explicit_count
                        FROM   releases r
                        LEFT JOIN tracks t ON t.release_id = r.id AND t.hidden = 0
                        WHERE  r.id = ?
                        GROUP  BY r.id
                    ''', [rel['id']]).fetchone()
                    if row:
                        full_releases.append(dict(row))
                canonical = next(m for m in full_releases if m['id'] == rel['id'])
                break
            console.print('  [red]Could not find release — try again[/red]')

        elif _extract_mbid(raw):
            mbid = _extract_mbid(raw)
            rel = conn.execute(
                'SELECT id, title FROM releases WHERE mbid = ?', [mbid]
            ).fetchone()
            if not rel:
                console.print(f'  Importing [dim]{raw}[/dim]')
                result = subprocess.run([PYTHON, MDB, 'import', raw])
                if result.returncode != 0:
                    console.print('  [red]Import failed — try again[/red]')
                    console.print(
                        '  [dim]number / \\[s]kip / \\[q]uit / Spotify or MB URL:[/dim] ', end=''
                    )
                    continue
                rel = conn.execute(
                    'SELECT id, title FROM releases WHERE mbid = ?', [mbid]
                ).fetchone()
            if rel:
                if not any(m['id'] == rel['id'] for m in full_releases):
                    row = conn.execute('''
                        SELECT r.id, r.title, r.release_date, r.type, r.type_secondary,
                               COUNT(t.id)                                    AS track_count,
                               SUM(CASE WHEN t.is_explicit = 1 THEN 1 ELSE 0 END) AS explicit_count
                        FROM   releases r
                        LEFT JOIN tracks t ON t.release_id = r.id AND t.hidden = 0
                        WHERE  r.id = ?
                        GROUP  BY r.id
                    ''', [rel['id']]).fetchone()
                    if row:
                        full_releases.append(dict(row))
                canonical = next(m for m in full_releases if m['id'] == rel['id'])
                break
            console.print('  [red]Could not find release — try again[/red]')

        else:
            console.print(f'  [yellow]Enter 1–{len(full_releases)}, s, q, or a Spotify/MB URL[/yellow]')

        console.print('  [dim]number / \\[s]kip / \\[q]uit / Spotify or MB URL:[/dim] ', end='')

    if not canonical:
        return

    # ── three-stage type assignment (mirrors cmd_release_variants) ────────────
    type_updates  = {}
    edition_links = []
    hide_ids      = set()
    aborted       = False

    all_members = [canonical] + [m for m in full_releases if m['id'] != canonical['id']]

    for sort_i, m in enumerate(all_members):
        is_canonical = (m['id'] == canonical['id'])
        role_label   = '[bold green]canonical[/bold green]' if is_canonical \
                       else f'[bold]variant {sort_i}[/bold]'
        console.rule(
            f'  {role_label}: [bold]{m["title"]}[/bold]'
            f'  [dim]{m.get("release_date") or "?"}[/dim]',
            style='dim',
        )

        cur_type = m.get('type') or 'album'
        chosen_type, quit_now, _ = _prompt_choice(
            'Stage 1 — Primary type', _PRIMARY_TYPES, current=cur_type
        )
        if quit_now:
            aborted = True
            break

        cur_sec = m.get('type_secondary') or 'none'
        chosen_sec, quit_now, _ = _prompt_choice(
            'Stage 2 — Secondary type', _SECONDARY_TYPES, current=cur_sec
        )
        if quit_now:
            type_updates[m['id']] = (chosen_type, m.get('type_secondary'))
            aborted = True
            break
        chosen_sec = None if chosen_sec == 'none' else chosen_sec
        type_updates[m['id']] = (chosen_type, chosen_sec)

        if not is_canonical:
            auto_ed = _detect_variant_type(m['title'])
            if auto_ed in ('live', 'remix'):
                auto_ed = None
            cur_ed = auto_ed or 'none'
            chosen_ed, quit_now, do_hide = _prompt_choice(
                'Stage 3 — Edition type', _EDITION_TYPES, current=cur_ed, allow_hide=True
            )
            if quit_now:
                aborted = True
                break
            if do_hide:
                hide_ids.add(m['id'])
                del type_updates[m['id']]
                continue
            edition_type = None if chosen_ed == 'none' else chosen_ed
            edition_links.append((m['id'], edition_type, sort_i))

    _write_variant_links(conn, canonical, type_updates, edition_links, hide_ids)

    parts = [f'+{len(edition_links)} variant(s)']
    if hide_ids:
        parts.append(f'{len(hide_ids)} hidden')

    if aborted:
        console.print(
            f'\n  [green]✓[/green]  Canonical: [bold]{canonical["title"]}[/bold]'
            f'  {", ".join(parts)}  [dim](partial)[/dim]\n'
        )
        console.print('  [dim]Quit — progress saved.[/dim]')
        sys.exit(0)

    console.print(
        f'\n  [green]✓[/green]  Canonical: [bold]{canonical["title"]}[/bold]'
        f'  {", ".join(parts)}\n'
    )


# ── status ────────────────────────────────────────────────────────────────────

def cmd_status(args):
    conn = open_db()

    total = conn.execute('SELECT COUNT(*) FROM listens').fetchone()[0]
    if total == 0:
        console.print('[yellow]No listens loaded yet. Run: sync fetch[/yellow]')
        conn.close()
        return

    matched   = conn.execute(
        'SELECT COUNT(*) FROM listens WHERE track_id IS NOT NULL'
    ).fetchone()[0]
    unmatched = total - matched
    skipped  = conn.execute(
        "SELECT COUNT(*) FROM legacy_track_map WHERE match_method = 'hide'"
    ).fetchone()[0]
    deferred = conn.execute(
        "SELECT COUNT(*) FROM legacy_track_map WHERE match_method = 'skip'"
    ).fetchone()[0]

    mn, mx = conn.execute(
        'SELECT MIN(timestamp), MAX(timestamp) FROM listens'
    ).fetchone()
    date_min = datetime.fromtimestamp(mn, tz=timezone.utc).strftime('%Y-%m-%d') if mn else '—'
    date_max = datetime.fromtimestamp(mx, tz=timezone.utc).strftime('%Y-%m-%d') if mx else '—'

    console.rule('[bold]SYNC STATUS[/bold]')
    console.print(f'  Total listens   [bold]{total:,}[/bold]')
    console.print(f'  Matched         [green]{matched:,}[/green]  ({100 * matched / total:.1f}%)')
    console.print(f'  Unresolved      [yellow]{unmatched:,}[/yellow]')
    console.print(f'  Hidden albums   [dim]{skipped}[/dim]')
    console.print(f'  Skipped albums  [dim]{deferred}[/dim]  [dim](sync match --skipped to revisit)[/dim]')
    console.print(f'  Date range      {date_min} → {date_max}')

    if unmatched:
        console.print()
        console.print('  Top unresolved albums:')
        rows = conn.execute('''
            SELECT   raw_album_name, raw_artist_name, COUNT(*) AS n
            FROM     listens
            WHERE    track_id IS NULL
            GROUP BY raw_album_name, raw_artist_name
            ORDER BY n DESC
            LIMIT    15
        ''').fetchall()
        for r in rows:
            console.print(
                f'    [dim]{r["n"]:4d}x[/dim]  '
                f'{r["raw_artist_name"]} — {r["raw_album_name"]}'
            )

    conn.close()


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    ap  = argparse.ArgumentParser(prog='sync', description='Listening history sync')
    sub = ap.add_subparsers(dest='cmd', required=True)

    # fetch
    pf = sub.add_parser('fetch', help='Sync listens from Last.fm API (and optionally legacy files)')
    pf.add_argument('--parquet', metavar='FILE',
                    help='Force-load recenttracks.parquet (legacy migration)')
    pf.add_argument('--sqlite',  metavar='FILE',
                    help='Force-load listening_history.sqlite (legacy migration)')
    pf.add_argument('--no-live', action='store_true',
                    help='Skip Last.fm API fetch (offline / parquet-only run)')
    pf.add_argument('--full',    action='store_true',
                    help='Re-fetch all scrobbles from Last.fm (ignores incremental since)')
    pf.add_argument('--since',   type=int, metavar='TIMESTAMP',
                    help='Override incremental since with an explicit Unix timestamp')
    pf.add_argument('--spotify', nargs='?', const='', metavar='DIR',
                    help='Import from Spotify Extended Streaming History '
                         f'(default dir: {_SP_HISTORY_DEFAULT})')

    # match
    pm = sub.add_parser('match', help='Interactive album-by-album resolution')
    pm.add_argument('--limit',    type=int, default=50,
                    help='Max albums to display per session (default: 50)')
    pm.add_argument('--skipped', action='store_true',
                    help='Include albums previously skipped with [s]')
    pm.add_argument('--recent',  action='store_true',
                    help='Sort by most recently listened instead of most-played')

    # status
    sub.add_parser('status', help='Show matched / unmatched breakdown')

    args = ap.parse_args()
    if   args.cmd == 'fetch':  cmd_fetch(args)
    elif args.cmd == 'match':  cmd_match(args)
    elif args.cmd == 'status': cmd_status(args)


if __name__ == '__main__':
    main()
