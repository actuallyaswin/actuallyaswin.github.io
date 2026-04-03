#!/usr/bin/env python3
"""
sync — Listening History Sync

Migrates scrobble history from parquet / old sqlite / Last.fm API
into master.sqlite, then interactively resolves unmatched listens
album-by-album.

Usage:
  sync fetch [--parquet FILE] [--sqlite FILE] [--live] [--since TIMESTAMP]
  sync match [--limit N]
  sync status
"""

import argparse
import base64
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
    detect_variant_type as _detect_variant_type,
    _PRIMARY_TYPES, _SECONDARY_TYPES, _EDITION_TYPES,
)

console = Console(width=80, highlight=False)

_DIR       = os.path.dirname(os.path.abspath(__file__))
DB_PATH    = os.path.join(_DIR, 'master.sqlite')
OLD_SQLITE = os.path.join(_DIR, 'listening_history.sqlite')
MDB        = os.path.join(_DIR, 'mdb.py')
PYTHON     = sys.executable

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
    conn.commit()


def bulk_rematch(conn: sqlite3.Connection) -> int:
    """
    Match unresolved listens to catalog tracks via MBID.
    raw_source_id holds the track MBID for 76% of scrobbles; the rest use
    artist|||track keys which require name-based matching (see match command).
    Returns the count of newly matched listens.
    """
    cur = conn.execute('''
        UPDATE listens
        SET track_id = (
            SELECT t.id FROM tracks t
            WHERE t.mbid = listens.raw_source_id
        )
        WHERE track_id IS NULL
          AND raw_source_id GLOB '????????-????-????-????-????????????'
          AND EXISTS (
              SELECT 1 FROM tracks t WHERE t.mbid = listens.raw_source_id
          )
    ''')
    conn.commit()
    return cur.rowcount


def bulk_rematch_by_name(conn: sqlite3.Connection,
                         release_ids: list,
                         raw_artist: str,
                         raw_album: str) -> int:
    """
    Secondary match for tracks with no MBIDs (e.g. no MusicBrainz result).
    Scopes to listens whose raw_artist_name matches the queue entry (or any known
    alias of the release's artists), then joins on lower(raw_track_name) = lower(tracks.title).
    Returns the count of newly matched listens.
    """
    if not release_ids:
        return 0

    rph = ','.join('?' * len(release_ids))

    # Collect canonical artist names + all known aliases for artists on these releases
    artist_name_set = {raw_artist.lower()}
    rows = conn.execute(f'''
        SELECT DISTINCT a.name, aa.alias
        FROM   artists a
        JOIN   track_artists ta ON ta.artist_id = a.id
        JOIN   tracks t         ON t.id = ta.track_id
        LEFT   JOIN artist_aliases aa ON aa.artist_id = a.id
        WHERE  t.release_id IN ({rph})
    ''', release_ids).fetchall()
    for r in rows:
        if r['name']:
            artist_name_set.add(r['name'].lower())
        if r['alias']:
            artist_name_set.add(r['alias'].lower())

    aph = ','.join('?' * len(artist_name_set))
    artist_names = list(artist_name_set)

    cur = conn.execute(f'''
        UPDATE listens
        SET track_id = (
            SELECT t.id FROM tracks t
            WHERE  t.release_id IN ({rph})
              AND  (
                lower(t.title) = lower(listens.raw_track_name)
                OR lower(CASE WHEN instr(lower(t.title), ' (feat.') > 0
                              THEN substr(t.title, 1, instr(lower(t.title), ' (feat.') - 1)
                              ELSE t.title END) = lower(listens.raw_track_name)
              )
            LIMIT 1
        )
        WHERE track_id IS NULL
          AND lower(raw_artist_name) IN ({aph})
          AND lower(raw_album_name)  = lower(?)
          AND raw_track_name IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM tracks t
              WHERE  t.release_id IN ({rph})
                AND  (
                  lower(t.title) = lower(listens.raw_track_name)
                  OR lower(CASE WHEN instr(lower(t.title), ' (feat.') > 0
                                THEN substr(t.title, 1, instr(lower(t.title), ' (feat.') - 1)
                                ELSE t.title END) = lower(listens.raw_track_name)
                )
          )
    ''', [*release_ids, *artist_names, raw_album, *release_ids])
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
            {
                'id':     item['id'],
                'name':   item['name'],
                'artist': item['artists'][0]['name'] if item.get('artists') else '',
                'year':   (item.get('release_date') or '')[:4],
                'url':    f"https://open.spotify.com/album/{item['id']}",
            }
            for item in data.get('albums', {}).get('items', [])
        ]
    except Exception as e:
        console.print(f'  [dim yellow]Spotify search failed: {e}[/dim yellow]')
        return []


# ── Shared insert helper ──────────────────────────────────────────────────────

_INSERT_SQL = '''
    INSERT OR IGNORE INTO listens
        (timestamp, year, month, raw_track_name, raw_artist_name,
         raw_album_name, raw_source_id, source)
    VALUES
        (:timestamp, :year, :month, :raw_track_name, :raw_artist_name,
         :raw_album_name, :raw_source_id, :source)
'''


def _insert_rows(conn: sqlite3.Connection, rows: list) -> int:
    conn.executemany(_INSERT_SQL, rows)
    conn.commit()
    return len(rows)


# ── fetch ─────────────────────────────────────────────────────────────────────

def cmd_fetch(args):
    conn = open_db()

    def _drain(label: str, source):
        """Consume an iterator in batches, insert with progress display."""
        batch = []
        n = 0
        for row in source:
            batch.append(row)
            n += 1
            if len(batch) >= 5_000:
                _insert_rows(conn, batch)
                batch = []
                console.print(f'  {n:,} rows…', end='\r')
        if batch:
            _insert_rows(conn, batch)
        console.print(f'  [green]{n:,} rows loaded from {label}[/green]      ')

    # 1 — Parquet (primary: has track names + mostly valid MBIDs)
    parquet_path = args.parquet or os.path.join(
        os.path.expanduser('~'), 'Downloads', 'recenttracks.parquet'
    )
    if os.path.exists(parquet_path):
        console.print(f'[bold]Parquet:[/bold] {parquet_path}')
        _drain('parquet', _iter_parquet(parquet_path))
    else:
        console.print(f'[dim]Parquet not found at {parquet_path}, skipping[/dim]')

    # 2 — Old sqlite (supplement: ~700 listens not in parquet + older data insurance)
    old_path = args.sqlite or OLD_SQLITE
    if os.path.exists(old_path):
        console.print(f'[bold]Old sqlite:[/bold] {old_path}')
        _drain('old sqlite', _iter_old_sqlite(old_path))
    else:
        console.print(f'[dim]Old sqlite not found at {old_path}, skipping[/dim]')

    # 3 — Live Last.fm API (optional, for incremental syncs going forward)
    if args.live:
        _load_env()
        api_key = os.environ.get('LASTFM_API_KEY')
        user    = os.environ.get('LASTFM_USER')
        if not (api_key and user):
            console.print('[red]Set LASTFM_API_KEY and LASTFM_USER in music/.env[/red]')
        else:
            console.print(f'[bold]Last.fm API:[/bold] {user}')
            since = args.since or 0
            if since:
                console.print(f'  fetching scrobbles after {datetime.fromtimestamp(since, tz=timezone.utc)}')
            _drain('Last.fm API', _iter_lastfm_api(api_key, user, since=since))

    # Auto-match whatever we can via track MBID
    console.print()
    console.print('[bold]Auto-matching by track MBID…[/bold]')
    matched = bulk_rematch(conn)
    console.print(f'  [green]{matched:,} listens matched to catalog tracks[/green]')

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

def _print_sp_result(i, r):
    """Print one Spotify search candidate, mirroring mdb.py _print_member layout."""
    vtype     = _detect_variant_type(r['name'])
    vtype_str = f'  [yellow]{vtype}[/yellow]' if vtype else ''
    console.print(
        f'  [bold]{i}.[/bold]  [bold]{r["name"]}[/bold]'
        f'  [dim]{r["year"] or "?"}[/dim]'
        f'  [dim]{r["artist"]}[/dim]'
        f'{vtype_str}'
    )
    console.print(f'       [dim cyan]sp:{r["id"]}[/dim cyan]')


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

            console.rule(f'[dim]{album_i}/{len(rows)}[/dim]', style='dim')
            console.print(f'  [bold]{artist}[/bold]  [dim]—  {album}[/dim]')
            if sort_recent:
                last_dt = datetime.fromtimestamp(row['last_listened'], tz=timezone.utc).strftime('%Y-%m-%d')
                console.print(f'  [dim]{count:,} listens in session · last played {last_dt}[/dim]\n')
            else:
                console.print(f'  [dim]{count:,} listens · {tracks} unique tracks[/dim]\n')

            # Auto-search Spotify
            sp_results = []
            if token and artist and album:
                sp_results = _sp_search_album(token, artist, album)
                for idx, r in enumerate(sp_results, 1):
                    _print_sp_result(idx, r)
                if sp_results:
                    console.print()

            # Build hint (escape brackets so Rich doesn't consume them as markup)
            if sp_results:
                if len(sp_results) == 1:
                    hint = '  [dim]Enter=import  \\[s]kip  \\[h]ide  \\[q]uit:[/dim] '
                else:
                    hint = (
                        f'  [dim]\\[1-{len(sp_results)}] import  \\[1 2] multi'
                        f'  \\[s]kip  \\[h]ide  \\[q]uit:[/dim] '
                    )
            else:
                hint = '  [dim]\\[s]kip  \\[h]ide  \\[q]uit:[/dim] '

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

                elif raw.startswith('http'):
                    _do_import(conn, raw, raw_artist=artist, raw_album=album)
                    break

                elif sp_results:
                    # Enter with a single result → auto-import it
                    if choice == '' and len(sp_results) == 1:
                        indices = [0]
                    else:
                        indices = _parse_indices(choice, len(sp_results))
                    if indices is not None:
                        selected = [sp_results[i] for i in indices]
                        _do_multi_import(conn, selected, raw_artist=artist, raw_album=album)
                        break
                    console.print(
                        f'  [yellow]Enter 1–{len(sp_results)}, '
                        f'multiple like "1 2", s, h, or q[/yellow]'
                    )
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
    _do_multi_import(conn, [{'id': _sp_id_from_url(url), 'url': url, 'name': url}],
                     raw_artist=raw_artist, raw_album=raw_album)


def _sp_id_from_url(url: str) -> str:
    """Extract Spotify album ID from a URL, or return the URL itself."""
    m = re.search(r'spotify\.com/album/([A-Za-z0-9]+)', url)
    return m.group(1) if m else url


def _do_multi_import(conn: sqlite3.Connection, selected: list,
                     raw_artist: str = None, raw_album: str = None):
    """
    Import one or more Spotify albums via mdb.py.
    After import, bulk-rematch all listens (MBID-based, then name-based if context given).
    If multiple albums imported, offer to link them as variants.
    """
    imported = []  # list of (sp_item, release_row)

    for item in selected:
        console.print(f'  Importing [dim]{item["url"]}[/dim]')
        result = subprocess.run([PYTHON, MDB, 'import', item['url']])
        if result.returncode == 0:
            sp_id = item.get('id') or _sp_id_from_url(item['url'])
            rel = conn.execute(
                'SELECT id, title FROM releases WHERE spotify_id = ?', [sp_id]
            ).fetchone()
            if rel:
                imported.append((item, rel))
        else:
            console.print(f'  [red]Import failed for {item.get("name", item["url"])}[/red]')

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
    console.print('  [dim]number / \\[s]kip / \\[q]uit / Spotify URL:[/dim] ', end='')

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
            sp_id = _sp_id_from_url(raw)
            rel = conn.execute(
                'SELECT id, title FROM releases WHERE spotify_id = ?', [sp_id]
            ).fetchone()
            if not rel:
                console.print(f'  Importing [dim]{raw}[/dim]')
                result = subprocess.run([PYTHON, MDB, 'import', raw])
                if result.returncode != 0:
                    console.print('  [red]Import failed — try again[/red]')
                    console.print(
                        '  [dim]number / \\[s]kip / \\[q]uit / Spotify URL:[/dim] ', end=''
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

        else:
            console.print(f'  [yellow]Enter 1–{len(full_releases)}, s, q, or a Spotify URL[/yellow]')

        console.print('  [dim]number / \\[s]kip / \\[q]uit / Spotify URL:[/dim] ', end='')

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
    pf = sub.add_parser('fetch', help='Load listens from parquet / old sqlite / Last.fm API')
    pf.add_argument('--parquet', metavar='FILE',
                    help='Path to recenttracks.parquet (default: ~/Downloads/recenttracks.parquet)')
    pf.add_argument('--sqlite',  metavar='FILE',
                    help='Path to listening_history.sqlite (default: music/listening_history.sqlite)')
    pf.add_argument('--live',    action='store_true',
                    help='Also fetch from Last.fm API (requires LASTFM_API_KEY + LASTFM_USER in .env)')
    pf.add_argument('--since',   type=int, metavar='TIMESTAMP',
                    help='Unix timestamp — only fetch scrobbles after this point (for --live)')

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
