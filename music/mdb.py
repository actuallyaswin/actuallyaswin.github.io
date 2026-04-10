#!/usr/bin/env python3
"""
mdb — Music Database CLI

Consolidated import and enrichment tool for master.sqlite.

Usage:
  mdb import  <album…|file>               Import Spotify album(s) + MB + AOTY + Wikipedia
  mdb enrich  aoty  [options]             Scrape AOTY for genres, dates, and types
  mdb enrich  art   [options]             Fill in missing album art (CAA → Spotify → manual)
  mdb enrich  dates [options]             Look up release dates via Wikipedia + MusicBrainz
  mdb enrich  tracks [options]            Fetch track MBIDs from MusicBrainz
  mdb delete  <releases|artists> <ID…>    Delete releases/artists (cascades to tracks)
  mdb hide    <artists|tracks|releases>   <csv>  Bulk hide/unhide
  mdb artist  images <csv>               Bulk update artist profile images
  mdb tracks  variants [--all]           Interactive editor for track variant groups

Default flags:
  --no-mb     skip MusicBrainz during import
  --no-aoty   skip AOTY scraping during import
  --no-wiki   skip Wikipedia date lookup during import
"""

import argparse
import csv
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from rich.console import Console
from mdb_strings import (
    resolve_title,
    is_valid_mbid,
    detect_variant_type, detect_variant_types, _base_title, VARIANT_TYPES,
    _PRIMARY_TYPES, _SECONDARY_TYPES, _EDITION_TYPES,
    ascii_key as _norm,
    MONTHS, _SOURCE_PRIORITY, _date_prec, _should_update_date, _parse_user_date,
    extract_mbid,
    extract_spotify_id,
)
from mdb_ops import (
    new_ulid, slugify, unique_slug, load_dotenv,
    DB_PATH, open_db, init_schema,
    _best_image, _sp_type, _VARIOUS_ARTISTS_SPOTIFY_ID, _is_various_artists,
    upsert_artist, upsert_release, upsert_tracks,
    upsert_artist_mb, upsert_release_mb, upsert_tracks_mb,
    populate_genre_relations,
    bulk_rematch, bulk_rematch_by_name,
    upsert_external_link, EL_ARTIST, EL_RELEASE, EL_SVC_WIKIPEDIA,
)
from mdb_apis import (
    SpotifyClient, SpotifyRelease,
    MusicBrainzRelease,
    MB_API, MB_UA, SP_TOKEN,
    AOTY_AHEAD, DATES_AHEAD,
    caa_fetch_front_image_url,
    _mb_get, _mb_get_safe,
    mb_find_release, mb_fetch_recording_ids, mb_fetch_artist_data,
    _EDITION_RE,
)
from mdb_websources import (
    AOTY_TYPE_MAP,
    find_aoty_url, scrape_aoty_page, fetch_aoty_data,
    _has_aoty, _fmt_aoty, save_aoty_data,
    fetch_wikipedia_date, search_wikipedia, fetch_date_candidates, _save_date,
    _wiki_url_to_id,
)
from mdb_cli import (
    _fmt_dur, _trunc,
    _format_mb_type, _print_member,
    _aoty_prompt, _dates_prompt, _prompt_choice,
    render_diff,
    cmd_track_variants,
)

try:
    import requests
    from bs4 import BeautifulSoup
    _AOTY_AVAILABLE = True
except ImportError:
    _AOTY_AVAILABLE = False

console = Console(width=80, highlight=False)
log = logging.getLogger(__name__)

# ── Batch file reader ─────────────────────────────────────────────────────────

_CSV_ID_COLS  = {'url', 'spotify_url', 'spotify_id', 'id', 'album_id'}
_RE_SP_URL    = re.compile(r'https?://open\.spotify\.com/(?:album|prerelease)/([A-Za-z0-9]+)(?:\?[^\s,]*)?',
                           re.IGNORECASE)
_RE_DISC_ANN  = re.compile(r'\(\s*discs?\s+([\d,\s\-]+?)(?:\s+only)?\s*\)', re.IGNORECASE)

def _parse_disc_annotation(text):
    """Parse '(disc 3 only)' / '(discs 1, 2)' / '(disc 1-2)' → list[int] or None."""
    m = _RE_DISC_ANN.search(text)
    if not m:
        return None
    raw     = m.group(1).strip()
    range_m = re.fullmatch(r'(\d+)\s*-\s*(\d+)', raw)
    if range_m:
        return list(range(int(range_m.group(1)), int(range_m.group(2)) + 1))
    return [int(x.strip()) for x in raw.split(',') if x.strip().isdigit()]

def _parse_group_line(line):
    """Parse one import-file line into a list of album-entry dicts.

    Each entry is either:
      {'url': str, 'album_id': str, 'discs': list[int]|None}   — Spotify
      {'url': str, 'mbid': str,     'discs': None}              — MusicBrainz
    A comma-separated line produces multiple entries (a variant group).
    Prerelease URLs (open.spotify.com/prerelease/…) are skipped.
    """
    entries = []
    for token in re.split(r',\s*', line.strip()):
        token = token.strip()
        if not token or token.startswith('#'):
            continue
        if re.search(r'open\.spotify\.com/prerelease/', token, re.IGNORECASE):
            console.print(f'[dim]  skip prerelease  {token[:60]}[/dim]')
            continue
        # MusicBrainz URL or bare MBID
        mbid = extract_mbid(token)
        if mbid:
            entries.append({
                'url':  token if 'musicbrainz.org' in token else f'https://musicbrainz.org/release/{mbid}',
                'mbid': mbid,
                'discs': None,
            })
            continue
        # Spotify URL
        m = _RE_SP_URL.search(token)
        if not m:
            continue
        album_id = m.group(1)
        after    = token[m.end():]
        entries.append({
            'url':      f'https://open.spotify.com/album/{album_id}',
            'album_id': album_id,
            'discs':    _parse_disc_annotation(after),
        })
    return entries

def read_ids_from_file(path):
    """Return list[list[dict]] — each inner list is a variant group of album entries."""
    ext    = os.path.splitext(path)[1].lower()
    lines  = []
    with open(path, newline='', encoding='utf-8') as f:
        if ext == '.csv':
            reader = csv.reader(f)
            first  = next(reader, None)
            if first is None:
                return []
            lower  = [c.strip().lower() for c in first]
            match  = next((i for i, h in enumerate(lower) if h in _CSV_ID_COLS), None)
            col    = match if match is not None else 0
            if match is None:
                val = first[0].strip()
                if val and not val.startswith('#'):
                    lines.append(val)
            for row in reader:
                if row:
                    val = row[col].strip() if col < len(row) else ''
                    if val and not val.startswith('#'):
                        lines.append(val)
        elif ext in ('.yaml', '.yml'):
            for line in f:
                line = line.strip()
                if line.startswith('-'):
                    val = line[1:].strip().strip('"\'')
                    if val and not val.startswith('#'):
                        lines.append(val)
        else:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    lines.append(line)
    groups = []
    for line in lines:
        group = _parse_group_line(line)
        if group:
            groups.append(group)
    return groups

def import_album(db_path, client, album_url, use_mb=True, discs=None):
    """Import a Spotify album. Returns (release_id, title, artist_name, release_date)."""
    album_id   = (re.search(r'(?:album|prerelease)/([A-Za-z0-9]+)', album_url) or type('m', (), {'group': lambda s, i: album_url})()).group(1)
    album      = client.get_album(album_id)
    all_tracks = album['_all_tracks']
    year       = (album.get('release_date') or '?')[:4]
    artist_str = ', '.join(a['name'] for a in album['artists'])

    console.print(f"[bold]{album['name']}[/bold]  "
                  f"[dim]{artist_str} · {year} · {len(all_tracks)} tracks[/dim]")

    all_artist_ids = list(dict.fromkeys(
        [a['id'] for a in album['artists']] +
        [a['id'] for t in all_tracks for a in t.get('artists', [])]
    ))
    sp_artists  = client.get_artists_batch(all_artist_ids)
    full_tracks = client.get_tracks_batch([t['id'] for t in all_tracks if t.get('id')])
    full_by_id  = {t['id']: t for t in full_tracks if t}
    enriched    = [full_by_id.get(t['id'], t) for t in all_tracks]
    if discs:
        enriched = [t for t in enriched if (t.get('disc_number') or 1) in discs]

    conn = open_db(db_path)
    cur  = conn.cursor()
    init_schema(conn)

    try:
        console.rule('[dim]Artists[/dim]', style='dim')
        artist_map = {}
        for sp_a in sp_artists:
            if not sp_a:
                continue
            if _is_various_artists(sp_a):
                console.print(f"  [dim]skip[/dim]  {sp_a['name']:<28} [dim](Various Artists — not stored)[/dim]")
                continue
            our_id, created = upsert_artist(cur, sp_a)
            artist_map[sp_a['id']] = our_id
            tag = '[green]new[/green]' if created else '[dim]upd[/dim]'
            console.print(f"  {tag}  {sp_a['name']:<28} [dim]{our_id}[/dim]")

        first_album_artist = album['artists'][0]
        primary_id = (None if _is_various_artists(first_album_artist)
                      else artist_map.get(first_album_artist['id']))
        release_id, r_new = upsert_release(cur, album, primary_id)
        tag               = '[green]new[/green]' if r_new else '[dim]upd[/dim]'
        console.print(f"\n  {tag}  [bold]{album['name']}[/bold] [dim]→ {release_id}[/dim]")

        cur.execute('DELETE FROM release_artists WHERE release_id = ?', (release_id,))
        for sp_a in album['artists']:
            aid = artist_map.get(sp_a['id'])
            if aid:
                try:
                    cur.execute('INSERT INTO release_artists (release_id, artist_id) VALUES (?, ?)',
                                (release_id, aid))
                except sqlite3.IntegrityError:
                    pass

        mb_by_isrc, mb_by_title = {}, {}
        if use_mb:
            console.rule('[dim]MusicBrainz[/dim]', style='dim')

            # Check if we already have a stable MBID — if so, skip the search
            # and just re-fetch recording IDs to avoid MBID churn on re-import
            existing_mb = conn.execute(
                'SELECT mbid FROM releases WHERE id = ?', (release_id,)
            ).fetchone()
            existing_mbid = existing_mb[0] if existing_mb else None

            if existing_mbid:
                mb_id, mb_score = existing_mbid, 100
                console.print(f'  [dim]using stored MBID[/dim]  [dim]{mb_id}[/dim]')
            else:
                year_int       = int(year) if year.isdigit() else 0
                mb_id, mb_score = mb_find_release(album['name'], album['artists'][0]['name'],
                                                   len(all_tracks), year_int)

            if mb_id:
                mb_by_isrc, mb_by_title, rg_mbid = mb_fetch_recording_ids(mb_id)
                updates = {}
                if not existing_mbid:
                    updates['mbid'] = mb_id
                if rg_mbid:
                    updates['release_group_mbid'] = rg_mbid
                mb_stored = True
                if updates:
                    set_clause = ', '.join(f'{k} = ?' for k in updates)
                    try:
                        cur.execute(f'UPDATE releases SET {set_clause} WHERE id = ?',
                                    (*updates.values(), release_id))
                    except sqlite3.IntegrityError:
                        # Another release already has this MBID — skip MB data
                        mb_by_isrc, mb_by_title, mb_stored = {}, {}, False
                        console.print(f'  [yellow]⚠[/yellow]  [dim]MBID {mb_id} already claimed — skipping MB[/dim]')
                if mb_stored:
                    matched = sum(
                        1 for t in enriched
                        if ((t.get('external_ids') or {}).get('isrc') in mb_by_isrc)
                        or (_norm(t.get('name', '')) in mb_by_title)
                    )
                    console.print(f"  [green]✓[/green]  [dim]{mb_id}[/dim]  "
                                  f"[dim]score {mb_score} · {matched}/{len(all_tracks)} tracks[/dim]")
            else:
                console.print('  [dim]no match found[/dim]')

        tr_created, tr_updated = upsert_tracks(cur, release_id, enriched, artist_map,
                                               mb_by_isrc, mb_by_title)
        conn.commit()

        # ── Infer primary artist for Various Artists releases ────────────────
        if primary_id is None:
            rows = conn.execute('''
                SELECT a.id, a.name, COUNT(*) as n
                FROM track_artists ta
                JOIN tracks t ON t.id = ta.track_id
                JOIN artists a ON a.id = ta.artist_id
                WHERE t.release_id = ? AND ta.role = 'main'
                GROUP BY a.id
                ORDER BY n DESC
                LIMIT 5
            ''', [release_id]).fetchall()

            if rows:
                total_tracks = conn.execute(
                    'SELECT COUNT(*) FROM tracks WHERE release_id = ?', [release_id]
                ).fetchone()[0]
                top_id, top_name, top_count = rows[0]
                console.rule('[dim]Primary Artist[/dim]', style='dim')
                for aid, aname, n in rows:
                    bar = '█' * round(n / total_tracks * 24)
                    console.print(f'  {aname:<30}  [dim]{n:>3}/{total_tracks}[/dim]  [green]{bar}[/green]')
                console.print()
                try:
                    ans = input(f'  Set "{top_name}" as primary artist? [Y/n] ').strip().lower()
                except (EOFError, KeyboardInterrupt):
                    ans = 'n'
                if ans in ('', 'y', 'yes'):
                    conn.execute(
                        'UPDATE releases SET primary_artist_id = ? WHERE id = ?',
                        [top_id, release_id]
                    )
                    conn.commit()
                    console.print(f'  [green]✓[/green]  primary artist → {top_name}')
                else:
                    console.print('  [dim]keeping primary_artist_id = NULL[/dim]')
    finally:
        conn.close()

    console.rule('[dim]Tracks[/dim]', style='dim')
    max_disc = max((t.get('disc_number') or 1) for t in enriched)
    any_isrc = any((t.get('external_ids') or {}).get('isrc') for t in enriched)
    cur_disc = None
    for t in enriched:
        disc  = t.get('disc_number') or 1
        num   = t.get('track_number', '?')
        title = t.get('name', '?')
        dur   = _fmt_dur(t.get('duration_ms'))
        isrc  = (t.get('external_ids') or {}).get('isrc', '')
        mb_ok = (isrc and isrc in mb_by_isrc) or _norm(title) in mb_by_title
        mb_ic = '[green]✓[/green]' if mb_ok else '[dim]·[/dim]'
        if max_disc > 1 and disc != cur_disc:
            cur_disc = disc
            console.print(f'\n  [bold dim]Disc {disc}[/bold dim]')
        isrc_col = f'  [dim]{isrc:<12}[/dim]' if any_isrc else ''
        console.print(f"  [dim]{num:>2}.[/dim]  {_trunc(title, 40):<40}  "
                      f"[dim]{dur:>5}[/dim]{isrc_col}  {mb_ic}")
    console.print(f'\n  [dim]{tr_created} created · {tr_updated} updated[/dim]  '
                  f'[bold]{os.path.basename(db_path)}[/bold]')

    return release_id, album['name'], album['artists'][0]['name'], album.get('release_date', '')

# ── Variant / source helpers ──────────────────────────────────────────────────

def pick_canonical(group_results):
    """Return index of the canonical release in a group.

    group_results: list of (release_id, title, release_date).
    Canonical = earliest-dated release with no edition qualifier in its title.
    Ties broken by date alone.
    """
    def score(item):
        _, title, date = item
        return (0 if detect_variant_type(title) is None else 1, date or '9999')
    return min(range(len(group_results)), key=lambda i: score(group_results[i]))

def _write_variant_links(conn, canonical_id, variants):
    """Insert release_variants rows.  variants: [(variant_id, title, sort_order)]"""
    for variant_id, title, sort_order in variants:
        vtypes = detect_variant_types(title)
        vtype_val = ','.join(vtypes) if vtypes else None
        conn.execute(
            '''INSERT INTO release_variants (canonical_id, variant_id, variant_type, sort_order)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(canonical_id, variant_id) DO UPDATE SET
                   variant_type = excluded.variant_type,
                   sort_order   = excluded.sort_order''',
            (canonical_id, variant_id, vtype_val, sort_order),
        )
    conn.commit()

# ── cmd: import ───────────────────────────────────────────────────────────────

def _import_aoty_step(db_path, release_id, release_title, artist_name):
    console.rule('[dim]AOTY[/dim]', style='dim')
    conn       = open_db(db_path)
    cached     = conn.execute('SELECT aoty_url FROM releases WHERE id = ?', (release_id,)).fetchone()
    cached_url = cached[0] if cached else None
    url, data  = fetch_aoty_data(release_title, artist_name, cached_url)
    if _has_aoty(data):
        save_aoty_data(conn, release_id, url, data)
        type_str  = f'  [{data["aoty_type"]}]' if data['aoty_type'] else ''
        date_str  = f'  {data["release_date"]}' if data['release_date'] else ''
        primary   = [n for _, n, _, p in data['genres'] if p]
        genre_str = f'  {", ".join(primary)}' if primary else ''
        console.print(f'  [green]✓[/green]  [dim]{url}[/dim]{type_str}{date_str}{genre_str}')
    else:
        console.print('  [dim]no match[/dim]')
    conn.close()

def _import_wiki_step(db_path, release_id, release_title, artist_name):
    console.rule('[dim]Wikipedia[/dim]', style='dim')
    conn = open_db(db_path)
    row  = conn.execute(
        'SELECT mbid, release_date, date_source FROM releases WHERE id = ?', (release_id,)
    ).fetchone()
    if not row or not row['mbid']:
        console.print('  [dim]no MBID — skipped[/dim]')
        conn.close()
        return
    mbid        = row['mbid']
    ex_date     = row['release_date']
    ex_source   = row['date_source']
    candidates, wiki_page_id = fetch_date_candidates(mbid, release_title, artist_name)
    if candidates:
        best     = candidates[0]
        src      = 'wikipedia' if 'Wikipedia' in best['source'] else 'musicbrainz'
        saved    = _save_date(conn, release_id, best['date'], wiki_page_id, source=src)
        wiki_disp = (f'  [dim]https://en.wikipedia.org/wiki/?curid={wiki_page_id}[/dim]'
                     if wiki_page_id else '')
        if saved:
            console.print(f"  [green]✓[/green]  {best['date']}  [dim]{best['source']}[/dim]"
                          + wiki_disp)
        else:
            console.print(f"  [dim]kept {ex_date} ({ex_source}) — "
                          f"{src} had {best['date']}[/dim]")
    else:
        if wiki_page_id:
            upsert_external_link(conn, EL_RELEASE, release_id, EL_SVC_WIKIPEDIA, str(wiki_page_id))
            conn.commit()
            console.print(f'  [dim]no date found — saved wiki page ID {wiki_page_id}[/dim]')
        else:
            console.print('  [dim]no date found[/dim]')
    conn.close()

# ── DBRelease — wraps a master.sqlite row to match the SpotifyRelease interface ─

class DBRelease:
    """Read-only view of a release in master.sqlite, compatible with render_diff."""

    def __init__(self, raw: str, conn=None):
        key = raw.strip()
        if key.lower().startswith('db:'):
            key = key[3:]
        self._owns_conn = conn is None
        self._conn      = conn or open_db()
        row = (
            self._conn.execute('SELECT * FROM releases WHERE id = ?',       [key]).fetchone() or
            self._conn.execute('SELECT * FROM releases WHERE spotify_id = ?',[key]).fetchone() or
            self._conn.execute('SELECT * FROM releases WHERE mbid = ?',      [key]).fetchone()
        )
        if not row:
            raise ValueError(f'Release not found in DB: {raw!r}')
        self._row    = dict(row)
        self._tracks = None  # lazy

    def __del__(self):
        if self._owns_conn:
            try:
                self._conn.close()
            except Exception:
                pass

    @property
    def id(self) -> str:
        return self._row['id']

    @property
    def name(self) -> str:
        return self._row['title']

    @property
    def artist(self) -> str:
        aid = self._row.get('primary_artist_id')
        if not aid:
            return ''
        row = self._conn.execute('SELECT name FROM artists WHERE id = ?', [aid]).fetchone()
        return row[0] if row else ''

    @property
    def year(self) -> str:
        return (self._row.get('release_date') or '')[:4]

    @property
    def date(self) -> str:
        return self._row.get('release_date') or ''

    @property
    def tracks(self) -> list:
        if self._tracks is None:
            self._tracks = self._load_tracks()
        return self._tracks

    def _load_tracks(self) -> list:
        rows = self._conn.execute('''
            SELECT t.id, t.title, t.duration_ms, t.is_explicit
            FROM   tracks t
            WHERE  t.release_id = ? AND t.hidden = 0
            ORDER  BY t.disc_number, t.track_number
        ''', [self._row['id']]).fetchall()
        result = []
        for row in rows:
            artist_rows = self._conn.execute('''
                SELECT a.name FROM artists a
                JOIN   track_artists ta ON ta.artist_id = a.id
                WHERE  ta.track_id = ?
                ORDER  BY ta.rowid
            ''', [row['id']]).fetchall()
            result.append({
                'name':        row['title'],
                'duration_ms': row['duration_ms'],
                'explicit':    bool(row['is_explicit']),
                'artists':     [{'name': r[0]} for r in artist_rows],
            })
        return result

    @property
    def track_count(self) -> int:
        return len(self.tracks)

    @property
    def explicit_count(self) -> int:
        return sum(1 for t in self.tracks if t.get('explicit'))

    @property
    def total_ms(self) -> int:
        return sum(t.get('duration_ms') or 0 for t in self.tracks)

    @property
    def label(self) -> str:
        return self._row.get('label') or ''

    @property
    def album_type(self) -> str:
        return self._row.get('type') or ''

    def canonical_score(self) -> tuple:
        d    = self.date
        prec = (3 if (len(d) == 10 and not d.endswith('-01-01'))
                else (2 if len(d) == 7 else 1))
        return (-prec, d, self.track_count,
                1 if _EDITION_RE.search(self.name) else 0,
                -self.explicit_count)


def cmd_diff(args):
    """Compare two or more Spotify, MusicBrainz, or DB releases."""
    load_dotenv()
    cid = os.environ.get('SPOTIFY_CLIENT_ID')
    csc = os.environ.get('SPOTIFY_CLIENT_SECRET')

    db_path  = getattr(args, 'db', None) or DB_PATH
    db_conn  = None   # shared connection for all DBRelease instances in this run
    client   = None
    releases = []
    for album in args.albums:
        key = album.strip()
        is_db = (
            key.lower().startswith('db:') or
            (re.match(r'^[0-9A-Z]{26}$', key) and not re.match(r'^[A-Za-z0-9]{22}$', key))
        )
        if is_db:
            if db_conn is None:
                db_conn = open_db(db_path)
                init_schema(db_conn)
            try:
                releases.append(DBRelease(key, conn=db_conn))
            except ValueError as e:
                console.print(f'[red]Error:[/red] {e}')
                sys.exit(1)
        else:
            mbid = extract_mbid(album)
            if mbid:
                releases.append(MusicBrainzRelease(mbid))
            else:
                if client is None:
                    if not (cid and csc):
                        console.print('[red]Error:[/red] SPOTIFY_CLIENT_ID and'
                                      ' SPOTIFY_CLIENT_SECRET must be set for Spotify entries.')
                        sys.exit(1)
                    client = SpotifyClient(cid, csc)
                releases.append(SpotifyRelease(album, client=client))
    render_diff(*releases)


def import_album_from_mb(db_path: str, mbid: str, *,
                         use_aoty: bool = True,
                         use_wiki: bool = True) -> 'tuple[str, str, str, str]':
    """Import a MusicBrainz release into master.sqlite.
    Returns (release_id, title, artist_name, release_date)."""
    rel = MusicBrainzRelease(mbid)
    rel._ensure_full()

    console.print(f"[bold]{rel.name}[/bold]  "
                  f"[dim]{rel.artist} · {rel.year} · {rel.track_count} tracks[/dim]")
    console.print(f"  [dim]source: MusicBrainz  {mbid}[/dim]")

    # Cover Art Archive
    console.print('  [dim]Fetching cover art from Cover Art Archive…[/dim]')
    try:
        image_url = caa_fetch_front_image_url(mbid)
    except Exception as e:
        log.debug('CAA fetch failed: %s', e)
        image_url = None
    console.print(f'  [dim]cover art: {image_url[:70] if image_url else "not found"}[/dim]')

    conn = open_db(db_path)
    cur  = conn.cursor()
    init_schema(conn)

    try:
        console.rule('[dim]Artists[/dim]', style='dim')

        # Collect all unique MB artist IDs from release + track credits
        artist_credits_seen: dict = {}  # {mb_artist_id: mb_artist_dict}
        for credit in (rel._data.get('artist-credit') or []):
            if isinstance(credit, dict) and 'artist' in credit:
                mb_a = credit['artist']
                artist_credits_seen[mb_a.get('id', '')] = mb_a
        for t in rel.tracks:
            for credit in (t.get('_artist_credit') or []):
                if isinstance(credit, dict) and 'artist' in credit:
                    mb_a   = credit['artist']
                    mb_aid = mb_a.get('id', '')
                    if mb_aid not in artist_credits_seen:
                        artist_credits_seen[mb_aid] = mb_a

        artist_map: dict = {}  # {mb_artist_id: our_artist_id}
        for mb_aid, mb_a in artist_credits_seen.items():
            our_id, created = upsert_artist_mb(cur, mb_a)
            artist_map[mb_aid] = our_id
            tag = '[green]new[/green]' if created else '[dim]upd[/dim]'
            console.print(f"  {tag}  {mb_a.get('name', ''):<28} [dim]{our_id}[/dim]")

        # Primary artist: first non-join credit on the release
        primary_id = None
        for credit in (rel._data.get('artist-credit') or []):
            if isinstance(credit, dict) and 'artist' in credit:
                primary_id = artist_map.get(credit['artist'].get('id', ''))
                break

        release_id, r_new = upsert_release_mb(cur, rel._data, primary_id, image_url)
        tag = '[green]new[/green]' if r_new else '[dim]upd[/dim]'
        console.print(f"\n  {tag}  [bold]{rel.name}[/bold] [dim]→ {release_id}[/dim]")

        cur.execute('DELETE FROM release_artists WHERE release_id = ?', (release_id,))
        for credit in (rel._data.get('artist-credit') or []):
            if isinstance(credit, dict) and 'artist' in credit:
                aid = artist_map.get(credit['artist'].get('id', ''))
                if aid:
                    try:
                        cur.execute(
                            'INSERT INTO release_artists (release_id, artist_id, role)'
                            ' VALUES (?, ?, ?)',
                            (release_id, aid, 'main'),
                        )
                    except sqlite3.IntegrityError:
                        pass

        console.rule('[dim]Tracks[/dim]', style='dim')
        n_created, n_updated = upsert_tracks_mb(cur, release_id, rel.tracks, artist_map)

        conn.commit()
    finally:
        conn.close()

    # ── Tracklist display ────────────────────────────────────────────────────
    max_disc = max((t.get('_disc_number') or 1) for t in rel.tracks) if rel.tracks else 1
    any_isrc = any(t.get('_isrcs') for t in rel.tracks)
    cur_disc = None
    for t in rel.tracks:
        disc  = t.get('_disc_number') or 1
        num   = t.get('_track_number', '?')
        title = t.get('name', '?')
        dur   = _fmt_dur(t.get('duration_ms'))
        isrcs = t.get('_isrcs') or []
        isrc  = isrcs[0] if isrcs else ''
        if max_disc > 1 and disc != cur_disc:
            cur_disc = disc
            console.print(f'\n  [bold dim]Disc {disc}[/bold dim]')
        isrc_col = f'  [dim]{isrc:<12}[/dim]' if any_isrc else ''
        console.print(f"  [dim]{num:>2}.[/dim]  {_trunc(title, 40):<40}  "
                      f"[dim]{dur:>5}[/dim]{isrc_col}")
    console.print(f'\n  [dim]{n_created} created · {n_updated} updated[/dim]  '
                  f'[bold]{os.path.basename(db_path)}[/bold]')

    return release_id, rel.name, rel.artist, rel.date


def _auto_rematch(db_path: str, release_id: str, artist_name: str, release_title: str) -> None:
    """Run listen matching for a freshly imported release.

    1. MBID sweep — matches any unmatched listen whose raw_source_id is a
       track MBID now present in the catalog.
    2. Name sweep — filters unmatched listens to groups whose album name
       ascii_key-matches this release's title, then runs bulk_rematch_by_name.
    """
    conn = open_db(db_path)
    try:
        mbid_n = bulk_rematch(conn)

        # Filter candidate groups cheaply in Python using ascii_key comparison,
        # avoiding a full catalog scan (db_search_releases) per group.
        target_key = _norm(release_title)
        groups = conn.execute('''
            SELECT DISTINCT raw_artist_name, raw_album_name
            FROM   listens
            WHERE  track_id IS NULL
              AND  raw_artist_name IS NOT NULL
              AND  raw_album_name  IS NOT NULL
        ''').fetchall()

        name_n = 0
        for raw_artist, raw_album in groups:
            k = _norm(raw_album)
            if k == target_key or target_key in k or k in target_key:
                name_n += bulk_rematch_by_name(conn, [release_id], raw_artist, raw_album)

        total = mbid_n + name_n
        if total:
            console.print(f'  [green]auto-matched {total:,} listen{"s" if total != 1 else ""}[/green]'
                          + (f'  [dim]({mbid_n} mbid, {name_n} name)[/dim]' if mbid_n and name_n else ''))
    finally:
        conn.close()


def cmd_import(args):
    load_dotenv()
    cid = os.environ.get('SPOTIFY_CLIENT_ID')
    csc = os.environ.get('SPOTIFY_CLIENT_SECRET')

    use_aoty = not args.no_aoty
    use_wiki = not args.no_wiki

    if use_aoty and not _AOTY_AVAILABLE:
        console.print('[yellow]Warning:[/yellow] AOTY disabled — pip install requests beautifulsoup4')
        use_aoty = False

    db_path = args.db or DB_PATH
    groups  = []
    for arg in args.albums:
        if os.path.isfile(arg):
            file_groups = read_ids_from_file(arg)
            if not file_groups:
                console.print(f'[yellow]Warning:[/yellow] {arg} contained no album IDs')
            groups.extend(file_groups)
        else:
            parsed = _parse_group_line(arg)
            if parsed:
                groups.append(parsed)
            else:
                groups.append([{'url': arg, 'album_id': arg, 'discs': None}])

    if not groups:
        console.print('[red]Error:[/red] No album IDs found.')
        sys.exit(1)

    # Lazily init Spotify client only if any entry requires it
    client  = None
    total   = sum(len(g) for g in groups)
    errors  = 0
    seq     = 0

    for group in groups:
        group_results = []  # (release_id, title, release_date) or None per entry

        for entry in group:
            seq += 1
            if total > 1:
                console.rule(f'[dim]{seq} / {total}[/dim]', style='dim')
            disc_note = f'  [dim]discs {entry["discs"]}[/dim]' if entry.get('discs') else ''
            if disc_note:
                console.print(disc_note)
            try:
                if entry.get('mbid'):
                    # MusicBrainz import path — no Spotify credentials needed
                    release_id, title, artist, rel_date = import_album_from_mb(
                        db_path, entry['mbid'],
                        use_aoty=use_aoty, use_wiki=use_wiki,
                    )
                else:
                    # Spotify import path — credentials required
                    if client is None:
                        if not (cid and csc):
                            console.print('[red]Error:[/red] SPOTIFY_CLIENT_ID and'
                                          ' SPOTIFY_CLIENT_SECRET must be set for Spotify imports.')
                            sys.exit(1)
                        client = SpotifyClient(cid, csc)
                    release_id, title, artist, rel_date = import_album(
                        db_path, client, entry['url'],
                        use_mb=not args.no_mb,
                        discs=entry['discs'],
                    )
                group_results.append((release_id, title, rel_date))
                if use_aoty and release_id:
                    _import_aoty_step(db_path, release_id, title, artist)
                if use_wiki and release_id and not args.no_mb:
                    _import_wiki_step(db_path, release_id, title, artist)
                if release_id:
                    _auto_rematch(db_path, release_id, artist, title)
            except urllib.error.HTTPError as e:
                console.print(f'[red]HTTP {e.code}:[/red] {e.reason}')
                errors += 1
                group_results.append(None)
            except Exception as e:
                console.print(f'[red]Error:[/red] {e}')
                if total == 1:
                    raise
                errors += 1
                group_results.append(None)

        valid = [x for x in group_results if x is not None]
        if len(valid) > 1:
            canon_idx   = pick_canonical(valid)
            canon_id, canon_title, _ = valid[canon_idx]
            variants    = [
                (rid, vtitle, order)
                for order, (rid, vtitle, _) in enumerate(valid)
                if rid != canon_id
            ]
            console.rule('[dim]Variants[/dim]', style='dim')
            console.print(f'  [bold]Canonical:[/bold] {canon_title}  [dim]{canon_id}[/dim]')
            conn = open_db(db_path)
            _write_variant_links(conn, canon_id, variants)
            conn.close()
            for vid, vtitle, _ in variants:
                vtypes = detect_variant_types(vtitle)
                vtype_label = ','.join(vtypes) if vtypes else 'variant'
                console.print(f'  [dim]{vtype_label}:[/dim]  {vtitle}  [dim]{vid}[/dim]')

    if total > 1:
        console.rule(style='dim')
        ok = total - errors
        console.print(f'  [dim]Batch:[/dim] {ok}/{total} succeeded'
                      + (f'  [red]{errors} failed[/red]' if errors else ''))

# ── cmd: enrich art ──────────────────────────────────────────────────────────

def cmd_enrich_art(args):
    """Fill in missing album art, or interactively replace existing art.

    Auto mode (default): tries Cover Art Archive then Spotify for each release
    with no album_art_url; auto-applies the first found URL without prompting.

    Interactive mode (--interactive): for every release in the queue, displays
    found URLs and prompts for confirmation or a custom URL.  Useful for
    reviewing and replacing art on already-populated releases (combine with
    --overwrite or --release-id).
    """
    load_dotenv()
    cid = os.environ.get('SPOTIFY_CLIENT_ID')
    csc = os.environ.get('SPOTIFY_CLIENT_SECRET')

    conn = open_db(args.db or DB_PATH)
    init_schema(conn)

    # ── Build query ──────────────────────────────────────────────────────────
    params = []

    if args.release_id:
        # Targeting a specific release always processes it regardless of art status
        where = 'WHERE r.id = ? AND r.hidden = 0'
        params = [args.release_id]
    else:
        art_clause    = '' if args.overwrite else "AND (r.album_art_url IS NULL OR r.album_art_url = '')"
        artist_clause = ''
        if args.artist:
            artist_clause = 'AND (ra.artist_id = ? OR LOWER(a.name) = LOWER(?))'
            params        = [args.artist, args.artist]
        where = f'WHERE r.hidden = 0 {art_clause} {artist_clause}'

    rows = conn.execute(f'''
        SELECT DISTINCT r.id, r.title, r.release_year, r.mbid, r.spotify_id,
               r.album_art_url, a.name AS artist_name
        FROM releases r
        LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
        LEFT JOIN artists a ON ra.artist_id = a.id
        {where}
        ORDER BY r.release_year DESC NULLS LAST, r.title
    ''', params).fetchall()

    queue = rows[args.skip:]
    if args.limit:
        queue = queue[:args.limit]

    if not queue:
        console.print('[dim]Nothing to process.[/dim]')
        conn.close()
        return

    console.print(f'[dim]{len(queue)} release{"s" if len(queue) != 1 else ""} to process'
                  + ('  (interactive)' if args.interactive else '') + '[/dim]\n')

    # ── Lazy Spotify client ──────────────────────────────────────────────────
    _sp_client = None
    def _get_sp():
        nonlocal _sp_client
        if _sp_client is None and cid and csc:
            _sp_client = SpotifyClient(cid, csc)
        return _sp_client

    updated = skipped = 0
    now     = int(time.time())

    for i, row in enumerate(queue):
        release_id  = row['id']
        title       = row['title']
        year        = row['release_year'] or '?'
        mbid        = row['mbid']
        spotify_id  = row['spotify_id']
        artist_name = row['artist_name'] or ''
        current_url = row['album_art_url']

        prefix = f'[dim][{i+1}/{len(queue)}][/dim]  '
        console.print(f'{prefix}[bold]{_trunc(title, 40)}[/bold]  [dim]{artist_name} · {year}[/dim]')

        # ── Fetch candidates ─────────────────────────────────────────────────
        caa_url = sp_url = None

        if mbid:
            try:
                caa_url = caa_fetch_front_image_url(mbid)
            except Exception as e:
                console.print(f'  [yellow]CAA error:[/yellow] {e}')

        if spotify_id:
            try:
                client = _get_sp()
                if client:
                    album  = client.get_album(spotify_id)
                    images = album.get('images') or []
                    if images:
                        sp_url = max(images, key=lambda x: (x.get('width') or 0))['url']
            except Exception as e:
                console.print(f'  [yellow]Spotify error:[/yellow] {e}')

        # CAA preferred over Spotify
        auto_url    = caa_url or sp_url
        auto_source = ('musicbrainz' if caa_url else 'spotify') if auto_url else None

        if not args.interactive:
            # ── Auto mode ────────────────────────────────────────────────────
            if auto_url:
                conn.execute(
                    'UPDATE releases SET album_art_url=?, album_art_source=?, updated_at=? WHERE id=?',
                    (auto_url, auto_source, now, release_id),
                )
                conn.commit()
                console.print(f'  [green]✓[/green]  [dim]{auto_source}[/dim]  [dim]{auto_url[:65]}[/dim]')
                updated += 1
            else:
                console.print('  [dim]no art found[/dim]')
                skipped += 1
        else:
            # ── Interactive mode ─────────────────────────────────────────────
            if caa_url:
                console.print(f'  [dim]CAA:[/dim]     {caa_url[:70]}')
            if sp_url:
                console.print(f'  [dim]Spotify:[/dim] {sp_url[:70]}')
            if current_url:
                console.print(f'  [dim]current:[/dim] {current_url[:70]}')
            if not caa_url and not sp_url:
                console.print('  [dim]no art sources found[/dim]')

            # Build keys based on what's available
            both    = caa_url and sp_url
            hint    = '[a] CAA  [b] Spotify' if both else '[a] accept' if auto_url else ''
            prompt  = '  ' + ('  '.join(filter(None, [hint, '[u]rl', '[s]kip', '[q]uit']))) + ': '

            chosen_url = chosen_source = None
            while True:
                try:
                    raw = input(prompt).strip()
                except EOFError:
                    raw = 'q'

                lo = raw.lower()
                if lo == 'q':
                    conn.close()
                    console.rule(style='dim')
                    console.print(f'  [dim]Updated: {updated}  Skipped: {skipped}[/dim]')
                    return
                elif lo in ('s', ''):
                    skipped += 1
                    break
                elif lo == 'a' and auto_url:
                    chosen_url    = caa_url if caa_url else sp_url
                    chosen_source = 'musicbrainz' if caa_url else 'spotify'
                    break
                elif lo == 'b' and sp_url:
                    chosen_url, chosen_source = sp_url, 'spotify'
                    break
                elif lo == 'u' or raw.startswith('http') or raw.startswith('spotify:'):
                    url_in = raw if (raw.startswith('http') or raw.startswith('spotify:')) else input('  URL: ').strip()
                    # Detect Spotify album URL/URI → resolve to actual image
                    sp_id = extract_spotify_id(url_in) if 'spotify' in url_in.lower() else None
                    if sp_id:
                        try:
                            client = _get_sp()
                            if client:
                                album  = client.get_album(sp_id)
                                images = album.get('images') or []
                                if images:
                                    fetched = max(images, key=lambda x: (x.get('width') or 0))['url']
                                    chosen_url, chosen_source = fetched, 'spotify'
                                    break
                                else:
                                    console.print('  [yellow]No images on that Spotify album[/yellow]')
                            else:
                                console.print('  [yellow]Spotify credentials not configured[/yellow]')
                        except Exception as e:
                            console.print(f'  [yellow]Spotify error:[/yellow] {e}')
                    elif url_in.startswith('http'):
                        # Validate the URL resolves to an image via HEAD request
                        try:
                            req = urllib.request.Request(
                                url_in, method='HEAD',
                                headers={'User-Agent': 'actuallyaswin-music/1.0'},
                            )
                            with urllib.request.urlopen(req, timeout=8) as resp:
                                ct = resp.headers.get('Content-Type', '')
                            if ct.startswith('image/'):
                                chosen_url, chosen_source = url_in, 'manual'
                                break
                            else:
                                console.print(f'  [yellow]Not an image URL (Content-Type: {ct or "unknown"})[/yellow]')
                        except Exception as e:
                            console.print(f'  [yellow]Could not validate URL ({e}) — saved anyway[/yellow]')
                            chosen_url, chosen_source = url_in, 'manual'
                            break
                    else:
                        console.print('  [dim]invalid URL[/dim]')
                else:
                    console.print('  [dim]?[/dim]')

            if chosen_url:
                conn.execute(
                    'UPDATE releases SET album_art_url=?, album_art_source=?, updated_at=? WHERE id=?',
                    (chosen_url, chosen_source, now, release_id),
                )
                conn.commit()
                tag = '[yellow]replaced[/yellow]' if current_url else '[green]set[/green]'
                console.print(f'  {tag}  [dim]{chosen_source}[/dim]')
                updated += 1

    conn.close()
    console.rule(style='dim')
    console.print(f'  [dim]Updated: {updated}  Skipped: {skipped}[/dim]')


# ── cmd: enrich aoty ─────────────────────────────────────────────────────────

def cmd_enrich_aoty(args):
    if not _AOTY_AVAILABLE:
        console.print('[red]Error:[/red] pip install requests beautifulsoup4')
        sys.exit(1)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format='  [%(levelname)s] %(message)s',
    )

    conn = open_db(args.db or DB_PATH)
    init_schema(conn)

    # Resolve --artist to an id
    artist_filter = None
    if args.artist:
        row = conn.execute(
            'SELECT id, name FROM artists WHERE id = ? OR LOWER(name) = LOWER(?)',
            (args.artist, args.artist)
        ).fetchone()
        if not row:
            rows = conn.execute(
                "SELECT id, name FROM artists WHERE LOWER(name) LIKE LOWER('%' || ? || '%')",
                (args.artist,)
            ).fetchall()
            if not rows:
                console.print(f'[red]Artist not found:[/red] {args.artist}')
                sys.exit(1)
            if len(rows) > 1:
                for rid, rname in rows:
                    console.print(f'  {rid}  {rname}')
                console.print('[yellow]Multiple matches — use exact name or id.[/yellow]')
                sys.exit(1)
            row = rows[0]
        artist_filter = row[0]
        console.print(f'[dim]Artist: {row[1]} ({artist_filter})[/dim]')

    not_found_clause = "AND aoty_url != 'not_found'" if args.force else ''
    done = set() if args.overwrite_genre else set(
        r[0] for r in conn.execute(f'''
            SELECT DISTINCT release_id FROM release_genres
            UNION
            SELECT id FROM releases WHERE aoty_url IS NOT NULL {not_found_clause}
        ''')
    )

    if args.release_id:
        row = conn.execute('''
            SELECT r.id, r.title, r.release_year, a.name
            FROM releases r
            LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
            LEFT JOIN artists a ON ra.artist_id = a.id
            WHERE r.id = ?
        ''', (args.release_id,)).fetchone()
        if not row:
            console.print(f'[red]Release not found:[/red] {args.release_id}')
            sys.exit(1)
        queue = [row]
    else:
        artist_clause = 'AND r.id IN (SELECT release_id FROM release_artists WHERE artist_id = ? AND role = \'main\')' if artist_filter else ''
        params        = (artist_filter,) if artist_filter else ()
        rows          = conn.execute(f'''
            SELECT r.id, r.title, r.release_year, a.name
            FROM releases r
            LEFT JOIN artists a ON r.primary_artist_id = a.id
            WHERE r.hidden = 0 {artist_clause}
            ORDER BY (
                SELECT COUNT(*) FROM tracks t
                JOIN listens l ON l.track_id = t.id
                WHERE t.release_id = r.id AND t.hidden = 0
            ) DESC, r.release_year DESC NULLS LAST, r.title
        ''', params).fetchall()
        queue = [r for r in rows if r[0] not in done]
        total_skipped = len(rows) - len(queue)
        console.print(f'[dim]{len(rows)} releases  ({total_skipped} already done, '
                      f'{len(queue)} to process)[/dim]')
        queue = queue[args.skip:]
        if args.limit:
            queue = queue[:args.limit]

    console.print(f'[dim]Processing {len(queue)}  '
                  f'(skip={args.skip}, limit={args.limit or "none"}, '
                  f'auto={"yes" if args.auto else "no"})[/dim]')
    if not args.auto:
        console.print('[dim]Press Ctrl+C or type q to stop.[/dim]')
    console.print()

    updated = skipped = marked = 0
    now = int(time.time())

    def submit(entry):
        cached = conn.execute('SELECT aoty_url FROM releases WHERE id = ?',
                               (entry[0],)).fetchone()
        cached_url = cached[0] if cached else None
        if cached_url == 'not_found':
            cached_url = None  # treat sentinel as no cache; do a fresh search
        return executor.submit(fetch_aoty_data, entry[1], entry[3], cached_url)

    with ThreadPoolExecutor(max_workers=AOTY_AHEAD) as executor:
        futures = deque(submit(queue[j]) for j in range(min(AOTY_AHEAD, len(queue))))
        i = 0
        while i < len(queue):
            release_id, release_name, release_year, artist_name = queue[i]
            aoty_url, data = futures.popleft().result()

            nxt = i + AOTY_AHEAD
            if nxt < len(queue):
                futures.append(submit(queue[nxt]))

            console.print(f'[dim][{i+1}/{len(queue)}][/dim]  ', end='')

            if args.auto:
                if _has_aoty(data):
                    save_aoty_data(conn, release_id, aoty_url, data,
                                   overwrite_date=args.overwrite_date,
                                   overwrite_type=args.overwrite_type)
                    type_str  = f'  [{data["aoty_type"]}]' if data['aoty_type'] else ''
                    date_str  = f'  {data["release_date"]}' if data['release_date'] else ''
                    primary   = [n for _, n, _, p in data['genres'] if p]
                    genre_str = f'  {", ".join(primary)}' if primary else ''
                    console.print(f'[bold]{release_name}[/bold]{type_str}{date_str}{genre_str}')
                    updated += 1
                else:
                    if not aoty_url:
                        conn.execute(
                            'UPDATE releases SET aoty_url = ?, updated_at = ? WHERE id = ?',
                            ('not_found', now, release_id)
                        )
                        conn.commit()
                        console.print(f'[dim]{release_name}  — not found (marked)[/dim]')
                        marked += 1
                    else:
                        console.print(f'[dim]{release_name}  — no data[/dim]')
                    skipped += 1
                i += 1
                continue

            action, val_url, val_data = _aoty_prompt(
                release_name, artist_name, aoty_url, data)

            if action == 'quit':
                for f in futures: f.cancel()
                break
            elif action == 'skip':
                if not aoty_url:
                    conn.execute(
                        'UPDATE releases SET aoty_url = ?, updated_at = ? WHERE id = ?',
                        ('not_found', now, release_id)
                    )
                    conn.commit()
                    console.print(f'  [dim]Marked as not found.[/dim]')
                    marked += 1
                skipped += 1
                i += 1
            elif action == 'url':
                new_url, new_data = val_url, scrape_aoty_page(val_url)
                if not _has_aoty(new_data):
                    console.print('  [yellow]Still no data — skipping.[/yellow]')
                    skipped += 1
                    i += 1
                    continue
                action2, _, val_data2 = _aoty_prompt(release_name, artist_name, new_url, new_data)
                if action2 == 'save':
                    save_aoty_data(conn, release_id, new_url, val_data2,
                                   overwrite_date=args.overwrite_date,
                                   overwrite_type=args.overwrite_type)
                    console.print(f'  [green]Saved.[/green]')
                    updated += 1
                else:
                    skipped += 1
                i += 1
            elif action == 'save':
                save_aoty_data(conn, release_id, val_url, val_data,
                               overwrite_date=args.overwrite_date,
                               overwrite_type=args.overwrite_type)
                primary = [n for _, n, _, p in val_data['genres'] if p]
                console.print(f'  [green]Saved:[/green] {", ".join(primary) or "(no genres)"}')
                updated += 1
                i += 1

    conn.close()
    console.rule(style='dim')
    console.print(f'  [dim]Updated: {updated}  Skipped: {skipped}  Marked not-found: {marked}[/dim]')

# ── cmd: enrich dates ─────────────────────────────────────────────────────────

def cmd_enrich_dates(args):
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format='  [%(levelname)s] %(message)s',
    )
    conn = open_db(args.db or DB_PATH)
    init_schema(conn)

    artist_clause = ''
    params        = []
    if args.artist:
        artist_clause = 'AND (ra.artist_id = ? OR LOWER(a.name) = LOWER(?))'
        params        = [args.artist, args.artist]
    release_clause = ''
    if args.release_id:
        release_clause = 'AND r.id = ?'
        params         = [args.release_id]

    overwrite_clause = '' if args.overwrite else 'AND (r.release_date IS NULL OR r.release_date = \'\')'

    rows = conn.execute(f'''
        SELECT DISTINCT r.id, r.title, r.release_year, a.name
        FROM releases r
        LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
        LEFT JOIN artists a ON ra.artist_id = a.id
        WHERE r.mbid IS NOT NULL AND r.hidden = 0
        {overwrite_clause} {artist_clause} {release_clause}
        ORDER BY r.release_year DESC NULLS LAST, r.title
    ''', params).fetchall()

    queue = rows[args.skip:]
    if args.limit:
        queue = queue[:args.limit]

    console.print(f'[dim]{len(rows)} releases need dates, processing {len(queue)}  '
                  f'(skip={args.skip}, limit={args.limit or "none"})[/dim]')
    console.print('[dim]Press Ctrl+C or type q to stop.[/dim]\n')

    updated = skipped = 0

    def submit(entry):
        return executor.submit(fetch_date_candidates, entry[0], entry[1], entry[3])

    with ThreadPoolExecutor(max_workers=DATES_AHEAD) as executor:
        futures = deque(submit(queue[j]) for j in range(min(DATES_AHEAD, len(queue))))

        for i, (release_id, release_name, release_year, artist_name) in enumerate(queue):
            candidates, wiki_page_id = futures.popleft().result()

            nxt = i + DATES_AHEAD
            if nxt < len(queue):
                futures.append(submit(queue[nxt]))

            console.print(f'[dim][{i+1}/{len(queue)}][/dim]  ', end='')

            if not candidates:
                if wiki_page_id:
                    upsert_external_link(conn, EL_RELEASE, release_id, EL_SVC_WIKIPEDIA, str(wiki_page_id))
                    conn.commit()
                console.print(f'[dim]{release_name}  — no date found[/dim]')
                skipped += 1
                continue

            choice = _dates_prompt(candidates, release_name, artist_name, release_year)

            if choice == 'QUIT':
                for f in futures: f.cancel()
                break
            elif choice is None:
                console.print('  [dim]Skipped.[/dim]')
                skipped += 1
            else:
                _save_date(conn, release_id, choice, wiki_page_id, source='manual')
                console.print(f'  [green]Saved:[/green] {choice}')
                updated += 1

    conn.close()
    console.rule(style='dim')
    console.print(f'  [dim]Updated: {updated}  Skipped: {skipped}[/dim]')

# ── cmd: enrich tracks ────────────────────────────────────────────────────────

def cmd_enrich_tracks(args):
    conn = open_db(args.db or DB_PATH)
    init_schema(conn)

    artist_clause  = ''
    release_clause = ''
    params         = []
    if args.artist:
        artist_clause = 'AND (ra.artist_id = ? OR LOWER(a.name) = LOWER(?))'
        params        = [args.artist, args.artist]
    if args.release_id:
        release_clause = 'AND r.id = ?'
        params         = [args.release_id]

    rows = conn.execute(f'''
        SELECT DISTINCT r.id, r.title, r.mbid
        FROM releases r
        LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
        LEFT JOIN artists a ON ra.artist_id = a.id
        WHERE r.mbid IS NOT NULL AND r.hidden = 0
        AND EXISTS (SELECT 1 FROM tracks t WHERE t.release_id = r.id AND t.mbid IS NULL)
        {artist_clause} {release_clause}
        ORDER BY r.title
    ''', params).fetchall()

    queue = rows[args.skip:]
    if args.limit:
        queue = queue[:args.limit]

    console.print(f'[dim]{len(rows)} releases need track MBIDs, processing {len(queue)}[/dim]\n')

    total_matched = 0
    for i, (release_id, title, mbid) in enumerate(queue):
        console.print(f'[dim][{i+1}/{len(queue)}][/dim]  [bold]{title}[/bold]', end='')
        by_isrc, by_title, _ = mb_fetch_recording_ids(mbid)
        if not by_isrc and not by_title:
            console.print('  [dim]no MB recordings[/dim]')
            continue
        tracks  = conn.execute(
            'SELECT id, title, isrc FROM tracks WHERE release_id = ? AND mbid IS NULL',
            (release_id,)
        ).fetchall()
        matched = 0
        now     = int(time.time())
        for track_id, track_title, isrc in tracks:
            mb_id = (by_isrc.get(isrc) if isrc else None) or by_title.get(_norm(track_title))
            if mb_id:
                conn.execute('UPDATE OR IGNORE tracks SET mbid = ?, updated_at = ? WHERE id = ?',
                             (mb_id, now, track_id))
                matched += 1
        conn.commit()
        console.print(f'  [dim]{matched}/{len(tracks)} matched[/dim]')
        total_matched += matched

    conn.close()
    console.rule(style='dim')
    console.print(f'  [dim]Matched {total_matched} track MBIDs across {len(queue)} releases[/dim]')

# ── cmd: enrich audio ─────────────────────────────────────────────────────────

def cmd_enrich_audio(args):
    """Fetch Spotify audio features for tracks that don't have them yet."""
    load_dotenv()
    cid = os.environ.get('SPOTIFY_CLIENT_ID')
    csc = os.environ.get('SPOTIFY_CLIENT_SECRET')
    if not cid or not csc:
        console.print('[red]SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET not set[/red]')
        return
    client = SpotifyClient(cid, csc)
    conn   = open_db(args.db or DB_PATH)
    init_schema(conn)

    where = 'WHERE t.spotify_id IS NOT NULL AND t.audio_features IS NULL AND t.hidden = 0'
    params = []
    if args.artist:
        row = _resolve_artist(conn, args.artist)
        if not row:
            console.print(f'[red]Artist not found: {args.artist}[/red]')
            conn.close(); return
        where += ' AND r.primary_artist_id = ?'
        params.append(row['id'])
    if args.release_id:
        where += ' AND t.release_id = ?'
        params.append(args.release_id)

    rows = conn.execute(f'''
        SELECT t.id, t.spotify_id, t.title, r.title AS album
        FROM tracks t JOIN releases r ON r.id = t.release_id
        {where}
        ORDER BY r.release_year DESC, r.title, t.disc_number, t.track_number
    ''', params).fetchall()

    if not rows:
        console.print('[dim]No tracks to enrich.[/dim]')
        conn.close(); return

    console.print(f'[dim]Fetching audio features for {len(rows)} tracks…[/dim]')
    sp_ids    = [r['spotify_id'] for r in rows]
    id_to_row = {r['spotify_id']: r for r in rows}
    features  = client.get_audio_features_batch(sp_ids)

    _AF_BLOB_KEYS = ('energy', 'danceability', 'valence', 'acousticness',
                     'instrumentalness', 'liveness', 'speechiness',
                     'key', 'mode', 'time_signature')

    updated = 0
    now     = int(time.time())
    for feat in features:
        if not feat:
            continue
        sid = feat.get('id')
        if sid not in id_to_row:
            continue
        tempo = feat.get('tempo')
        blob  = {k: feat[k] for k in _AF_BLOB_KEYS if feat.get(k) is not None}
        conn.execute(
            'UPDATE tracks SET tempo_bpm = ?, audio_features = ?, updated_at = ? WHERE spotify_id = ?',
            (tempo, json.dumps(blob) if blob else None, now, sid)
        )
        updated += 1

    conn.commit()
    conn.close()
    console.rule(style='dim')
    console.print(f'  [dim]Updated {updated}/{len(rows)} tracks with audio features[/dim]')

# ── cmd: enrich artists ────────────────────────────────────────────────────────

def cmd_enrich_artists(args):
    """Fetch artist metadata from MusicBrainz (type, gender, country, dates)."""
    conn = open_db(args.db or DB_PATH)
    init_schema(conn)

    where  = 'WHERE 1=1'
    params = []
    if not args.overwrite:
        # Skip artists already attempted (have MBID) or searched but not found (mb_attempted=1)
        where += ' AND a.mbid IS NULL AND a.mb_attempted = 0'
    if args.artist:
        row = _resolve_artist(conn, args.artist)
        if not row:
            console.print(f'[red]Artist not found: {args.artist}[/red]')
            conn.close(); return
        where += ' AND a.id = ?'
        params.append(row['id'])

    queue = conn.execute(
        f'SELECT id, name, mbid FROM artists a {where} ORDER BY name', params
    ).fetchall()

    if not queue:
        console.print('[dim]No artists to enrich.[/dim]')
        conn.close(); return

    console.print(f'[dim]Enriching {len(queue)} artists from MusicBrainz…[/dim]')
    console.rule(style='dim')

    _MB_COL_MAP = {
        'type':           'type',
        'gender':         'gender',
        'country':        'country',
        'sort_name':      'sort_name',
        'disambiguation': 'disambiguation',
        'formed_year':    'formed_year',
        'disbanded_year': 'disbanded_year',
    }
    updated = skipped = 0
    now     = int(time.time())

    for artist in queue:
        mbid = artist['mbid']

        if not mbid:
            search = _mb_get_safe('/artist/', {
                'query': f'artist:"{artist["name"]}"',
                'limit': 3,
            })
            candidates = (search or {}).get('artists') or []
            best = next(
                (c for c in candidates if c.get('score', 0) >= 90
                 and _norm(c.get('name', '')) == _norm(artist['name'])),
                None
            )
            if not best:
                console.print(f'  [dim]·[/dim]  {artist["name"]}  [dim]no MB match[/dim]')
                conn.execute('UPDATE artists SET mb_attempted = 1 WHERE id = ?', (artist['id'],))
                conn.commit()
                skipped += 1
                continue
            mbid = best['id']
            try:
                conn.execute('UPDATE artists SET mbid = ? WHERE id = ?', (mbid, artist['id']))
                conn.commit()
            except sqlite3.IntegrityError:
                console.print(f'  [dim]·[/dim]  {artist["name"]}  [dim]MBID already assigned to another artist[/dim]')
                skipped += 1
                continue

        data = mb_fetch_artist_data(mbid)
        if not data:
            console.print(f'  [dim]·[/dim]  {artist["name"]}  [dim]no MB data[/dim]')
            skipped += 1
            continue
        wiki_url = data.pop('wikipedia_url', None)
        updates = {col: data[key] for key, col in _MB_COL_MAP.items() if key in data}
        if wiki_url:
            wiki_page_id = _wiki_url_to_id(wiki_url)
            if wiki_page_id:
                upsert_external_link(conn, EL_ARTIST, artist['id'], EL_SVC_WIKIPEDIA, str(wiki_page_id))
        if not updates:
            skipped += 1
            continue
        updates['updated_at'] = now
        set_clause = ', '.join(f'{k} = ?' for k in updates)
        conn.execute(f'UPDATE artists SET {set_clause} WHERE id = ?',
                     (*updates.values(), artist['id']))
        parts = []
        if 'type'           in updates: parts.append(updates['type'])
        if 'gender'         in updates: parts.append(updates['gender'])
        if 'country'        in updates: parts.append(updates['country'])
        if 'formed_year'    in updates: parts.append(str(updates['formed_year']))
        if 'disbanded_year' in updates: parts.append(f'–{updates["disbanded_year"]}')
        console.print(f'  [green]✓[/green]  {artist["name"]:<30}  [dim]{" · ".join(parts)}[/dim]')
        updated += 1

    conn.commit()
    conn.close()
    console.rule(style='dim')
    console.print(f'  [dim]Updated: {updated}  Skipped: {skipped}[/dim]')

# ── cmd: hide ─────────────────────────────────────────────────────────────────

def cmd_hide(args):
    conn   = open_db(args.db or DB_PATH)
    init_schema(conn)
    action = 'unhide' if args.unhide else 'hide'
    hval   = 0 if args.unhide else 1

    table_map = {
        'artists':  ('artists',  'name',  'id'),
        'tracks':   ('tracks',   'title', 'id'),
        'releases': ('releases', 'title', 'id'),
    }
    table, name_col, id_col = table_map[args.entity]

    names = []
    with open(args.csv_file, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if 'name' not in (reader.fieldnames or []):
            console.print('[red]Error:[/red] CSV must have a "name" column')
            sys.exit(1)
        for row in reader:
            names.append(row['name'].strip())

    console.print(f'[dim]{len(names)} {args.entity} to {action}[/dim]\n')
    ok, not_found = 0, []
    now = int(time.time())
    for name in names:
        row = conn.execute(
            f'SELECT {id_col} FROM {table} WHERE LOWER({name_col}) = LOWER(?)', (name,)
        ).fetchone()
        if not row:
            not_found.append(name)
            console.print(f'  [dim]not found:[/dim] {name}')
            continue
        conn.execute(f'UPDATE {table} SET hidden = ?, updated_at = ? WHERE {id_col} = ?',
                     (hval, now, row[0]))
        tag = '[green]shown[/green]' if args.unhide else '[dim]hidden[/dim]'
        console.print(f'  {tag}  {name}')
        ok += 1

    conn.commit()
    conn.close()
    console.rule(style='dim')
    console.print(f'  [dim]{ok} {action}d  ·  {len(not_found)} not found[/dim]')

# ── cmd: delete ───────────────────────────────────────────────────────────────

def _resolve_for_delete(conn, raw: str, entity: str):
    """Resolve sp:ID, db:ULID, bare ULID, or bare Spotify ID to a (id, name) row.
    entity: 'releases' or 'artists'
    """
    raw      = raw.strip()
    name_col = 'title' if entity == 'releases' else 'name'
    if raw.lower().startswith('sp:'):
        return conn.execute(
            f'SELECT id, {name_col} FROM {entity} WHERE spotify_id = ?', [raw[3:]]
        ).fetchone()
    if raw.lower().startswith('db:'):
        return conn.execute(
            f'SELECT id, {name_col} FROM {entity} WHERE id = ?', [raw[3:]]
        ).fetchone()
    # Bare ULID: 26 uppercase Crockford base32 chars — try internal ID first
    if re.match(r'^[0-9A-Z]{26}$', raw):
        row = conn.execute(
            f'SELECT id, {name_col} FROM {entity} WHERE id = ?', [raw]
        ).fetchone()
        if row:
            return row
    # Bare Spotify ID or fallback
    return conn.execute(
        f'SELECT id, {name_col} FROM {entity} WHERE spotify_id = ?', [raw]
    ).fetchone()


def _gather_release_impact(conn, release_id: str) -> dict:
    """Return counts of tracks, listens, and variant link rows for a release."""
    track_ids = [r[0] for r in conn.execute(
        'SELECT id FROM tracks WHERE release_id = ?', [release_id]
    ).fetchall()]
    listens = 0
    if track_ids:
        ph      = ','.join('?' * len(track_ids))
        listens = conn.execute(
            f'SELECT COUNT(*) FROM listens WHERE track_id IN ({ph})', track_ids
        ).fetchone()[0]
    rv_rows = conn.execute(
        'SELECT canonical_id, variant_id FROM release_variants'
        ' WHERE canonical_id = ? OR variant_id = ?', [release_id, release_id]
    ).fetchall()
    return {
        'track_ids':    track_ids,
        'tracks':       len(track_ids),
        'listens':      listens,
        'variant_rows': [(r[0], r[1]) for r in rv_rows],
    }


def _execute_delete_release(conn, release_id: str) -> dict:
    """Delete a release and its exclusive tracks. Returns stats dict.
    Caller is responsible for pre-flight listen checks."""
    impact    = _gather_release_impact(conn, release_id)
    track_ids = impact['track_ids']

    deleted_listens = 0
    if track_ids:
        ph = ','.join('?' * len(track_ids))
        deleted_listens = conn.execute(
            f'DELETE FROM listens WHERE track_id IN ({ph})', track_ids
        ).rowcount
        conn.execute(f'DELETE FROM legacy_track_map WHERE track_id IN ({ph})', track_ids)
        conn.execute(f'DELETE FROM track_artists WHERE track_id IN ({ph})', track_ids)
        # Unlink other tracks that pointed to our tracks as canonical
        conn.execute(
            f'UPDATE tracks SET canonical_track_id = NULL, track_variant_type = NULL'
            f' WHERE canonical_track_id IN ({ph})', track_ids
        )
        conn.execute(f'DELETE FROM tracks WHERE id IN ({ph})', track_ids)

    conn.execute(
        'DELETE FROM release_variants WHERE canonical_id = ? OR variant_id = ?',
        [release_id, release_id],
    )
    conn.execute('DELETE FROM release_genres   WHERE release_id = ?', [release_id])
    conn.execute('DELETE FROM release_artists  WHERE release_id = ?', [release_id])
    conn.execute('DELETE FROM release_aliases  WHERE release_id = ?', [release_id])
    conn.execute(
        'DELETE FROM release_sources WHERE compilation_id = ? OR source_id = ?',
        [release_id, release_id],
    )
    conn.execute(
        f'DELETE FROM external_links WHERE entity_type = {EL_RELEASE} AND entity_id = ?',
        [release_id],
    )
    conn.execute('DELETE FROM releases WHERE id = ?', [release_id])
    return {'tracks': impact['tracks'], 'listens': deleted_listens}


def cmd_delete(args):
    db_path = getattr(args, 'db', None) or DB_PATH
    conn    = open_db(db_path)
    init_schema(conn)
    force   = args.force
    entity  = args.entity  # 'releases' or 'artists'

    # ── Resolve all IDs ────────────────────────────────────────────────────────
    resolved = []
    for raw in args.ids:
        row = _resolve_for_delete(conn, raw, entity)
        if not row:
            console.print(f'  [red]Not found:[/red] {raw}')
            conn.close()
            sys.exit(1)
        resolved.append((row[0], row[1]))  # (id, display_name)

    # ── Gather and display impact summary ──────────────────────────────────────
    if entity == 'releases':
        impacts = {}
        total_tracks = total_listens = 0
        for rid, rname in resolved:
            imp = _gather_release_impact(conn, rid)
            impacts[rid] = imp
            total_tracks  += imp['tracks']
            total_listens += imp['listens']
            listen_tag = ''
            if imp['listens'] > 0:
                listen_tag = (
                    f'  [red]{imp["listens"]} listen(s)[/red]' if not force
                    else f'  [yellow]{imp["listens"]} listen(s) will be deleted[/yellow]'
                )
            console.print(
                f'  [bold]{rname}[/bold]  [dim]{imp["tracks"]} track(s)[/dim]{listen_tag}'
            )
            if imp['variant_rows']:
                console.print(
                    f'    [dim]→ {len(imp["variant_rows"])} variant link(s) will be removed[/dim]'
                )

        if total_listens > 0 and not force:
            console.print(
                f'\n[red]Aborted:[/red] {total_listens} listen(s) would be lost. '
                f'Pass [bold]--force[/bold] to delete them too.'
            )
            conn.close()
            sys.exit(1)

        console.print(
            f'\n[dim]Will delete: {len(resolved)} release(s) · '
            f'{total_tracks} track(s)'
            + (f' · {total_listens} listen(s)' if total_listens else '') + '[/dim]'
        )

    else:  # artists
        artist_releases = {}
        total_releases = total_tracks = total_listens = 0
        for aid, aname in resolved:
            rel_rows = conn.execute(
                'SELECT id, title FROM releases WHERE primary_artist_id = ?', [aid]
            ).fetchall()
            artist_releases[aid] = rel_rows

            all_track_ids = [
                t[0]
                for r in rel_rows
                for t in conn.execute(
                    'SELECT id FROM tracks WHERE release_id = ?', [r[0]]
                ).fetchall()
            ]
            rel_track_count = len(all_track_ids)
            lcount = 0
            if all_track_ids:
                ph     = ','.join('?' * len(all_track_ids))
                lcount = conn.execute(
                    f'SELECT COUNT(*) FROM listens WHERE track_id IN ({ph})', all_track_ids
                ).fetchone()[0]

            listen_tag = ''
            if lcount > 0:
                listen_tag = (
                    f'  [red]{lcount} listen(s)[/red]' if not force
                    else f'  [yellow]{lcount} listen(s) will be deleted[/yellow]'
                )
            console.print(
                f'  [bold]{aname}[/bold]  '
                f'[dim]{len(rel_rows)} release(s) · {rel_track_count} track(s)[/dim]{listen_tag}'
            )
            total_releases += len(rel_rows)
            total_tracks   += rel_track_count
            total_listens  += lcount

        if total_listens > 0 and not force:
            console.print(
                f'\n[red]Aborted:[/red] {total_listens} listen(s) would be lost. '
                f'Pass [bold]--force[/bold] to delete them too.'
            )
            conn.close()
            sys.exit(1)

        console.print(
            f'\n[dim]Will delete: {len(resolved)} artist(s) · '
            f'{total_releases} release(s) · {total_tracks} track(s)'
            + (f' · {total_listens} listen(s)' if total_listens else '') + '[/dim]'
        )

    # ── Confirm ────────────────────────────────────────────────────────────────
    try:
        answer = input('\n  Proceed? [y/N] ').strip().lower()
    except (EOFError, KeyboardInterrupt):
        console.print('\n[dim]Cancelled.[/dim]')
        conn.close()
        sys.exit(0)
    if answer != 'y':
        console.print('[dim]Cancelled.[/dim]')
        conn.close()
        return

    # ── Execute ────────────────────────────────────────────────────────────────
    if entity == 'releases':
        for rid, rname in resolved:
            s = _execute_delete_release(conn, rid)
            console.print(f'  [green]deleted[/green]  {rname}')

    else:
        for aid, aname in resolved:
            deleted_tracks = deleted_listens = 0
            for rel in artist_releases[aid]:
                s = _execute_delete_release(conn, rel[0])
                deleted_tracks  += s['tracks']
                deleted_listens += s['listens']
            # Remove feature/co-artist credits on any remaining releases
            conn.execute('DELETE FROM track_artists   WHERE artist_id = ?', [aid])
            conn.execute('DELETE FROM release_artists WHERE artist_id = ?', [aid])
            conn.execute('DELETE FROM artist_aliases  WHERE artist_id = ?', [aid])
            conn.execute(
                'DELETE FROM artist_relations'
                ' WHERE from_artist_id = ? OR to_artist_id = ?', [aid, aid],
            )
            conn.execute(
                'DELETE FROM artist_members'
                ' WHERE group_artist_id = ? OR member_artist_id = ?', [aid, aid],
            )
            conn.execute(
                f'DELETE FROM external_links WHERE entity_type = {EL_ARTIST} AND entity_id = ?',
                [aid],
            )
            conn.execute('DELETE FROM artists WHERE id = ?', [aid])
            detail = f'{len(artist_releases[aid])} release(s) · {deleted_tracks} track(s)'
            if deleted_listens:
                detail += f' · {deleted_listens} listen(s)'
            console.print(f'  [green]deleted[/green]  {aname}  [dim]({detail})[/dim]')

    conn.commit()
    conn.close()
    console.rule(style='dim')

# ── cmd: artist images ────────────────────────────────────────────────────────

def cmd_artist_images(args):
    conn = open_db(args.db or DB_PATH)
    init_schema(conn)

    updates = []
    with open(args.csv_file, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            updates.append({
                'name': row['artist_name'].strip(),
                'url':  row['profile_image_url'].strip(),
                'crop': row.get('profile_image_crop', '').strip() or None,
            })

    console.print(f'[dim]{len(updates)} artists in CSV[/dim]\n')
    ok, not_found = 0, []
    now = int(time.time())
    for u in updates:
        row = conn.execute('SELECT id FROM artists WHERE LOWER(name) = LOWER(?)',
                           (u['name'],)).fetchone()
        if not row:
            not_found.append(u['name'])
            console.print(f'  [dim]not found:[/dim] {u["name"]}')
            continue
        conn.execute(
            "UPDATE artists SET image_url = ?, image_source = 'manual',"
            " image_position = ?, updated_at = ? WHERE id = ?",
            (u['url'], u['crop'], now, row[0])
        )
        console.print(f'  [green]upd[/green]  {u["name"]}')
        ok += 1

    conn.commit()
    conn.close()
    console.rule(style='dim')
    console.print(f'  [dim]{ok} updated  ·  {len(not_found)} not found[/dim]')

# ── cmd: link sources ─────────────────────────────────────────────────────────

def cmd_link_sources(args):
    db_path = args.db or DB_PATH
    conn    = open_db(db_path)
    init_schema(conn)

    def resolve(url_or_id):
        row = conn.execute('SELECT id, title FROM releases WHERE id = ?',
                           (url_or_id,)).fetchone()
        if row:
            return row
        m    = _RE_SP_URL.search(url_or_id)
        spid = m.group(1) if m else url_or_id
        return conn.execute('SELECT id, title FROM releases WHERE spotify_id = ?',
                            (spid,)).fetchone()

    compilation = resolve(args.compilation)
    if not compilation:
        console.print(f'[red]Compilation not found:[/red] {args.compilation}')
        sys.exit(1)
    console.print(f'  Compilation: [bold]{compilation[1]}[/bold]  [dim]{compilation[0]}[/dim]\n')

    ok = 0
    for spec in args.sources:
        disc   = None
        disc_m = re.search(r':disc=(\d+)$', spec)
        if disc_m:
            disc = int(disc_m.group(1))
            spec = spec[:disc_m.start()]
        src = resolve(spec.strip())
        if not src:
            console.print(f'  [red]not found:[/red] {spec}')
            continue
        conn.execute(
            '''INSERT INTO release_sources (compilation_id, source_id, disc_number)
               VALUES (?, ?, ?)
               ON CONFLICT(compilation_id, source_id) DO UPDATE SET
                   disc_number = excluded.disc_number''',
            (compilation[0], src[0], disc),
        )
        disc_str = f'  disc {disc}' if disc else ''
        console.print(f'  [green]linked[/green]  {src[1]}{disc_str}  [dim]{src[0]}[/dim]')
        ok += 1

    conn.commit()
    conn.close()
    console.print(f'\n  [dim]{ok} source(s) linked to {compilation[1]}[/dim]')

# ── cmd: alias ────────────────────────────────────────────────────────────────

def _resolve_artist(conn, key: str):
    """Look up an artist by internal ID, slug, Spotify ID, or name (case-insensitive). Returns row or None."""
    return (
        conn.execute('SELECT id, name FROM artists WHERE id = ?',                 [key]).fetchone() or
        conn.execute('SELECT id, name FROM artists WHERE slug = ?',               [key]).fetchone() or
        conn.execute('SELECT id, name FROM artists WHERE spotify_id = ?',         [key]).fetchone() or
        conn.execute('SELECT id, name FROM artists WHERE lower(name) = lower(?)', [key]).fetchone()
    )

def cmd_alias(args):
    conn = open_db(getattr(args, 'db', None) or DB_PATH)
    init_schema(conn)
    artist = _resolve_artist(conn, args.artist)
    if not artist:
        console.print(f'[red]Artist not found:[/red] {args.artist}')
        sys.exit(1)

    if args.alias_cmd == 'add':
        conn.execute(
            '''INSERT INTO artist_aliases (artist_id, alias, alias_type, language, source, sort_order)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(artist_id, alias) DO UPDATE SET
                   alias_type = excluded.alias_type,
                   language   = excluded.language,
                   source     = excluded.source,
                   sort_order = excluded.sort_order''',
            [artist['id'], args.alias, args.alias_type, getattr(args, 'language', None), args.source, getattr(args, 'sort_order', 0)],
        )
        conn.commit()
        type_tag = f'  [dim]{args.alias_type}[/dim]' if args.alias_type != 'common' else ''
        console.print(f'  [green]✓[/green]  "{args.alias}"{type_tag}  →  {artist["name"]}  [dim]({args.source})[/dim]')

    elif args.alias_cmd == 'remove':
        cur = conn.execute(
            'DELETE FROM artist_aliases WHERE artist_id = ? AND lower(alias) = lower(?)',
            [artist['id'], args.alias],
        )
        conn.commit()
        if cur.rowcount:
            console.print(f'  [green]✓[/green]  Removed "{args.alias}" from {artist["name"]}')
        else:
            console.print(f'  [yellow]Not found:[/yellow] "{args.alias}" on {artist["name"]}')

    elif args.alias_cmd == 'list':
        rows = conn.execute(
            'SELECT alias, alias_type, language, source FROM artist_aliases WHERE artist_id = ? ORDER BY sort_order, alias_type, alias',
            [artist['id']],
        ).fetchall()
        console.print(f'  Aliases for [bold]{artist["name"]}[/bold]:')
        if rows:
            for r in rows:
                lang_tag = f'  [dim]{r["language"]}[/dim]' if r['language'] else ''
                console.print(f'    {r["alias"]}  [dim]({r["alias_type"]}){lang_tag}  ({r["source"]})[/dim]')
        else:
            console.print('    [dim]none[/dim]')
    conn.close()


# ── cmd: artist merge ──────────────────────────────────────────────────────────

def cmd_artist_merge(args):
    """
    Merge FROM artist into TO artist (the canonical record to keep).

    - Repoints release_artists, track_artists, releases.primary_artist_id
    - Inserts FROM artist's name as a past_name alias on TO (unless --no-alias)
    - Transfers missing metadata fields from FROM → TO
    - Deletes the FROM artist row
    """
    conn = open_db(args.db or DB_PATH)
    init_schema(conn)

    from_artist = _resolve_artist(conn, args.from_artist)
    to_artist   = _resolve_artist(conn, args.to_artist)

    if not from_artist:
        console.print(f'[red]Artist not found:[/red] {args.from_artist}')
        sys.exit(1)
    if not to_artist:
        console.print(f'[red]Artist not found:[/red] {args.to_artist}')
        sys.exit(1)
    if from_artist['id'] == to_artist['id']:
        console.print('[yellow]FROM and TO are the same artist — nothing to do.[/yellow]')
        sys.exit(0)

    from_id, from_name = from_artist['id'], from_artist['name']
    to_id,   to_name   = to_artist['id'],   to_artist['name']

    console.print(f'  Merging [bold]{from_name}[/bold] [dim]({from_id})[/dim]')
    console.print(f'      → [bold]{to_name}[/bold] [dim]({to_id})[/dim]\n')

    # Counts before
    ra_count  = conn.execute('SELECT COUNT(*) FROM release_artists WHERE artist_id = ?', [from_id]).fetchone()[0]
    ta_count  = conn.execute('SELECT COUNT(*) FROM track_artists   WHERE artist_id = ?', [from_id]).fetchone()[0]
    rel_count = conn.execute('SELECT COUNT(*) FROM releases WHERE primary_artist_id = ?', [from_id]).fetchone()[0]

    conn.execute('PRAGMA foreign_keys = OFF')
    now = int(time.time())

    # Remove FROM rows where TO is already present (avoid UNIQUE constraint violations)
    dup_ra = conn.execute('''
        DELETE FROM release_artists
        WHERE artist_id = ?
        AND release_id IN (SELECT release_id FROM release_artists WHERE artist_id = ?)
    ''', [from_id, to_id]).rowcount
    dup_ta = conn.execute('''
        DELETE FROM track_artists
        WHERE artist_id = ?
        AND track_id IN (SELECT track_id FROM track_artists WHERE artist_id = ?)
    ''', [from_id, to_id]).rowcount
    if dup_ra or dup_ta:
        console.print(f'  [dim]Removed {dup_ra} duplicate release_artists, {dup_ta} duplicate track_artists[/dim]')

    # Repoint FK references
    conn.execute('UPDATE release_artists SET artist_id = ? WHERE artist_id = ?', [to_id, from_id])
    conn.execute('UPDATE track_artists   SET artist_id = ? WHERE artist_id = ?', [to_id, from_id])
    conn.execute('UPDATE releases SET primary_artist_id = ? WHERE primary_artist_id = ?', [to_id, from_id])
    conn.execute('UPDATE artist_aliases  SET artist_id = ? WHERE artist_id = ?', [to_id, from_id])
    conn.execute('UPDATE artist_relations SET from_artist_id = ? WHERE from_artist_id = ?', [to_id, from_id])
    conn.execute('UPDATE artist_relations SET to_artist_id   = ? WHERE to_artist_id   = ?', [to_id, from_id])

    console.print(f'  [dim]Repointed {ra_count} release_artists, {ta_count} track_artists, {rel_count} primary releases[/dim]')

    # Transfer missing metadata fields (TO takes priority for existing values)
    fields_to_transfer = [
        'sort_name', 'spotify_id', 'mbid', 'lastfm_url',
        'image_url', 'image_source', 'image_position', 'hero_image_url',
        'country', 'formed_year', 'disbanded_year', 'bio',
        'aoty_id', 'aoty_url', 'type', 'gender', 'disambiguation',
    ]
    from_row = conn.execute(f'SELECT * FROM artists WHERE id = ?', [from_id]).fetchone()
    to_row   = conn.execute(f'SELECT * FROM artists WHERE id = ?', [to_id]).fetchone()
    transferred = []
    for field in fields_to_transfer:
        try:
            if to_row[field] is None and from_row[field] is not None:
                conn.execute(f'UPDATE artists SET {field} = ?, updated_at = ? WHERE id = ?',
                             [from_row[field], now, to_id])
                transferred.append(field)
        except IndexError:
            pass  # column might not exist on older schema
    # Transfer external_links from FROM → TO (INSERT OR IGNORE to not overwrite TO's links)
    conn.execute(
        'INSERT OR IGNORE INTO external_links (entity_type, entity_id, service, link_value)'
        ' SELECT entity_type, ?, service, link_value FROM external_links'
        ' WHERE entity_type = ? AND entity_id = ?',
        [to_id, EL_ARTIST, from_id],
    )
    if transferred:
        console.print(f'  [dim]Transferred metadata: {", ".join(transferred)}[/dim]')

    # Add FROM name as past_name alias on TO (unless suppressed)
    if not getattr(args, 'no_alias', False):
        conn.execute(
            '''INSERT INTO artist_aliases (artist_id, alias, alias_type, source, sort_order)
               VALUES (?, ?, 'past_name', 'manual', 0)
               ON CONFLICT(artist_id, alias) DO NOTHING''',
            [to_id, from_name],
        )
        console.print(f'  [dim]Added past_name alias: "{from_name}"[/dim]')

    # Delete the FROM artist
    conn.execute('DELETE FROM artists WHERE id = ?', [from_id])
    console.print(f'  [dim]Deleted artist row: {from_name} ({from_id})[/dim]')

    conn.commit()
    conn.execute('PRAGMA foreign_keys = ON')
    conn.close()
    console.print(f'\n  [green]✓[/green]  Merged [bold]{from_name}[/bold] → [bold]{to_name}[/bold]')


# ── cmd: artist members ────────────────────────────────────────────────────────

def cmd_artist_members(args):
    conn = open_db(args.db or DB_PATH)
    init_schema(conn)

    if args.members_cmd == 'list':
        group = _resolve_artist(conn, args.group)
        if not group:
            console.print(f'[red]Artist not found:[/red] {args.group}')
            conn.close(); return
        rows = conn.execute(
            '''SELECT a.name, a.id, am.sort_order
               FROM artist_members am
               JOIN artists a ON a.id = am.member_artist_id
               WHERE am.group_artist_id = ?
               ORDER BY am.sort_order, a.name''',
            [group['id']]
        ).fetchall()
        console.print(f'[bold]{group["name"]}[/bold]  ({len(rows)} members)')
        for r in rows:
            console.print(f'  {r["sort_order"]:2}  {r["name"]}  [dim]{r["id"]}[/dim]')
        # Also show which other groups list this artist as a member
        groups_for = conn.execute(
            '''SELECT a.name FROM artist_members am
               JOIN artists a ON a.id = am.group_artist_id
               WHERE am.member_artist_id = ?''',
            [group['id']]
        ).fetchall()
        if groups_for:
            console.print(f'\n  [dim]Also listed as member of: {", ".join(r["name"] for r in groups_for)}[/dim]')

    elif args.members_cmd == 'add':
        group = _resolve_artist(conn, args.group)
        if not group:
            console.print(f'[red]Group not found:[/red] {args.group}')
            conn.close(); return
        # Next sort_order after existing members
        cur_max = conn.execute(
            'SELECT COALESCE(MAX(sort_order), -1) FROM artist_members WHERE group_artist_id = ?',
            [group['id']]
        ).fetchone()[0]
        added = 0
        for i, member_key in enumerate(args.members):
            member = _resolve_artist(conn, member_key)
            if not member:
                console.print(f'  [yellow]Not found:[/yellow] {member_key}  — skipped (use Spotify ID or exact name)')
                continue
            try:
                conn.execute(
                    'INSERT INTO artist_members (group_artist_id, member_artist_id, sort_order) VALUES (?, ?, ?)',
                    [group['id'], member['id'], cur_max + 1 + i]
                )
                console.print(f'  [green]added[/green]  {member["name"]}  →  {group["name"]}')
                added += 1
            except Exception:
                console.print(f'  [dim]already linked:[/dim]  {member["name"]}')
        conn.commit()
        console.print(f'\n  {added} member(s) added to [bold]{group["name"]}[/bold]')

    elif args.members_cmd == 'remove':
        group = _resolve_artist(conn, args.group)
        member = _resolve_artist(conn, args.member)
        if not group:
            console.print(f'[red]Group not found:[/red] {args.group}')
            conn.close(); return
        if not member:
            console.print(f'[red]Member not found:[/red] {args.member}')
            conn.close(); return
        deleted = conn.execute(
            'DELETE FROM artist_members WHERE group_artist_id = ? AND member_artist_id = ?',
            [group['id'], member['id']]
        ).rowcount
        conn.commit()
        if deleted:
            console.print(f'  [green]removed[/green]  {member["name"]}  from  {group["name"]}')
        else:
            console.print(f'  [yellow]No link found[/yellow] between {member["name"]} and {group["name"]}')

    conn.close()



def _resolve_release(conn, key: str):
    """Look up a release by internal ID, Spotify ID, MusicBrainz ID, or title (case-insensitive)."""
    return (
        conn.execute('SELECT id, title FROM releases WHERE id = ?',                  [key]).fetchone() or
        conn.execute('SELECT id, title FROM releases WHERE spotify_id = ?',          [key]).fetchone() or
        conn.execute('SELECT id, title FROM releases WHERE mbid = ?',                [key]).fetchone() or
        conn.execute('SELECT id, title FROM releases WHERE lower(title) = lower(?)', [key]).fetchone()
    )

def cmd_release_alias(args):
    conn = open_db(getattr(args, 'db', None) or DB_PATH)
    init_schema(conn)
    release = _resolve_release(conn, args.release)
    if not release:
        console.print(f'[red]Release not found:[/red] {args.release}')
        sys.exit(1)

    if args.release_alias_cmd == 'add':
        is_def = 1 if getattr(args, 'definitive', False) else 0
        conn.execute(
            '''INSERT INTO release_aliases (release_id, alias, is_definitive, source)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(release_id, alias) DO UPDATE SET
                   is_definitive = excluded.is_definitive,
                   source        = excluded.source''',
            [release['id'], args.alias, is_def, args.source],
        )
        conn.commit()
        def_label = '  [bold](definitive)[/bold]' if is_def else ''
        console.print(f'  [green]✓[/green]  "{args.alias}"{def_label}  →  {release["title"]}  [dim]({args.source})[/dim]')

    elif args.release_alias_cmd == 'remove':
        cur = conn.execute(
            'DELETE FROM release_aliases WHERE release_id = ? AND lower(alias) = lower(?)',
            [release['id'], args.alias],
        )
        conn.commit()
        if cur.rowcount:
            console.print(f'  [green]✓[/green]  Removed "{args.alias}" from {release["title"]}')
        else:
            console.print(f'  [yellow]Not found:[/yellow] "{args.alias}" on {release["title"]}')

    elif args.release_alias_cmd == 'list':
        rows = conn.execute(
            '''SELECT alias, is_definitive, source
               FROM release_aliases WHERE release_id = ?
               ORDER BY is_definitive DESC, alias''',
            [release['id']],
        ).fetchall()
        console.print(f'  Aliases for [bold]{release["title"]}[/bold]:')
        if rows:
            for r in rows:
                def_tag = '  [bold dim](definitive)[/bold dim]' if r['is_definitive'] else ''
                console.print(f'    {r["alias"]}{def_tag}  [dim]({r["source"]})[/dim]')
        else:
            console.print('    [dim]none[/dim]')

    conn.close()


# ── cmd: relation ──────────────────────────────────────────────────────────────

def cmd_relation(args):
    conn = open_db(getattr(args, 'db', None) or DB_PATH)
    init_schema(conn)

    if args.relation_cmd == 'list':
        artist = _resolve_artist(conn, args.artist)
        if not artist:
            console.print(f'[red]Artist not found:[/red] {args.artist}')
            sys.exit(1)
        rows = conn.execute('''
            SELECT ar.relation_type, ar.source,
                   a_from.name AS from_name, a_to.name AS to_name,
                   ar.from_artist_id, ar.to_artist_id
            FROM   artist_relations ar
            JOIN   artists a_from ON a_from.id = ar.from_artist_id
            JOIN   artists a_to   ON a_to.id   = ar.to_artist_id
            WHERE  ar.from_artist_id = ? OR ar.to_artist_id = ?
            ORDER  BY ar.relation_type, a_from.name
        ''', [artist['id'], artist['id']]).fetchall()
        console.print(f'  Relations for [bold]{artist["name"]}[/bold]:')
        if rows:
            for r in rows:
                arrow = f'{r["from_name"]} → [dim]{r["relation_type"]}[/dim] → {r["to_name"]}'
                console.print(f'    {arrow}  [dim]({r["source"]})[/dim]')
        else:
            console.print('    [dim]none[/dim]')
        conn.close()
        return

    from_artist = _resolve_artist(conn, args.from_artist)
    to_artist   = _resolve_artist(conn, args.to_artist)
    if not from_artist:
        console.print(f'[red]Artist not found:[/red] {args.from_artist}')
        sys.exit(1)
    if not to_artist:
        console.print(f'[red]Artist not found:[/red] {args.to_artist}')
        sys.exit(1)

    if args.relation_cmd == 'add':
        conn.execute(
            '''INSERT OR REPLACE INTO artist_relations
               (from_artist_id, to_artist_id, relation_type, source)
               VALUES (?, ?, ?, 'manual')''',
            [from_artist['id'], to_artist['id'], args.type],
        )
        conn.commit()
        console.print(
            f'  [green]✓[/green]  {from_artist["name"]} → [dim]{args.type}[/dim] → {to_artist["name"]}'
        )
    elif args.relation_cmd == 'remove':
        cur = conn.execute(
            '''DELETE FROM artist_relations
               WHERE from_artist_id = ? AND to_artist_id = ? AND relation_type = ?''',
            [from_artist['id'], to_artist['id'], args.type],
        )
        conn.commit()
        if cur.rowcount:
            console.print(f'  [green]✓[/green]  Removed')
        else:
            console.print(f'  [yellow]Not found[/yellow]')
    conn.close()


# ── cmd: migrate artist-slugs ─────────────────────────────────────────────────

def cmd_migrate_artist_slugs(args):
    """
    One-time migration: convert legacy slug-as-id artists to ULID ids.
    For every artist where slug IS NULL, the current id is the slug.
    Generates a new ULID id, backfills slug, and updates all FK references.
    """
    conn = open_db(args.db or DB_PATH)
    init_schema(conn)  # ensures slug column exists

    artists = conn.execute(
        'SELECT id, name FROM artists WHERE slug IS NULL ORDER BY name'
    ).fetchall()

    if not artists:
        console.print('  [dim]All artists already have slugs — nothing to do.[/dim]')
        conn.close()
        return

    console.print(f'  Migrating [bold]{len(artists)}[/bold] artist(s) to ULID ids...')
    conn.execute('PRAGMA foreign_keys = OFF')

    for artist in artists:
        old_id = artist['id']
        new_id = new_ulid()
        slug   = old_id  # current id IS the slug for legacy rows

        conn.execute('UPDATE releases        SET primary_artist_id = ? WHERE primary_artist_id = ?', [new_id, old_id])
        conn.execute('UPDATE release_artists SET artist_id          = ? WHERE artist_id          = ?', [new_id, old_id])
        conn.execute('UPDATE track_artists   SET artist_id          = ? WHERE artist_id          = ?', [new_id, old_id])
        conn.execute('UPDATE artist_aliases  SET artist_id          = ? WHERE artist_id          = ?', [new_id, old_id])
        conn.execute('UPDATE artist_relations SET from_artist_id    = ? WHERE from_artist_id     = ?', [new_id, old_id])
        conn.execute('UPDATE artist_relations SET to_artist_id      = ? WHERE to_artist_id       = ?', [new_id, old_id])
        conn.execute('UPDATE artists SET id = ?, slug = ? WHERE id = ?', [new_id, slug, old_id])

        console.print(f'    [dim]{slug:<30}[/dim] {new_id}')

    conn.commit()
    conn.execute('PRAGMA foreign_keys = ON')
    console.print(f'  [green]✓ Done.[/green]')
    conn.close()


# ── cmd: migrate genres ────────────────────────────────────────────────────────

def cmd_migrate_genres(args):
    """Copy genres from the legacy overrides DB into master.sqlite, matching by release MBID."""
    legacy_path = args.legacy_db
    if not os.path.exists(legacy_path):
        console.print(f'[red]Legacy DB not found:[/red] {legacy_path}')
        sys.exit(1)

    master = open_db(args.db or DB_PATH)
    init_schema(master)
    legacy = sqlite3.connect(legacy_path)

    # Pull all genres from legacy
    legacy_genres = {
        row[0]: (row[1], row[2])
        for row in legacy.execute('SELECT aoty_id, name, slug FROM genres')
    }

    # Find releases in master that have an MBID and could have legacy genres
    candidates = master.execute(
        'SELECT id, title, mbid FROM releases WHERE mbid IS NOT NULL'
    ).fetchall()

    console.print(f'[dim]{len(candidates)} releases with MBIDs to check[/dim]\n')

    total_genres  = 0
    total_releases = 0
    now = int(time.time())

    for release_id, title, mbid in candidates:
        rows = legacy.execute(
            'SELECT aoty_genre_id, is_primary FROM release_genres WHERE release_mbid = ?',
            (mbid,)
        ).fetchall()
        if not rows:
            continue

        added = 0
        for aoty_id, is_primary in rows:
            if aoty_id not in legacy_genres:
                continue
            name, slug = legacy_genres[aoty_id]

            # Upsert genre into master
            master.execute(
                'INSERT INTO genres (aoty_id, name, slug) VALUES (?, ?, ?)'
                ' ON CONFLICT(aoty_id) DO UPDATE SET name = excluded.name, slug = excluded.slug',
                (aoty_id, name, slug)
            )

            master.execute('''
                INSERT INTO release_genres (release_id, aoty_genre_id, is_primary) VALUES (?, ?, ?)
                ON CONFLICT(release_id, aoty_genre_id) DO UPDATE SET is_primary = excluded.is_primary
            ''', (release_id, aoty_id, int(is_primary)))
            added += 1

        if added:
            console.print(f'  [green]{added:2d} genre(s)[/green]  {title}  [dim]{mbid}[/dim]')
            total_genres  += added
            total_releases += 1

    master.commit()
    master.close()
    legacy.close()

    console.rule(style='dim')
    console.print(f'  [dim]Migrated {total_genres} genre tags across {total_releases} releases[/dim]')

# ── cmd: release variants ─────────────────────────────────────────────────────

def _find_variant_groups(conn, include_linked=False):
    """
    Return a list of candidate variant groups.  Each group is a list of dicts:
        { id, title, release_date, primary_artist_id, artist_name,
          release_group_mbid, spotify_id, mbid,
          track_count, explicit_count, existing_canonical_id }

    Two detection passes:
      1. Same release_group_mbid (high confidence — MusicBrainz data)
      2. Same primary_artist_id + same _base_title() (catches releases without MB data)

    Groups where every member already appears in release_variants (as canonical or
    variant) are skipped unless include_linked=True.
    """
    rows = conn.execute('''
        SELECT r.id, r.title, r.release_date, r.primary_artist_id,
               a.name AS artist_name, r.release_group_mbid,
               r.spotify_id, r.mbid,
               r.type, r.type_secondary,
               COUNT(t.id)                                    AS track_count,
               SUM(CASE WHEN t.is_explicit = 1 THEN 1 ELSE 0 END) AS explicit_count
        FROM   releases r
        JOIN   artists  a ON a.id = r.primary_artist_id
        LEFT JOIN tracks t ON t.release_id = r.id
        WHERE  r.hidden = 0
        GROUP  BY r.id
        ORDER  BY a.name, r.release_date
    ''').fetchall()

    # Build lookup: release_id → which canonical/variant rows it appears in
    linked_ids = set()
    for row in conn.execute('''
        SELECT canonical_id FROM release_variants
        UNION
        SELECT variant_id   FROM release_variants
    ''').fetchall():
        linked_ids.add(row[0])

    # Index rows
    by_mbgrp  = {}   # release_group_mbid → [row]
    by_artist = {}   # (primary_artist_id, base_title_lower) → [row]

    for row in rows:
        rid, title, date, artist_id, artist_name, rg_mbid = row[:6]

        if rg_mbid:
            by_mbgrp.setdefault(rg_mbid, []).append(dict(row))

        bt = _base_title(title).lower().strip()
        if artist_id:
            by_artist.setdefault((artist_id, bt), []).append(dict(row))

    seen_sets = []   # list of frozensets of ids, to deduplicate
    groups    = []

    def _add_group(members):
        ids = frozenset(m['id'] for m in members)
        if len(ids) < 2:
            return
        # deduplicate against already-seen groups
        for s in seen_sets:
            if s == ids:
                return
        seen_sets.append(ids)

        if not include_linked and ids.issubset(linked_ids):
            return  # skip groups that are already fully linked

        # Annotate each member with its existing canonical_id (if any)
        for m in members:
            existing = conn.execute(
                'SELECT canonical_id FROM release_variants WHERE variant_id = ?',
                (m['id'],)
            ).fetchone()
            m['existing_canonical_id'] = existing[0] if existing else None

        groups.append(members)

    # Pass 1: MusicBrainz release-group
    for rg_mbid, members in by_mbgrp.items():
        if len(members) >= 2:
            _add_group(members)

    # Pass 2: title similarity
    for key, members in by_artist.items():
        if len(members) >= 2:
            _add_group(members)

    return groups


def _fetch_release_row(conn, rid):
    """Re-fetch a single release row in the same shape as _find_variant_groups uses."""
    row = conn.execute('''
        SELECT r.id, r.title, r.release_date, r.primary_artist_id,
               a.name AS artist_name, r.release_group_mbid,
               r.spotify_id, r.mbid,
               r.type, r.type_secondary,
               COUNT(t.id)                                    AS track_count,
               SUM(CASE WHEN t.is_explicit = 1 THEN 1 ELSE 0 END) AS explicit_count
        FROM   releases r
        JOIN   artists  a ON a.id = r.primary_artist_id
        LEFT JOIN tracks t ON t.release_id = r.id
        WHERE  r.id = ?
        GROUP  BY r.id
    ''', (rid,)).fetchone()
    if not row:
        return None
    m = dict(row)
    existing = conn.execute(
        'SELECT canonical_id FROM release_variants WHERE variant_id = ?', (rid,)
    ).fetchone()
    m['existing_canonical_id'] = existing[0] if existing else None
    return m


_RE_UUID = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I
)

def cmd_release_variants(args):
    """
    Interactive CLI: walk through potential variant groups.

    For every release in the group (including canonical):
      Stage 1 — primary type   (releases.type)
      Stage 2 — secondary type (releases.type_secondary)

    For every non-canonical release:
      Stage 3 — edition type   (release_variants.variant_type)
    """
    db_path = getattr(args, 'db', None) or DB_PATH
    conn    = open_db(db_path)
    init_schema(conn)

    include_linked = getattr(args, 'all', False)
    groups = _find_variant_groups(conn, include_linked=include_linked)

    if not groups:
        console.print('  [dim]No unlinked variant groups found.[/dim]')
        conn.close()
        return

    console.print(
        f'  Found [bold]{len(groups)}[/bold] candidate group(s).  '
        f'[dim]\\[s]kip  \\[q]uit  \\[a]dd release[/dim]\n'
    )

    saved    = 0
    gi       = 0
    quit_all = False

    while gi < len(groups) and not quit_all:
        group = groups[gi]
        gi   += 1
        console.rule(f'[dim]Group {gi}/{len(groups)}[/dim]', style='dim')

        group.sort(key=lambda m: (
            1 if detect_variant_type(m['title']) else 0,
            m['release_date'] or '9999',
        ))

        artist_name = group[0]['artist_name']

        # ── canonical selection loop (allows [a]dd to re-prompt) ──────────────
        canonical = None
        while True:
            console.print(f'  [bold]{artist_name}[/bold]')
            for i, m in enumerate(group, 1):
                _print_member(i, m)
            console.print()
            console.print(
                '  [bold]Canonical?[/bold]  '
                '[dim]number / \\[a] Spotify URL or MBID to add / \\[s]kip / \\[q]uit:[/dim] ',
                end='',
            )
            raw = input().strip()
            rl  = raw.lower()

            if rl == 'q':
                quit_all = True
                break

            if rl == 's' or rl == '':
                console.print('  [dim]Skipped.[/dim]\n')
                break

            # ── [a]dd ─────────────────────────────────────────────────────────
            sp_m    = _RE_SP_URL.search(raw)
            is_uuid = _RE_UUID.match(raw.strip())
            if rl == 'a' or sp_m or is_uuid:
                if rl == 'a':
                    console.print('  Paste Spotify URL or MBID UUID: ', end='')
                    raw     = input().strip()
                    sp_m    = _RE_SP_URL.search(raw)
                    is_uuid = _RE_UUID.match(raw.strip())

                existing_id = None
                if sp_m:
                    row = conn.execute(
                        'SELECT id FROM releases WHERE spotify_id = ?', (sp_m.group(1),)
                    ).fetchone()
                    existing_id = row[0] if row else None
                elif is_uuid:
                    row = conn.execute(
                        'SELECT id FROM releases WHERE mbid = ?', (raw.strip(),)
                    ).fetchone()
                    existing_id = row[0] if row else None

                if not existing_id:
                    console.print(f'  [dim]Importing {raw[:80]}…[/dim]')
                    result = subprocess.run(
                        [sys.executable, os.path.abspath(__file__), 'import', raw, '--db', db_path],
                        capture_output=False,
                    )
                    if result.returncode != 0:
                        console.print('  [red]Import failed — skipping add.[/red]')
                        continue
                    if sp_m:
                        row = conn.execute(
                            'SELECT id FROM releases WHERE spotify_id = ?', (sp_m.group(1),)
                        ).fetchone()
                    elif is_uuid:
                        row = conn.execute(
                            'SELECT id FROM releases WHERE mbid = ?', (raw.strip(),)
                        ).fetchone()
                    existing_id = row[0] if row else None

                if not existing_id:
                    console.print('  [red]Could not find release after import.[/red]')
                    continue
                if any(m['id'] == existing_id for m in group):
                    console.print('  [yellow]Already in this group.[/yellow]')
                    continue

                new_member = _fetch_release_row(conn, existing_id)
                if not new_member:
                    console.print('  [red]Release not found in DB.[/red]')
                    continue

                group.append(new_member)
                group.sort(key=lambda m: (
                    1 if detect_variant_type(m['title']) else 0,
                    m['release_date'] or '9999',
                ))
                console.print()
                continue  # re-display updated group

            if not raw.isdigit() or not (1 <= int(raw) <= len(group)):
                console.print(f'  [red]Enter 1–{len(group)}, a, s, or q.[/red]')
                continue

            canonical = group[int(raw) - 1]
            break

        if quit_all or canonical is None:
            continue

        # ── per-release type assignment (stages 1 & 2 for every member) ───────
        type_updates  = {}   # release_id → (type, type_secondary)
        edition_links = []   # (variant_id, edition_type, sort_order)
        hide_ids      = set()
        aborted       = False

        all_members = [canonical] + [m for m in group if m['id'] != canonical['id']]

        for sort_i, m in enumerate(all_members):
            is_canonical = (m['id'] == canonical['id'])
            role_label   = '[bold green]canonical[/bold green]' if is_canonical \
                           else f'[bold]variant {sort_i}[/bold]'
            console.rule(
                f'  {role_label}: [bold]{m["title"]}[/bold]'
                f'  [dim]{m["release_date"] or "?"}[/dim]',
                style='dim',
            )

            # Stages 1→2→3 with [b]ack support
            chosen_type = None
            chosen_sec  = None
            stage       = 1
            while stage <= (3 if not is_canonical else 2):
                if stage == 1:
                    cur_type = m.get('type') or 'album'
                    chosen_type, quit_now, _, do_back = _prompt_choice(
                        'Stage 1 — Primary type', _PRIMARY_TYPES, current=cur_type
                    )
                    if quit_now:
                        aborted  = True
                        quit_all = True
                        break
                    stage = 2

                elif stage == 2:
                    cur_sec = m.get('type_secondary') or 'none'
                    if cur_sec == 'none':
                        _sec_set = set(_SECONDARY_TYPES)
                        for _vt in detect_variant_types(m['title']):
                            if _vt in _sec_set:
                                cur_sec = _vt
                                break
                    chosen_sec, quit_now, _, do_back = _prompt_choice(
                        'Stage 2 — Secondary type', _SECONDARY_TYPES, current=cur_sec,
                        allow_back=True,
                    )
                    if quit_now:
                        aborted  = True
                        quit_all = True
                        break
                    if do_back:
                        stage = 1
                        continue
                    chosen_sec = None if chosen_sec == 'none' else chosen_sec
                    type_updates[m['id']] = (chosen_type, chosen_sec)
                    stage = 3

                elif stage == 3:
                    # Auto-detect from title; suppress live/remix (captured in stage 2)
                    auto_eds = [t for t in detect_variant_types(m['title'])
                                if t not in ('live', 'remix')]
                    cur_eds = auto_eds if auto_eds else ['none']
                    chosen_eds, quit_now, do_hide, do_back = _prompt_choice(
                        'Stage 3 — Edition type', _EDITION_TYPES, current=cur_eds,
                        allow_hide=True, allow_back=True, multi=True,
                    )
                    if quit_now:
                        aborted  = True
                        quit_all = True
                        break
                    if do_back:
                        type_updates.pop(m['id'], None)
                        stage = 2
                        continue
                    if do_hide:
                        hide_ids.add(m['id'])
                        type_updates.pop(m['id'], None)
                    else:
                        if chosen_eds == ['none']:
                            edition_type = None
                        else:
                            edition_type = ','.join(t for t in chosen_eds if t != 'none') or None
                        edition_links.append((m['id'], edition_type, sort_i))
                    stage = 4  # done

            if aborted:
                break

        # ── write everything accumulated so far (even on partial abort) ────────
        _write_group(conn, canonical, type_updates, edition_links, hide_ids)
        saved += len(edition_links)

        parts = [f'+{len(edition_links)} variant(s)']
        if hide_ids:
            parts.append(f'{len(hide_ids)} hidden')

        if aborted:
            console.print(
                f'\n  [green]✓[/green]  Canonical: [bold]{canonical["title"]}[/bold]'
                f'  {", ".join(parts)}  [dim](partial)[/dim]\n'
            )
            console.print('  [dim]Quit — progress saved.[/dim]')
            break

        console.print(
            f'\n  [green]✓[/green]  Canonical: [bold]{canonical["title"]}[/bold]'
            f'  {", ".join(parts)}\n'
        )

    console.rule(style='dim')
    console.print(f'  [dim]Done — {saved} variant link(s) saved.[/dim]')
    conn.close()


def _merge_variant_tracks(conn, canonical_id, variant_id):
    """Move listens from shared variant tracks → canonical tracks (by ISRC, title fallback),
    then hide the shared tracks on the variant.  Returns (listens_moved, tracks_hidden)."""
    canon_rows = conn.execute(
        'SELECT id, isrc, title FROM tracks WHERE release_id=? AND hidden=0', [canonical_id]
    ).fetchall()
    by_isrc  = {r[1]: r[0] for r in canon_rows if r[1]}
    by_title = {_norm(r[2]): r[0] for r in canon_rows}

    var_rows = conn.execute(
        'SELECT id, isrc, title FROM tracks WHERE release_id=? AND hidden=0', [variant_id]
    ).fetchall()

    listens_moved = tracks_hidden = 0
    for vid, visrc, vtitle in var_rows:
        canon_tid = (by_isrc.get(visrc) if visrc else None) or by_title.get(_norm(vtitle))
        if not canon_tid:
            continue
        listens_moved += conn.execute(
            'UPDATE listens SET track_id=? WHERE track_id=?', [canon_tid, vid]
        ).rowcount
        conn.execute('UPDATE tracks SET hidden=1 WHERE id=?', [vid])
        tracks_hidden += 1
    return listens_moved, tracks_hidden


def _write_group(conn, canonical, type_updates, edition_links, hide_ids):
    """Write all accumulated type/variant/hide changes for one group."""
    for rid in hide_ids:
        conn.execute('UPDATE releases SET hidden = 1 WHERE id = ?', (rid,))

    for rid, (ptype, stype) in type_updates.items():
        conn.execute(
            'UPDATE releases SET type = ?, type_secondary = ? WHERE id = ?',
            (ptype, stype, rid),
        )

    for variant_id, edition_type, sort_order in edition_links:
        conn.execute(
            '''INSERT INTO release_variants (canonical_id, variant_id, variant_type, sort_order)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(canonical_id, variant_id) DO UPDATE SET
                   variant_type = excluded.variant_type,
                   sort_order   = excluded.sort_order''',
            (canonical['id'], variant_id, edition_type, sort_order),
        )
        # Always hide variant and merge shared-track listens to canonical
        conn.execute('UPDATE releases SET hidden=1 WHERE id=?', [variant_id])
        _merge_variant_tracks(conn, canonical['id'], variant_id)

    conn.commit()


# ── cmd: certs refresh ────────────────────────────────────────────────────────

_CERT_THRESHOLDS = [
    ('diamond',  1000),
    ('platinum',  500),
    ('gold',      250),
]


def cmd_genre_relations(args):
    """Populate genre_relations from a tab-indented tree file."""
    import os
    tree_path = args.tree or os.path.join(os.path.expanduser('~'), 'genre_tree.txt')
    if not os.path.exists(tree_path):
        console.print(f'[red]Tree file not found: {tree_path}[/red]')
        return
    conn = open_db(args.db or DB_PATH)
    init_schema(conn)
    # Clear existing relations so a re-run is idempotent
    conn.execute('DELETE FROM genre_relations')
    conn.commit()
    inserted, skipped = populate_genre_relations(conn, tree_path)
    conn.close()
    console.print(f'  [green]✓ {inserted} genre relations inserted[/green]'
                  f'  [dim]({skipped} tree entries not in DB)[/dim]')


# -- Genre Commit Graph --

_TOP_GENRE_HSL: dict[str, tuple[int, int, int]] = {
    'Rock':                 ( 18, 78, 54),
    'Electronic':           (191, 72, 50),
    'Hip Hop':              ( 44, 82, 54),
    'Pop':                  (323, 70, 61),
    'R&B':                  (258, 57, 60),
    'Metal':                (  4, 76, 49),
    'Jazz':                 ( 33, 72, 54),
    'Folk':                 ( 97, 55, 50),
    'Experimental':         (233, 38, 52),  # blue-indigo (was 205, too close to Ambient)
    'Punk':                 (160, 60, 50),
    'Classical':            (283, 50, 57),
    'Ambient':              (207, 58, 59),
    'Dance':                (178, 67, 50),
    'Funk':                 ( 28, 82, 55),  # boosted S to separate from Jazz/Rock
    'Country':              ( 68, 62, 50),  # yellow-green/pastoral (was 38, too close to Jazz)
    'Singer-Songwriter':    ( 35, 50, 63),  # muted warm (was 30,64,55 — too close to Marching Band)
    'Psychedelia':          (292, 53, 58),
    'Industrial':           (216, 28, 49),
    'Reggae':               (145, 57, 48),
    'Blues':                (223, 58, 54),
    'Darkwave':             (248, 45, 44),
    'Spoken Word':          (200, 18, 60),
    'Glitch Pop':           (305, 68, 60),  # magenta (was 295, too close to Psychedelia)
    'Hypnagogic Pop':       (310, 60, 63),
    'Ambient Pop':          (217, 44, 68),  # periwinkle (was 200, too close to Ambient/Spoken Word)
    'Sampledelia':          (188, 55, 54),
    'Mashup':               (240, 52, 58),  # indigo (was 278, too close to Classical)
    'Vapor':                (300, 50, 70),  # pastel magenta/lilac (was 228, too close to Blues)
    'Field Recordings':     ( 28, 30, 48),
    'Easy Listening':       (182, 42, 67),  # pastel teal (was 52, too close to warm cluster)
    'New Age':              (168, 40, 59),
    'Gospel':               ( 50, 60, 57),
    'CCM':                  (265, 45, 67),  # soft lavender (was 48, too close to warm cluster)
    'Ska':                  (132, 52, 50),
    'Flamenco':             (348, 72, 44),  # wine-dark red (was 356, identical to Christmas)
    'Regional':             ( 24, 42, 51),
    'Standards':            ( 42, 46, 58),
    'Comedy':               ( 58, 55, 62),
    'Ragtime':              ( 35, 58, 52),
    'Toypop':               (330, 65, 68),
    'Polka':                ( 20, 55, 60),
    'Marching Band':        ( 28, 52, 56),
    'Chanson':              ( 14, 50, 58),
    'MPB':                  (112, 48, 52),
    'Hymns':                ( 50, 40, 60),
    "Children's Music":     ( 55, 60, 68),
    'Christmas':            (128, 65, 46),  # holly green (was 355, identical to Flamenco)
    'ASMR':                 (190, 28, 68),  # very soft blue-gray (was 170, too close to New Age)
    'Musical Parody':       ( 60, 48, 62),
    'Musical Theatre & Entertainment': (45, 55, 63),
}
_DEFAULT_HSL = (210, 20, 60)  # fallback gray-blue for unmapped roots


def _hsl_to_hex(h: float, s: float, l: float) -> str:
    """Convert HSL (h 0-360, s 0-100, l 0-100) to '#rrggbb'."""
    s /= 100.0
    l /= 100.0
    c = (1 - abs(2 * l - 1)) * s
    x = c * (1 - abs((h / 60) % 2 - 1))
    m = l - c / 2
    if   0   <= h < 60:  r, g, b = c, x, 0
    elif 60  <= h < 120: r, g, b = x, c, 0
    elif 120 <= h < 180: r, g, b = 0, c, x
    elif 180 <= h < 240: r, g, b = 0, x, c
    elif 240 <= h < 300: r, g, b = x, 0, c
    else:                r, g, b = c, 0, x
    return f'#{int((r+m)*255):02x}{int((g+m)*255):02x}{int((b+m)*255):02x}'


def _build_genre_root_map(tree_path: str) -> dict[str, dict[str, float]]:
    """
    Parse tab-indented genre tree and return {genre_name: {root_name: weight}}.
    Weights sum to 1.0 per genre. Multi-parent genres split weight equally up
    the hierarchy until reaching top-level (parentless) genres.
    """
    parents: dict[str, set[str]] = {}
    stack: dict[int, str] = {}

    with open(tree_path, encoding='utf-8') as f:
        for line in f:
            stripped = line.rstrip('\n')
            if not stripped.strip():
                continue
            depth = len(stripped) - len(stripped.lstrip('\t'))
            name  = stripped.strip()
            # Prune stale deeper entries
            for d in [d for d in stack if d > depth]:
                del stack[d]
            stack[depth] = name
            parents.setdefault(name, set())
            if depth > 0 and (parent := stack.get(depth - 1)):
                parents[name].add(parent)

    cache: dict[str, dict[str, float]] = {}

    def find_roots(start: str) -> dict[str, float]:
        if start in cache:
            return cache[start]
        current = {start: 1.0}
        result: dict[str, float] = {}
        for _ in range(25):  # max depth guard against cycles
            if not current:
                break
            nxt: dict[str, float] = {}
            for name, weight in current.items():
                pars = parents.get(name, set())
                if not pars:
                    result[name] = result.get(name, 0.0) + weight
                else:
                    share = weight / len(pars)
                    for p in pars:
                        nxt[p] = nxt.get(p, 0.0) + share
            current = nxt
        cache[start] = result
        return result

    return {name: find_roots(name) for name in parents}


def _blend_genres(
    root_weights: dict[str, float],
) -> tuple[str, list[dict]]:
    """
    Blend genre colors via circular mean of hue, arithmetic mean of S/L.
    Returns (hex_color, top_genres_list) where each entry is
    {'genre': str, 'pct': float, 'color': str}.
    """
    import math

    total = sum(root_weights.values())
    if total == 0:
        return '#64748B', []

    sin_sum = cos_sum = s_sum = l_sum = 0.0
    for genre, weight in root_weights.items():
        h, s, l = _TOP_GENRE_HSL.get(genre, _DEFAULT_HSL)
        frac = weight / total
        rad   = math.radians(h)
        sin_sum += math.sin(rad) * frac
        cos_sum += math.cos(rad) * frac
        s_sum   += s * frac
        l_sum   += l * frac

    avg_h = math.degrees(math.atan2(sin_sum, cos_sum)) % 360
    color = _hsl_to_hex(avg_h, s_sum, l_sum)

    top = [
        {
            'genre': g,
            'pct':   round(w / total * 100, 1),
            'color': _hsl_to_hex(*_TOP_GENRE_HSL.get(g, _DEFAULT_HSL)),
        }
        for g, w in sorted(root_weights.items(), key=lambda x: x[1], reverse=True)
        if w / total >= 0.02
    ][:6]

    return color, top


def cmd_commits_refresh(args):
    """Compute monthly genre profiles and cache to monthly_genre_profile table."""
    import json
    import os

    tree_path = args.tree or os.path.join(os.path.expanduser('~'), 'genre_tree.txt')
    if not os.path.exists(tree_path):
        console.print(f'[red]Tree file not found: {tree_path}[/red]')
        return

    conn = open_db(args.db or DB_PATH)
    init_schema(conn)

    console.print('[dim]Building genre root map…[/dim]')
    genre_root_map = _build_genre_root_map(tree_path)

    months = conn.execute(
        'SELECT DISTINCT year, month FROM listens ORDER BY year, month'
    ).fetchall()
    console.print(f'[dim]Processing {len(months)} months…[/dim]')

    rows = []
    for year, month in months:
        listen_count = conn.execute(
            'SELECT COUNT(*) FROM listens WHERE year=? AND month=?', (year, month)
        ).fetchone()[0]

        genre_rows = conn.execute('''
            SELECT l.id, g.name
            FROM listens l
            JOIN tracks t          ON t.id = l.track_id AND t.hidden = 0
            JOIN release_genres rg ON rg.release_id = t.release_id
            JOIN genres g          ON g.aoty_id = rg.aoty_genre_id
            WHERE l.year = ? AND l.month = ?
        ''', (year, month)).fetchall()

        if not genre_rows:
            rows.append((year, month, listen_count, '#64748B', '#64748B', None, None))
            continue

        listen_genres: dict[int, list[str]] = {}
        for lid, gname in genre_rows:
            listen_genres.setdefault(lid, []).append(gname)

        root_weights: dict[str, float] = {}
        for genres in listen_genres.values():
            genre_wt = 1.0 / len(genres)
            for gname in genres:
                for root, rw in genre_root_map.get(gname, {gname: 1.0}).items():
                    root_weights[root] = root_weights.get(root, 0.0) + genre_wt * rw

        color, top = _blend_genres(root_weights)
        dominant    = top[0]['genre'] if top else None
        top_genre_color = _hsl_to_hex(*_TOP_GENRE_HSL.get(dominant, _DEFAULT_HSL)) if dominant else '#64748B'
        genres_json = json.dumps(top) if top else None
        rows.append((year, month, listen_count, color, top_genre_color, dominant, genres_json))

    conn.execute('DELETE FROM monthly_genre_profile')
    conn.executemany(
        'INSERT INTO monthly_genre_profile '
        '(year, month, listen_count, color_hex, top_genre_color_hex, dominant_genre, genres_json) '
        'VALUES (?, ?, ?, ?, ?, ?, ?)',
        rows,
    )
    conn.commit()
    conn.close()
    console.print(f'  [green]✓ {len(rows)} months cached[/green]')


def cmd_certs_refresh(args):
    """Recompute cert tiers for all artists based on all-time listen counts."""
    conn = open_db(args.db or DB_PATH)
    init_schema(conn)

    rows = conn.execute('''
        SELECT a.id, a.name, COUNT(l.id) AS total
        FROM   artists a
        JOIN   release_artists ra ON ra.artist_id = a.id AND ra.role = 'main'
        JOIN   tracks t           ON t.release_id = ra.release_id AND (t.hidden IS NULL OR t.hidden = 0)
        JOIN   listens l          ON l.track_id = t.id
        WHERE  (a.hidden IS NULL OR a.hidden = 0)
        GROUP  BY a.id
    ''').fetchall()

    counts = {r['id']: (r['name'], r['total']) for r in rows}

    # Compute new cert for every artist (NULL if below gold threshold)
    updates = {}
    for artist_id, (name, total) in counts.items():
        cert = None
        for tier, threshold in _CERT_THRESHOLDS:
            if total >= threshold:
                cert = tier
                break
        updates[artist_id] = cert

    # Also clear cert for artists with no listens (hidden releases, etc.)
    all_artists = conn.execute('SELECT id FROM artists').fetchall()
    for row in all_artists:
        if row['id'] not in updates:
            updates[row['id']] = None

    conn.executemany('UPDATE artists SET cert = ? WHERE id = ?',
                     [(cert, aid) for aid, cert in updates.items()])
    conn.commit()
    conn.close()

    tier_counts = {}
    for cert in updates.values():
        if cert:
            tier_counts[cert] = tier_counts.get(cert, 0) + 1

    total_certified = sum(tier_counts.values())
    console.print(f'[bold]Certs refreshed[/bold]  ({total_certified} artists certified)')
    for tier, _ in _CERT_THRESHOLDS:
        n = tier_counts.get(tier, 0)
        if n:
            console.print(f'  {tier:10s}  {n}')


# ── main ──────────────────────────────────────────────────────────────────────


def cmd_track_variants_wrapper(args):
    """Dispatch to the interactive track-variants loop in mdb_cli."""
    db_path = getattr(args, 'db', None) or DB_PATH
    conn    = open_db(db_path)
    init_schema(conn)
    include_linked = getattr(args, 'all', False)
    try:
        cmd_track_variants(conn, include_linked=include_linked)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        prog='mdb',
        description='Music database import and enrichment tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest='cmd', required=True)

    # diff
    p_diff = sub.add_parser('diff', help='Compare two or more Spotify, MusicBrainz, or DB releases')
    p_diff.add_argument('albums', nargs='+', metavar='ALBUM',
                        help='Spotify URLs/IDs, MusicBrainz URLs/UUIDs, or db:ULID / bare ULID (2 or more)')
    p_diff.add_argument('--db', metavar='PATH', help='Path to master.sqlite (for DB releases)')
    p_diff.set_defaults(func=cmd_diff)

    # import
    p = sub.add_parser('import', help='Import Spotify album(s)')
    p.add_argument('albums', nargs='+', metavar='ALBUM',
                   help='Spotify album URL/ID or a batch file (csv/yaml/txt)')
    p.add_argument('--no-mb',   action='store_true', help='Skip MusicBrainz lookup')
    p.add_argument('--no-aoty', action='store_true', help='Skip AOTY enrichment')
    p.add_argument('--no-wiki', action='store_true', help='Skip Wikipedia date lookup')
    p.add_argument('--db',      metavar='PATH',      help='Path to master.sqlite')
    p.set_defaults(func=cmd_import)

    # enrich
    p_enrich = sub.add_parser('enrich', help='Enrich existing DB entries')
    es       = p_enrich.add_subparsers(dest='enrich_cmd', required=True)

    def _add_filter_args(p_):
        p_.add_argument('--artist',     metavar='NAME_OR_ID', help='Limit to one artist')
        p_.add_argument('--release-id', metavar='ID',         help='Process a single release')
        p_.add_argument('--skip',       type=int, default=0,  help='Skip first N')
        p_.add_argument('--limit',      type=int,             help='Process at most N')
        p_.add_argument('--db',         metavar='PATH',       help='Path to master.sqlite')

    p_aoty = es.add_parser('aoty', help='Scrape Album of the Year for genres/dates/types')
    _add_filter_args(p_aoty)
    p_aoty.add_argument('--auto',           action='store_true', help='Auto-accept without prompting')
    p_aoty.add_argument('--overwrite-date', action='store_true', help='Overwrite existing dates')
    p_aoty.add_argument('--overwrite-type', action='store_true', help='Overwrite existing types')
    p_aoty.add_argument('--overwrite-genre',action='store_true', help='Re-process already-tagged releases')
    p_aoty.add_argument('--force',          action='store_true', help='Re-try releases previously marked as not-found')
    p_aoty.add_argument('--verbose',        action='store_true', help='Debug scraping output')
    p_aoty.set_defaults(func=cmd_enrich_aoty)

    p_dates = es.add_parser('dates', help='Look up release dates via Wikipedia + MusicBrainz')
    _add_filter_args(p_dates)
    p_dates.add_argument('--overwrite', action='store_true', help='Overwrite existing dates')
    p_dates.add_argument('--verbose',   action='store_true', help='Debug output')
    p_dates.set_defaults(func=cmd_enrich_dates)

    p_tracks = es.add_parser('tracks', help='Fetch track MBIDs from MusicBrainz')
    _add_filter_args(p_tracks)
    p_tracks.set_defaults(func=cmd_enrich_tracks)

    p_audio = es.add_parser('audio', help='Fetch Spotify audio features (BPM, energy, etc.)')
    _add_filter_args(p_audio)
    p_audio.set_defaults(func=cmd_enrich_audio)

    p_artists_enrich = es.add_parser('artists', help='Fetch artist metadata from MusicBrainz')
    p_artists_enrich.add_argument('--artist',    metavar='NAME_OR_ID', help='Limit to one artist')
    p_artists_enrich.add_argument('--overwrite', action='store_true',  help='Re-fetch even if already populated')
    p_artists_enrich.add_argument('--db',        metavar='PATH',       help='Path to master.sqlite')
    p_artists_enrich.set_defaults(func=cmd_enrich_artists)

    p_art = es.add_parser('art', help='Fill in or replace album art (CAA → Spotify → manual URL)')
    _add_filter_args(p_art)
    p_art.add_argument('--overwrite',    action='store_true', help='Re-process releases that already have art')
    p_art.add_argument('--interactive',  action='store_true', help='Prompt for each release instead of auto-applying')
    p_art.set_defaults(func=cmd_enrich_art)

    # hide
    p = sub.add_parser('hide', help='Bulk hide or unhide artists, tracks, or releases')
    p.add_argument('entity',   choices=['artists', 'tracks', 'releases'])
    p.add_argument('csv_file', metavar='CSV')
    p.add_argument('--unhide', action='store_true')
    p.add_argument('--db',     metavar='PATH')
    p.set_defaults(func=cmd_hide)

    # delete
    p_del = sub.add_parser('delete', help='Delete releases or artists (cascades to tracks)')
    p_del.add_argument('entity', choices=['releases', 'artists'])
    p_del.add_argument('ids', nargs='+', metavar='ID',
                       help='One or more: sp:SPOTIFY_ID, SPOTIFY_ID, db:ULID, or bare ULID')
    p_del.add_argument('--force', action='store_true',
                       help='Also delete listens that reference the affected tracks')
    p_del.add_argument('--db', metavar='PATH')
    p_del.set_defaults(func=cmd_delete)

    # artist
    p_artist = sub.add_parser('artist', help='Manage artist metadata')
    as_      = p_artist.add_subparsers(dest='artist_cmd', required=True)
    p_img    = as_.add_parser('images', help='Bulk update artist profile images from CSV')
    p_img.add_argument('csv_file', metavar='CSV',
                       help='CSV with columns: artist_name, profile_image_url[, profile_image_crop]')
    p_img.add_argument('--db', metavar='PATH')
    p_img.set_defaults(func=cmd_artist_images)

    p_merge = as_.add_parser('merge', help='Merge FROM artist into TO (canonical) artist')
    p_merge.add_argument('from_artist', metavar='FROM',
                         help='Artist to remove (slug, ID, or name)')
    p_merge.add_argument('to_artist',   metavar='TO',
                         help='Canonical artist to keep (slug, ID, or name)')
    p_merge.add_argument('--no-alias', action='store_true',
                         help="Don't add FROM name as a past_name alias on TO")
    p_merge.add_argument('--db', metavar='PATH')
    p_merge.set_defaults(func=cmd_artist_merge)

    p_members = as_.add_parser('members', help='Manage supergroup membership')
    p_members.add_argument('--db', metavar='PATH')
    ms_ = p_members.add_subparsers(dest='members_cmd', required=True)

    p_mem_add = ms_.add_parser('add', help='Add member(s) to a group artist')
    p_mem_add.add_argument('group',   metavar='GROUP')
    p_mem_add.add_argument('members', metavar='MEMBER', nargs='+')
    p_mem_add.set_defaults(func=cmd_artist_members)

    p_mem_rm = ms_.add_parser('remove', help='Remove a member from a group')
    p_mem_rm.add_argument('group',  metavar='GROUP')
    p_mem_rm.add_argument('member', metavar='MEMBER')
    p_mem_rm.set_defaults(func=cmd_artist_members)

    p_mem_ls = ms_.add_parser('list', help='List members of a group artist')
    p_mem_ls.add_argument('group', metavar='GROUP')
    p_mem_ls.set_defaults(func=cmd_artist_members)

    # release
    p_release = sub.add_parser('release', help='Manage release metadata')
    rs2_      = p_release.add_subparsers(dest='release_cmd', required=True)
    p_rvariants = rs2_.add_parser('variants', help='Interactive editor for release variant groups')
    p_rvariants.add_argument('--all', action='store_true',
                             help='Include groups already fully linked in release_variants')
    p_rvariants.add_argument('--db', metavar='PATH', help='Path to master.sqlite')
    p_rvariants.set_defaults(func=cmd_release_variants)

    p_ralias  = rs2_.add_parser('alias', help='Manage release title aliases')
    ras_      = p_ralias.add_subparsers(dest='release_alias_cmd', required=True)

    p_ra_add = ras_.add_parser('add', help='Add a title alias for a release')
    p_ra_add.add_argument('release', metavar='RELEASE',
                          help='Spotify URL/ID, MusicBrainz ID, internal ID, or title')
    p_ra_add.add_argument('alias',   metavar='ALIAS', help='Alias title to add')
    p_ra_add.add_argument('--definitive', action='store_true',
                          help='Mark as the authoritative/official alternate title')
    p_ra_add.add_argument('--source', choices=['manual', 'musicbrainz'], default='manual')
    p_ra_add.add_argument('--db', metavar='PATH')
    p_ra_add.set_defaults(func=cmd_release_alias)

    p_ra_rm = ras_.add_parser('remove', help='Remove a title alias')
    p_ra_rm.add_argument('release', metavar='RELEASE')
    p_ra_rm.add_argument('alias',   metavar='ALIAS')
    p_ra_rm.add_argument('--db', metavar='PATH')
    p_ra_rm.set_defaults(func=cmd_release_alias)

    p_ra_ls = ras_.add_parser('list', help='List title aliases for a release')
    p_ra_ls.add_argument('release', metavar='RELEASE')
    p_ra_ls.add_argument('--db', metavar='PATH')
    p_ra_ls.set_defaults(func=cmd_release_alias)

    # tracks
    p_tracks_cmd = sub.add_parser('tracks', help='Manage track metadata')
    ts_          = p_tracks_cmd.add_subparsers(dest='tracks_cmd', required=True)
    p_tvariants  = ts_.add_parser('variants',
                                  help='Interactive editor for track variant groups')
    p_tvariants.add_argument('--all', action='store_true',
                             help='Include groups already fully linked')
    p_tvariants.add_argument('--db', metavar='PATH', help='Path to master.sqlite')
    p_tvariants.set_defaults(func=cmd_track_variants_wrapper)

    # link
    p_link  = sub.add_parser('link', help='Link release relationships')
    ls_     = p_link.add_subparsers(dest='link_cmd', required=True)
    p_src   = ls_.add_parser('sources',
                              help='Record which releases a compilation was assembled from')
    p_src.add_argument('compilation', metavar='COMPILATION',
                       help='Spotify URL/ID or internal ID of the compilation release')
    p_src.add_argument('sources', nargs='+', metavar='SOURCE[:disc=N]',
                       help='Source release(s) with optional :disc=N annotation')
    p_src.add_argument('--db', metavar='PATH')
    p_src.set_defaults(func=cmd_link_sources)

    # migrate
    p_migrate = sub.add_parser('migrate', help='One-time data migrations')
    ms_       = p_migrate.add_subparsers(dest='migrate_cmd', required=True)
    p_mg      = ms_.add_parser('genres', help='Copy genres from legacy overrides DB by MBID')
    p_mg.add_argument('legacy_db', metavar='LEGACY_DB',
                      help='Path to listening_history_overrides.sqlite')
    p_mg.add_argument('--db', metavar='PATH', help='Path to master.sqlite')
    p_mg.set_defaults(func=cmd_migrate_genres)

    p_mas = ms_.add_parser('artist-slugs',
                           help='Backfill ULID ids + slug column for legacy artists')
    p_mas.add_argument('--db', metavar='PATH', help='Path to master.sqlite')
    p_mas.set_defaults(func=cmd_migrate_artist_slugs)

    # alias
    p_alias = sub.add_parser('alias', help='Manage artist name aliases')
    als_    = p_alias.add_subparsers(dest='alias_cmd', required=True)
    p_al_add = als_.add_parser('add', help='Add an alias for an artist')
    p_al_add.add_argument('artist', metavar='ARTIST',
                          help='Artist slug, Spotify ID, or internal ID')
    p_al_add.add_argument('alias', metavar='ALIAS',
                          help='Alias name (e.g. "Totally Enormous Extinct Dinosaurs")')
    p_al_add.add_argument('--type', dest='alias_type',
                          choices=['past_name', 'native_script', 'common'],
                          default='common',
                          help='Alias type (default: common)')
    p_al_add.add_argument('--language', metavar='LANG',
                          help='BCP-47 language tag, e.g. "ja" for Japanese')
    p_al_add.add_argument('--sort-order', dest='sort_order', type=int, default=0,
                          help='Sort order within alias type (lower = first)')
    p_al_add.add_argument('--source', choices=['manual', 'lastfm', 'musicbrainz'],
                          default='manual')
    p_al_add.add_argument('--db', metavar='PATH')
    p_al_add.set_defaults(func=cmd_alias)
    p_al_rm = als_.add_parser('remove', help='Remove an alias')
    p_al_rm.add_argument('artist', metavar='ARTIST')
    p_al_rm.add_argument('alias',  metavar='ALIAS')
    p_al_rm.add_argument('--db', metavar='PATH')
    p_al_rm.set_defaults(func=cmd_alias)
    p_al_ls = als_.add_parser('list', help='List aliases for an artist')
    p_al_ls.add_argument('artist', metavar='ARTIST')
    p_al_ls.add_argument('--db', metavar='PATH')
    p_al_ls.set_defaults(func=cmd_alias)

    # relation
    p_rel = sub.add_parser('relation', help='Manage artist-to-artist relationships')
    rs_   = p_rel.add_subparsers(dest='relation_cmd', required=True)
    _rel_types = ['member', 'collaboration', 'side_project']
    p_r_add = rs_.add_parser('add', help='Add a relationship between two artists')
    p_r_add.add_argument('from_artist', metavar='FROM',
                         help='Artist who is the member / subject')
    p_r_add.add_argument('to_artist', metavar='TO',
                         help='Group / project they belong to')
    p_r_add.add_argument('type', metavar='TYPE', choices=_rel_types,
                         help=f'Relationship type: {", ".join(_rel_types)}')
    p_r_add.add_argument('--db', metavar='PATH')
    p_r_add.set_defaults(func=cmd_relation)
    p_r_rm = rs_.add_parser('remove', help='Remove a relationship')
    p_r_rm.add_argument('from_artist', metavar='FROM')
    p_r_rm.add_argument('to_artist',   metavar='TO')
    p_r_rm.add_argument('type', metavar='TYPE', choices=_rel_types)
    p_r_rm.add_argument('--db', metavar='PATH')
    p_r_rm.set_defaults(func=cmd_relation)
    p_r_ls = rs_.add_parser('list', help='List relationships for an artist')
    p_r_ls.add_argument('artist', metavar='ARTIST')
    p_r_ls.add_argument('--db', metavar='PATH')
    p_r_ls.set_defaults(func=cmd_relation)

    # certs
    p_certs  = sub.add_parser('certs', help='Manage certification tiers')
    cs_      = p_certs.add_subparsers(dest='certs_cmd', required=True)
    p_c_ref  = cs_.add_parser('refresh', help='Recompute gold/platinum/diamond tiers for all artists')
    p_c_ref.add_argument('--db', metavar='PATH', help='Path to master.sqlite')
    p_c_ref.set_defaults(func=cmd_certs_refresh)

    # genre-relations
    p_gr = sub.add_parser('genre-relations', help='Populate genre parent/child relations from tree file')
    p_gr.add_argument('--tree', metavar='PATH', help='Path to tab-indented genre tree file')
    p_gr.add_argument('--db',   metavar='PATH', help='Path to master.sqlite')
    p_gr.set_defaults(func=cmd_genre_relations)

    # commits
    p_commits  = sub.add_parser('commits', help='Genre commit graph')
    cs2_       = p_commits.add_subparsers(dest='commits_cmd', required=True)
    p_c2_ref   = cs2_.add_parser('refresh', help='Compute monthly genre profiles and cache to DB')
    p_c2_ref.add_argument('--tree', metavar='PATH', help='Path to tab-indented genre tree file')
    p_c2_ref.add_argument('--db',   metavar='PATH', help='Path to master.sqlite')
    p_c2_ref.set_defaults(func=cmd_commits_refresh)

    args = parser.parse_args()
    try:
        args.func(args)
    except KeyboardInterrupt:
        console.print('\n[dim]Interrupted.[/dim]')
        sys.exit(0)


if __name__ == '__main__':
    main()
