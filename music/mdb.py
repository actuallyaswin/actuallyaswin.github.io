#!/usr/bin/env python3
"""
mdb — Music Database CLI

Consolidated import and enrichment tool for master.sqlite.

Usage:
  mdb import  <album…|file>               Import Spotify album(s) + MB + AOTY + Wikipedia
  mdb enrich  aoty  [options]             Scrape AOTY for genres, dates, and types
  mdb enrich  art   [options]             Fill in missing album art (Apple Music → Spotify → manual)
  mdb enrich  dates [options]             Look up release dates via Wikipedia + MusicBrainz
  mdb enrich  tracks [options]            Fetch track MBIDs from MusicBrainz
  mdb enrich  soundtracks [options]       Tag soundtrack releases with source type, region, and language
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
import difflib
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import struct
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import hashlib
import getpass
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from rich.console import Console
from rich.rule import Rule
from rich.table import Table
from rich import box as rbox
from mdb_strings import (
    is_valid_mbid,
    detect_variant_type, detect_variant_types, detect_variant_label,
    _base_title, same_song_key,
    _PRIMARY_TYPES, _SECONDARY_TYPES, _EDITION_TYPES,
    ascii_key as _norm,
    _should_update_date, _parse_user_date,
    extract_mbid,
    extract_spotify_id,
    parse_track_title,
    normalize_text,
)
from mdb_ops import (
    load_dotenv,
    DB_PATH, open_db, init_schema, managed_db,
    upsert_artist_mb, upsert_release_mb, upsert_tracks_mb,
    populate_genre_relations,
    bulk_rematch, bulk_rematch_by_name,
    upsert_external_link, EL_ARTIST, EL_RELEASE, EL_SVC_WIKIPEDIA,
    EL_SVC_BEATPORT, EL_SVC_BANDCAMP, EL_SVC_DEEZER,
    EL_SVC_DISCOGS, EL_SVC_SPOTIFY, link_discogs,
    upsert_service_link,
    resolve_artist,
    save_aoty_data, save_release_date,
    upsert_artist_alias, upsert_release_alias,
    merge_variant_tracks,
    db_search_releases,
)
from mdb_apis import (
    SpotifyClient,
    MusicBrainzRelease,
    BeatportRelease, ItunesRelease, BandcampRelease, DeezerRelease,
    MB_UA,
    ITUNES_LOOKUP, ITUNES_SEARCH,
    AOTY_AHEAD, DATES_AHEAD,
    caa_fetch_front_image_url,
    itunes_fetch_artwork_url,
    itunes_lookup_by_upc,
    itunes_search_by_title,
    apple_music_fetch_editorial_note,
    _mb_get_safe, _http_get_json, _itunes_lim,
    mb_fetch_recording_ids, mb_fetch_artist_data,
    mb_fetch_release_group_releases,
    mb_canonical_score,
    mb_release_reasons,
    mb_rg_from_wiki_url,
    mb_find_release_group,
    _EDITION_RE,
)
from mdb_errors import NoSourcesAvailable, SourceError
from mdb_merge import (
    ReleaseMerge, MDBRelease,
    upsert_release_mdb, upsert_tracks_mdb,
    resolve_by_gtin,
    _resolve_artist_credit,
)
from mdb_websources import (
    scrape_aoty_page, fetch_aoty_data,
    scrape_aoty_genre_relations,
    _has_aoty,
    fetch_date_candidates,
)
from mdb_cli import (
    _fmt_dur, _trunc,
    _print_member,
    _aoty_prompt, _dates_prompt, _prompt_choice,
    cmd_track_variants,
    cmd_enrich_soundtracks,
)

try:
    import requests
    from bs4 import BeautifulSoup
    _AOTY_AVAILABLE = True
except ImportError:
    _AOTY_AVAILABLE = False

console = Console(width=80, highlight=False)
log = logging.getLogger(__name__)

# Convenience mirror of the most recent import_album_unified() call's structured
# warnings. NOT thread/concurrency-safe — it's overwritten on every call, so it's
# only useful for single-threaded scripts that don't want to thread a list
# through. Callers that need per-call isolation should pass warnings_out=... .
LAST_IMPORT_WARNINGS: list = []


def _paginate(rows: list, args) -> list:
    """Apply --skip/--limit to a fetched row list (CLI subcommands page in Python, not SQL)."""
    if args.skip:
        rows = rows[args.skip:]
    if args.limit:
        rows = rows[:args.limit]
    return rows


def _artist_filter_clause(conn, artist_name):
    """Resolve an --artist name into a WHERE fragment + params for release
    queries. release_artists isn't populated for every release (e.g.
    direct-SQL imports), so this also matches the primary_artist_id
    fallback. Returns (None, None) if the name doesn't resolve — the
    caller should report the failure and bail out.
    """
    row = resolve_artist(conn, artist_name)
    if not row:
        return None, None
    return 'AND (ra.artist_id = ? OR r.primary_artist_id = ?)', [row['id'], row['id']]


# ── Batch file reader ─────────────────────────────────────────────────────────

_CSV_ID_COLS  = {'url', 'spotify_url', 'spotify_id', 'id', 'album_id'}
_RE_SP_URL    = re.compile(r'https?://open\.spotify\.com/(?:album|prerelease)/([A-Za-z0-9]+)(?:\?[^\s,]*)?',
                           re.IGNORECASE)
_RE_BP_URL    = re.compile(r'https?://(?:www\.)?beatport\.com/release/([^/?#,\s]+)/(\d+)',
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
        # Beatport URL
        m = _RE_BP_URL.search(token)
        if m:
            bp_slug, bp_id = m.group(1), int(m.group(2))
            entries.append({
                'url':         f'https://www.beatport.com/release/{bp_slug}/{bp_id}',
                'beatport_id': bp_id,
                'discs':       None,
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
    """Import a Spotify album. Thin wrapper around import_album_unified.

    Returns (release_id, title, artist_name, release_date).
    Kept for backwards compatibility with any callers that pass a pre-built
    SpotifyClient — import_album_unified re-uses it via the _sp_client() closure.
    """
    return import_album_unified(
        db_path, album_url,
        client=client,
        no_gtin=False,
        no_mb=not use_mb,
        # AOTY/wiki are handled by cmd_import's post-import steps, not here
        use_aoty=False,
        use_wiki=False,
    )


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
    with managed_db(db_path) as conn:
        cached     = conn.execute('SELECT aoty_url FROM releases WHERE id = ?', (release_id,)).fetchone()
        cached_url = cached[0] if cached else None
        url, data  = fetch_aoty_data(release_title, artist_name, cached_url)
        if _has_aoty(data):
            save_aoty_data(conn, release_id, url, data)
            primary   = [n for _, n, _, p in data['genres'] if p]
            genre_str = '  ·  ' + '  ·  '.join(primary) if primary else ''
            score_str = ''
            if data.get('score_critic') is not None:
                score_str = f'  [dim]({data["score_critic"]}/100)[/dim]'
            console.print(f'      [dim]·  AOTY{genre_str}[/dim]{score_str}')

def _import_wiki_step(db_path, release_id, release_title, artist_name):
    # No rule separator — only prints when it actually changes something.
    with managed_db(db_path) as conn:
        row  = conn.execute(
            'SELECT mbid, release_date, date_source, type, type_secondary FROM releases WHERE id = ?',
            (release_id,)
        ).fetchone()
        if not row or not row['mbid']:
            # silent — no MBID
            return
        rtype = (row['type'] or '').lower()
        rsec  = (row['type_secondary'] or '').lower()
        if rtype == 'single' or rsec in ('remix', 'dj-mix'):
            # silent — singles/remixes skip Wikipedia
            return
        mbid         = row['mbid']
        release_year = (row['release_date'] or '')[:4] or None
        candidates, wiki_page_id = fetch_date_candidates(
            mbid, release_title, artist_name,
            release_year=release_year,
            release_type=rtype or None,
        )
        if candidates:
            best  = candidates[0]
            saved = save_release_date(conn, release_id, best['date'], wiki_page_id, source='musicbrainz')
            if saved:
                console.print(f'      [dim]·  date updated → {best["date"]}  (MusicBrainz)[/dim]')
        else:
            if wiki_page_id:
                upsert_external_link(conn, EL_RELEASE, release_id, EL_SVC_WIKIPEDIA, str(wiki_page_id))
                conn.commit()
            # Silent when no date found — not finding a date is normal

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
        # lazy
        self._tracks = None

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


def import_album_from_mb(db_path: str, mbid: str, *,
                         use_aoty: bool = True,
                         use_wiki: bool = True) -> 'tuple[str, str, str, str]':
    """Import a MusicBrainz release into master.sqlite.
    Returns (release_id, title, artist_name, release_date)."""
    rel = MusicBrainzRelease(mbid)
    rel._ensure_full()

    try:
        image_url = caa_fetch_front_image_url(mbid)
    except Exception as e:
        log.debug('CAA fetch failed: %s', e)
        image_url = None

    conn = open_db(db_path)
    cur  = conn.cursor()
    init_schema(conn)

    try:
        # Collect all unique MB artist IDs from release + track credits
        artist_credits_seen: dict = {}
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

        artist_map: dict = {}
        for mb_aid, mb_a in artist_credits_seen.items():
            our_id, created = upsert_artist_mb(cur, mb_a)
            artist_map[mb_aid] = our_id

        primary_id = None
        for credit in (rel._data.get('artist-credit') or []):
            if isinstance(credit, dict) and 'artist' in credit:
                primary_id = artist_map.get(credit['artist'].get('id', ''))
                break

        release_id, r_new = upsert_release_mb(cur, rel._data, primary_id, image_url)

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

        n_created, n_updated = upsert_tracks_mb(cur, release_id, rel.tracks, artist_map)
        conn.commit()
    finally:
        conn.close()

    # ── Result header (matches import_album_unified style) ────────────────────
    art_note = f'  [dim]art: {image_url.split("/")[-1][:20]}[/dim]' if image_url else ''
    status   = '[green]→ imported[/green]' if n_created else '[dim]→ updated[/dim]'
    console.print(
        f'[bold]{rel.name}[/bold]  '
        f'[dim]{rel.artist}  ·  {rel.year}  ·  {rel.track_count} tracks[/dim]  '
        f'[dim][MB][/dim]  {status}{art_note}'
    )

    # ── Tracklist (Rich table, no box) ────────────────────────────────────────
    console.rule(style='dim')
    tbl = Table(box=None, padding=(0, 1, 0, 0), show_header=False, show_edge=False)
    tbl.add_column('#',     style='dim',  width=3,  justify='right', no_wrap=True)
    tbl.add_column('Title', style='',     min_width=10, max_width=38, no_wrap=True)
    tbl.add_column('Dur',   style='dim',  width=6,  justify='right', no_wrap=True)
    tbl.add_column('ISRC',  style='dim',  width=13, no_wrap=True)

    max_disc = max((t.get('_disc_number') or 1) for t in rel.tracks) if rel.tracks else 1
    cur_disc = None
    for t in rel.tracks:
        disc  = t.get('_disc_number') or 1
        num   = str(t.get('_track_number', '?'))
        title = t.get('name', '?')
        dur   = _fmt_dur(t.get('duration_ms'))
        isrcs = t.get('_isrcs') or []
        isrc  = isrcs[0] if isrcs else ''
        if max_disc > 1 and disc != cur_disc:
            cur_disc = disc
            tbl.add_row('', f'[bold dim]Disc {disc}[/bold dim]', '', '')
        tbl.add_row(num, _trunc(title, 38), dur, isrc)
    console.print(tbl)
    console.rule(style='dim')
    console.print(f'  [dim]{n_created} created · {n_updated} updated[/dim]')

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
        if not target_key:
            # Release title is non-ASCII (e.g. CJK) — skip name matching to
            # avoid matching every unmatched listen via empty-string collision.
            return

        # This pass runs with no human confirming the artist, so raw_artist_name
        # must be checked against the release's OWN credited artists here —
        # bulk_rematch_by_name() itself will also fold any raw_artist it's given
        # into its "valid artist" set (by design, for its interactive callers in
        # sync.py where a human already picked the release), so passing an
        # unverified raw_artist through would silently bypass that safety net
        # and let a same-named track from an unrelated artist match this release.
        release_artist_keys = {
            _norm(r[0])
            for r in conn.execute('''
                SELECT DISTINCT a.name FROM artists a
                JOIN track_artists ta ON ta.artist_id = a.id
                JOIN tracks t ON t.id = ta.track_id
                WHERE t.release_id = ?
                UNION
                SELECT DISTINCT a.name FROM artists a
                JOIN release_artists ra ON ra.artist_id = a.id
                WHERE ra.release_id = ?
                UNION
                SELECT a.name FROM artists a
                JOIN releases r ON r.primary_artist_id = a.id
                WHERE r.id = ?
            ''', [release_id, release_id, release_id]).fetchall()
            if r[0]
        }
        release_artist_keys.add(_norm(artist_name))

        groups = conn.execute('''
            SELECT DISTINCT raw_artist_name, raw_album_name
            FROM   listens
            WHERE  track_id IS NULL
              AND  raw_artist_name IS NOT NULL
              AND  raw_album_name  IS NOT NULL
        ''').fetchall()
        groups = [
            (raw_artist, raw_album) for raw_artist, raw_album in groups
            if _norm(raw_artist) in release_artist_keys
        ]

        name_n = 0
        already_matched_groups = set()
        for raw_artist, raw_album in groups:
            k = _norm(raw_album)
            if k == target_key or target_key in k or k in target_key:
                name_n += bulk_rematch_by_name(conn, [release_id], raw_artist, raw_album)
                already_matched_groups.add((raw_artist, raw_album))

        # Supplementary pass: catch listens scrobbled from a single whose
        # raw_album_name matches a track title on this album rather than the
        # album title itself.  Handles two patterns:
        #   1. Exact:  raw_album="Cheval"        → track "Cheval"
        #   2. Feat:   raw_album="BUZZCUT (feat. Danny Brown)" → track "BUZZCUT"
        #      The remainder after the track title must look like a feat credit,
        #      NOT a sequel ("SATURATION II") or subtitle ("DEAR LORD, PT. 2").
        # (raw_artist is verified above to be one of this release's own credited
        # artists, so a same-named track by an unrelated artist cannot match.)
        _FEAT_PREFIX_RE = re.compile(
            r'^[ (]+(feat(?:uring)?|ft|with|x)\b', re.IGNORECASE
        )
        track_keys = {
            _norm(r[0])
            for r in conn.execute(
                'SELECT title FROM tracks WHERE release_id=? AND hidden=0', [release_id]
            ).fetchall()
            if r[0]
        }
        for raw_artist, raw_album in groups:
            if (raw_artist, raw_album) in already_matched_groups:
                continue
            k = _norm(raw_album)
            # Exact match
            if k in track_keys:
                name_n += bulk_rematch_by_name(conn, [release_id], raw_artist, raw_album)
                continue
            # Feat-credit prefix match: "BUZZCUT feat. Danny Brown" → track "BUZZCUT"
            for tk in track_keys:
                if len(tk) >= 4 and k.startswith(tk) and _FEAT_PREFIX_RE.match(k[len(tk):]):
                    name_n += bulk_rematch_by_name(conn, [release_id], raw_artist, raw_album)
                    break

        total = mbid_n + name_n
        if total:
            console.print(f'      [green]Successfully matched {total:,} listen{"s" if total != 1 else ""}[/green]'
                          + (f'  [dim]({mbid_n} mbid, {name_n} name)[/dim]' if mbid_n and name_n else ''))
    finally:
        conn.close()


# ── Unified import helpers ─────────────────────────────────────────────────────

_SP_ALBUM_URL_RE2 = re.compile(r'open\.spotify\.com/album/([A-Za-z0-9]{22})', re.I)
_SP_BARE_ID_RE2   = re.compile(r'^[A-Za-z0-9]{22}$')
_MB_RELEASE_URL_RE2 = re.compile(
    r'musicbrainz\.org/release/'
    r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})',
    re.I,
)
_BP_RELEASE_URL_RE2 = re.compile(r'beatport\.com/release/', re.I)
_AM_URL_RE2 = re.compile(r'music\.apple\.com/[a-z]{2}/album/[^/]+/(\d{7,12})', re.I)
_AM_BARE_RE2 = re.compile(r'^\d{7,12}$')
_BC_URL_RE2 = re.compile(r'https?://[^./]+\.bandcamp\.com/album/', re.I)
_DZ_URL_RE2 = re.compile(r'deezer\.com/(?:[a-z]{2}/)?album/(\d+)', re.I)

_ART_SOURCE_RANK = {'apple_music': 0, 'bandcamp': 1, 'beatport': 2,
                    'coverartarchive': 3, 'spotify': 4}
_ART_SOURCE_SIZE = {'apple_music': '3000px', 'bandcamp': '3000px', 'beatport': '1400px',
                    'coverartarchive': '1200px', 'spotify': '640px'}

_SRC_ABBREV = {'sp': 'Sp', 'mb': 'MB', 'am': 'AM', 'bp': 'Bp', 'dz': 'Dz', 'bc': 'Bc'}


def _fmt_src(source_data: dict) -> str:
    """Return compact source token string, e.g. '[Sp MB AM]'."""
    tokens = [_SRC_ABBREV.get(k, k.upper()) for k in source_data if k in _SRC_ABBREV]
    return f'[{" ".join(tokens)}]' if tokens else ''


def _parse_import_url(url_or_id: str) -> 'tuple[str, str]':
    """Detect URL type. Returns (source_key, id_or_url).
    For 'bp' and 'bc', the second element is the full URL.
    """
    s = str(url_or_id).strip()
    if _BP_RELEASE_URL_RE2.search(s):
        return 'bp', s
    m = _AM_URL_RE2.search(s)
    if m:
        return 'am', m.group(1)
    if _BC_URL_RE2.match(s):
        return 'bc', s
    m = _DZ_URL_RE2.search(s)
    if m:
        return 'dz', m.group(1)
    m = _MB_RELEASE_URL_RE2.search(s)
    if m:
        return 'mb', m.group(1).lower()
    if is_valid_mbid(s):
        return 'mb', s.lower()
    m = _SP_ALBUM_URL_RE2.search(s)
    if m:
        return 'sp', m.group(1)
    if _SP_BARE_ID_RE2.match(s):
        return 'sp', s
    if _AM_BARE_RE2.match(s):
        return 'am', s
    # Default: Spotify
    return 'sp', s


def _extract_upc_from_initial(source: str, obj) -> 'str | None':
    if obj is None:
        return None
    if source == 'sp' and isinstance(obj, dict):
        return (obj.get('external_ids') or {}).get('upc')
    if source == 'mb':
        data = obj._data if hasattr(obj, '_data') else obj
        return data.get('barcode') or None
    if source == 'bp':
        data = obj._data if hasattr(obj, '_data') else obj
        raw = data.get('upc')
        return str(raw) if raw else None
    if source == 'bc':
        return obj.upc if hasattr(obj, 'upc') else None
    if source == 'dz':
        return obj.upc if hasattr(obj, 'upc') else None
    # 'am' — iTunes API doesn't return UPC in lookup response
    return None


def _discover_sources(
    url_or_id: str,
    client: 'SpotifyClient | None' = None,
    no_gtin: bool = False,
    skip_sources: frozenset = frozenset(),
    errors_out: 'list | None' = None,
) -> 'tuple[dict, dict]':
    """Parse URL, fetch initial source, GTIN-broadcast to discover others.

    The initial source is fetched first to get the UPC.  Secondary sources
    (Spotify, MusicBrainz, iTunes) are then fetched in parallel — each has an
    independent rate limiter so they safely overlap.

    errors_out: optional output list. If provided, a structured dict is
    appended for every per-source fetch failure — {'type': 'source_fetch_failed',
    'source': 'am', 'error_type': 'SourceDataUnavailable', 'message': '...'} —
    so a caller can tell "this source doesn't have the data" (SourceDataUnavailable
    or SourceNotFound, permanent, another source may still work) apart from
    "this source is rate-limited" (SourceRateLimited, transient, worth retrying
    later) without parsing the printed message text.

    Returns (source_data, sp_full) where:
      source_data = {'sp': album_dict, 'mb': MusicBrainzRelease, 'bp': BeatportRelease, ...}
      sp_full     = {track_id: full_track_dict}
    """
    from concurrent.futures import as_completed

    source, source_id = _parse_import_url(url_or_id)
    source_data: dict = {}
    sp_full: dict = {}

    def _sp_client() -> 'SpotifyClient | None':
        nonlocal client
        if client is not None:
            return client
        cid = os.environ.get('SPOTIFY_CLIENT_ID', '')
        csc = os.environ.get('SPOTIFY_CLIENT_SECRET', '')
        if cid and csc:
            client = SpotifyClient(cid, csc)
        return client

    # Fetch initial source (sequential — needed before GTIN broadcast)
    try:
        if source == 'sp':
            sp = _sp_client()
            if sp:
                source_data['sp'] = sp.get_album(source_id)
        elif source == 'mb':
            rel = MusicBrainzRelease(source_id)
            rel._ensure_full()
            source_data['mb'] = rel
        elif source == 'bp':
            rel = BeatportRelease(url_or_id)
            rel._ensure_full()
            source_data['bp'] = rel
        elif source == 'am':
            rel = ItunesRelease(source_id)
            rel._ensure_full()
            source_data['am'] = rel
        elif source == 'bc':
            rel = BandcampRelease(url_or_id)
            rel._ensure_full()
            source_data['bc'] = rel
        elif source == 'dz':
            rel = DeezerRelease(source_id)
            rel._ensure_full()
            source_data['dz'] = rel
    except Exception as e:
        error_type = type(e).__name__
        console.print(f'  [red]Failed to fetch {source}:{source_id} ({error_type}): {e}[/red]')
        if errors_out is not None:
            errors_out.append({
                'type': 'source_fetch_failed', 'source': source,
                'error_type': error_type, 'message': str(e),
            })

    # GTIN broadcast — build parallel fetch tasks for all secondary sources
    if not no_gtin:
        upc = _extract_upc_from_initial(source, source_data.get(source))
        if upc:
            skip = frozenset([source]) | skip_sources
            try:
                discovered = resolve_by_gtin(upc, skip=skip)
            except Exception:
                discovered = {}

            def _fetch_sp(sp_id):
                sp = _sp_client()
                return 'sp', sp.get_album(sp_id) if sp else None

            def _fetch_mb(mb_id):
                rel = MusicBrainzRelease(mb_id)
                rel._ensure_full()
                return 'mb', rel

            def _fetch_am(am_id):
                rel = ItunesRelease(am_id)
                rel._ensure_full()
                return 'am', rel

            def _fetch_dz(dz_id):
                rel = DeezerRelease(dz_id)
                rel._ensure_full()
                return 'dz', rel

            tasks = {}
            if 'sp' in discovered and 'sp' not in source_data:
                tasks['sp'] = (_fetch_sp, discovered['sp'])
            if 'mb' in discovered and 'mb' not in source_data:
                tasks['mb'] = (_fetch_mb, discovered['mb'])
            if 'am' in discovered and 'am' not in source_data:
                tasks['am'] = (_fetch_am, discovered['am'])
            if 'dz' in discovered and 'dz' not in source_data:
                tasks['dz'] = (_fetch_dz, discovered['dz'])

            if tasks:
                labels = {'sp': 'Spotify', 'mb': 'MusicBrainz', 'am': 'iTunes', 'dz': 'Deezer'}
                with ThreadPoolExecutor(max_workers=len(tasks)) as ex:
                    futs = {ex.submit(fn, arg): key
                            for key, (fn, arg) in tasks.items()}
                    for fut in as_completed(futs):
                        key = futs[fut]
                        try:
                            _, result = fut.result()
                            if result is not None:
                                source_data[key] = result
                        except Exception as e:
                            error_type = type(e).__name__
                            console.print(f'  [dim]{labels.get(key, key)} ({error_type}): {e}[/dim]')
                            if errors_out is not None:
                                errors_out.append({
                                    'type': 'source_fetch_failed', 'source': key,
                                    'error_type': error_type, 'message': str(e),
                                })

    # Fetch Spotify full-track data (ISRCs, popularity)
    sp_album = source_data.get('sp')
    if sp_album and isinstance(sp_album, dict):
        sp = _sp_client()
        if sp:
            try:
                ids = [t['id'] for t in (sp_album.get('_all_tracks') or []) if t.get('id')]
                if ids:
                    sp_full = {t['id']: t for t in sp.get_tracks_batch(ids) if t}
            except Exception as e:
                console.print(f'  [dim]Spotify track details: {e}[/dim]')

    return source_data, sp_full


def _find_existing_release_mdb(cur, mdb_r: MDBRelease, primary_artist_id: 'str | None' = None) -> 'str | None':
    """Return release_id if a matching non-hidden release already exists in the DB."""
    if mdb_r.spotify_id:
        row = cur.execute('SELECT id FROM releases WHERE spotify_id = ? AND (hidden IS NULL OR hidden = 0)',
                          (mdb_r.spotify_id,)).fetchone()
        if row:
            return row[0]
    if mdb_r.mbid:
        row = cur.execute('SELECT id FROM releases WHERE mbid = ? AND (hidden IS NULL OR hidden = 0)',
                          (mdb_r.mbid,)).fetchone()
        if row:
            return row[0]
    if mdb_r.beatport_id:
        row = cur.execute(
            'SELECT el.entity_id FROM external_links el'
            ' JOIN releases r ON r.id = el.entity_id'
            ' WHERE el.entity_type = ? AND el.service = ? AND el.link_value = ?'
            '   AND (r.hidden IS NULL OR r.hidden = 0)',
            (EL_RELEASE, EL_SVC_BEATPORT, str(mdb_r.beatport_id)),
        ).fetchone()
        if row:
            return row[0]
    if mdb_r.apple_music_id:
        row = cur.execute('SELECT id FROM releases WHERE apple_music_id = ? AND (hidden IS NULL OR hidden = 0)',
                          (mdb_r.apple_music_id,)).fetchone()
        if row:
            return row[0]
    # Fall back to title + primary artist whenever no ID-based lookup found a
    # match — not just when the source carries no platform ID at all. A
    # release can have every field an incoming source doesn't recognize (a
    # stale or wrong stored ID, a different regional catalog entry for the
    # same platform) and still be the same album; skipping this fallback in
    # that case creates a duplicate release instead of finding the real one.
    if primary_artist_id and mdb_r.title:
        row = cur.execute(
            'SELECT id FROM releases WHERE primary_artist_id = ? AND lower(title) = lower(?)'
            '   AND (hidden IS NULL OR hidden = 0)',
            (primary_artist_id, mdb_r.title),
        ).fetchone()
        if row:
            return row[0]
    return None


def _upsert_primary_artist_mdb(cur, mdb_r: MDBRelease) -> 'tuple[str, str] | tuple[None, None]':
    """Resolve the release's primary artist. Returns (artist_id, credited_as).

    credited_as is the display name to show on this release when it differs
    from the artist's canonical name (e.g. a pseudonym/alias credit) — None
    when the credited name matches the canonical name.
    """
    credit = mdb_r.primary_artist
    if credit is None:
        return None, None
    aid = _resolve_artist_credit(cur, credit)
    if aid is None:
        return None, None
    credited_as = credit.credited_name
    if not credited_as and credit.name:
        row = cur.execute('SELECT name FROM artists WHERE id = ?', (aid,)).fetchone()
        canonical_name = row[0] if row else None
        if canonical_name and canonical_name.lower() != credit.name.lower():
            credited_as = credit.name
    return aid, credited_as


def _build_enrich_diff(cur, release_id: str, mdb_r: MDBRelease, mdb_tracks: list) -> list:
    """Return list of (field, current_val, proposed_val, source) for fields that would change."""
    row = cur.execute('SELECT * FROM releases WHERE id = ?', (release_id,)).fetchone()
    if not row:
        return []

    def get(col):
        try:
            return row[col]
        except Exception:
            return None

    diffs = []

    # Platform IDs
    if mdb_r.beatport_id:
        has_bp = cur.execute(
            'SELECT 1 FROM external_links'
            ' WHERE entity_type=? AND service=? AND link_value=?',
            (EL_RELEASE, EL_SVC_BEATPORT, str(mdb_r.beatport_id)),
        ).fetchone()
        if not has_bp:
            diffs.append(('beatport_id', '—', str(mdb_r.beatport_id), 'bp'))

    if mdb_r.apple_music_id and not get('apple_music_id'):
        diffs.append(('apple_music_id', '—', mdb_r.apple_music_id, 'am'))

    if mdb_r.mbid and not get('mbid'):
        diffs.append(('mbid', '—', mdb_r.mbid[:16] + '…', 'mb'))

    if mdb_r.release_group_mbid and not get('release_group_mbid'):
        diffs.append(('release_group_mbid', '—', mdb_r.release_group_mbid[:16] + '…', 'mb'))

    # Release date
    from mdb_strings import _should_update_date as _sud
    if mdb_r.release_date and _sud(get('release_date'), get('date_source'),
                                    mdb_r.release_date, mdb_r.date_source):
        diffs.append((
            'release_date',
            f"{get('release_date')} [{get('date_source')}]",
            f"{mdb_r.release_date} [{mdb_r.date_source}]",
            mdb_r.date_source,
        ))

    # Album art — upgrade to higher-quality source
    cur_art_src = get('album_art_source') or ''
    new_art_src = mdb_r.album_art_source or ''
    cur_art_url = get('album_art_url') or ''
    if mdb_r.album_art_url:
        cur_rank = _ART_SOURCE_RANK.get(cur_art_src, 99)
        new_rank = _ART_SOURCE_RANK.get(new_art_src, 99)
        if not cur_art_url or new_rank < cur_rank:
            cur_label = f'{cur_art_src} ({_ART_SOURCE_SIZE.get(cur_art_src, "?")})' if cur_art_src else '—'
            new_label = f'{new_art_src} ({_ART_SOURCE_SIZE.get(new_art_src, "?")})'
            diffs.append(('album_art_url', cur_label, new_label, new_art_src))

    # Label
    new_label = mdb_r.label.name if mdb_r.label else None
    if new_label and not get('label'):
        diffs.append(('label', '—', new_label, mdb_r.source_map.get('label', '')))

    # Track-level enrichments — collect preview data for bpm/key rows
    bp_new = mx_new = gn_new = 0
    # [(track_number, display_title, bpm, key, camelot)]
    bpm_preview: list = []
    for t in mdb_tracks:
        if not t.isrc:
            continue
        ex = cur.execute(
            'SELECT tempo_bpm, mix_name, beatport_genre FROM tracks WHERE isrc = ?',
            (t.isrc,),
        ).fetchone()
        if ex:
            if t.bpm is not None and not ex['tempo_bpm']:
                bp_new += 1
                # Build display title: base + (mix_name) unless it's "Original Mix"
                display_title = t.title
                if t.mix_name and t.mix_name != 'Original Mix':
                    display_title = f'{t.title} ({t.mix_name})'
                bpm_preview.append((
                    t.track_number or 0,
                    display_title,
                    t.bpm,
                    t.musical_key or '',
                    t.key_camelot or '',
                ))
            if t.mix_name and not ex['mix_name']:
                mx_new += 1
            if t.beatport_genre and not ex['beatport_genre']:
                gn_new += 1

    # Missing tracks — release in DB has 0 tracks but we have data
    db_track_count = cur.execute(
        'SELECT COUNT(*) FROM tracks WHERE release_id = ?',
        (release_id,),
    ).fetchone()[0]
    if db_track_count == 0 and mdb_tracks:
        src = 'mb' if any(t.mbid for t in mdb_tracks) else 'sp'
        diffs.append(('tracks', '0 tracks in DB',
                      f'{len(mdb_tracks)} tracks', src, None))

    if bp_new:
        diffs.append(('tracks.bpm/key', f'NULL ({bp_new} tracks)',
                      f'filled ({bp_new} tracks)', 'bp', bpm_preview))
    if mx_new:
        diffs.append(('tracks.mix_name', f'NULL ({mx_new} tracks)',
                      f'filled ({mx_new} tracks)', 'bp', None))
    if gn_new:
        diffs.append(('tracks.genre', f'NULL ({gn_new} tracks)',
                      f'filled ({gn_new} tracks)', 'bp', None))

    return diffs


def _show_enrich_diff(mdb_r: MDBRelease, diffs: list, source_data: dict) -> None:
    """Print a compact diff of proposed enrichment changes."""
    fw, cw, pw = 20, 28, 26
    console.print(f"  [dim]{'Field':<{fw}}  {'Current':<{cw}}  Proposed[/dim]")
    console.print(f"  {'─' * (fw + cw + pw + 2)}")

    for d in diffs:
        field, current, proposed, src = d[0], d[1], d[2], d[3]
        preview = d[4] if len(d) > 4 else None
        src_tag = f' [{src}]' if src else ''
        console.print(f"  {field:<{fw}}  {str(current or '—')[:cw]:<{cw}}  "
                      f"[green]{str(proposed)[:pw]}{src_tag}[/green]")

        # BPM/key track preview — per-track for ≤8, compact summary for >8
        if field == 'tracks.bpm/key' and preview:
            if len(preview) <= 8:
                for tnum, title, bpm, key, camelot in sorted(preview):
                    short    = (title[:34] + '…') if len(title) > 35 else title
                    key_str  = f'{key:<12}' if key else f'{"?":12}'
                    cam_str  = f' [{camelot}]' if camelot else ''
                    console.print(
                        f"    [dim]{tnum:>2}.[/dim]  {short:<35} "
                        f"[dim]{bpm:>4} bpm  {key_str}{cam_str}[/dim]"
                    )
            else:
                bpms     = sorted({bpm for _, _, bpm, _, _ in preview if bpm})
                keys     = [k for _, _, _, k, _ in preview if k]
                # preserve order, deduplicate
                unique_k = list(dict.fromkeys(keys))
                bpm_str  = (f'{bpms[0]} bpm' if len(bpms) == 1
                            else f'{bpms[0]}–{bpms[-1]} bpm') if bpms else ''
                key_str  = ', '.join(unique_k[:5])
                if len(set(keys)) > 5:
                    key_str += f' +{len(set(keys)) - 5} more'
                console.print(f"    [dim]{bpm_str}  ·  {key_str}[/dim]")

    console.print()


def _store_external_links_mdb(conn, release_id: str, source_data: dict) -> None:
    """Store Bandcamp URL and Deezer ID in external_links after import."""
    if 'bc' in source_data:
        bc = source_data['bc']
        bc_url = bc.url if hasattr(bc, 'url') else None
        if bc_url:
            upsert_external_link(conn, EL_RELEASE, release_id, EL_SVC_BANDCAMP, bc_url)
    if 'dz' in source_data:
        dz = source_data['dz']
        dz_id = dz.deezer_id if hasattr(dz, 'deezer_id') else None
        if dz_id:
            upsert_external_link(conn, EL_RELEASE, release_id, EL_SVC_DEEZER, dz_id)
    conn.commit()


def _select_variants_unified(
    db_path: str,
    rg_mbid: str,
    current_release_id: str,
    use_aoty: bool = False,
    use_wiki: bool = False,
) -> None:
    """Show MB release-group variants and offer interactive import."""
    all_releases = mb_fetch_release_group_releases(rg_mbid)
    if len(all_releases) <= 1:
        # nothing to show beyond the release we just imported
        return

    with managed_db(db_path) as conn:
        in_db: list[tuple] = []
        candidates: list[tuple] = []
        for r in all_releases:
            mbid_r = r.get('id')
            if not mbid_r:
                continue
            row = conn.execute(
                'SELECT id, title FROM releases WHERE mbid = ?', (mbid_r,)
            ).fetchone()
            if row:
                if row[0] != current_release_id:
                    in_db.append((row[0], r.get('title', ''), r.get('date', '')))
            else:
                n = sum(m.get('track-count', 0) for m in (r.get('media') or []))
                candidates.append((
                    mbid_r, r.get('title', ''), r.get('date', ''),
                    r.get('status', ''), r.get('country', ''), n,
                ))

    if not candidates and not in_db:
        return

    # ── Candidate filtering ────────────────────────────────────────────────────
    # We track releases at release-group granularity (one canonical per work).
    # Only surface candidates that represent genuinely different musical content.

    # 1. Drop withdrawn/pre-release stubs — never worth importing.
    candidates = [c for c in candidates if (c[3] or '').lower() != 'withdrawn']

    # 2. Drop same-content pressings vs the just-imported release
    #    (same date + same track count = MB artifact for regional digital release).
    with managed_db(db_path) as _vconn:
        cur_row = _vconn.execute(
            'SELECT release_date, total_tracks FROM releases WHERE id = ?',
            [current_release_id]
        ).fetchone()
    cur_date   = (cur_row['release_date'] or '') if cur_row else ''
    cur_tracks = (cur_row['total_tracks']  or 0) if cur_row else 0

    candidates = [
        c for c in candidates
        if not (c[2] == cur_date and c[5] == cur_tracks)
    ]

    # 3. Deduplicate regional variants within the remaining candidate list:
    #    same title + same track count = same music, different pressing region.
    #    Keep the worldwide (XW) release if present, otherwise the earliest date.
    # (norm_title, track_count) → index in deduped
    seen: dict = {}
    deduped: list = []
    for c in candidates:
        mbid_r, title, date, status, country, n = c
        key = (_norm(title), n)
        if key not in seen:
            seen[key] = len(deduped)
            deduped.append(c)
        else:
            existing = deduped[seen[key]]
            # Prefer worldwide (XW) over country-specific
            if country == 'XW' and existing[4] != 'XW':
                deduped[seen[key]] = c
    candidates = deduped

    if not candidates and not in_db:
        return

    # ── Variant display (compact bullets, no full-width rule) ─────────────────
    # Already-in-DB same-group releases
    unlinked_in_db = []
    if in_db:
        with managed_db(db_path) as conn:
            for db_id, title, date in in_db:
                already = conn.execute(
                    'SELECT 1 FROM release_variants'
                    ' WHERE (canonical_id = ? AND variant_id = ?)'
                    '    OR (canonical_id = ? AND variant_id = ?)',
                    [current_release_id, db_id, db_id, current_release_id]
                ).fetchone()
                if not already:
                    unlinked_in_db.append((db_id, title, date))

    if unlinked_in_db:
        console.print(f'      [dim]·  {len(unlinked_in_db)} unlinked variant'
                      f'{"s" if len(unlinked_in_db) != 1 else ""} in release group:[/dim]')
        for db_id, title, date in unlinked_in_db:
            console.print(f'         [dim]{title}  [{date}][/dim]')
        raw_link = console.input(
            f'         Link as variant{"s" if len(unlinked_in_db) != 1 else ""}? [Y/n]: '
        ).strip().lower()
        if raw_link in ('', 'y', 'yes'):
            with managed_db(db_path) as conn:
                for db_id, title, date in unlinked_in_db:
                    vtypes = detect_variant_types(title)
                    vtype_val = ','.join(vtypes) if vtypes else 'reissue'
                    conn.execute(
                        'INSERT INTO release_variants'
                        ' (canonical_id, variant_id, variant_type, sort_order)'
                        ' VALUES (?, ?, ?, ?)'
                        ' ON CONFLICT(canonical_id, variant_id) DO UPDATE SET'
                        '   variant_type = COALESCE(excluded.variant_type, variant_type)',
                        (current_release_id, db_id, vtype_val, 0),
                    )
                    conn.commit()
                    console.print(f'         [green]Linked:[/green] [dim]{title}[/dim]')

    if not candidates:
        return

    # ── Score + reason generation ──────────────────────────────────────────────
    # Identify which candidate is most canonical so we can label the
    # already-imported release and annotate each candidate with a reason.
    with managed_db(db_path) as _sc:
        cur_row2 = _sc.execute(
            'SELECT release_date, total_tracks, mbid FROM releases WHERE id = ?',
            [current_release_id],
        ).fetchone()
    cur_mbid = (cur_row2['mbid'] or '') if cur_row2 else ''

    # Build a synthetic MB dict for the already-imported release so it
    # participates in scoring alongside the candidates.
    imported_stub = {
        'id':     cur_mbid,
        'title':  (cur_row2 and cur_row2['release_date'] and cur_row2) and '',
        'date':   (cur_row2['release_date'] or '') if cur_row2 else '',
        'status': 'Official',
        'country': 'XW',
        'media':  [{'track-count': (cur_row2['total_tracks'] or 0) if cur_row2 else 0}],
    }
    # Fetch title from DB properly
    with managed_db(db_path) as _sc2:
        title_row = _sc2.execute(
            'SELECT title FROM releases WHERE id = ?', [current_release_id]
        ).fetchone()
    imported_stub['title'] = title_row['title'] if title_row else ''

    candidate_dicts = [
        {'id': mbid_r, 'title': title, 'date': date,
         'status': status, 'country': country,
         'media': [{'track-count': n}]}
        for mbid_r, title, date, status, country, n in candidates
    ]
    all_mb_pool = [imported_stub] + candidate_dicts
    all_mb_pool.sort(key=mb_canonical_score)
    pool_canonical = all_mb_pool[0]
    reasons = mb_release_reasons(
        [r for r in all_mb_pool if r['id'] != pool_canonical['id']],
        pool_canonical,
    )
    imported_is_canonical = (pool_canonical['id'] == cur_mbid)

    # ── Variant display ────────────────────────────────────────────────────────
    console.print('      [dim]·  Variants:[/dim]')
    for i, (mbid_r, title, date, status, country, n) in enumerate(candidates, 1):
        rs = reasons.get(mbid_r) or []
        reason_str = ('; '.join(rs)) if rs else ''
        console.print(
            f'         [dim]{i}.[/dim]  [dim]{title}  {date or "?"}  ·  {n} tracks[/dim]'
            + (f'\n             [dim]→ {reason_str}[/dim]' if reason_str else '')
        )
    # Show canonical label referencing the already-imported release
    imp_title = imported_stub['title']
    imp_date  = imported_stub['date']
    imp_n     = (cur_row2['total_tracks'] or 0) if cur_row2 else 0
    if imported_is_canonical:
        console.print(
            f'         [green]✓ canonical:[/green]  '
            f'[dim]{imp_title}  {imp_date}  ·  {imp_n} tracks  (already imported)[/dim]'
        )
    else:
        imp_rs = reasons.get(cur_mbid) or []
        imp_reason = ('; '.join(imp_rs)) if imp_rs else ''
        console.print(
            f'         [dim]  imported:[/dim]  '
            f'[dim]{imp_title}  {imp_date}  ·  {imp_n} tracks[/dim]'
            + (f'  [dim]→ {imp_reason}[/dim]' if imp_reason else '')
        )
        pool_c_n = sum(m.get('track-count', 0) for m in (pool_canonical.get('media') or []))
        # Find which numbered candidate the pool canonical is
        canon_num = next(
            (i for i, (mbid_r, *_) in enumerate(candidates, 1)
             if mbid_r == pool_canonical['id']),
            None,
        )
        num_hint = f'  [dim](= {canon_num})[/dim]' if canon_num else ''
        console.print(
            f'         [green]✓ canonical:[/green]  '
            f'[dim]{pool_canonical["title"]}  {pool_canonical["date"]}  '
            f'·  {pool_c_n} tracks[/dim]{num_hint}'
        )

    raw = console.input(
        '         [dim]Import? number(s) · db:ULID · or Enter to skip:[/dim] '
    ).strip()
    if not raw:
        return

    # candidates to import from MB
    selected_mbids: list[str] = []
    # existing DB IDs to link directly
    direct_links:   list[str] = []

    _ulid_re = re.compile(r'^[0-9A-Z]{26}$')
    for token in raw.replace(',', ' ').split():
        token = token.strip()
        if not token:
            continue
        # db:ULID or bare 26-char ULID → link an existing release directly
        bare = token[3:] if token.lower().startswith('db:') else token
        if _ulid_re.match(bare):
            direct_links.append(bare)
            continue
        try:
            idx = int(token) - 1
            if 0 <= idx < len(candidates):
                selected_mbids.append(candidates[idx][0])
        except ValueError:
            pass

    # Link existing DB releases directly (no import needed)
    if direct_links:
        with managed_db(db_path) as conn:
            for db_id in direct_links:
                row = conn.execute(
                    'SELECT title FROM releases WHERE id = ?', [db_id]
                ).fetchone()
                if not row:
                    console.print(f'  [red]Not found:[/red] {db_id}')
                    continue
                vtypes = detect_variant_types(row[0])
                vtype_val = ','.join(vtypes) if vtypes else None
                conn.execute(
                    'INSERT INTO release_variants'
                    ' (canonical_id, variant_id, variant_type, sort_order)'
                    ' VALUES (?, ?, ?, ?)'
                    ' ON CONFLICT(canonical_id, variant_id) DO UPDATE SET'
                    '   variant_type = COALESCE(excluded.variant_type, variant_type)',
                    (current_release_id, db_id, vtype_val, 0),
                )
                conn.commit()
                console.print(f'  [green]Linked:[/green] {row[0]}  [dim]{db_id}[/dim]')

    for mbid_r in selected_mbids:
        try:
            vid, vtitle, _, _ = import_album_from_mb(
                db_path, mbid_r, use_aoty=use_aoty, use_wiki=use_wiki,
            )
            vtypes = detect_variant_types(vtitle)
            vtype_val = ','.join(vtypes) if vtypes else None
            with managed_db(db_path) as conn:
                conn.execute(
                    'INSERT INTO release_variants'
                    ' (canonical_id, variant_id, variant_type, sort_order)'
                    ' VALUES (?, ?, ?, ?)'
                    ' ON CONFLICT(canonical_id, variant_id) DO UPDATE SET'
                    '   variant_type = COALESCE(excluded.variant_type, variant_type)',
                    (current_release_id, vid, vtype_val, 0),
                )
                conn.commit()
            console.print(f'         [green]Linked:[/green] [dim]{vtitle}[/dim]')
        except Exception as e:
            console.print(f'         [red]Error:[/red] {e}')


def _discover_and_merge_sources(url_or_id, *, client, no_gtin, no_mb, warnings_out, _warnings):
    """Fetch metadata for a release from every available source and merge it
    into a single MDBRelease + track list. Raises NoSourcesAvailable if
    nothing could be fetched."""
    skip_sources = frozenset(['mb']) if no_mb else frozenset()
    source_data, sp_full = _discover_sources(
        url_or_id, client=client, no_gtin=no_gtin, skip_sources=skip_sources,
        errors_out=_warnings,
    )
    if not source_data:
        if warnings_out is not None:
            warnings_out.extend(_warnings)
        raise NoSourcesAvailable(f'Could not fetch any metadata for: {url_or_id!r}')

    merge = ReleaseMerge(source_data, sp_full=sp_full)
    return source_data, sp_full, merge.release(), merge.tracks()


def _try_isrc_absorption(db_path, mdb_r, mdb_tracks):
    """If this is a 1-track single and its ISRC already exists on a
    non-variant album in the DB, skip importing and rematch listens there
    instead — prevents importing singles that were later absorbed into full
    albums with the same ISRC. Returns the full import_album_unified return
    tuple if absorbed, else None."""
    if len(mdb_tracks) != 1 or not mdb_tracks[0].isrc:
        return None
    with managed_db(db_path) as _chk:
        existing_t = _chk.execute('''
            SELECT t.id, t.release_id, r.title
            FROM tracks t JOIN releases r ON r.id = t.release_id
            WHERE t.isrc = ? AND t.hidden = 0
              AND NOT EXISTS (
                  SELECT 1 FROM release_variants rv WHERE rv.variant_id = r.id
              )
        ''', (mdb_tracks[0].isrc,)).fetchone()
    if not existing_t:
        return None
    console.print(
        f'[dim]{mdb_r.title}  →  already on "{existing_t["title"]}" '
        f'(ISRC match) · skipping single[/dim]'
    )
    artist_name = mdb_r.primary_artist.name if mdb_r.primary_artist else ''
    _auto_rematch(db_path, existing_t['release_id'], artist_name, mdb_r.title)
    return existing_t['release_id'], existing_t['title'], artist_name, mdb_r.release_date or ''


def _build_conflict_lines(mdb_r, _warnings):
    """Compact inline notes for source-metadata conflicts, and record each as
    a structured warning."""
    conflict_lines = []
    for c in (mdb_r.conflicts or []):
        short = c.replace('release_date: ', '').replace('track_count: ', '')
        conflict_lines.append(f'      [dim]·  ⚠ {short[:70]}[/dim]')
        _warnings.append({'type': 'source_conflict', 'message': c})
    return conflict_lines


def _persist_existing_release(conn, cur, db_path, existing_id, mdb_r, mdb_tracks, source_data,
                                primary_artist_id, credited_as, artist_name, year_str,
                                tracks_str, src_tok, conflict_lines, auto, _warnings):
    """Update path for a release that already exists in the DB: absorb the
    incoming metadata as a variant pressing if it has more tracks than the
    canonical, otherwise diff-and-update in place.

    Returns (release_id, early_result). early_result is the full
    import_album_unified return tuple when the release is already up to
    date (post-import steps should be skipped entirely), else None.
    """
    # ── Variant guard on the update path ─────────────────────────────
    # If the found release is the canonical and the incoming has more tracks,
    # it's a variant pressing — absorb it rather than overwriting.
    _is_variant_of_canonical = False
    if mdb_r.release_group_mbid:
        _is_not_variant = not conn.execute(
            'SELECT 1 FROM release_variants WHERE variant_id=?', [existing_id]
        ).fetchone()
        if _is_not_variant:
            _canon_tc = conn.execute(
                'SELECT COUNT(*) FROM tracks WHERE release_id=? AND hidden=0 AND variant_section IS NULL',
                [existing_id]
            ).fetchone()[0]
            _incoming_tc = len(mdb_tracks)
            if _incoming_tc > _canon_tc:
                _is_variant_of_canonical = True

    if _is_variant_of_canonical:
        # More tracks than the canonical → treat as a variant
        _rdate = mdb_r.release_date or ''
        _has_qualifier = _base_title(mdb_r.title or '') != (mdb_r.title or '')
        v_label = detect_variant_label(mdb_r.title or '') if _has_qualifier \
            else (_rdate[:4] + ' Reissue' if _rdate else 'Variant')
        sp_id = (source_data.get('sp') or {}).get('id')
        if sp_id:
            upsert_service_link(conn, existing_id, EL_SVC_SPOTIFY,
                                sp_id, variant_label=v_label,
                                release_date=mdb_r.release_date)
        n_created, n_updated = upsert_tracks_mdb(
            cur, existing_id, mdb_tracks, variant_section=v_label
        )
        conn.commit()
        console.print(
            f'[bold]{mdb_r.title}[/bold]  '
            f'[dim]{artist_name}  ·  {year_str}{tracks_str}[/dim]  '
            f'[dim]{src_tok}[/dim]  '
            f'[green]→ absorbed into canonical[/green] '
            f'[dim]({n_created} new, {n_updated} updated, section "{v_label}")[/dim]'
        )
        return existing_id, None

    diffs = _build_enrich_diff(cur, existing_id, mdb_r, mdb_tracks)
    if diffs:
        console.print(
            f'[bold]{mdb_r.title}[/bold]  '
            f'[dim]{artist_name}  ·  {year_str}{tracks_str}[/dim]  '
            f'[dim]{src_tok}[/dim]  [yellow]→ updating[/yellow]'
        )
        for cl in conflict_lines:
            console.print(cl)
        _show_enrich_diff(mdb_r, diffs, source_data)
        if auto:
            apply = True
        else:
            raw = console.input('  Apply these changes? [Y/n]: ').strip().lower()
            apply = raw in ('', 'y', 'yes')
        if apply:
            upsert_release_mdb(cur, mdb_r, primary_artist_id, credited_as)
            upsert_tracks_mdb(cur, existing_id, mdb_tracks)
            conn.commit()
            _store_external_links_mdb(conn, existing_id, source_data)
            console.print('      [green]Updated.[/green]')
        return existing_id, None

    console.print(
        f'[bold]{mdb_r.title}[/bold]  '
        f'[dim]{artist_name}  ·  {year_str}  →  up to date[/dim]'
    )
    # Nothing changed — skip all post-import steps (variants, AOTY,
    # Wikipedia). Only rematch in case new listens arrived since last import.
    _auto_rematch(db_path, existing_id, artist_name, mdb_r.title)
    return existing_id, (existing_id, mdb_r.title, artist_name, mdb_r.release_date or '')


def _persist_new_release(conn, cur, db_path, mdb_r, mdb_tracks, source_data,
                           primary_artist_id, credited_as, artist_name, year_str,
                           tracks_str, src_tok, conflict_lines, client, use_aoty, use_wiki,
                           no_gtin, _warnings):
    """Insert path for a release with no existing DB match: absorb into an
    existing canonical release for the same release group if the incoming
    title is a variant edition of it, otherwise import the MB primary
    pressing first so this release attaches to it as a variant, or fall back
    to a standalone insert. Returns release_id."""
    # ── Canonical detection: if a canonical already exists for this
    # release group and the incoming title is a variant, absorb the
    # variant-exclusive tracks into the canonical instead of creating
    # a new releases row. ────────────────────────────────────────────
    existing_canonical = None
    if mdb_r.release_group_mbid:
        _canon_row = conn.execute(
            '''SELECT r.id, r.title FROM releases r
               WHERE r.release_group_mbid = ?
                 AND r.hidden = 0
                 AND NOT EXISTS (
                     SELECT 1 FROM release_variants rv WHERE rv.variant_id = r.id
                 )
               LIMIT 1''',
            [mdb_r.release_group_mbid],
        ).fetchone()
        if _canon_row:
            canon_base   = _base_title(_canon_row['title'] or '')
            incoming_base = _base_title(mdb_r.title or '')
            if canon_base.lower() == incoming_base.lower() and \
                    (mdb_r.title or '') != (_canon_row['title'] or ''):
                existing_canonical = _canon_row['id']

    if existing_canonical:
        # This is a variant — absorb into canonical, no new releases row
        v_label = detect_variant_label(mdb_r.title or '')
        # Store the variant's Spotify ID in release_service_links
        sp_id = (source_data.get('sp') or {}).get('id')
        if sp_id:
            upsert_service_link(conn, existing_canonical, EL_SVC_SPOTIFY,
                                sp_id, variant_label=v_label,
                                release_date=mdb_r.release_date)
        # Import variant-exclusive tracks into canonical with variant_section tag
        n_created, n_updated = upsert_tracks_mdb(
            cur, existing_canonical, mdb_tracks, variant_section=v_label
        )
        conn.commit()
        console.print(
            f'[bold]{mdb_r.title}[/bold]  '
            f'[dim]{artist_name}  ·  {year_str}{tracks_str}[/dim]  '
            f'[dim]{src_tok}[/dim]  '
            f'[green]→ absorbed into canonical[/green] '
            f'[dim]({n_created} new, {n_updated} updated, section "{v_label}")[/dim]'
        )
        return existing_canonical

    if mdb_r.release_group_mbid and _base_title(mdb_r.title or '') != (mdb_r.title or ''):
        # ── Proactive MB canonical lookup ────────────────────────────
        # No canonical in DB yet, but the incoming title has edition
        # qualifiers → it's a variant of something. Fetch the MB release
        # group to find the primary pressing and import it first so this
        # variant absorbs cleanly into it.
        rg_releases = mb_fetch_release_group_releases(mdb_r.release_group_mbid)
        # Score candidates; lowest score = most canonical
        scored = sorted(rg_releases, key=mb_canonical_score)
        primary_mb = scored[0] if scored else None
        # Only auto-import if the primary is genuinely different from what
        # we're importing, or this recurses. Note MusicBrainzRelease stores
        # its MBID as .id, not ._mbid.
        if primary_mb and primary_mb.get('id') != (source_data.get('mb') and
                getattr(source_data['mb'], 'id', None)):
            primary_mbid = primary_mb['id']
            primary_title = primary_mb.get('title', '')
            primary_tracks = sum(m.get('track-count', 0)
                                 for m in (primary_mb.get('media') or []))
            console.print(
                '[dim]No canonical found — importing primary release from MB first:[/dim]'
            )
            console.print(
                f'  [bold]{primary_title}[/bold]  '
                f'[dim]{primary_tracks} tracks  mbid:{primary_mbid}[/dim]'
            )
            try:
                canon_id, _, _, _ = import_album_unified(
                    db_path,
                    f'https://musicbrainz.org/release/{primary_mbid}',
                    client=client,
                    use_aoty=use_aoty,
                    use_wiki=use_wiki,
                    no_gtin=no_gtin,
                    # avoid recursive variant prompts
                    no_variants=True,
                    auto=True,
                )
                # Now absorb current album as variant of the freshly imported canonical
                v_label = detect_variant_label(mdb_r.title or '')
                sp_id = (source_data.get('sp') or {}).get('id')
                if sp_id:
                    upsert_service_link(conn, canon_id, EL_SVC_SPOTIFY,
                                        sp_id, variant_label=v_label,
                                        release_date=mdb_r.release_date)
                n_created, n_updated = upsert_tracks_mdb(
                    cur, canon_id, mdb_tracks, variant_section=v_label
                )
                conn.commit()
                console.print(
                    f'[bold]{mdb_r.title}[/bold]  '
                    f'[dim]{artist_name}  ·  {year_str}{tracks_str}[/dim]  '
                    f'[dim]{src_tok}[/dim]  '
                    f'[green]→ absorbed into canonical[/green] '
                    f'[dim]({n_created} new, {n_updated} updated, section "{v_label}")[/dim]'
                )
                return canon_id
            except Exception as _e:
                console.print(f'[yellow]⚠ MB canonical import failed ({_e}); importing as standalone[/yellow]')
                _warnings.append({
                    'type': 'mb_canonical_import_failed',
                    'message': f'MB canonical import failed ({_e}); importing as standalone',
                    'error': str(_e),
                })
                release_id, _ = upsert_release_mdb(cur, mdb_r, primary_artist_id, credited_as)
                upsert_tracks_mdb(cur, release_id, mdb_tracks)
                conn.commit()
                _store_external_links_mdb(conn, release_id, source_data)
                return release_id
        else:
            release_id, _ = upsert_release_mdb(cur, mdb_r, primary_artist_id, credited_as)
            upsert_tracks_mdb(cur, release_id, mdb_tracks)
            conn.commit()
            _store_external_links_mdb(conn, release_id, source_data)
            console.print(
                f'[bold]{mdb_r.title}[/bold]  '
                f'[dim]{artist_name}  ·  {year_str}{tracks_str}[/dim]  '
                f'[dim]{src_tok}[/dim]  [green]→ imported[/green]'
            )
            return release_id

    release_id, _ = upsert_release_mdb(cur, mdb_r, primary_artist_id, credited_as)
    upsert_tracks_mdb(cur, release_id, mdb_tracks)
    conn.commit()
    _store_external_links_mdb(conn, release_id, source_data)
    console.print(
        f'[bold]{mdb_r.title}[/bold]  '
        f'[dim]{artist_name}  ·  {year_str}{tracks_str}[/dim]  '
        f'[dim]{src_tok}[/dim]  [green]→ imported[/green]'
    )
    for cl in conflict_lines:
        console.print(cl)
    return release_id


def _persist_release_to_db(db_path, mdb_r, mdb_tracks, source_data, artist_name, year_str,
                             tracks_str, src_tok, conflict_lines, client, use_aoty, use_wiki,
                             no_gtin, auto, _warnings):
    """Find or create the releases row for this merged metadata and upsert
    its tracks. Returns (release_id, early_result) — early_result is the
    full import_album_unified return tuple when the update path finds
    nothing to change, else None."""
    release_id: str = ''
    early_result = None
    with managed_db(db_path) as conn:
        cur = conn.cursor()
        primary_artist_id, credited_as = _upsert_primary_artist_mdb(cur, mdb_r)
        existing_id = _find_existing_release_mdb(cur, mdb_r, primary_artist_id)

        if existing_id:
            release_id, early_result = _persist_existing_release(
                conn, cur, db_path, existing_id, mdb_r, mdb_tracks, source_data,
                primary_artist_id, credited_as, artist_name, year_str, tracks_str,
                src_tok, conflict_lines, auto, _warnings,
            )
        else:
            release_id = _persist_new_release(
                conn, cur, db_path, mdb_r, mdb_tracks, source_data,
                primary_artist_id, credited_as, artist_name, year_str, tracks_str,
                src_tok, conflict_lines, client, use_aoty, use_wiki, no_gtin, _warnings,
            )
    return release_id, early_result


def _finish_import(db_path, mdb_r, release_id, artist_name, no_variants, use_aoty, use_wiki,
                    _warnings):
    """Post-write steps for a completed insert/update: variant selection,
    AOTY/Wikipedia enrichment, and an import-completeness check that flags
    fewer tracks landing than the source(s) reported — catching gaps here
    instead of letting them surface later as bad listen matches (a raw title
    with no real track to match falls back to the closest fuzzy title, which
    is often a remix/variant of a totally different track)."""
    # Variant selection (after closing main DB context)
    if not no_variants and mdb_r.release_group_mbid:
        _select_variants_unified(
            db_path, mdb_r.release_group_mbid, release_id,
            use_aoty=use_aoty, use_wiki=use_wiki,
        )

    if use_aoty or use_wiki:
        from concurrent.futures import wait as _wait
        _enrichment_futs = []
        with ThreadPoolExecutor(max_workers=2) as _ex:
            if use_aoty:
                _enrichment_futs.append(
                    _ex.submit(_import_aoty_step, db_path, release_id, mdb_r.title, artist_name)
                )
            if use_wiki:
                _enrichment_futs.append(
                    _ex.submit(_import_wiki_step, db_path, release_id, mdb_r.title, artist_name)
                )
            _wait(_enrichment_futs)

    if mdb_r.total_tracks:
        with managed_db(db_path) as _cc:
            _actual_tc = _cc.execute(
                'SELECT COUNT(*) FROM tracks WHERE release_id=? AND hidden=0 AND variant_section IS NULL',
                [release_id]
            ).fetchone()[0]
        if _actual_tc < mdb_r.total_tracks:
            console.print(
                f'      [yellow]⚠ only {_actual_tc}/{mdb_r.total_tracks} tracks imported — '
                f'source listed more; run `mdb tracks audit --release-id {release_id}` '
                f'or re-check the source tracklist[/yellow]'
            )
            _warnings.append({
                'type': 'track_count_mismatch',
                'message': f'only {_actual_tc}/{mdb_r.total_tracks} tracks imported — '
                           f'source listed more',
                'expected': mdb_r.total_tracks,
                'actual': _actual_tc,
                'release_id': release_id,
            })

    _auto_rematch(db_path, release_id, artist_name, mdb_r.title)


def import_album_unified(
    db_path: str,
    url_or_id: str,
    *,
    client: 'SpotifyClient | None' = None,
    use_aoty: bool = True,
    use_wiki: bool = True,
    no_gtin: bool = False,
    no_variants: bool = False,
    no_mb: bool = False,
    auto: bool = False,
    warnings_out: 'list | None' = None,
) -> 'tuple[str, str, str, str]':
    """Unified import for any URL type (Spotify/MB/Beatport/Apple Music/Bandcamp).

    Discovers all available sources via GTIN broadcast, merges them into a
    MDBRelease, then either imports as new or enriches an existing release.
    Returns (release_id, title, artist_name, release_date).

    warnings_out: optional output list. If provided, structured warning dicts
    (e.g. {'type': 'track_count_mismatch', 'message': '...', 'expected': 11,
    'actual': 8}) are appended to it as they're generated, in addition to the
    usual console.print() output — a machine-readable channel for callers that
    need to detect "this import needs a human/agent to look at it" without
    parsing terminal text. Also mirrored onto the module-level
    LAST_IMPORT_WARNINGS list (convenience only, not concurrency-safe).
    """
    _warnings: list = []
    global LAST_IMPORT_WARNINGS
    LAST_IMPORT_WARNINGS = _warnings

    source_data, sp_full, mdb_r, mdb_tracks = _discover_and_merge_sources(
        url_or_id, client=client, no_gtin=no_gtin, no_mb=no_mb,
        warnings_out=warnings_out, _warnings=_warnings,
    )

    absorbed = _try_isrc_absorption(db_path, mdb_r, mdb_tracks)
    if absorbed is not None:
        if warnings_out is not None:
            warnings_out.extend(_warnings)
        return absorbed

    artist_name = mdb_r.primary_artist.name if mdb_r.primary_artist else ''
    src_tok     = _fmt_src(source_data)
    tracks_str  = f' · {mdb_r.total_tracks} tracks' if mdb_r.total_tracks else ''
    year_str    = (mdb_r.release_date or '')[:4]
    conflict_lines = _build_conflict_lines(mdb_r, _warnings)

    release_id, early_result = _persist_release_to_db(
        db_path, mdb_r, mdb_tracks, source_data, artist_name, year_str, tracks_str,
        src_tok, conflict_lines, client, use_aoty, use_wiki, no_gtin, auto, _warnings,
    )
    if early_result is not None:
        if warnings_out is not None:
            warnings_out.extend(_warnings)
        return early_result

    _finish_import(db_path, mdb_r, release_id, artist_name, no_variants, use_aoty, use_wiki,
                   _warnings)

    if warnings_out is not None:
        warnings_out.extend(_warnings)

    return release_id, mdb_r.title, artist_name, mdb_r.release_date or ''


def cmd_import(args):
    load_dotenv()

    use_aoty = not args.no_aoty
    use_wiki = not args.no_wiki
    no_gtin  = getattr(args, 'no_gtin', False)
    no_variants = getattr(args, 'no_variants', False)
    auto     = getattr(args, 'auto', False)

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

    # Lazily init Spotify client; reused across all entries for token efficiency
    client: 'SpotifyClient | None' = None
    cid = os.environ.get('SPOTIFY_CLIENT_ID')
    csc = os.environ.get('SPOTIFY_CLIENT_SECRET')
    if cid and csc:
        client = SpotifyClient(cid, csc)

    total  = sum(len(g) for g in groups)
    errors = 0
    seq    = 0
    want_json   = args.json
    json_results: list = []

    for group in groups:
        # (release_id, title, release_date) or None per entry
        group_results = []

        for entry in group:
            seq += 1
            if total > 1:
                console.rule(f'[dim]{seq} / {total}[/dim]', style='dim')
            disc_note = f'  [dim]discs {entry["discs"]}[/dim]' if entry.get('discs') else ''
            if disc_note:
                console.print(disc_note)
            url = entry.get('url') or entry.get('album_id') or ''
            try:
                _import_warnings: list = []
                release_id, title, artist, rel_date = import_album_unified(
                    db_path, url,
                    client=client,
                    use_aoty=use_aoty,
                    use_wiki=use_wiki,
                    no_gtin=no_gtin,
                    no_variants=no_variants,
                    no_mb=args.no_mb,
                    auto=auto,
                    warnings_out=_import_warnings if want_json else None,
                )
                group_results.append((release_id, title, rel_date))
                if want_json:
                    json_results.append({
                        'release_id': release_id, 'title': title,
                        'artist': artist, 'date': rel_date,
                        'warnings': _import_warnings,
                    })
            except urllib.error.HTTPError as e:
                console.print(f'[red]HTTP {e.code}:[/red] {e.reason}')
                errors += 1
                group_results.append(None)
                if want_json:
                    json_results.append({'url': url, 'error': f'HTTP {e.code}: {e.reason}'})
            except Exception as e:
                console.print(f'[red]Error:[/red] {e}')
                if total == 1:
                    raise
                errors += 1
                group_results.append(None)

        # Multi-URL group: link as variants (batch file CSV groups)
        valid = [x for x in group_results if x is not None]
        if len(valid) > 1:
            canon_idx = pick_canonical(valid)
            canon_id, canon_title, _ = valid[canon_idx]
            variants = [
                (rid, vtitle, order)
                for order, (rid, vtitle, _) in enumerate(valid)
                if rid != canon_id
            ]
            console.rule('[dim]Variants[/dim]', style='dim')
            console.print(f'  [bold]Canonical:[/bold] {canon_title}  [dim]{canon_id}[/dim]')
            with managed_db(db_path) as conn:
                _write_variant_links(conn, canon_id, variants)
            for vid, vtitle, _ in variants:
                vtypes = detect_variant_types(vtitle)
                vtype_label = ','.join(vtypes) if vtypes else 'variant'
                console.print(f'  [dim]{vtype_label}:[/dim]  {vtitle}  [dim]{vid}[/dim]')

    if total > 1:
        console.rule(style='dim')
        ok = total - errors
        console.print(f'  [dim]Batch:[/dim] {ok}/{total} succeeded'
                      + (f'  [red]{errors} failed[/red]' if errors else ''))

    if want_json:
        # Plain stdout JSON as the last line — easy for callers to grab without
        # regex-parsing the Rich console output above.
        if len(json_results) == 1:
            print(json.dumps(json_results[0]))
        else:
            print(json.dumps({'imports': json_results}))


# ── cmd: discography ──────────────────────────────────────────────────────────

def cmd_discography(args):
    """Import a full discography from a YAML file, or a wikitext dump with --wikitext.

    Each entry must have `album_title` and at least one of:
      - `article`  — Wikipedia URL (most reliable; resolved to MB release group)
      - `spotify_id` / `mb_release_id` / any URL  — passed directly to import

    Optional fields: `release_date` (used as manual date override after import),
    `artist` (used as MB search fallback when Wikipedia lookup fails).

    Lookup order per entry:
      1. `article` Wikipedia URL → MB release group → canonical release MBID
      2. Any explicit `url` field → passed straight to import_album_unified
      3. MB title+artist search fallback via mb_find_release

    --wikitext parses `parse_wikitext_discographies` entries into this same
    shape instead of reading YAML, so both formats share this one resolution
    path — re-running either is safe: entries already in the catalog are
    absorbed rather than duplicated (see import_album_unified).
    """
    path = args.discography

    if getattr(args, 'wikitext', False):
        from mdb_strings import parse_wikitext_discographies
        sections = tuple(s.strip().lower() for s in args.sections.split(','))
        try:
            with open(path, encoding='utf-8') as f:
                text = f.read()
        except FileNotFoundError:
            console.print(f'[red]File not found:[/red] {path}')
            return
        entries = parse_wikitext_discographies(text, sections=sections)
        if not entries:
            console.print('[yellow]No `%%% Artist` blocks or matching sections found[/yellow]')
            return
    else:
        import yaml as _yaml
        try:
            with open(path, encoding='utf-8') as f:
                entries = _yaml.safe_load(f)
        except FileNotFoundError:
            console.print(f'[red]File not found:[/red] {path}')
            return
        except Exception as e:
            console.print(f'[red]YAML parse error:[/red] {e}')
            return
        if not isinstance(entries, list):
            console.print('[red]YAML must be a list of album entries[/red]')
            return

    db_path = args.db or DB_PATH
    use_aoty = not args.no_aoty and _AOTY_AVAILABLE
    use_wiki = not getattr(args, 'no_wiki', False)
    artist_hint = getattr(args, 'artist', None) or ''

    total = len(entries)
    console.print(f'── Discography import  ·  {total} entries  ·  {path}')

    ok = skipped = errors = 0
    for idx, entry in enumerate(entries, 1):
        title  = (entry.get('album_title') or entry.get('title') or '').strip()
        wiki   = (entry.get('article') or '').strip()
        url    = (entry.get('url') or '').strip()
        artist = (entry.get('artist') or artist_hint).strip()
        manual_date = (entry.get('release_date') or '').strip()

        # Extract artist hint from Wikipedia URL disambiguator, e.g.
        # .../Jazz_(Queen_album) → "Queen"  when no artist is known
        if not artist and wiki:
            import re as _re
            m = _re.search(r'\(([^)]+)_album\)', wiki)
            if m:
                artist = urllib.parse.unquote(m.group(1)).replace('_', ' ')

        console.rule(style='dim')
        console.print(f'[dim]{idx}/{total}[/dim]  [bold]{title or "(untitled)"}[/bold]'
                      + (f'  [dim]{manual_date}[/dim]' if manual_date else ''))

        import_url: str | None = None

        # 1. Wikipedia URL → MB release group → canonical MBID
        if wiki:
            rg_mbid = mb_rg_from_wiki_url(wiki)
            if rg_mbid:
                releases = mb_fetch_release_group_releases(rg_mbid)
                if releases:
                    releases_sorted = sorted(releases, key=mb_canonical_score)
                    canonical_r = releases_sorted[0]
                    # bare MBID
                    import_url = canonical_r.get('id')
                    if not import_url:
                        console.print('      [yellow]Wikipedia → RG found but no release MBID[/yellow]')
                else:
                    console.print(f'      [yellow]Wikipedia → RG {rg_mbid[:16]}… has no releases[/yellow]')
            else:
                console.print('      [yellow]Wikipedia URL not found in MB, trying title search[/yellow]')

        # 2. Explicit URL field
        if not import_url and url:
            import_url = url

        # 3. MB release-group title+artist search fallback
        if not import_url and title:
            year = None
            if manual_date:
                import re as _re
                m = _re.search(r'\b(\d{4})\b', manual_date)
                year = int(m.group(1)) if m else None
            rg_mbid = mb_find_release_group(title, artist, year or 0)
            if rg_mbid:
                releases = mb_fetch_release_group_releases(rg_mbid)
                if releases:
                    releases_sorted = sorted(releases, key=mb_canonical_score)
                    canonical_r = releases_sorted[0]
                    import_url = canonical_r.get('id')
            if not import_url:
                console.print('      [yellow]MB title search found nothing, skipping[/yellow]')

        if not import_url:
            console.print('      [red]Could not resolve import URL — skipped[/red]')
            skipped += 1
            continue

        try:
            release_id, imp_title, imp_artist, imp_date = import_album_unified(
                db_path,
                import_url,
                client=None,
                use_aoty=use_aoty,
                use_wiki=use_wiki,
                no_gtin=False,
                # never prompt for variants in batch mode
                no_variants=True,
                # apply enrichment without prompting
                auto=True,
            )

            # Apply manual date override if provided and more precise than what was stored
            if manual_date and release_id:
                from mdb_strings import _parse_user_date, _should_update_date
                parsed = _parse_user_date(manual_date)
                if parsed:
                    with managed_db(db_path) as _conn:
                        row = _conn.execute(
                            'SELECT release_date, date_source FROM releases WHERE id = ?',
                            [release_id],
                        ).fetchone()
                        if row and _should_update_date(
                            row['release_date'], row['date_source'], parsed, 'manual'
                        ):
                            _conn.execute(
                                'UPDATE releases SET release_date=?, date_source=? WHERE id=?',
                                [parsed, 'manual', release_id],
                            )
                            _conn.commit()
                            console.print(f'      [dim]·  date overridden → {parsed} [manual][/dim]')

            ok += 1

        except Exception as e:  # noqa: BLE001
            console.print(f'      [red]Error:[/red] {e}')
            errors += 1

    console.rule(style='dim')
    parts = [f'[green]{ok} imported[/green]']
    if skipped:
        parts.append(f'[yellow]{skipped} skipped[/yellow]')
    if errors:
        parts.append(f'[red]{errors} errors[/red]')
    console.print('  ' + '  ·  '.join(parts))


# ── cmd: enrich art ──────────────────────────────────────────────────────────

def _chafa_available() -> bool:
    return shutil.which('chafa') is not None


def _image_dims(data: bytes) -> 'tuple | None':
    """Parse (width, height) from raw JPEG or PNG bytes without any imaging
    library — album art URLs here are always one or the other."""
    if data[:8] == b'\x89PNG\r\n\x1a\n':
        if len(data) < 24:
            return None
        w, h = struct.unpack('>II', data[16:24])
        return (w, h)
    # JPEG
    if data[:2] == b'\xff\xd8':
        i = 2
        while i + 4 <= len(data):
            if data[i] != 0xFF:
                return None
            marker = data[i + 1]
            if marker in (0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7,
                          0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF):
                if i + 9 > len(data):
                    return None
                h, w = struct.unpack('>HH', data[i + 5:i + 9])
                return (w, h)
            elif marker == 0xD8 or 0xD0 <= marker <= 0xD7:
                i += 2
            else:
                seglen = struct.unpack('>H', data[i + 2:i + 4])[0]
                i += 2 + seglen
        return None
    return None


def _fetch_image_dims(url: str, timeout: int = 15) -> 'tuple | None':
    """Download just enough of the image to read its dimensions. Never
    raises — a failed fetch just means dims stay unknown for this release."""
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'actuallyaswin-music/1.0'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
        return _image_dims(data)
    except Exception:
        return None


def _fetch_image_phash(url: str, timeout: int = 15):
    """Download an image and return its perceptual hash (imagehash.phash),
    or None on any failure. Robust to resizing/recompression — this is what
    lets a 600px thumbnail and a 3000px large image be compared directly
    despite being completely different files."""
    try:
        import imagehash
        from PIL import Image
        import io
        req = urllib.request.Request(url, headers={'User-Agent': 'actuallyaswin-music/1.0'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
        return imagehash.phash(Image.open(io.BytesIO(data)))
    except Exception:
        return None


def _preview_art(url: str, label: str, render: bool = True) -> None:
    """Print a labeled URL and, if chafa is installed and rendering wasn't
    disabled, render the image inline in the terminal below it. Never
    raises — a failed download or render just falls back to the printed
    label + URL."""
    console.print(f'  [bold]{label}[/bold]  [dim]{url[:80]}[/dim]')
    if not render or not _chafa_available():
        return
    tmp_path = None
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'actuallyaswin-music/1.0'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = resp.read()
        suffix = os.path.splitext(urllib.parse.urlparse(url).path)[1] or '.jpg'
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
            f.write(data)
            tmp_path = f.name
        subprocess.run(
            ['chafa', '--size', '50x', '-c', '240', '--color-space', 'rgb', '-w', '1', tmp_path],
            check=False,
        )
    except Exception as e:
        console.print(f'  [dim yellow](preview failed: {e})[/dim yellow]')
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


def _fetch_art_candidates(apple_music_id: 'str | None', spotify_id: 'str | None',
                          get_sp_client) -> dict:
    """Fetch Apple Music + Spotify art candidates concurrently, one thread per
    provider, so a release with both IDs costs one round trip instead of two.
    Returns {'apple_music': url_or_None, 'spotify': url_or_None}."""
    def _sp_fetch():
        client = get_sp_client()
        if not client:
            return None
        album  = client.get_album(spotify_id)
        images = album.get('images') or []
        return max(images, key=lambda x: (x.get('width') or 0))['url'] if images else None

    tasks = {}
    if apple_music_id:
        tasks['apple_music'] = lambda: itunes_fetch_artwork_url(apple_music_id)
    if spotify_id:
        tasks['spotify'] = _sp_fetch

    results = {}
    if not tasks:
        return results
    with ThreadPoolExecutor(max_workers=len(tasks)) as ex:
        futures = {ex.submit(fn): source for source, fn in tasks.items()}
        for fut in futures:
            source = futures[fut]
            try:
                results[source] = fut.result()
            except Exception as e:
                console.print(f'  [yellow]{source} error:[/yellow] {e}')
                results[source] = None
    return results


# ── cmd: enrich art ──────────────────────────────────────────────────────────

def cmd_enrich_art(args):
    """Fill in missing album art, or interactively replace existing art.

    Auto mode (default): tries Apple Music then Spotify for each release
    with no album_art_url (fetched concurrently, not sequentially);
    auto-applies the first found URL without prompting.

    Interactive mode (--interactive): for every release in the queue,
    previews each found candidate inline via chafa (if installed) and
    prompts for a choice, confirmation, or a custom URL. Useful for
    reviewing and replacing art on already-populated releases (combine with
    --overwrite or --release-id).
    """
    load_dotenv()
    cid = os.environ.get('SPOTIFY_CLIENT_ID')
    csc = os.environ.get('SPOTIFY_CLIENT_SECRET')

    if args.interactive and not _chafa_available():
        console.print(
            '[dim yellow]chafa not found on PATH — previews will be text-only '
            '(brew install chafa for inline image rendering)[/dim yellow]\n'
        )

    updated = skipped = 0
    try:
        with managed_db(args.db or DB_PATH) as conn:
            # ── Build query ────────────────────────────────────────────────────
            params = []

            if args.release_id:
                # Targeting a specific release always processes it regardless of art status
                where = 'WHERE r.id = ? AND r.hidden = 0'
                params = [args.release_id]
            else:
                art_clause    = '' if args.force else "AND (r.album_art_url IS NULL OR r.album_art_url = '')"
                artist_clause = ''
                if args.artist:
                    artist_clause, artist_params = _artist_filter_clause(conn, args.artist)
                    if artist_clause is None:
                        console.print(f'[red]Artist not found:[/red] {args.artist}')
                        return
                    params = artist_params
                where = f'WHERE r.hidden = 0 {art_clause} {artist_clause}'

            rows = conn.execute(f'''
                SELECT DISTINCT r.id, r.title, r.release_year, r.mbid, r.spotify_id,
                       r.apple_music_id, r.album_art_url, a.name AS artist_name
                FROM releases r
                LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
                LEFT JOIN artists a ON a.id = COALESCE(ra.artist_id, r.primary_artist_id)
                {where}
                ORDER BY r.release_year DESC NULLS LAST, r.title
            ''', params).fetchall()

            queue = _paginate(rows, args)

            if not queue:
                console.print('[dim]Nothing to process.[/dim]')
                return

            console.print(f'[dim]{len(queue)} release{"s" if len(queue) != 1 else ""} to process'
                          + ('  (interactive)' if args.interactive else '') + '[/dim]\n')

            # ── Lazy Spotify client ────────────────────────────────────────────
            _sp_client = None
            def _get_sp():
                nonlocal _sp_client
                if _sp_client is None and cid and csc:
                    _sp_client = SpotifyClient(cid, csc)
                return _sp_client

            now = int(time.time())

            for i, row in enumerate(queue):
                release_id     = row['id']
                title          = row['title']
                year           = row['release_year'] or '?'
                spotify_id     = row['spotify_id']
                apple_music_id = row['apple_music_id']
                artist_name    = row['artist_name'] or ''
                current_url    = row['album_art_url']

                prefix = f'[dim][{i+1}/{len(queue)}][/dim]  '
                console.print(f'{prefix}[bold]{_trunc(title, 40)}[/bold]  [dim]{artist_name} · {year}[/dim]')

                # ── Fetch candidates (Apple Music + Spotify concurrently) ──────
                if apple_music_id or spotify_id:
                    with console.status('  [dim]fetching art…[/dim]', spinner='dots'):
                        candidates = _fetch_art_candidates(apple_music_id, spotify_id, _get_sp)
                else:
                    candidates = {}
                am_url = candidates.get('apple_music')
                sp_url = candidates.get('spotify')
                caa_url = None
                if not am_url and not sp_url and row['mbid']:
                    try:
                        caa_url = caa_fetch_front_image_url(row['mbid'])
                    except Exception as e:
                        console.print(f'  [yellow]caa error:[/yellow] {e}')

                # Apple Music preferred over Spotify; CAA is the last resort
                # when neither has an ID or an image (e.g. compilations that
                # never made it to streaming but do have a MusicBrainz entry).
                auto_url    = am_url or sp_url or caa_url
                auto_source = ('apple_music' if am_url else 'spotify' if sp_url else 'caa') if auto_url else None

                if not args.interactive:
                    # ── Auto mode ──────────────────────────────────────────────
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
                    # ── Interactive mode: sequential preview walk-through ───────
                    ordered = [('apple_music', am_url), ('spotify', sp_url), ('caa', caa_url)]
                    ordered = [(src, url) for src, url in ordered if url]
                    if current_url:
                        console.print(f'  [dim]current:[/dim] {current_url[:70]}')

                    chosen_url = chosen_source = None
                    if not ordered:
                        console.print('  [dim]no art sources found[/dim]')
                        skipped += 1

                    idx = 0
                    while idx < len(ordered):
                        src, url = ordered[idx]
                        label = f'[{idx+1}/{len(ordered)}]  {src}'
                        _preview_art(url, label)

                        nxt_hint = '  [n]ext' if idx + 1 < len(ordered) else ''
                        prompt = (f'  [p]ick this{nxt_hint}  [u]rl  [s]kip  [q]uit: ')
                        try:
                            raw = input(prompt).strip().lower()
                        except EOFError:
                            raw = 'q'

                        if raw == 'q':
                            return
                        elif raw == 'p':
                            chosen_url, chosen_source = url, src
                            break
                        elif raw == 'n' and idx + 1 < len(ordered):
                            idx += 1
                            continue
                        elif raw in ('s', ''):
                            skipped += 1
                            break
                        elif raw == 'u':
                            url_in = input('  URL: ').strip()
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

    except KeyboardInterrupt:
        console.print('\n  [yellow]Interrupted.[/yellow]')
    console.rule(style='dim')
    console.print(f'  [dim]Updated: {updated} · Skipped: {skipped}[/dim]')


# ── cmd: enrich aoty ─────────────────────────────────────────────────────────

def cmd_enrich_aoty(args):
    if not _AOTY_AVAILABLE:
        console.print('[red]Error:[/red] pip install requests beautifulsoup4')
        sys.exit(1)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format='  [%(levelname)s] %(message)s',
    )

    updated = skipped = marked = 0
    try:
        with managed_db(args.db or DB_PATH) as conn:
            # Resolve --artist to an id
            artist_filter = None
            if args.artist:
                row = resolve_artist(conn, args.artist)
                if not row:
                    console.print(f'[red]Artist not found:[/red] {args.artist}')
                    sys.exit(1)
                artist_filter = row['id']
                console.print(f'[dim]Artist: {row["name"]} ({artist_filter})[/dim]')

            not_found_clause = "AND aoty_url != 'not_found'" if args.force else ''
            done = set() if args.force else set(
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
                queue = _paginate(queue, args)

            console.print(f'[dim]Processing {len(queue)}  '
                          f'(skip={args.skip}, limit={args.limit or "none"}, '
                          f'auto={"yes" if args.auto else "no"})[/dim]')
            if not args.auto:
                console.print('[dim]Press Ctrl+C or type q to stop.[/dim]')
            console.print()

            now = int(time.time())

            def submit(entry):
                cached = conn.execute('SELECT aoty_url FROM releases WHERE id = ?',
                                       (entry[0],)).fetchone()
                cached_url = cached[0] if cached else None
                if cached_url == 'not_found':
                    # treat sentinel as no cache; do a fresh search
                    cached_url = None
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
                                           force=args.force)
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
                            console.print('  [dim]Marked as not found.[/dim]')
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
                            save_aoty_data(conn, release_id, new_url, val_data2, force=args.force)
                            console.print('  [green]Saved.[/green]')
                            updated += 1
                        else:
                            skipped += 1
                        i += 1
                    elif action == 'save':
                        save_aoty_data(conn, release_id, val_url, val_data, force=args.force)
                        primary = [n for _, n, _, p in val_data['genres'] if p]
                        console.print(f'  [green]Saved:[/green] {", ".join(primary) or "(no genres)"}')
                        updated += 1
                        i += 1
    except KeyboardInterrupt:
        console.print('\n  [yellow]Interrupted.[/yellow]')
    console.rule(style='dim')
    console.print(f'  [dim]Updated: {updated} · Skipped: {skipped} · Marked not-found: {marked}[/dim]')

# ── cmd: enrich dates ─────────────────────────────────────────────────────────

def cmd_enrich_dates(args):
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format='  [%(levelname)s] %(message)s',
    )
    updated = skipped = 0
    try:
        with managed_db(args.db or DB_PATH) as conn:
            artist_clause = ''
            params        = []
            if args.artist:
                row = resolve_artist(conn, args.artist)
                if not row:
                    console.print(f'[red]Artist not found:[/red] {args.artist}')
                    return
                artist_clause = 'AND ra.artist_id = ?'
                params        = [row['id']]
            release_clause = ''
            if args.release_id:
                release_clause = 'AND r.id = ?'
                params         = [args.release_id]

            overwrite_clause = '' if args.force else 'AND (r.release_date IS NULL OR r.release_date = \'\')'

            rows = conn.execute(f'''
                SELECT DISTINCT r.id, r.title, r.release_year, a.name
                FROM releases r
                LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
                LEFT JOIN artists a ON ra.artist_id = a.id
                WHERE r.mbid IS NOT NULL AND r.hidden = 0
                {overwrite_clause} {artist_clause} {release_clause}
                ORDER BY r.release_year DESC NULLS LAST, r.title
            ''', params).fetchall()

            queue = _paginate(rows, args)

            console.print(f'[dim]{len(rows)} releases need dates, processing {len(queue)}  '
                          f'(skip={args.skip}, limit={args.limit or "none"})[/dim]')
            console.print('[dim]Press Ctrl+C or type q to stop.[/dim]\n')

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
                        save_release_date(conn, release_id, choice, wiki_page_id, source='manual')
                        console.print(f'  [green]Saved:[/green] {choice}')
                        updated += 1
    except KeyboardInterrupt:
        console.print('\n  [yellow]Interrupted.[/yellow]')
    console.rule(style='dim')
    console.print(f'  [dim]Updated: {updated} · Skipped: {skipped}[/dim]')

# ── cmd: enrich tracks ────────────────────────────────────────────────────────

def cmd_enrich_tracks(args):
    missing_tracks = getattr(args, 'missing_tracks', False)
    with managed_db(args.db or DB_PATH) as conn:
        artist_clause  = ''
        release_clause = ''
        params: list   = []
        if args.artist:
            row = resolve_artist(conn, args.artist)
            if not row:
                console.print(f'[red]Artist not found:[/red] {args.artist}')
                return
            artist_clause = 'AND ra.artist_id = ?'
            params        = [row['id']]
        if args.release_id:
            release_clause = 'AND r.id = ?'
            params         = [args.release_id]

        if missing_tracks:
            # Re-fetch full tracklist for MB releases that have 0 track rows
            rows = conn.execute(f'''
                SELECT DISTINCT r.id, r.title, r.mbid
                FROM releases r
                LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
                LEFT JOIN artists a ON ra.artist_id = a.id
                WHERE r.mbid IS NOT NULL AND r.hidden = 0
                AND NOT EXISTS (SELECT 1 FROM tracks t WHERE t.release_id = r.id)
                {artist_clause} {release_clause}
                ORDER BY r.title
            ''', params).fetchall()

            queue = _paginate(rows, args)

            console.print(f'[dim]{len(rows)} MB releases have no tracks, processing {len(queue)}[/dim]\n')
            total_created = 0
            for i, row in enumerate(queue):
                release_id, title, mbid = row['id'], row['title'], row['mbid']
                console.print(f'[dim][{i+1}/{len(queue)}][/dim]  [bold]{title}[/bold]', end='')
                try:
                    rel = MusicBrainzRelease(mbid)
                    rel._ensure_full()
                except Exception as e:
                    console.print(f'  [red]MB fetch failed: {e}[/red]')
                    continue

                # Build artist_map from track credits
                from mdb_ops import upsert_artist_mb as _upsert_artist_mb, upsert_tracks_mb as _upsert_tracks_mb
                cur = conn.cursor()
                artist_credits_seen: dict = {}
                for credit in (rel._data.get('artist-credit') or []):
                    if isinstance(credit, dict) and 'artist' in credit:
                        mb_a = credit['artist']
                        artist_credits_seen[mb_a.get('id', '')] = mb_a
                for t in rel.tracks:
                    for credit in (t.get('_artist_credit') or []):
                        if isinstance(credit, dict) and 'artist' in credit:
                            mb_a = credit['artist']
                            mb_aid = mb_a.get('id', '')
                            if mb_aid not in artist_credits_seen:
                                artist_credits_seen[mb_aid] = mb_a
                artist_map: dict = {}
                for mb_aid, mb_a in artist_credits_seen.items():
                    our_id, _ = _upsert_artist_mb(cur, mb_a)
                    artist_map[mb_aid] = our_id

                n_created, n_updated = _upsert_tracks_mb(
                    cur, release_id, rel.tracks, artist_map,
                    no_release_reassign=True,
                )
                conn.commit()
                skipped = len(rel.tracks) - n_created - n_updated
                note = f'  [dim yellow]{skipped} already on other release[/dim yellow]' if skipped else ''
                console.print(f'  [dim]{n_created} created, {n_updated} updated ({len(rel.tracks)} total)[/dim]{note}')
                total_created += n_created
            console.rule(style='dim')
            console.print(f'  [dim]Created {total_created} tracks across {len(queue)} releases[/dim]')
            return

        # Default: fill missing MBIDs on existing tracks
        total_matched = 0
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

        queue = _paginate(rows, args)

        console.print(f'[dim]{len(rows)} releases need track MBIDs, processing {len(queue)}[/dim]\n')

        for i, row in enumerate(queue):
            release_id, title, mbid = row['id'], row['title'], row['mbid']
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
        console.rule(style='dim')
        console.print(f'  [dim]Matched {total_matched} track MBIDs across {len(queue)} releases[/dim]')

# ── cmd: enrich deezer-links ──────────────────────────────────────────────────

def cmd_enrich_deezer_links(args):
    """Backfill Deezer external links for releases that have a UPC but no Deezer link.

    UPC is re-fetched from Spotify (batch 50/call) or MusicBrainz (barcode field).
    """
    load_dotenv()

    with managed_db(args.db or DB_PATH) as conn:
        artist_clause  = ''
        release_clause = ''
        params: list   = [EL_RELEASE, EL_SVC_DEEZER]
        if args.artist:
            artist_clause, artist_params = _artist_filter_clause(conn, args.artist)
            if artist_clause is None:
                console.print(f'[red]Artist not found:[/red] {args.artist}')
                return
            params.extend(artist_params)
        if args.release_id:
            release_clause = 'AND r.id = ?'
            params.append(args.release_id)

        # Releases without a Deezer external link
        rows = conn.execute(f'''
            SELECT DISTINCT r.id, r.title, r.spotify_id, r.mbid, r.upc
            FROM   releases r
            LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
            WHERE  r.hidden = 0
              AND  NOT EXISTS (
                  SELECT 1 FROM external_links el
                  WHERE  el.entity_type = ? AND el.service = ? AND el.entity_id = r.id
              )
              {artist_clause} {release_clause}
            ORDER BY r.title
        ''', params).fetchall()

        queue = _paginate(rows, args)

        console.print(f'[dim]{len(rows)} releases without Deezer link, processing {len(queue)}[/dim]\n')

        # ── Phase 1: batch-fetch UPCs from Spotify (for rows where upc IS NULL) ─
        # release_id → upc
        sp_upc: dict[str, str] = {}
        sp_rows = [(r['id'], r['spotify_id']) for r in queue
                   if r['spotify_id'] and not r['upc']]
        if sp_rows:
            cid = os.environ.get('SPOTIFY_CLIENT_ID', '')
            csc = os.environ.get('SPOTIFY_CLIENT_SECRET', '')
            if cid and csc:
                client = SpotifyClient(cid, csc)
                for chunk_start in range(0, len(sp_rows), 50):
                    chunk = sp_rows[chunk_start:chunk_start + 50]
                    ids   = [sid for _, sid in chunk]
                    try:
                        data   = client.get(f'/albums?ids={",".join(ids)}')
                        albums = data.get('albums') or []
                        for (rel_id, _), album in zip(chunk, albums):
                            if not album:
                                continue
                            upc = (album.get('external_ids') or {}).get('upc')
                            if upc:
                                from mdb_strings import normalize_upc as _nupc
                                n = _nupc(upc)
                                if n:
                                    sp_upc[rel_id] = n
                                    conn.execute('UPDATE releases SET upc=? WHERE id=? AND upc IS NULL',
                                                 (n, rel_id))
                    except Exception as e:
                        console.print(f'  [dim yellow]Spotify batch {chunk_start//50+1} failed: {e}[/dim yellow]')
            else:
                console.print('  [dim yellow]No Spotify credentials — skipping Spotify UPC fetch[/dim yellow]')

        # ── Phase 2: per-release MB barcode fallback + Deezer lookup ─────────
        found = skipped = errors = 0
        for r in queue:
            rel_id = r['id']
            title  = r['title']

            upc = r['upc'] or sp_upc.get(rel_id)

            if not upc and r['mbid']:
                try:
                    from mdb_apis import _mb_get
                    data    = _mb_get(f'/release/{r["mbid"]}', {'inc': ''})
                    barcode = (data.get('barcode') or '').strip()
                    if barcode:
                        from mdb_strings import normalize_upc as _nupc
                        upc = _nupc(barcode) or None
                        if upc:
                            conn.execute('UPDATE releases SET upc=? WHERE id=?', (upc, rel_id))
                except Exception:
                    pass

            if not upc:
                skipped += 1
                continue

            # Deezer UPC lookup — Deezer's own catalog/lookup uses unpadded
            # UPC-A (12 digits), while we store EAN-13 (zero-padded to 13).
            # Try the stored form first, then the unpadded form if that
            # 404s — a leading-zero mismatch alone caused most "not on
            # Deezer" results to be false negatives.
            try:
                import json as _json
                import urllib.request as _urlreq

                candidates = [upc]
                if len(upc) == 13 and upc[0] == '0':
                    candidates.append(upc[1:])

                data = None
                for candidate in candidates:
                    req = _urlreq.Request(
                        f'https://api.deezer.com/album/upc:{candidate}',
                        headers={'User-Agent': 'mdb/1.0'},
                    )
                    with _urlreq.urlopen(req, timeout=8) as resp:
                        data = _json.loads(resp.read())
                    if data.get('id') and not data.get('error'):
                        break

                if data and data.get('id') and not data.get('error'):
                    dz_id = str(data['id'])
                    upsert_external_link(conn, EL_RELEASE, rel_id, EL_SVC_DEEZER, dz_id)
                    conn.commit()
                    console.print(f'  [green]✓[/green]  {title}  [dim]→ deezer:{dz_id}[/dim]')
                    found += 1
                else:
                    console.print(f'  [dim]–  {title}  (not on Deezer)[/dim]')
                    skipped += 1
            except Exception as e:
                console.print(f'  [red]✗[/red]  {title}  [dim]{e}[/dim]')
                errors += 1

        console.rule(style='dim')
        console.print(f'  [dim]Found: {found} · No UPC/match: {skipped} · Errors: {errors}[/dim]')


def cmd_enrich_descriptions(args):
    """Backfill editorial 'About this Album' blurbs scraped from Apple
    Music's web player for releases that have an apple_music_id but no
    editorial_note yet. Most albums don't have one — that's expected."""
    with managed_db(args.db or DB_PATH) as conn:
        where = 'WHERE r.hidden = 0 AND r.apple_music_id IS NOT NULL'
        params: list = []
        if not args.force:
            where += " AND (r.editorial_note IS NULL OR r.editorial_note = '')"
        if args.artist:
            artist_clause, artist_params = _artist_filter_clause(conn, args.artist)
            if artist_clause is None:
                console.print(f'[red]Artist not found:[/red] {args.artist}')
                return
            where += ' ' + artist_clause
            params.extend(artist_params)
        if args.release_id:
            where += ' AND r.id = ?'
            params.append(args.release_id)

        rows = conn.execute(f'''
            SELECT DISTINCT r.id, r.title, r.apple_music_id
            FROM releases r
            LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
            {where}
            ORDER BY r.title
        ''', params).fetchall()

        queue = _paginate(rows, args)

        console.print(f'[dim]{len(rows)} releases without an editorial note, processing {len(queue)}[/dim]\n')

        found = skipped = errors = 0
        for r in queue:
            try:
                note = apple_music_fetch_editorial_note(r['apple_music_id'])
                if note:
                    conn.execute('UPDATE releases SET editorial_note=? WHERE id=?', (note, r['id']))
                    conn.commit()
                    console.print(f'  [green]✓[/green]  {r["title"]}  [dim]({len(note)} chars)[/dim]')
                    found += 1
                else:
                    console.print(f'  [dim]–  {r["title"]}  (no editorial note)[/dim]')
                    skipped += 1
            except Exception as e:
                console.print(f'  [red]✗[/red]  {r["title"]}  [dim]{e}[/dim]')
                errors += 1

        console.rule(style='dim')
        console.print(f'  [dim]Found: {found} · No note: {skipped} · Errors: {errors}[/dim]')


def cmd_enrich_apple_links(args):
    """Backfill Apple Music IDs for releases that have a UPC but no apple_music_id.

    UPC is re-fetched from Spotify (batch 50/call) or MusicBrainz (barcode field),
    same as deezer-links. Unlike Deezer, iTunes matches on padded or unpadded UPC.
    """
    load_dotenv()

    with managed_db(args.db or DB_PATH) as conn:
        artist_clause  = ''
        release_clause = ''
        params: list   = []
        if args.artist:
            artist_clause, artist_params = _artist_filter_clause(conn, args.artist)
            if artist_clause is None:
                console.print(f'[red]Artist not found:[/red] {args.artist}')
                return
            params.extend(artist_params)
        if args.release_id:
            release_clause = 'AND r.id = ?'
            params.append(args.release_id)

        rows = conn.execute(f'''
            SELECT DISTINCT r.id, r.title, r.spotify_id, r.mbid, r.upc, a.name AS artist_name
            FROM   releases r
            LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE  r.hidden = 0
              AND  r.apple_music_id IS NULL
              {artist_clause} {release_clause}
            ORDER BY r.title
        ''', params).fetchall()

        queue = _paginate(rows, args)

        console.print(f'[dim]{len(rows)} releases without Apple Music ID, processing {len(queue)}[/dim]\n')

        # ── Phase 1: batch-fetch UPCs from Spotify (for rows where upc IS NULL) ─
        sp_upc: dict[str, str] = {}
        sp_rows = [(r['id'], r['spotify_id']) for r in queue
                   if r['spotify_id'] and not r['upc']]
        if sp_rows:
            cid = os.environ.get('SPOTIFY_CLIENT_ID', '')
            csc = os.environ.get('SPOTIFY_CLIENT_SECRET', '')
            if cid and csc:
                client = SpotifyClient(cid, csc)
                for chunk_start in range(0, len(sp_rows), 50):
                    chunk = sp_rows[chunk_start:chunk_start + 50]
                    ids   = [sid for _, sid in chunk]
                    try:
                        data   = client.get(f'/albums?ids={",".join(ids)}')
                        albums = data.get('albums') or []
                        for (rel_id, _), album in zip(chunk, albums):
                            if not album:
                                continue
                            upc = (album.get('external_ids') or {}).get('upc')
                            if upc:
                                from mdb_strings import normalize_upc as _nupc
                                n = _nupc(upc)
                                if n:
                                    sp_upc[rel_id] = n
                                    conn.execute('UPDATE releases SET upc=? WHERE id=? AND upc IS NULL',
                                                 (n, rel_id))
                    except Exception as e:
                        console.print(f'  [dim yellow]Spotify batch {chunk_start//50+1} failed: {e}[/dim yellow]')
            else:
                console.print('  [dim yellow]No Spotify credentials — skipping Spotify UPC fetch[/dim yellow]')

        # ── Phase 2: per-release MB barcode fallback + iTunes UPC lookup ─────
        found = skipped = errors = 0
        for r in queue:
            rel_id = r['id']
            title  = r['title']

            upc = r['upc'] or sp_upc.get(rel_id)

            if not upc and r['mbid']:
                try:
                    from mdb_apis import _mb_get
                    data    = _mb_get(f'/release/{r["mbid"]}', {'inc': ''})
                    barcode = (data.get('barcode') or '').strip()
                    if barcode:
                        from mdb_strings import normalize_upc as _nupc
                        upc = _nupc(barcode) or None
                        if upc:
                            conn.execute('UPDATE releases SET upc=? WHERE id=?', (upc, rel_id))
                except Exception:
                    pass

            if not upc and not r['artist_name']:
                skipped += 1
                continue

            try:
                am_id = itunes_lookup_by_upc(upc) if upc else None
                if not am_id and r['artist_name']:
                    am_id = itunes_search_by_title(title, r['artist_name'])
                if am_id:
                    conn.execute('UPDATE releases SET apple_music_id=? WHERE id=?', (am_id, rel_id))
                    conn.commit()
                    console.print(f'  [green]✓[/green]  {title}  [dim]→ apple:{am_id}[/dim]')
                    found += 1
                else:
                    console.print(f'  [dim]–  {title}  (not on Apple Music)[/dim]')
                    skipped += 1
            except Exception as e:
                console.print(f'  [red]✗[/red]  {title}  [dim]{e}[/dim]')
                errors += 1

        console.rule(style='dim')
        console.print(f'  [dim]Found: {found} · No UPC/match: {skipped} · Errors: {errors}[/dim]')


# ── cmd: enrich apple-verify ─────────────────────────────────────────────────

def cmd_enrich_apple_verify(args):
    """Strictly re-check every release's apple_music_id against Apple's own
    title + artist for that ID — no fuzzy scoring, no track-count heuristics.

    A prior ad hoc pass tried to auto-fix mismatches by picking the search
    result with the closest track count, which silently swapped in a handful
    of wrong albums entirely (different artist, same rough track count). This
    command only *flags*; it never rewrites apple_music_id itself. Results
    land in apple_match_status for `enrich apple-review` to resolve by hand.
    """
    with managed_db(args.db or DB_PATH) as conn:
        artist_clause  = ''
        release_clause = ''
        params: list   = []
        if args.artist:
            artist_clause, artist_params = _artist_filter_clause(conn, args.artist)
            if artist_clause is None:
                console.print(f'[red]Artist not found:[/red] {args.artist}')
                return
            params.extend(artist_params)
        if args.release_id:
            release_clause = 'AND r.id = ?'
            params.append(args.release_id)

        rows = conn.execute(f'''
            SELECT DISTINCT r.id, r.title, r.apple_music_id, r.upc, a.name AS artist_name
            FROM   releases r
            LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
            LEFT JOIN artists a ON a.id = COALESCE(ra.artist_id, r.primary_artist_id)
            WHERE  r.hidden = 0
              {artist_clause} {release_clause}
            ORDER BY r.title
        ''', params).fetchall()

        rows = _paginate(rows, args)

        now = int(time.time())
        verified = needs_review = unmatched = 0

        with_id    = [r for r in rows if r['apple_music_id']]
        without_id = [r for r in rows if not r['apple_music_id']]

        for r in without_id:
            conn.execute(
                'INSERT INTO apple_match_status (release_id, status, checked_at) VALUES (?,?,?) '
                'ON CONFLICT(release_id) DO UPDATE SET status=excluded.status, checked_at=excluded.checked_at',
                (r['id'], 'unmatched', now)
            )
            unmatched += 1
        conn.commit()

        # (row, status) needing the UPC pass below
        flagged = []

        for chunk_start in range(0, len(with_id), 100):
            chunk = with_id[chunk_start:chunk_start + 100]
            ids = ','.join(r['apple_music_id'] for r in chunk)
            try:
                raw = _http_get_json(f'{ITUNES_LOOKUP}?id={ids}&entity=album&country=US',
                                      headers={'User-Agent': MB_UA}, lim=_itunes_lim, timeout=15)
            except Exception as e:
                console.print(f'  [red]✗[/red]  batch {chunk_start//100+1}  [dim]{e}[/dim]')
                continue
            by_id = {}
            for res in raw.get('results') or []:
                if res.get('wrapperType') == 'collection':
                    by_id[str(res.get('collectionId'))] = (
                        res.get('collectionName') or '', res.get('artistName') or ''
                    )
            for r in chunk:
                info = by_id.get(str(r['apple_music_id']))
                if not info:
                    # ID no longer resolves — dead/delisted
                    status = 'needs_review'
                else:
                    apple_title, apple_artist = info
                    match = (_norm(r['title']) == _norm(apple_title) and
                             _norm(r['artist_name'] or '') == _norm(apple_artist))
                    status = 'verified' if match else 'needs_review'
                conn.execute(
                    'INSERT INTO apple_match_status (release_id, status, checked_at) VALUES (?,?,?) '
                    'ON CONFLICT(release_id) DO UPDATE SET status=excluded.status, checked_at=excluded.checked_at',
                    (r['id'], status, now)
                )
                if status == 'verified':
                    verified += 1
                else:
                    needs_review += 1
                    flagged.append(r)
            conn.commit()
            console.print(f'  [dim]batch {chunk_start//100+1}/{(len(with_id)-1)//100+1} checked[/dim]')

        # ── UPC pass ──────────────────────────────────────────────────────
        # A barcode identifies the exact pressing, which is a much stronger
        # signal than title+artist text — resolves most reissue/remaster
        # mismatches automatically, before anyone has to look at a candidate
        # list by hand. Runs for everything flagged above plus every
        # never-matched release, wherever a UPC is on file.
        upc_queue = [r for r in flagged + without_id if r['upc']]
        if upc_queue:
            console.print(f'\n  [dim]UPC pass: {len(upc_queue)} flagged/unmatched releases have a barcode on file[/dim]')
            upc_fixed = 0
            for n, r in enumerate(upc_queue, start=1):
                try:
                    raw = _http_get_json(f'{ITUNES_LOOKUP}?upc={r["upc"]}&entity=album&country=US',
                                          headers={'User-Agent': MB_UA}, lim=_itunes_lim, timeout=10)
                except Exception:
                    continue
                album = next((
                    a for a in raw.get('results') or [] if a.get('wrapperType') == 'collection'
                    and _norm(r['title']) == _norm(a.get('collectionName') or '')
                    and _norm(r['artist_name'] or '') == _norm(a.get('artistName') or '')
                ), None)
                if not album:
                    continue
                art = (album.get('artworkUrl100') or '').strip()
                if not art:
                    continue
                art_url = re.sub(r'\b\d+x\d+bb\b', '3000x3000bb', art)
                conn.execute(
                    'UPDATE releases SET apple_music_id=?, album_art_url=?, album_art_source=?, '
                    'album_art_thumb_url=NULL, updated_at=? WHERE id=?',
                    (str(album.get('collectionId')), art_url, 'apple_music', now, r['id'])
                )
                conn.execute("UPDATE apple_match_status SET status='verified', checked_at=? WHERE release_id=?",
                             (now, r['id']))
                conn.commit()
                upc_fixed += 1
                if r in flagged:
                    needs_review -= 1
                else:
                    unmatched -= 1
                verified += 1
                if n % 25 == 0:
                    console.print(f'  [dim]{n}/{len(upc_queue)} checked, {upc_fixed} auto-matched by barcode[/dim]')
            console.print(f'  [green]UPC pass matched {upc_fixed}/{len(upc_queue)} by barcode[/green]')

        console.rule(style='dim')
        console.print(f'  [green]Verified:[/green] {verified}   '
                       f'[yellow]Needs review:[/yellow] {needs_review}   '
                       f'[dim]Unmatched:[/dim] {unmatched}')
        if needs_review or unmatched:
            console.print('  [dim]Run [bold]mdb.py enrich apple-review[/bold] to resolve them.[/dim]')


# ── cmd: enrich apple-review ─────────────────────────────────────────────────

def _itunes_search_candidates(title: str, artist: str, limit: int = 5) -> list:
    """Top N Apple Music album candidates for a title+artist search, each as
    (collection_id, name, artist_name, year, track_count, artwork_3000_url)."""
    term = urllib.parse.quote(f'{artist} {title}')
    try:
        raw = _http_get_json(f'{ITUNES_SEARCH}?term={term}&entity=album&country=US&limit=25',
                              headers={'User-Agent': MB_UA}, lim=_itunes_lim, timeout=10)
    except Exception:
        return []
    out = []
    for res in raw.get('results') or []:
        if res.get('wrapperType') != 'collection':
            continue
        art = (res.get('artworkUrl100') or '').strip()
        if not art:
            continue
        year_match = re.match(r'(\d{4})', res.get('releaseDate') or '')
        out.append((
            str(res.get('collectionId')),
            res.get('collectionName') or '',
            res.get('artistName') or '',
            year_match.group(1) if year_match else '?',
            res.get('trackCount'),
            re.sub(r'\b\d+x\d+bb\b', '3000x3000bb', art),
        ))
        if len(out) >= limit:
            break
    return out


def _prompt_manual_apple_id(prefill=None):
    """Resolve a pasted Apple Music URL or bare collection ID to
    (id, name, artist, year, track_count, art_url), or None if
    cancelled/not found."""
    if prefill is not None:
        raw = prefill.strip()
    else:
        try:
            raw = input('  Paste Apple Music URL or ID (blank to cancel): ').strip()
        except EOFError:
            return None
    if not raw:
        return None
    m = re.search(r'(\d{6,})', raw)
    if not m:
        console.print('  [yellow]No numeric ID found in that input.[/yellow]')
        return None
    cid = m.group(1)
    try:
        raw_result = _http_get_json(f'{ITUNES_LOOKUP}?id={cid}&entity=album&country=US',
                                     headers={'User-Agent': MB_UA}, lim=_itunes_lim, timeout=10)
    except Exception as e:
        console.print(f'  [red]Lookup failed:[/red] {e}')
        return None
    album = next((r for r in raw_result.get('results') or [] if r.get('wrapperType') == 'collection'), None)
    if not album:
        console.print(f'  [yellow]No album found for ID {cid}.[/yellow]')
        return None
    art = (album.get('artworkUrl100') or '').strip()
    if not art:
        console.print('  [yellow]Found the album but it has no artwork.[/yellow]')
        return None
    year_match = re.match(r'(\d{4})', album.get('releaseDate') or '')
    return (
        cid, album.get('collectionName') or '', album.get('artistName') or '',
        year_match.group(1) if year_match else '?', album.get('trackCount'),
        re.sub(r'\b\d+x\d+bb\b', '3000x3000bb', art),
    )


def cmd_enrich_apple_review(args):
    """Interactive triage for releases `enrich apple-verify` flagged as
    needs_review/unmatched — one release at a time, top 5 Apple Music search
    candidates rendered inline (via chafa, unless --no-preview), pick with
    input()+Enter. Candidates for the next release prefetch in the
    background while you're still deciding on the current one.

    Deliberately uses input() rather than raw single-keypress reads: an
    earlier version tried bare keypresses plus paste-sniffing to save a
    keystroke, but terminals vary in how they deliver pasted text (some
    byte-by-byte with real gaps, indistinguishable from typing), which
    caused two separate incidents of releases getting silently corrupted
    or skipped. input() reads one whole line as a single atomic unit, so a
    pasted URL is just... the input, with no ambiguity to get wrong.
    """
    import queue, threading

    render = not getattr(args, 'no_preview', False)
    if render and not _chafa_available():
        console.print('[dim yellow]chafa not found on PATH — previews will be text-only '
                       '(brew install chafa for inline image rendering, or pass --no-preview)[/dim yellow]\n')

    with managed_db(args.db or DB_PATH) as conn:
        rows = conn.execute(f'''
            SELECT r.id, r.title, r.release_year, r.total_tracks, r.apple_music_id,
                   r.album_art_url, r.album_art_source,
                   a.name AS artist_name, ams.status
            FROM apple_match_status ams
            JOIN releases r ON r.id = ams.release_id
            LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
            LEFT JOIN artists a ON a.id = COALESCE(ra.artist_id, r.primary_artist_id)
            WHERE ams.status IN ('needs_review', 'unmatched')
            ORDER BY r.title
            {"LIMIT " + str(args.limit) if args.limit else ""}
        ''').fetchall()

        if not rows:
            console.print('[dim]Nothing queued for review. Run enrich apple-verify first.[/dim]')
            return

        console.print(f'[dim]{len(rows)} release{"s" if len(rows) != 1 else ""} queued  '
                       f'([bold]1-5[/bold] pick · [bold]0[/bold] keep current · '
                       f'paste a URL/ID directly · [bold]z[/bold] undo last · '
                       f'[bold]s[/bold] skip · [bold]n[/bold] no match · [bold]q[/bold] quit)[/dim]\n')

        # Background producer: fetches search candidates a couple releases
        # ahead so there's no network wait between decisions. One thread,
        # so it naturally shares the existing iTunes rate limiter safely.
        prefetch: 'queue.Queue' = queue.Queue(maxsize=3)
        stop = threading.Event()

        def _producer():
            for r in rows:
                if stop.is_set():
                    return
                candidates = _itunes_search_candidates(r['title'], r['artist_name'] or '')
                prefetch.put((r, candidates))

        t = threading.Thread(target=_producer, daemon=True)
        t.start()

        def _write_release(release_id, apple_music_id, art_url, art_source, ts):
            conn.execute(
                'UPDATE releases SET apple_music_id=?, album_art_url=?, album_art_source=?, '
                'album_art_thumb_url=NULL, updated_at=? WHERE id=?',
                (apple_music_id, art_url, art_source, ts, release_id)
            )

        def _write_status(release_id, status, ts):
            conn.execute("UPDATE apple_match_status SET status=?, checked_at=? WHERE release_id=?",
                         (status, ts, release_id))

        # (title, release_id, prev_apple_music_id, prev_art_url, prev_art_source, prev_status)
        last_undo = None
        reviewed = fixed = kept = skipped = no_match = 0
        try:
            for i in range(len(rows)):
                r, candidates = prefetch.get()

                while True:
                    console.rule(f'[{i+1}/{len(rows)}]  {r["title"]}', style='dim')
                    console.print(f'  [bold]{r["title"]}[/bold]  [dim]{r["artist_name"] or "?"} · '
                                   f'{r["release_year"] or "?"} · {r["total_tracks"] or "?"} tracks · '
                                   f'status: {r["status"]}[/dim]\n')

                    if r['apple_music_id']:
                        console.print(f'  [dim][0] keep current apple_music_id={r["apple_music_id"]}[/dim]\n')

                    if not candidates:
                        console.print('  [yellow]No Apple Music search results at all.[/yellow]')

                    for idx, (cid, name, artist_name, year, tc, art) in enumerate(candidates, start=1):
                        label = f'[{idx}] {name}  —  {artist_name} · {year} · {tc} tracks'
                        _preview_art(art, label, render=render)

                    try:
                        raw_input = input(
                            '\n  1-5 pick · 0 keep current · paste a URL/ID directly · '
                            'z undo last · s skip · n no match · q quit: '
                        ).strip()
                    except EOFError:
                        raw_input = 'q'
                    console.print()

                    now = int(time.time())

                    if raw_input == 'z':
                        if not last_undo:
                            console.print('  [yellow]Nothing to undo.[/yellow]\n')
                            continue
                        u_title, u_id, u_am_id, u_art, u_art_src, u_status = last_undo
                        _write_release(u_id, u_am_id, u_art, u_art_src, now)
                        _write_status(u_id, u_status, now)
                        conn.commit()
                        console.print(f'  [green]↺[/green] undid last change on "{u_title}"  '
                                       f'[dim]apple_music_id restored to {u_am_id or "(none)"}, '
                                       f'status restored to {u_status}[/dim]\n')
                        last_undo = None
                        # re-show the same item, decision not yet made
                        continue

                    break

                # A pasted URL/ID lands here as plain multi-character text —
                # distinguishing it from the single-char commands below needs
                # no guessing since input() already delimited it by Enter.
                if raw_input not in ('q', '0', '1', '2', '3', '4', '5', 's', 'n') and raw_input:
                    key = 'u'
                    pasted_text = raw_input
                else:
                    key = raw_input or 's'
                    pasted_text = None

                if key == 'q':
                    console.print('[dim]Stopping — progress saved.[/dim]')
                    break
                elif key == '0' and r['apple_music_id']:
                    last_undo = (r['title'], r['id'], r['apple_music_id'], r['album_art_url'],
                                 r['album_art_source'], r['status'])
                    _write_status(r['id'], 'verified', now)
                    conn.commit()
                    console.print('  [dim]kept — apple_match_status set to verified, no columns on '
                                   'releases changed[/dim]')
                    kept += 1
                elif key in '12345' and int(key) <= len(candidates):
                    cid, name, artist_name, year, tc, art = candidates[int(key) - 1]
                    last_undo = (r['title'], r['id'], r['apple_music_id'], r['album_art_url'],
                                 r['album_art_source'], r['status'])
                    _write_release(r['id'], cid, art, 'apple_music', now)
                    _write_status(r['id'], 'verified', now)
                    conn.commit()
                    console.print(f'  [green]✓[/green] release {r["id"]}')
                    console.print(f'      apple_music_id  {r["apple_music_id"] or "(none)"} → {cid}')
                    console.print(f'      album_art_url   → "{name}" · {art[:60]}')
                    fixed += 1
                elif key == 'n':
                    last_undo = (r['title'], r['id'], r['apple_music_id'], r['album_art_url'],
                                 r['album_art_source'], r['status'])
                    conn.execute(
                        "UPDATE releases SET apple_music_id=NULL, album_art_url=NULL, album_art_source=NULL, "
                        "album_art_thumb_url=NULL, updated_at=? WHERE id=? AND album_art_source='apple_music'",
                        (now, r['id'])
                    )
                    _write_status(r['id'], 'no_match_available', now)
                    conn.commit()
                    console.print(f'  [yellow]cleared[/yellow] release {r["id"]}: apple_music_id, '
                                   f'album_art_url, album_art_source, album_art_thumb_url all set to NULL')
                    no_match += 1
                elif key == 'u':
                    manual = _prompt_manual_apple_id(prefill=pasted_text)
                    if manual:
                        cid, name, artist_name, year, tc, art = manual
                        _preview_art(art, f'Confirm: {name}  —  {artist_name} · {year} · {tc} tracks', render=render)
                        try:
                            confirm = input('  Apply this match? [y/n]: ').strip().lower()
                        except EOFError:
                            confirm = 'n'
                        console.print()
                        if confirm == 'y':
                            now2 = int(time.time())
                            last_undo = (r['title'], r['id'], r['apple_music_id'], r['album_art_url'],
                                         r['album_art_source'], r['status'])
                            _write_release(r['id'], cid, art, 'apple_music', now2)
                            _write_status(r['id'], 'verified', now2)
                            conn.commit()
                            console.print(f'  [green]✓[/green] release {r["id"]}  (manual)')
                            console.print(f'      apple_music_id  {r["apple_music_id"] or "(none)"} → {cid}')
                            console.print(f'      album_art_url   → "{name}" · {art[:60]}')
                            fixed += 1
                        else:
                            console.print('  [dim]Cancelled — left queued.[/dim]')
                            skipped += 1
                    else:
                        skipped += 1
                else:
                    # 's' or anything unrecognized — leave queued for next time
                    skipped += 1
                reviewed += 1
        finally:
            stop.set()

        console.rule(style='dim')
        console.print(f'  [dim]Reviewed: {reviewed} · Fixed: {fixed} · Kept: {kept} · '
                       f'No match: {no_match} · Skipped: {skipped}[/dim]')
        if skipped:
            console.print(f'  [dim]{skipped} left queued — run enrich apple-review again to continue.[/dim]')


def cmd_enrich_spotify_links(args):
    """Backfill Spotify album IDs for releases that have a UPC (or MB barcode) but no spotify_id.

    Mirrors apple-links: UPC comes from the release row or the MB barcode field,
    then a UPC search against Spotify's catalog.
    """
    load_dotenv()
    cid = os.environ.get('SPOTIFY_CLIENT_ID')
    csc = os.environ.get('SPOTIFY_CLIENT_SECRET')
    if not cid or not csc:
        console.print('[red]SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET not set[/red]')
        return
    client = SpotifyClient(cid, csc)

    with managed_db(args.db or DB_PATH) as conn:
        artist_clause  = ''
        release_clause = ''
        params: list   = []
        if args.artist:
            artist_clause, artist_params = _artist_filter_clause(conn, args.artist)
            if artist_clause is None:
                console.print(f'[red]Artist not found:[/red] {args.artist}')
                return
            params.extend(artist_params)
        if args.release_id:
            release_clause = 'AND r.id = ?'
            params.append(args.release_id)

        rows = conn.execute(f'''
            SELECT DISTINCT r.id, r.title, r.mbid, r.upc, a.name AS artist_name
            FROM   releases r
            LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE  r.hidden = 0
              AND  (r.spotify_id IS NULL OR r.spotify_id = '')
              {artist_clause} {release_clause}
            ORDER BY r.title
        ''', params).fetchall()

        queue = _paginate(rows, args)

        console.print(f'[dim]{len(rows)} releases without a Spotify ID, processing {len(queue)}[/dim]\n')

        found = skipped = errors = 0
        for r in queue:
            rel_id = r['id']
            title  = r['title']

            upc = r['upc']
            if not upc and r['mbid']:
                try:
                    from mdb_apis import _mb_get
                    data    = _mb_get(f'/release/{r["mbid"]}', {'inc': ''})
                    barcode = (data.get('barcode') or '').strip()
                    if barcode:
                        from mdb_strings import normalize_upc as _nupc
                        upc = _nupc(barcode) or None
                        if upc:
                            conn.execute('UPDATE releases SET upc=? WHERE id=?', (upc, rel_id))
                except Exception:
                    pass

            if not upc and not r['artist_name']:
                skipped += 1
                continue

            try:
                sp_id = None
                if upc:
                    data  = client.get('/search', {'q': f'upc:{upc}', 'type': 'album', 'limit': 1})
                    items = (data.get('albums') or {}).get('items') or []
                    if items:
                        sp_id = items[0]['id']
                if not sp_id and r['artist_name']:
                    # UPCs drift across pressings/distributors — fall back to a text
                    # search, but only accept an exact normalised title match so we
                    # don't silently substitute a deluxe/remaster edition.
                    data2  = client.get('/search', {
                        'q': f"album:{title} artist:{r['artist_name']}", 'type': 'album', 'limit': 10,
                    })
                    items2 = (data2.get('albums') or {}).get('items') or []
                    target = normalize_text(title)
                    for it in items2:
                        if normalize_text(it['name']) == target:
                            sp_id = it['id']
                            break

                if sp_id:
                    conn.execute('UPDATE releases SET spotify_id=? WHERE id=?', (sp_id, rel_id))
                    conn.commit()
                    console.print(f'  [green]✓[/green]  {title}  [dim]→ spotify:{sp_id}[/dim]')
                    found += 1
                else:
                    console.print(f'  [dim]–  {title}  (not on Spotify)[/dim]')
                    skipped += 1
            except sqlite3.IntegrityError:
                console.print(f'  [yellow]⚠[/yellow]  {title}  [dim]spotify_id already claimed by another release[/dim]')
                skipped += 1
            except Exception as e:
                console.print(f'  [red]✗[/red]  {title}  [dim]{e}[/dim]')
                errors += 1

        console.rule(style='dim')
        console.print(f'  [dim]Found: {found} · No UPC/match: {skipped} · Errors: {errors}[/dim]')


# ── cmd: enrich thumbnails ───────────────────────────────────────────────────────

def cmd_enrich_thumbnails(args):
    """Backfill album_art_thumb_url — derived directly from Apple Music's
    3000px art where available, else Spotify's ~600px image, else a small
    Apple Music rendering fetched by ID.

    Apple-derived thumbs are a pure URL rewrite (…/3000x3000bb.jpg →
    …/600x600bb.jpg), not a re-fetch from Spotify — this matters because a
    release's stored spotify_id can point at the wrong recording (e.g. a
    tribute-band cover sharing the same title) even when album_art_url is
    correct, so trusting the already-verified Apple art avoids reintroducing
    a wrong image. Spotify is only used as a fallback when there's no Apple
    Music art at all.
    """
    load_dotenv()
    cid = os.environ.get('SPOTIFY_CLIENT_ID')
    csc = os.environ.get('SPOTIFY_CLIENT_SECRET')
    client = SpotifyClient(cid, csc) if (cid and csc) else None

    with managed_db(args.db or DB_PATH) as conn:
        artist_clause  = ''
        release_clause = ''
        params: list   = []
        if args.artist:
            artist_clause, artist_params = _artist_filter_clause(conn, args.artist)
            if artist_clause is None:
                console.print(f'[red]Artist not found:[/red] {args.artist}')
                return
            params.extend(artist_params)
        if args.release_id:
            release_clause = 'AND r.id = ?'
            params.append(args.release_id)

        force_clause = '' if getattr(args, 'force', False) else \
            "AND (r.album_art_thumb_url IS NULL OR r.album_art_thumb_url = '' OR r.album_art_thumb_url = r.album_art_url)"

        rows = conn.execute(f'''
            SELECT DISTINCT r.id, r.title, r.spotify_id, r.apple_music_id,
                   r.album_art_url, r.album_art_source
            FROM   releases r
            LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
            WHERE  r.hidden = 0
              AND  ((r.spotify_id IS NOT NULL AND r.spotify_id != '')
                    OR (r.apple_music_id IS NOT NULL AND r.apple_music_id != ''))
              {force_clause}
              {artist_clause} {release_clause}
            ORDER BY r.title
        ''', params).fetchall()

        queue = _paginate(rows, args)

        console.print(f'[dim]{len(rows)} releases missing a small thumbnail, processing {len(queue)}[/dim]\n')

        found = skipped = 0
        now = int(time.time())

        # Apple-derived thumbs first — a plain URL rewrite, no network call.
        apple_art_re = re.compile(r'/\d+x\d+bb\.jpg$')
        remaining = []
        for r in queue:
            url = r['album_art_url']
            if r['album_art_source'] == 'apple_music' and url and apple_art_re.search(url):
                thumb = apple_art_re.sub('/600x600bb.jpg', url)
                conn.execute('UPDATE releases SET album_art_thumb_url=?, updated_at=? WHERE id=?',
                             (thumb, now, r['id']))
                found += 1
            else:
                remaining.append(r)
        conn.commit()
        console.print(f'  [dim]{found} thumbnails derived directly from Apple Music art[/dim]\n')

        sp_queue = [r for r in remaining if r['spotify_id']]
        am_only  = [r for r in remaining if not r['spotify_id'] and r['apple_music_id']]

        if client:
            for chunk_start in range(0, len(sp_queue), 20):
                chunk = sp_queue[chunk_start:chunk_start + 20]
                try:
                    albums = client.get_albums_batch([r['spotify_id'] for r in chunk])
                except Exception as e:
                    console.print(f'  [red]✗[/red]  batch {chunk_start//20+1}  [dim]{e}[/dim]')
                    continue
                now = int(time.time())
                for r, album in zip(chunk, albums):
                    images = (album or {}).get('images') or []
                    if not images:
                        console.print(f'  [dim]–  {r["title"]}  (no images)[/dim]')
                        skipped += 1
                        continue
                    # closest to 600px, not smallest — tiles were blurry at 64px
                    best = min(images, key=lambda i: abs((i.get('width') or 0) - 600))
                    conn.execute('UPDATE releases SET album_art_thumb_url=?, updated_at=? WHERE id=?',
                                 (best['url'], now, r['id']))
                    found += 1
                conn.commit()
        elif sp_queue:
            console.print('  [dim yellow]No Spotify credentials — skipping Spotify thumbnails[/dim yellow]')
            skipped += len(sp_queue)

        # Apple-only fallback: same lookup batching as the art-source backfill,
        # just requesting a small rendering instead of 3000x3000.
        now = int(time.time())
        for chunk_start in range(0, len(am_only), 100):
            chunk = am_only[chunk_start:chunk_start + 100]
            ids   = ','.join(r['apple_music_id'] for r in chunk)
            try:
                raw = _http_get_json(f'{ITUNES_LOOKUP}?id={ids}&entity=album&country=US',
                                      headers={'User-Agent': MB_UA}, lim=_itunes_lim, timeout=15)
            except Exception as e:
                console.print(f'  [red]✗[/red]  Apple batch {chunk_start//100+1}  [dim]{e}[/dim]')
                continue
            by_id = {}
            for res in raw.get('results') or []:
                if res.get('wrapperType') == 'collection':
                    art = (res.get('artworkUrl100') or '').strip()
                    if art:
                        by_id[str(res.get('collectionId'))] = re.sub(r'\b\d+x\d+bb\b', '600x600bb', art)
            for r in chunk:
                thumb = by_id.get(str(r['apple_music_id']))
                if thumb:
                    conn.execute('UPDATE releases SET album_art_thumb_url=?, updated_at=? WHERE id=?',
                                 (thumb, now, r['id']))
                    found += 1
                else:
                    console.print(f'  [dim]–  {r["title"]}  (no Apple artwork)[/dim]')
                    skipped += 1
            conn.commit()

        console.rule(style='dim')
        console.print(f'  [dim]Found: {found} · Skipped: {skipped}[/dim]')


# ── cmd: enrich art-dims ─────────────────────────────────────────────────────

def cmd_enrich_art_dims(args):
    """Backfill album_art_width/height and album_art_thumb_width/height by
    downloading each image once and reading its real header — not the size
    requested in the URL, which Apple's CDN will happily lie about (a
    3000x3000bb URL can resolve to a much smaller real asset when that's
    all the label ever uploaded).

    Once populated, "is anything still low-res" is a plain SQL WHERE clause
    instead of a re-download-everything script — that's the point of this
    command existing at all.
    """
    with managed_db(args.db or DB_PATH) as conn:
        artist_clause  = ''
        release_clause = ''
        params: list   = []
        if args.artist:
            artist_clause, artist_params = _artist_filter_clause(conn, args.artist)
            if artist_clause is None:
                console.print(f'[red]Artist not found:[/red] {args.artist}')
                return
            params.extend(artist_params)
        if args.release_id:
            release_clause = 'AND r.id = ?'
            params.append(args.release_id)

        force_clause = '' if getattr(args, 'force', False) else \
            'AND (r.album_art_width IS NULL OR (r.album_art_thumb_url IS NOT NULL AND r.album_art_thumb_width IS NULL))'

        rows = conn.execute(f'''
            SELECT DISTINCT r.id, r.title, r.album_art_url, r.album_art_thumb_url
            FROM   releases r
            LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
            WHERE  r.hidden = 0 AND r.album_art_url IS NOT NULL
              {force_clause}
              {artist_clause} {release_clause}
            ORDER BY r.title
        ''', params).fetchall()

        queue = _paginate(rows, args)

        console.print(f'[dim]{len(rows)} releases missing art dimensions, processing {len(queue)}[/dim]\n')

        found = skipped = 0
        now = int(time.time())
        for i, r in enumerate(queue, start=1):
            large_dims = _fetch_image_dims(r['album_art_url'])
            thumb_dims = _fetch_image_dims(r['album_art_thumb_url']) if r['album_art_thumb_url'] else None

            if not large_dims and not thumb_dims:
                console.print(f'  [dim]–  {r["title"]}  (fetch failed)[/dim]')
                skipped += 1
                continue

            conn.execute(
                'UPDATE releases SET album_art_width=?, album_art_height=?, '
                'album_art_thumb_width=?, album_art_thumb_height=?, updated_at=? WHERE id=?',
                (large_dims[0] if large_dims else None, large_dims[1] if large_dims else None,
                 thumb_dims[0] if thumb_dims else None, thumb_dims[1] if thumb_dims else None,
                 now, r['id'])
            )
            found += 1
            if i % 50 == 0:
                conn.commit()
                console.print(f'  [dim]{i}/{len(queue)} processed[/dim]')

        conn.commit()
        console.rule(style='dim')
        console.print(f'  [dim]Found: {found} · Skipped: {skipped}[/dim]')


# ── cmd: enrich art-verify ────────────────────────────────────────────────────

def cmd_enrich_art_verify(args):
    """Cross-check that a release's thumb and large art are actually the
    same photo at different sizes, via perceptual hash (imagehash.phash) —
    a signal independent of the title/artist text matching apple-verify
    does. Catches cases text matching can't: e.g. a correct title+artist
    match whose thumb happens to be a stale image from a different edition
    or a totally different source than the large art.

    A Hamming distance of 0-2 between hashes reliably means "same image,
    different scale/compression" — phash is designed to be robust to
    exactly that, unlike raw pixel comparison. Read-only: reports
    mismatches, never rewrites anything.
    """
    try:
        import imagehash  # noqa: F401
    except ImportError:
        console.print('[red]Error:[/red] pip install imagehash Pillow  (see requirements.txt)')
        return

    with managed_db(args.db or DB_PATH) as conn:
        artist_clause  = ''
        release_clause = ''
        params: list   = []
        if args.artist:
            artist_clause, artist_params = _artist_filter_clause(conn, args.artist)
            if artist_clause is None:
                console.print(f'[red]Artist not found:[/red] {args.artist}')
                return
            params.extend(artist_params)
        if args.release_id:
            release_clause = 'AND r.id = ?'
            params.append(args.release_id)
        list_clause = ''
        if getattr(args, 'list_id', None):
            list_clause = 'AND r.id IN (SELECT release_id FROM canonical_list_entries WHERE list_id = ?)'
            params.append(args.list_id)

        rows = conn.execute(f'''
            SELECT DISTINCT r.id, r.title, r.album_art_url, r.album_art_thumb_url
            FROM   releases r
            LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
            WHERE  r.hidden = 0 AND r.album_art_url IS NOT NULL AND r.album_art_thumb_url IS NOT NULL
              {artist_clause} {release_clause} {list_clause}
            ORDER BY r.title
        ''', params).fetchall()

        queue = _paginate(rows, args)

        console.print(f'[dim]{len(rows)} releases have both thumb and large art, checking {len(queue)}[/dim]\n')

        # Same URL apart from the requested size suffix (…/600x600bb.jpg vs
        # …/3000x3000bb.jpg) means the same underlying asset by construction
        # — skip both the download and the hash, and don't bother phash at
        # all. This isn't just an optimization: at extreme scale ratios
        # (600 vs 3000, a 5x downsample) phash can drift a few bits on
        # high-contrast covers even for a genuinely identical image, so
        # this same-asset check also avoids a real class of false positive.
        size_suffix_re = re.compile(r'/\d+x\d+bb\.jpg$')

        threshold = args.threshold
        want_json = args.json
        mismatch_list: list = []
        error_list: list = []
        mismatches = errors = same_asset = 0
        for i, r in enumerate(queue, start=1):
            thumb_prefix = size_suffix_re.sub('', r['album_art_thumb_url'])
            large_prefix = size_suffix_re.sub('', r['album_art_url'])
            if thumb_prefix and thumb_prefix == large_prefix:
                same_asset += 1
                continue

            large_hash = _fetch_image_phash(r['album_art_url'])
            thumb_hash = _fetch_image_phash(r['album_art_thumb_url'])
            if large_hash is None or thumb_hash is None:
                errors += 1
                error_list.append({
                    'release_id': r['id'], 'title': r['title'],
                    'error': 'fetch failed',
                })
                continue
            dist = large_hash - thumb_hash
            if dist > threshold:
                console.print(f'  [yellow]![/yellow]  {r["title"]}  [dim](hamming distance {dist})[/dim]')
                console.print(f'      thumb  {r["album_art_thumb_url"][:70]}')
                console.print(f'      large  {r["album_art_url"][:70]}')
                mismatches += 1
                mismatch_list.append({
                    'release_id': r['id'], 'title': r['title'],
                    # imagehash's `-` operator returns numpy.int64, not a
                    # plain int — json.dumps can't serialize it as-is.
                    'hamming_distance': int(dist),
                    'thumb_url': r['album_art_thumb_url'],
                    'large_url': r['album_art_url'],
                })
            if i % 25 == 0:
                console.print(f'  [dim]{i}/{len(queue)} checked[/dim]')

        console.rule(style='dim')
        console.print(f'  [dim]Checked: {len(queue)} · Same asset (skipped): {same_asset} · '
                       f'Mismatches: {mismatches} · Fetch errors: {errors}[/dim]')

        if want_json:
            print(json.dumps({
                'checked': len(queue),
                'same_asset': same_asset,
                'mismatches': mismatch_list,
                'errors': error_list,
            }))


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

    _AF_BLOB_KEYS = ('energy', 'danceability', 'valence', 'acousticness',
                     'instrumentalness', 'liveness', 'speechiness',
                     'key', 'mode', 'time_signature')

    updated = 0
    with managed_db(args.db or DB_PATH) as conn:
        where = 'WHERE t.spotify_id IS NOT NULL AND t.audio_features IS NULL AND t.hidden = 0'
        params = []
        if args.artist:
            row = resolve_artist(conn, args.artist)
            if not row:
                console.print(f'[red]Artist not found:[/red] {args.artist}')
                return
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
            return

        console.print(f'[dim]Fetching audio features for {len(rows)} tracks…[/dim]')
        sp_ids    = [r['spotify_id'] for r in rows]
        id_to_row = {r['spotify_id']: r for r in rows}
        features  = client.get_audio_features_batch(sp_ids)

        now = int(time.time())
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
    console.rule(style='dim')
    console.print(f'  [dim]Updated {updated}/{len(rows)} tracks with audio features[/dim]')

# ── cmd: enrich spotify-tracks ──────────────────────────────────────────────────

def cmd_enrich_spotify_tracks(args):
    """Backfill missing track-level Spotify IDs on releases that already have one.

    Fetches each release's full Spotify tracklist and matches by ISRC — this
    is the only reliable cross-source key, since track titles/positions vary
    between providers (bonus tracks, remasters, disc splits).
    """
    load_dotenv()
    cid = os.environ.get('SPOTIFY_CLIENT_ID')
    csc = os.environ.get('SPOTIFY_CLIENT_SECRET')
    if not cid or not csc:
        console.print('[red]SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET not set[/red]')
        return
    client = SpotifyClient(cid, csc)

    with managed_db(args.db or DB_PATH) as conn:
        params: list = []
        artist_clause = ''
        if args.artist:
            row = resolve_artist(conn, args.artist)
            if not row:
                console.print(f'[red]Artist not found:[/red] {args.artist}')
                return
            artist_clause = '''
              AND r.id IN (
                SELECT DISTINCT t2.release_id FROM tracks t2
                JOIN track_artists ta ON ta.track_id = t2.id
                WHERE ta.artist_id = ? AND ta.role = 'main'
              )'''
            params.append(row['id'])

        release_clause = ''
        if args.release_id:
            release_clause = ' AND r.id = ?'
            params.append(args.release_id)

        rows = conn.execute(f'''
            SELECT DISTINCT r.id, r.title, r.spotify_id
            FROM releases r
            WHERE r.hidden = 0 AND r.spotify_id IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM tracks t WHERE t.release_id = r.id AND t.hidden = 0
                  AND (t.spotify_id IS NULL OR t.spotify_id = '')
              )
              {artist_clause}{release_clause}
            ORDER BY r.title
        ''', params).fetchall()

        rows = _paginate(rows, args)

        if not rows:
            console.print('[green]Nothing to process.[/green]')
            return

        console.print(f'  [bold]{len(rows)}[/bold] release(s) with incomplete Spotify track IDs\n')

        releases_touched = tracks_matched = tracks_unmatched = 0
        for r in rows:
            try:
                album = client.get_album(r['spotify_id'])
            except Exception as e:
                console.print(f'  [red]✗[/red]  {r["title"]}  [dim]{e}[/dim]')
                continue

            sp_tracks = album.get('_all_tracks') or []
            sp_ids    = [t['id'] for t in sp_tracks if t.get('id')]
            full_map  = {t['id']: t for t in client.get_tracks_batch(sp_ids)} if sp_ids else {}

            isrc_to_sp: dict = {}
            for t in sp_tracks:
                full_t = full_map.get(t['id'], t)
                isrc   = (full_t.get('external_ids') or {}).get('isrc')
                if isrc:
                    isrc_to_sp[isrc.upper()] = full_t

            db_tracks = conn.execute('''
                SELECT id, title, isrc, track_number, disc_number FROM tracks
                WHERE release_id = ? AND hidden = 0 AND (spotify_id IS NULL OR spotify_id = '')
            ''', (r['id'],)).fetchall()

            # Fallback for tracks with no ISRC at all (e.g. imported from a source that
            # doesn't expose one): match by normalised title, only when unambiguous.
            title_to_sp: dict = {}
            ambiguous_titles: set = set()
            for t in sp_tracks:
                full_t = full_map.get(t['id'], t)
                key = normalize_text(t.get('name', ''))
                if not key:
                    continue
                if key in title_to_sp:
                    ambiguous_titles.add(key)
                else:
                    title_to_sp[key] = full_t

            now = int(time.time())
            matched_this = 0
            for dbt in db_tracks:
                sp_t = isrc_to_sp.get(dbt['isrc'].upper()) if dbt['isrc'] else None
                if not sp_t and not dbt['isrc']:
                    key = normalize_text(dbt['title'] or '')
                    if key and key not in ambiguous_titles:
                        sp_t = title_to_sp.get(key)
                if not sp_t:
                    continue
                try:
                    conn.execute(
                        'UPDATE tracks SET spotify_id = ?, spotify_popularity = ?, updated_at = ? WHERE id = ?',
                        (sp_t['id'], sp_t.get('popularity'), now, dbt['id']),
                    )
                except sqlite3.IntegrityError:
                    # spotify_id already claimed by another track row — skip rather than crash
                    continue
                matched_this += 1
            tracks_matched   += matched_this
            tracks_unmatched += len(db_tracks) - matched_this
            if matched_this:
                releases_touched += 1
                console.print(
                    f'  [green]✓[/green]  {r["title"]}  '
                    f'[dim]{matched_this}/{len(db_tracks)} tracks matched[/dim]'
                )
            else:
                console.print(
                    f'  [yellow]⚠[/yellow]  {r["title"]}  '
                    f'[dim]0/{len(db_tracks)} matched — no ISRC overlap[/dim]'
                )
            conn.commit()

        console.rule(style='dim')
        console.print(
            f'  [dim]{tracks_matched} track(s) matched across {releases_touched} release(s)'
            f'  ·  {tracks_unmatched} still unmatched[/dim]'
        )

# ── cmd: enrich popularity ─────────────────────────────────────────────────────

def cmd_enrich_popularity(args):
    """Refresh Spotify popularity snapshots for artists, releases, and tracks."""
    load_dotenv()
    cid = os.environ.get('SPOTIFY_CLIENT_ID')
    csc = os.environ.get('SPOTIFY_CLIENT_SECRET')
    if not cid or not csc:
        console.print('[red]SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET not set[/red]')
        return
    client    = SpotifyClient(cid, csc)
    overwrite = getattr(args, 'force', False)

    a_updated = r_updated = t_updated = 0
    with managed_db(args.db or DB_PATH) as conn:
        now = int(time.time())

        artist_clause = ''
        artist_params = []
        if args.artist:
            row = resolve_artist(conn, args.artist)
            if not row:
                console.print(f'[red]Artist not found:[/red] {args.artist}')
                return
            artist_clause = ' AND a.id = ?'
            artist_params = [row['id']]

        # ── Phase 1: artists ───────────────────────────────────────────────────
        pop_filter = '' if overwrite else ' AND a.spotify_popularity IS NULL'
        a_rows = conn.execute(f'''
            SELECT a.id, a.spotify_id, a.name
            FROM artists a
            WHERE a.spotify_id IS NOT NULL AND a.hidden = 0
              {pop_filter}{artist_clause}
            ORDER BY a.name
        ''', artist_params).fetchall()
        a_rows = _paginate(a_rows, args)

        if a_rows:
            console.print(f'[dim]Fetching popularity for {len(a_rows)} artists…[/dim]')
            sp_ids   = [r['spotify_id'] for r in a_rows]
            id_to_id = {r['spotify_id']: r['id'] for r in a_rows}
            artists_data = client.get_artists_batch(sp_ids)
            for a in artists_data:
                if not a:
                    continue
                db_id = id_to_id.get(a['id'])
                if not db_id:
                    continue
                conn.execute(
                    'UPDATE artists SET spotify_popularity = ?, spotify_followers = ?,'
                    ' updated_at = ? WHERE id = ?',
                    (a.get('popularity'), a.get('followers', {}).get('total'), now, db_id)
                )
                a_updated += 1
            conn.commit()

        # ── Phase 2: releases ──────────────────────────────────────────────────
        pop_filter = '' if overwrite else ' AND r.spotify_popularity IS NULL'
        r_join     = 'JOIN artists a ON a.id = r.primary_artist_id' if args.artist else ''
        r_rows     = conn.execute(f'''
            SELECT r.id, r.spotify_id, r.title
            FROM releases r {r_join}
            WHERE r.spotify_id IS NOT NULL AND r.hidden = 0
              {pop_filter}{artist_clause.replace('a.id', 'r.primary_artist_id') if args.artist else ''}
            ORDER BY r.release_year DESC NULLS LAST, r.title
        ''', artist_params).fetchall()
        r_rows = _paginate(r_rows, args)

        if r_rows:
            console.print(f'[dim]Fetching popularity for {len(r_rows)} releases…[/dim]')
            sp_ids    = [r['spotify_id'] for r in r_rows]
            id_to_id  = {r['spotify_id']: r['id'] for r in r_rows}
            albums_data = client.get_albums_batch(sp_ids)
            for alb in albums_data:
                if not alb:
                    continue
                db_id = id_to_id.get(alb['id'])
                if not db_id:
                    continue
                conn.execute(
                    'UPDATE releases SET spotify_popularity = ?, updated_at = ? WHERE id = ?',
                    (alb.get('popularity'), now, db_id)
                )
                r_updated += 1
            conn.commit()

        # ── Phase 3: tracks ────────────────────────────────────────────────────
        pop_filter = '' if overwrite else ' AND t.spotify_popularity IS NULL'
        t_join     = 'JOIN releases r ON r.id = t.release_id' if args.artist else ''
        t_rows     = conn.execute(f'''
            SELECT t.id, t.spotify_id, t.title
            FROM tracks t {t_join}
            WHERE t.spotify_id IS NOT NULL AND t.hidden = 0
              {pop_filter}{artist_clause.replace('a.id', 'r.primary_artist_id') if args.artist else ''}
            ORDER BY t.id
        ''', artist_params).fetchall()
        t_rows = _paginate(t_rows, args)

        if t_rows:
            console.print(f'[dim]Fetching popularity for {len(t_rows)} tracks…[/dim]')
            sp_ids   = [r['spotify_id'] for r in t_rows]
            id_to_id = {r['spotify_id']: r['id'] for r in t_rows}
            tracks_data = client.get_tracks_batch(sp_ids)
            for tr in tracks_data:
                if not tr:
                    continue
                db_id = id_to_id.get(tr['id'])
                if not db_id:
                    continue
                conn.execute(
                    'UPDATE tracks SET spotify_popularity = ?, updated_at = ? WHERE id = ?',
                    (tr.get('popularity'), now, db_id)
                )
                t_updated += 1
            conn.commit()

    console.rule(style='dim')
    console.print(
        f'  [dim]Updated popularity: {a_updated} artists · '
        f'{r_updated} releases · {t_updated} tracks[/dim]'
    )

# ── cmd: audit aoty ───────────────────────────────────────────────────────────

_SLUG_STOP = frozenset({'a', 'an', 'the', 'of', 'and', 'in', 'on', 'at', 'to', 'is', 'it', 'for'})


def _aoty_slug_words(aoty_url: str) -> 'frozenset[str]':
    """Meaningful words from the AOTY URL slug (the part after the numeric ID)."""
    m = re.search(r'/album/\d+-(.+?)(?:\.php)?$', aoty_url)
    if not m:
        return frozenset()
    words = re.split(r'[-_]+', m.group(1).lower())
    return frozenset(w for w in words if w and w not in _SLUG_STOP)


def _slug_overlap(title: str, aoty_url: str) -> float:
    """Fraction of normalised title words present in the AOTY URL slug (0..1).
    Uses substring containment as a fallback so that 'section.80' matches
    the slug token 'section80' and '99.9%' matches '999'."""
    slug_raw = _aoty_slug_words(aoty_url)
    if not slug_raw:
        return 1.0
    # Expand each slug token by splitting further on non-alnum boundaries
    slug_atoms = frozenset(
        atom
        for w in slug_raw
        for atom in re.split(r'[^a-z0-9]+', w)
        if atom and atom not in _SLUG_STOP
    )
    # Full set of raw slug tokens (unsplit) for substring check
    slug_full = frozenset(w for w in slug_raw if w not in _SLUG_STOP)

    # Normalize title: replace non-alnum with spaces before splitting
    base = re.sub(r'[^a-z0-9]+', ' ', _base_title(title).lower()).strip()
    title_atoms = [w for w in base.split() if w and w not in _SLUG_STOP]
    if not title_atoms:
        return 1.0

    matched = sum(
        1 for w in title_atoms
        if w in slug_atoms or any(w in s for s in slug_full)
    )
    return matched / len(title_atoms)




# ── cmd: enrich artists ────────────────────────────────────────────────────────

def cmd_enrich_artists(args):
    """Fetch artist metadata from MusicBrainz (type, gender, country, dates),
    and optionally Spotify photo/followers/popularity (--spotify).
    """
    updated = skipped = 0
    do_spotify = getattr(args, 'spotify', False)
    sp_client = None
    if do_spotify:
        cid = os.environ.get('SPOTIFY_CLIENT_ID', '')
        csc = os.environ.get('SPOTIFY_CLIENT_SECRET', '')
        if cid and csc:
            sp_client = SpotifyClient(cid, csc)
        else:
            console.print('[yellow]--spotify requested but SPOTIFY_CLIENT_ID/SECRET are not set — '
                          'skipping Spotify enrichment.[/yellow]')
            do_spotify = False

    try:
        with managed_db(args.db or DB_PATH) as conn:
            where  = 'WHERE 1=1'
            params = []
            if not args.force:
                # Skip artists already attempted (mb_attempted=1 covers both "searched but no match"
                # and "successfully enriched"). Artists imported via mdb import have mbid set but
                # mb_attempted=0, so they are correctly included here.
                #
                # With --spotify, also include artists that passed the MB step but
                # still have no photo.
                if do_spotify:
                    where += ' AND (a.mb_attempted = 0 OR a.image_url IS NULL)'
                else:
                    where += ' AND a.mb_attempted = 0'
            if args.artist:
                row = resolve_artist(conn, args.artist)
                if not row:
                    console.print(f'[red]Artist not found:[/red] {args.artist}')
                    return
                where += ' AND a.id = ?'
                params.append(row['id'])

            queue = conn.execute(
                f'''SELECT a.id, a.name, a.mbid, a.mb_attempted, a.spotify_id, a.image_url,
                           COUNT(CASE WHEN t.hidden = 0 THEN l.id END) as total_listens
                    FROM artists a
                    LEFT JOIN track_artists ta ON ta.artist_id = a.id AND ta.role = "main"
                    LEFT JOIN tracks t ON t.id = ta.track_id
                    LEFT JOIN listens l ON l.track_id = t.id
                    {where}
                    GROUP BY a.id
                    ORDER BY total_listens DESC, a.name''',
                params
            ).fetchall()

            queue = _paginate(queue, args)

            if not queue:
                console.print('[dim]No artists to enrich.[/dim]')
                return

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
            now = int(time.time())

            for artist in queue:
                mb_parts = []
                wiki_url = None
                skip_this = False

                # Skip the MB step entirely if it already ran (mb_attempted=1) and
                # this pass was only pulled in by --spotify's "missing photo" filter —
                # otherwise every already-MB-enriched artist would get needlessly
                # re-fetched from MB just to reach the Spotify step below.
                needs_mb = args.force or not artist['mb_attempted']

                if needs_mb:
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
                            skip_this = True
                        else:
                            mbid = best['id']
                            try:
                                conn.execute('UPDATE artists SET mbid = ? WHERE id = ?', (mbid, artist['id']))
                                conn.commit()
                            except sqlite3.IntegrityError:
                                console.print(f'  [dim]·[/dim]  {artist["name"]}  [dim]MBID already assigned to another artist[/dim]')
                                skip_this = True

                    if not skip_this:
                        data = mb_fetch_artist_data(mbid)
                        if not data:
                            console.print(f'  [dim]·[/dim]  {artist["name"]}  [dim]no MB data[/dim]')
                            skip_this = True
                        else:
                            wiki_url = data.pop('wikipedia_url', None)
                            members  = data.pop('members', None)
                            collaborators = data.pop('collaborators', None)
                            updates = {col: data[key] for key, col in _MB_COL_MAP.items() if key in data}
                            if wiki_url:
                                upsert_external_link(conn, EL_ARTIST, artist['id'], EL_SVC_WIKIPEDIA, wiki_url)
                            # mark done regardless of whether fields changed
                            updates['mb_attempted'] = 1
                            updates['updated_at'] = now
                            set_clause = ', '.join(f'{k} = ?' for k in updates)
                            conn.execute(f'UPDATE artists SET {set_clause} WHERE id = ?',
                                         (*updates.values(), artist['id']))

                            members_added = 0
                            if members:
                                cur_max = conn.execute(
                                    'SELECT COALESCE(MAX(sort_order), -1) FROM artist_members WHERE group_artist_id = ?',
                                    (artist['id'],)
                                ).fetchone()[0]
                                for i, m in enumerate(members):
                                    if m['ended']:
                                        # only link current members automatically
                                        continue
                                    member_id, _ = upsert_artist_mb(conn.cursor(), {'id': m['mbid'], 'name': m['name']})
                                    try:
                                        conn.execute(
                                            'INSERT INTO artist_members (group_artist_id, member_artist_id, sort_order) '
                                            'VALUES (?, ?, ?)',
                                            (artist['id'], member_id, cur_max + 1 + i)
                                        )
                                        members_added += 1
                                    except sqlite3.IntegrityError:
                                        # already linked
                                        pass

                            collabs_added = 0
                            if collaborators:
                                # Only link collaborators that already exist in our catalog by
                                # MBID — unlike members, we don't want to mint a new artist row
                                # for every one-off MB collaboration credit (too noisy; would
                                # create stub artists with no releases of their own).
                                for c in collaborators:
                                    other = conn.execute(
                                        'SELECT id FROM artists WHERE mbid = ?', (c['mbid'],)
                                    ).fetchone()
                                    if not other:
                                        continue
                                    try:
                                        conn.execute(
                                            '''INSERT INTO artist_relations
                                               (from_artist_id, to_artist_id, relation_type, source)
                                               VALUES (?, ?, 'collaboration', 'musicbrainz')''',
                                            (artist['id'], other['id'])
                                        )
                                        collabs_added += 1
                                    except sqlite3.IntegrityError:
                                        # already linked
                                        pass
                            conn.commit()
                            if 'type'           in updates: mb_parts.append(updates['type'])
                            if 'gender'         in updates: mb_parts.append(updates['gender'])
                            if 'country'        in updates: mb_parts.append(updates['country'])
                            if 'formed_year'    in updates: mb_parts.append(str(updates['formed_year']))
                            if 'disbanded_year' in updates: mb_parts.append(f'–{updates["disbanded_year"]}')
                            if wiki_url:                    mb_parts.append(f'[link={wiki_url}]wikipedia[/link]')
                            if members_added:               mb_parts.append(f'{members_added} member(s)')
                            if collabs_added:                mb_parts.append(f'{collabs_added} collab(s)')

                    if skip_this and not do_spotify:
                        skipped += 1
                        continue

                sp_parts = []
                if do_spotify and sp_client and (not artist['image_url'] or args.force):
                    try:
                        sp_artist = None
                        if artist['spotify_id']:
                            sp_artist = sp_client.get(f"/artists/{artist['spotify_id']}")
                        else:
                            search = sp_client.get('/search', {'q': artist['name'], 'type': 'artist', 'limit': 5})
                            cands = (search.get('artists') or {}).get('items') or []
                            sp_artist = next(
                                (c for c in cands if _norm(c.get('name', '')) == _norm(artist['name'])
                                 and c.get('images')),
                                None
                            )
                        if sp_artist and sp_artist.get('images'):
                            images = sp_artist['images']
                            full  = images[0]['url'] if images else None
                            thumb = images[1]['url'] if len(images) > 1 else full
                            conn.execute(
                                'UPDATE artists SET spotify_id = ?, image_url = ?, image_thumb_url = ?,'
                                ' image_source = ?, spotify_followers = ?, spotify_popularity = ?,'
                                ' updated_at = ? WHERE id = ?',
                                (sp_artist['id'], full, thumb, 'spotify',
                                 (sp_artist.get('followers') or {}).get('total'),
                                 sp_artist.get('popularity'), now, artist['id'])
                            )
                            conn.commit()
                            sp_parts.append('spotify photo')
                        else:
                            sp_parts.append('[dim]no spotify match[/dim]')
                    except sqlite3.IntegrityError:
                        sp_parts.append('[dim]spotify_id already assigned to another artist[/dim]')
                    except Exception as e:
                        sp_parts.append(f'[dim]spotify error: {e}[/dim]')

                if skip_this and not sp_parts:
                    skipped += 1
                    continue

                all_parts = mb_parts + sp_parts
                console.print(f'  [green]✓[/green]  {artist["name"]:<30}  [dim]{" · ".join(all_parts)}[/dim]')
                updated += 1

    except KeyboardInterrupt:
        console.print('\n  [yellow]Interrupted.[/yellow]')
    console.rule(style='dim')
    console.print(f'  [dim]Updated: {updated} · Skipped: {skipped}[/dim]')

# ── cmd: enrich soundtracks ───────────────────────────────────────────────────

def cmd_enrich_soundtracks_wrapper(args):
    with managed_db(args.db or DB_PATH) as conn:
        cmd_enrich_soundtracks(
            conn,
            skip=args.skip,
            limit=args.limit,
            release_id=getattr(args, 'release_id', None),
            overwrite=getattr(args, 'force', False),
        )

# ── cmd: hide ─────────────────────────────────────────────────────────────────

def cmd_hide(args):
    with managed_db(args.db or DB_PATH) as conn:
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
    """Return counts of tracks, listens, and variant link rows for a release.

    Also returns per-track listen counts for the interactive unlink prompt.
    """
    track_rows = conn.execute(
        'SELECT id, title FROM tracks WHERE release_id = ? ORDER BY disc_number, track_number',
        [release_id]
    ).fetchall()
    track_ids = [r[0] for r in track_rows]

    listens = 0
    # [(title, listen_count)] for tracks that have listens
    per_track = []
    if track_ids:
        ph = ','.join('?' * len(track_ids))
        listens = conn.execute(
            f'SELECT COUNT(*) FROM listens WHERE track_id IN ({ph})', track_ids
        ).fetchone()[0]
        # Per-track breakdown for display
        counts = {
            r[0]: r[1]
            for r in conn.execute(
                f'SELECT track_id, COUNT(*) FROM listens WHERE track_id IN ({ph})'
                f' GROUP BY track_id', track_ids
            ).fetchall()
        }
        per_track = [(r[1], counts[r[0]]) for r in track_rows if r[0] in counts]

    rv_rows = conn.execute(
        'SELECT canonical_id, variant_id FROM release_variants'
        ' WHERE canonical_id = ? OR variant_id = ?', [release_id, release_id]
    ).fetchall()
    return {
        'track_ids':    track_ids,
        'tracks':       len(track_ids),
        'listens':      listens,
        # [(title, count)] for tracks with >0 listens
        'per_track':    per_track,
        'variant_rows': [(r[0], r[1]) for r in rv_rows],
    }


def _execute_delete_release(conn, release_id: str, purge: bool = False) -> dict:
    """Delete a release and its tracks.

    purge=False (default): unlinks listens (track_id → NULL), preserving raw
      scrobble history in the match queue.
    purge=True: hard-deletes listen rows.

    Returns {'tracks': n, 'listens': n} counts.
    """
    impact    = _gather_release_impact(conn, release_id)
    track_ids = impact['track_ids']

    affected_listens = 0
    if track_ids:
        ph = ','.join('?' * len(track_ids))
        if purge:
            affected_listens = conn.execute(
                f'DELETE FROM listens WHERE track_id IN ({ph})', track_ids
            ).rowcount
        else:
            affected_listens = conn.execute(
                f'UPDATE listens SET track_id = NULL WHERE track_id IN ({ph})', track_ids
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
    return {'tracks': impact['tracks'], 'listens': affected_listens}


def cmd_delete(args):
    db_path = getattr(args, 'db', None) or DB_PATH
    with managed_db(db_path) as conn:
        purge  = getattr(args, 'purge', False)
        yes    = getattr(args, 'yes',   False)
        # 'releases' or 'artists'
        entity = args.entity

        # ── Resolve all IDs ────────────────────────────────────────────────────────
        resolved = []
        for raw in args.ids:
            row = _resolve_for_delete(conn, raw, entity)
            if not row:
                console.print(f'  [red]Not found:[/red] {raw}')
                sys.exit(1)
            # (id, display_name)
            resolved.append((row[0], row[1]))

        # ── Gather and display impact summary ──────────────────────────────────────
        if entity == 'releases':
            impacts = {}
            total_tracks = total_listens = 0
            for rid, rname in resolved:
                imp = _gather_release_impact(conn, rid)
                impacts[rid] = imp
                total_tracks  += imp['tracks']
                total_listens += imp['listens']
                console.print(
                    f'  [bold]{rname}[/bold]  '
                    f'[dim]{imp["tracks"]} track(s)[/dim]'
                    + (f'  [dim]{imp["listens"]} listen(s)[/dim]' if imp['listens'] else '')
                )
                if imp['variant_rows']:
                    console.print(
                        f'    [dim]→ {len(imp["variant_rows"])} variant link(s) will be removed[/dim]'
                    )
                # Per-track breakdown when listens exist and we're not purging
                if imp['listens'] and not purge:
                    for title, count in imp['per_track']:
                        short = (title[:42] + '…') if len(title) > 43 else title
                        console.print(f'    [dim]{short:<43} {count:>4} listen(s)[/dim]')

            if total_listens:
                if purge:
                    console.print(
                        f'\n  [red]⚠  {total_listens} listen(s) will be permanently deleted.[/red]'
                    )
                else:
                    console.print(
                        f'\n  {total_listens} listen(s) will be [bold]unlinked[/bold] '
                        f'→ returned to the match queue'
                        f'\n  [dim]Run \'sync match\' afterwards to re-assign them.[/dim]'
                    )

        # artists
        else:
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

                console.print(
                    f'  [bold]{aname}[/bold]  '
                    f'[dim]{len(rel_rows)} release(s) · {rel_track_count} track(s)[/dim]'
                    + (f'  [dim]{lcount} listen(s)[/dim]' if lcount else '')
                )
                total_releases += len(rel_rows)
                total_tracks   += rel_track_count
                total_listens  += lcount

            if total_listens:
                if purge:
                    console.print(
                        f'\n  [red]⚠  {total_listens} listen(s) will be permanently deleted.[/red]'
                    )
                else:
                    console.print(
                        f'\n  {total_listens} listen(s) will be [bold]unlinked[/bold] '
                        f'→ returned to the match queue'
                    )

        console.print(
            f'\n[dim]Will delete: {len(resolved)} {entity} · {total_tracks} track(s)'
            + (f' · {total_listens} listen(s) '
               + ('purged' if purge else 'unlinked') if total_listens else '')
            + '[/dim]'
        )

        # ── Confirm ────────────────────────────────────────────────────────────────
        if not yes:
            try:
                answer = input('\n  Proceed? [Y/n] ').strip().lower()
            except (EOFError, KeyboardInterrupt):
                console.print('\n[dim]Cancelled.[/dim]')
                sys.exit(0)
            if answer not in ('', 'y', 'yes'):
                console.print('[dim]Cancelled.[/dim]')
                return

        # ── Execute ────────────────────────────────────────────────────────────────
        if entity == 'releases':
            for rid, rname in resolved:
                s = _execute_delete_release(conn, rid, purge=purge)
                listen_note = ''
                if s['listens']:
                    listen_note = (
                        f'  [dim]{s["listens"]} listen(s) purged[/dim]' if purge
                        else f'  [dim]{s["listens"]} listen(s) unlinked[/dim]'
                    )
                console.print(f'  [green]deleted[/green]  {rname}{listen_note}')

        else:
            for aid, aname in resolved:
                deleted_tracks = affected_listens = 0
                for rel in artist_releases[aid]:
                    s = _execute_delete_release(conn, rel[0], purge=purge)
                    deleted_tracks   += s['tracks']
                    affected_listens += s['listens']
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
                if affected_listens:
                    verb = 'purged' if purge else 'unlinked'
                    detail += f' · {affected_listens} listen(s) {verb}'
                console.print(f'  [green]deleted[/green]  {aname}  [dim]({detail})[/dim]')

        conn.commit()
    console.rule(style='dim')

# ── cmd: artist images ────────────────────────────────────────────────────────

def cmd_artist_images(args):
    with managed_db(args.db or DB_PATH) as conn:
        updates = []
        with open(args.csv_file, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                updates.append({
                    'name': row['artist_name'].strip(),
                    'url':  row['profile_image_url'].strip(),
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
                " updated_at = ? WHERE id = ?",
                (u['url'], now, row[0])
            )
            console.print(f'  [green]upd[/green]  {u["name"]}')
            ok += 1

        conn.commit()
    console.rule(style='dim')
    console.print(f'  [dim]{ok} updated  ·  {len(not_found)} not found[/dim]')

# ── cmd: link sources ─────────────────────────────────────────────────────────

def cmd_link_sources(args):
    with managed_db(args.db or DB_PATH) as conn:
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
    console.print(f'\n  [dim]{ok} source(s) linked to {compilation[1]}[/dim]')

# ── cmd: alias ────────────────────────────────────────────────────────────────

def cmd_alias(args):
    with managed_db(getattr(args, 'db', None) or DB_PATH) as conn:
        artist = resolve_artist(conn, args.artist)
        if not artist:
            console.print(f'[red]Artist not found:[/red] {args.artist}')
            sys.exit(1)

        if args.alias_cmd == 'add':
            upsert_artist_alias(conn, artist['id'], args.alias,
                                alias_type=args.alias_type,
                                language=getattr(args, 'language', None),
                                source=args.source,
                                sort_order=getattr(args, 'sort_order', 0))
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


# ── cmd: artist merge ──────────────────────────────────────────────────────────

def cmd_artist_merge(args):
    """
    Merge FROM artist into TO artist (the canonical record to keep).

    - Repoints release_artists, track_artists, releases.primary_artist_id,
      artist_aliases, artist_relations, artist_members
    - Inserts FROM artist's name as a past_name alias on TO (unless --no-alias)
    - Transfers missing metadata fields from FROM → TO
    - Deletes the FROM artist row
    """
    with managed_db(args.db or DB_PATH) as conn:
        from_artist = resolve_artist(conn, args.from_artist)
        to_artist   = resolve_artist(conn, args.to_artist)

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

        # Drop relation rows that would collide with TO's existing relations once
        # repointed (same PK: from_artist_id, to_artist_id, relation_type), then
        # repoint. Mirrors the artist_members collision handling below.
        conn.execute('''
            DELETE FROM artist_relations AS r1
            WHERE from_artist_id = ?
              AND EXISTS (
                  SELECT 1 FROM artist_relations AS r2
                  WHERE r2.from_artist_id = ?
                    AND r2.to_artist_id   = r1.to_artist_id
                    AND r2.relation_type  = r1.relation_type
              )
        ''', [from_id, to_id])
        conn.execute('''
            DELETE FROM artist_relations AS r1
            WHERE to_artist_id = ?
              AND EXISTS (
                  SELECT 1 FROM artist_relations AS r2
                  WHERE r2.to_artist_id    = ?
                    AND r2.from_artist_id  = r1.from_artist_id
                    AND r2.relation_type   = r1.relation_type
              )
        ''', [from_id, to_id])
        conn.execute('UPDATE artist_relations SET from_artist_id = ? WHERE from_artist_id = ?', [to_id, from_id])
        conn.execute('UPDATE artist_relations SET to_artist_id   = ? WHERE to_artist_id   = ?', [to_id, from_id])
        # A merge can make an artist relate to itself; that edge is meaningless.
        conn.execute('DELETE FROM artist_relations WHERE from_artist_id = to_artist_id')

        # artist_year_medals has no natural "combine" — TO keeps its own medals
        # (they're independently computed per artist, not additive facts to
        # merge) and any leftover FROM rows are simply dropped so the later
        # DELETE FROM artists doesn't fail on the artist_id foreign key.
        conn.execute('DELETE FROM artist_year_medals WHERE artist_id = ?', [from_id])

        # Drop rows that would collide with TO's existing membership, then repoint.
        conn.execute('''
            DELETE FROM artist_members
            WHERE group_artist_id = ?
              AND member_artist_id IN (SELECT member_artist_id FROM artist_members
                                       WHERE group_artist_id = ?)
        ''', [from_id, to_id])
        conn.execute('''
            DELETE FROM artist_members
            WHERE member_artist_id = ?
              AND group_artist_id IN (SELECT group_artist_id FROM artist_members
                                      WHERE member_artist_id = ?)
        ''', [from_id, to_id])
        conn.execute('UPDATE artist_members SET group_artist_id  = ? WHERE group_artist_id  = ?', [to_id, from_id])
        conn.execute('UPDATE artist_members SET member_artist_id = ? WHERE member_artist_id = ?', [to_id, from_id])
        # A merge can make an artist its own member; that edge is meaningless.
        conn.execute('DELETE FROM artist_members WHERE group_artist_id = member_artist_id')

        console.print(f'  [dim]Repointed {ra_count} release_artists, {ta_count} track_artists, {rel_count} primary releases[/dim]')

        # Transfer missing metadata fields (TO takes priority for existing values)
        fields_to_transfer = [
            'sort_name', 'spotify_id', 'mbid',
            'image_url', 'image_source',
            'country', 'formed_year', 'disbanded_year', 'bio',
            'aoty_id', 'aoty_url', 'type', 'gender', 'disambiguation',
        ]
        from_row = conn.execute('SELECT * FROM artists WHERE id = ?', [from_id]).fetchone()
        to_row   = conn.execute('SELECT * FROM artists WHERE id = ?', [to_id]).fetchone()
        transferred = []
        for field in fields_to_transfer:
            try:
                if to_row[field] is None and from_row[field] is not None:
                    conn.execute(f'UPDATE artists SET {field} = ?, updated_at = ? WHERE id = ?',
                                 [from_row[field], now, to_id])
                    transferred.append(field)
            except IndexError:
                # column might not exist on older schema
                pass
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
            upsert_artist_alias(conn, to_id, from_name, alias_type='past_name', source='manual')
            console.print(f'  [dim]Added past_name alias: "{from_name}"[/dim]')

        # Delete the FROM artist
        conn.execute('DELETE FROM artists WHERE id = ?', [from_id])
        console.print(f'  [dim]Deleted artist row: {from_name} ({from_id})[/dim]')

        conn.commit()
    console.print(f'\n  [green]✓[/green]  Merged [bold]{from_name}[/bold] → [bold]{to_name}[/bold]')


# ── cmd: artist members ────────────────────────────────────────────────────────

def cmd_artist_members(args):
    with managed_db(args.db or DB_PATH) as conn:
        if args.members_cmd == 'list':
            group = resolve_artist(conn, args.group)
            if not group:
                console.print(f'[red]Artist not found:[/red] {args.group}')
                return
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
            group = resolve_artist(conn, args.group)
            if not group:
                console.print(f'[red]Group not found:[/red] {args.group}')
                return
            # Next sort_order after existing members
            cur_max = conn.execute(
                'SELECT COALESCE(MAX(sort_order), -1) FROM artist_members WHERE group_artist_id = ?',
                [group['id']]
            ).fetchone()[0]
            added = 0
            for i, member_key in enumerate(args.members):
                member = resolve_artist(conn, member_key)
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
            group = resolve_artist(conn, args.group)
            member = resolve_artist(conn, args.member)
            if not group:
                console.print(f'[red]Group not found:[/red] {args.group}')
                return
            if not member:
                console.print(f'[red]Member not found:[/red] {args.member}')
                return
            deleted = conn.execute(
                'DELETE FROM artist_members WHERE group_artist_id = ? AND member_artist_id = ?',
                [group['id'], member['id']]
            ).rowcount
            conn.commit()
            if deleted:
                console.print(f'  [green]removed[/green]  {member["name"]}  from  {group["name"]}')
            else:
                console.print(f'  [yellow]No link found[/yellow] between {member["name"]} and {group["name"]}')



def _resolve_release(conn, key: str):
    """Look up a release by internal ID, Spotify ID, MusicBrainz ID, or title (case-insensitive)."""
    return (
        conn.execute('SELECT id, title FROM releases WHERE id = ?',                  [key]).fetchone() or
        conn.execute('SELECT id, title FROM releases WHERE spotify_id = ?',          [key]).fetchone() or
        conn.execute('SELECT id, title FROM releases WHERE mbid = ?',                [key]).fetchone() or
        conn.execute('SELECT id, title FROM releases WHERE lower(title) = lower(?)', [key]).fetchone()
    )

def cmd_release_alias(args):
    with managed_db(getattr(args, 'db', None) or DB_PATH) as conn:
        release = _resolve_release(conn, args.release)
        if not release:
            console.print(f'[red]Release not found:[/red] {args.release}')
            sys.exit(1)

        if args.release_alias_cmd == 'add':
            is_def = 1 if getattr(args, 'definitive', False) else 0
            upsert_release_alias(conn, release['id'], args.alias,
                                 is_definitive=is_def,
                                 language=getattr(args, 'language', None),
                                 source=args.source,
                                 alias_type=args.type)
            conn.commit()
            def_label = '  [bold](definitive)[/bold]' if is_def else ''
            console.print(f'  [green]✓[/green]  "{args.alias}"{def_label}  →  {release["title"]}  [dim]({args.source}, {args.type})[/dim]')

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
                '''SELECT alias, is_definitive, source, alias_type
                   FROM release_aliases WHERE release_id = ?
                   ORDER BY is_definitive DESC, alias''',
                [release['id']],
            ).fetchall()
            console.print(f'  Aliases for [bold]{release["title"]}[/bold]:')
            if rows:
                for r in rows:
                    def_tag = '  [bold dim](definitive)[/bold dim]' if r['is_definitive'] else ''
                    console.print(f'    {r["alias"]}{def_tag}  [dim]({r["source"]}, {r["alias_type"]})[/dim]')
            else:
                console.print('    [dim]none[/dim]')


# ── cmd: relation ──────────────────────────────────────────────────────────────

def cmd_relation(args):
    with managed_db(getattr(args, 'db', None) or DB_PATH) as conn:
        if args.relation_cmd == 'list':
            artist = resolve_artist(conn, args.artist)
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
            return

        from_artist = resolve_artist(conn, args.from_artist)
        to_artist   = resolve_artist(conn, args.to_artist)
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
                console.print('  [green]✓[/green]  Removed')
            else:
                console.print('  [yellow]Not found[/yellow]')

# ── cmd: release variants ─────────────────────────────────────────────────────

_VARIANT_ROW_SELECT = '''
    SELECT r.id, r.title, r.release_date, r.primary_artist_id,
           a.name AS artist_name, r.release_group_mbid,
           r.spotify_id, r.mbid,
           r.type, r.type_secondary,
           COUNT(t.id)                                    AS track_count,
           SUM(CASE WHEN t.is_explicit = 1 THEN 1 ELSE 0 END) AS explicit_count
    FROM   releases r
    JOIN   artists  a ON a.id = r.primary_artist_id
    LEFT JOIN tracks t ON t.release_id = r.id
'''


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
    rows = conn.execute(_VARIANT_ROW_SELECT + '''
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
    # release_group_mbid → [row]
    by_mbgrp  = {}
    # (primary_artist_id, base_title_lower) → [row]
    by_artist = {}

    for row in rows:
        rid, title, date, artist_id, artist_name, rg_mbid = row[:6]

        if rg_mbid:
            by_mbgrp.setdefault(rg_mbid, []).append(dict(row))

        bt = _base_title(title).lower().strip()
        if artist_id:
            by_artist.setdefault((artist_id, bt), []).append(dict(row))

    # list of frozensets of ids, to deduplicate
    seen_sets = []
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
            # skip groups that are already fully linked
            return

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
    row = conn.execute(_VARIANT_ROW_SELECT + '''
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
    saved   = 0
    try:
        with managed_db(db_path) as conn:
            include_linked = getattr(args, 'all', False)
            groups = _find_variant_groups(conn, include_linked=include_linked)

            if not groups:
                console.print('  [dim]No unlinked variant groups found.[/dim]')
                return

            console.print(
                f'  Found [bold]{len(groups)}[/bold] candidate group(s).  '
                f'[dim]\\[s]kip  \\[q]uit  \\[a]dd release[/dim]\n'
            )

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
                        # re-display updated group
                        continue

                    if not raw.isdigit() or not (1 <= int(raw) <= len(group)):
                        console.print(f'  [red]Enter 1–{len(group)}, a, s, or q.[/red]')
                        continue

                    canonical = group[int(raw) - 1]
                    break

                if quit_all or canonical is None:
                    continue

                # ── per-release type assignment (stages 1 & 2 for every member) ───────
                # release_id → (type, type_secondary)
                type_updates  = {}
                # (variant_id, edition_type, sort_order)
                edition_links = []
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
                            # done
                            stage = 4

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
    except KeyboardInterrupt:
        console.print('\n  [yellow]Interrupted.[/yellow]')
    console.rule(style='dim')
    console.print(f'  [dim]Done — {saved} variant link(s) saved.[/dim]')


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
        merge_variant_tracks(conn, canonical['id'], variant_id)

    conn.commit()


# ── cmd: certs refresh ────────────────────────────────────────────────────────

_CERT_THRESHOLDS = [
    ('diamond',  1000),
    ('platinum',  500),
    ('gold',      250),
]


def cmd_list_import_csv(args):
    """Create/refresh a canonical_lists row + its ranked entries from a CSV
    or JSON file (format auto-detected by extension).

    CSV: columns given by --rank-col/--artist-col/--album-col/--year-col.
    JSON: a list of objects with keys rank/artist/album/year (year optional)
    and an optional position_label (e.g. "2025 #1") for lists that have no
    single global ranking — a per-year top-5 list, say — where `rank` is
    still required (as a dense/unique internal sort key) but the UI should
    show position_label instead of a bare "#rank".

    Idempotent: re-running with the same --id replaces that list's entries
    (matched release_ids are preserved by re-matching on artist+title, not
    wiped — a rank can shift between editions of the same underlying source
    without losing prior matches).
    """
    import csv as csv_mod
    if not os.path.exists(args.csv):
        console.print(f'[red]File not found:[/red] {args.csv}')
        return

    entries = []
    if args.csv.lower().endswith('.json'):
        with open(args.csv, encoding='utf-8') as f:
            data = json.load(f)
        for row in data:
            rank = row.get('rank')
            album = str(row.get('album') or '').strip()
            artist = str(row.get('artist') or '').strip()
            if rank is None or not album or not artist:
                continue
            year = row.get('year')
            year = int(year) if isinstance(year, (int, str)) and str(year).isdigit() else None
            label = row.get('position_label')
            label = str(label).strip() if label else None
            entries.append((int(rank), artist, album, year, label))
    else:
        if not args.rank_col:
            console.print('[red]--rank-col is required for CSV input.[/red]')
            return
        with open(args.csv, newline='', encoding='utf-8') as f:
            reader = csv_mod.DictReader(f)
            if args.rank_col not in reader.fieldnames:
                console.print(f'[red]Column not found:[/red] {args.rank_col!r}')
                console.print(f'  [dim]Available: {", ".join(reader.fieldnames)}[/dim]')
                return
            for row in reader:
                raw_rank = (row.get(args.rank_col) or '').strip()
                if not raw_rank:
                    continue
                try:
                    rank = int(raw_rank)
                except ValueError:
                    # footnote rows ("added 2023", "prior RS", etc.) — not part of the ranked list
                    continue
                album  = (row.get(args.album_col) or '').strip()
                artist = (row.get(args.artist_col) or '').strip()
                if not album or not artist:
                    continue
                year_raw = (row.get(args.year_col) or '').strip() if args.year_col else ''
                year = int(year_raw) if year_raw.isdigit() else None
                entries.append((rank, artist, album, year, None))

    if not entries:
        console.print('[red]No ranked rows found — check --rank-col (CSV) or the JSON shape.[/red]')
        return
    entries.sort(key=lambda e: e[0])

    now = int(time.time())
    with managed_db(args.db or DB_PATH) as conn:
        # Preserve existing release_id matches across a re-import: key by
        # (artist_name, album_title) since rank can legitimately shift
        # between an existing DB copy and a freshly re-exported CSV.
        existing_matches = {}
        if conn.execute('SELECT 1 FROM canonical_lists WHERE id=?', (args.id,)).fetchone():
            for r in conn.execute(
                'SELECT artist_name, album_title, release_id FROM canonical_list_entries '
                'WHERE list_id=? AND release_id IS NOT NULL', (args.id,)
            ).fetchall():
                existing_matches[(r['artist_name'], r['album_title'])] = r['release_id']

        conn.execute('''
            INSERT INTO canonical_lists (id, name, short_name, source_url, total_count, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name, short_name=excluded.short_name,
                source_url=excluded.source_url, total_count=excluded.total_count,
                updated_at=excluded.updated_at
        ''', (args.id, args.name, args.short_name, args.source_url, len(entries), now, now))

        conn.execute('DELETE FROM canonical_list_entries WHERE list_id=?', (args.id,))
        for rank, artist, album, year, label in entries:
            release_id = existing_matches.get((artist, album))
            conn.execute(
                'INSERT INTO canonical_list_entries (list_id, rank, release_id, artist_name, album_title, year, position_label) '
                'VALUES (?,?,?,?,?,?,?)',
                (args.id, rank, release_id, artist, album, year, label)
            )
        conn.commit()

    console.print(f'[green]✓[/green] {args.id}: {len(entries)} entries '
                  f'({len(existing_matches)} pre-matched carried over)')
    console.print(f'  [dim]Run `mdb.py list match --id {args.id}` to match/backfill releases.[/dim]')


def cmd_list_match(args):
    """Match canonical_list_entries against existing releases by artist+title.

    Read-only against `releases`/`artists` — never imports anything. Use
    `mdb.py import <url>` (see rs500-discography skill) to actually bring in
    missing albums, then re-run this to link them.
    """
    from mdb_strings import ascii_key

    with managed_db(args.db or DB_PATH) as conn:
        lst = conn.execute('SELECT * FROM canonical_lists WHERE id=?', (args.id,)).fetchone()
        if not lst:
            console.print(f'[red]List not found:[/red] {args.id}')
            return

        artists = conn.execute("SELECT id, name FROM artists WHERE hidden=0").fetchall()
        artist_by_key = {}
        for a in artists:
            artist_by_key.setdefault(ascii_key(a['name']), []).append(a['id'])
        for al in conn.execute('SELECT artist_id, alias FROM artist_aliases').fetchall():
            artist_by_key.setdefault(ascii_key(al['alias']), []).append(al['artist_id'])

        releases_by_artist: dict = {}
        for r in conn.execute("SELECT id, title, primary_artist_id FROM releases WHERE hidden=0").fetchall():
            releases_by_artist.setdefault(r['primary_artist_id'], []).append(r)

        entries = conn.execute(
            'SELECT rank, artist_name, album_title, release_id FROM canonical_list_entries '
            'WHERE list_id=? ORDER BY rank', (args.id,)
        ).fetchall()

        matched = 0
        now = int(time.time())
        for e in entries:
            if e['release_id'] and not args.force:
                continue
            a_key = ascii_key(e['artist_name'])
            # Strip a leading "the " for matching only (RS500-style CSVs
            # frequently drop it: "Beatles" vs DB's "The Beatles").
            a_key_stripped = re.sub(r'^the\s+', '', a_key)
            candidate_ids = artist_by_key.get(a_key) or artist_by_key.get(a_key_stripped) or []
            if not candidate_ids:
                for k, ids in artist_by_key.items():
                    if re.sub(r'^the\s+', '', k) == a_key_stripped:
                        candidate_ids = ids
                        break
            if not candidate_ids:
                continue

            t_key = ascii_key(e['album_title'])
            all_rels = []
            for aid in candidate_ids:
                all_rels.extend(releases_by_artist.get(aid, []))

            # Exact-key match always wins outright — checked across ALL of the
            # artist's releases before any fallback, so "Led Zeppelin" (list)
            # can't accidentally bind to "Led Zeppelin II" just because that
            # release happened to be examined first by a substring check.
            exact = [r['id'] for r in all_rels if ascii_key(r['title']) == t_key]
            if len(exact) == 1:
                found_rid = exact[0]
            elif len(exact) > 1:
                # genuine ambiguity (e.g. two editions with the same title) — leave for manual review
                found_rid = None
            else:
                # Substring fallback only for genuinely unambiguous cases —
                # e.g. list says "Led Zeppelin IV", DB has the bracketed
                # "[Led Zeppelin IV]", or list says "Abbey Road (2019 Mix)"
                # and DB just has "Abbey Road". Numbered/sequel titles ("Led
                # Zeppelin" vs "Led Zeppelin II") must NOT satisfy this: guard
                # by rejecting only when the extra text IS (in full) nothing
                # but a sequel marker — a bare roman/arabic numeral, optionally
                # preceded by "part"/"vol(ume)". A longer descriptive phrase
                # that happens to contain a digit ("2012 remaster", "36
                # chambers", "20th anniversary") is real decoration, not a
                # sequel number, and must NOT be rejected.
                _SEQUEL_ONLY_RE = re.compile(r'^(part|vol|volume)?\s*(i{1,3}|iv|v|vi{1,3}|ix|x|\d+)$')

                def _is_decoration_only(shorter, longer):
                    extra = longer.replace(shorter, '', 1).strip()
                    return not _SEQUEL_ONLY_RE.match(extra)

                candidates = []
                for r in all_rels:
                    rt_key = ascii_key(r['title'])
                    if t_key == rt_key:
                        # already handled above
                        continue
                    if t_key in rt_key and _is_decoration_only(t_key, rt_key):
                        candidates.append(r['id'])
                    elif rt_key in t_key and _is_decoration_only(rt_key, t_key):
                        candidates.append(r['id'])
                found_rid = candidates[0] if len(candidates) == 1 else None

            if found_rid:
                conn.execute(
                    'UPDATE canonical_list_entries SET release_id=? WHERE list_id=? AND rank=?',
                    (found_rid, args.id, e['rank'])
                )
                matched += 1

        conn.execute('UPDATE canonical_lists SET updated_at=? WHERE id=?', (now, args.id))
        conn.commit()

    console.print(f'[green]✓[/green] matched {matched} new entries')


def cmd_list_status(args):
    """Print completion summary for one or all canonical lists."""
    with managed_db(args.db or DB_PATH) as conn:
        lists = conn.execute('SELECT * FROM canonical_lists' + (' WHERE id=?' if args.id else ''),
                              (args.id,) if args.id else ()).fetchall()
        if not lists:
            console.print('[dim]No canonical lists found.[/dim]' if not args.id else f'[red]List not found:[/red] {args.id}')
            return
        for lst in lists:
            total = conn.execute('SELECT COUNT(*) FROM canonical_list_entries WHERE list_id=?', (lst['id'],)).fetchone()[0]
            matched = conn.execute(
                'SELECT COUNT(*) FROM canonical_list_entries WHERE list_id=? AND release_id IS NOT NULL',
                (lst['id'],)
            ).fetchone()[0]
            heard = conn.execute('''
                SELECT COUNT(*) FROM canonical_list_entries cle
                JOIN releases r ON r.id = cle.release_id
                WHERE cle.list_id=? AND EXISTS (
                    SELECT 1 FROM tracks t JOIN listens l ON l.track_id=t.id
                    WHERE t.release_id = r.id
                )
            ''', (lst['id'],)).fetchone()[0]
            console.print(f"[bold]{lst['name']}[/bold]  [dim]({lst['id']})[/dim]")
            console.print(f"  matched: {matched}/{total}   heard: {heard}/{total}")


def cmd_genre_relations(args):
    """Populate genre_relations from a tab-indented tree file."""
    import os
    tree_path = args.tree or os.path.join(os.path.expanduser('~'), 'genre_tree.txt')
    if not os.path.exists(tree_path):
        console.print(f'[red]Tree file not found: {tree_path}[/red]')
        return
    with managed_db(args.db or DB_PATH) as conn:
        # Clear existing relations so a re-run is idempotent
        conn.execute('DELETE FROM genre_relations')
        conn.commit()
        inserted, skipped = populate_genre_relations(conn, tree_path)
    console.print(f'  [green]✓ {inserted} genre relations inserted[/green]'
                  f'  [dim]({skipped} tree entries not in DB)[/dim]')


def cmd_genre_relations_sync(args):
    """Rebuild genre_relations from AOTY's live Parent/Child Genres sidebar.

    genre_tree.txt is a stale manual transcription that's missing real AOTY
    parent genres (e.g. Mambo is filed under Latin American Music, Regional,
    and Spanish Caribbean Music on AOTY — the tree file only had "Regional").
    This walks every genre in the local DB, scrapes its actual AOTY page, and
    inserts any newly-discovered parent/child genre stubs plus the relations
    between them, so the tree matches AOTY's own multi-parent taxonomy.
    """
    limit = getattr(args, 'limit', None)
    with managed_db(args.db or DB_PATH) as conn:
        genres = conn.execute('SELECT aoty_id, name, slug FROM genres ORDER BY aoty_id').fetchall()
        if limit:
            genres = genres[:limit]

        known_ids = {r['aoty_id'] for r in genres}
        # id -> (name, slug)
        new_genres = {}
        # (parent_id, child_id)
        relations = set()
        errors = 0

        console.print(f'[dim]Scraping {len(genres)} AOTY genre pages...[/dim]\n')
        to_scrape = [(g['aoty_id'], g['name'], g['slug']) for g in genres]
        scraped_ids = set()

        while to_scrape:
            gid, gname, gslug = to_scrape.pop()
            if gid in scraped_ids:
                continue
            scraped_ids.add(gid)
            if len(scraped_ids) % 25 == 0:
                console.print(f'[dim]  {len(scraped_ids)} genre pages scraped so far...[/dim]')
            try:
                rel = scrape_aoty_genre_relations(gid, gslug)
            except Exception as e:
                log.warning('Genre relations scrape failed for %s (%s): %s', gname, gid, e)
                errors += 1
                continue

            for pid, pname, pslug in rel['parents']:
                if pid not in known_ids and pid not in new_genres:
                    new_genres[pid] = (pname, pslug)
                    # walk up to a connected root
                    to_scrape.append((pid, pname, pslug))
                relations.add((pid, gid))
            for cid, cname, cslug in rel['children']:
                if cid not in known_ids and cid not in new_genres:
                    new_genres[cid] = (cname, cslug)
                relations.add((gid, cid))

        for gid, (name, slug) in new_genres.items():
            conn.execute(
                'INSERT OR IGNORE INTO genres (aoty_id, name, slug) VALUES (?, ?, ?)',
                (gid, name, slug),
            )
        conn.execute('DELETE FROM genre_relations')
        for pid, cid in relations:
            conn.execute(
                'INSERT OR IGNORE INTO genre_relations (parent_aoty_id, child_aoty_id) VALUES (?, ?)',
                (pid, cid),
            )
        conn.commit()

    console.print(
        f'  [green]✓ {len(relations)} genre relations[/green]  '
        f'[dim]({len(new_genres)} new genre stubs inserted, {len(scraped_ids)} pages scraped, {errors} failed)[/dim]'
    )
    console.print('  Run [bold]./generate_genre_tree.py[/bold] next to refresh genre-tree.js')


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
    # blue-indigo (was 205, too close to Ambient)
    'Experimental':         (233, 38, 52),
    'Punk':                 (160, 60, 50),
    'Classical':            (283, 50, 57),
    'Ambient':              (207, 58, 59),
    'Dance':                (178, 67, 50),
    # boosted S to separate from Jazz/Rock
    'Funk':                 ( 28, 82, 55),
    # yellow-green/pastoral (was 38, too close to Jazz)
    'Country':              ( 68, 62, 50),
    # muted warm (was 30,64,55 — too close to Marching Band)
    'Singer-Songwriter':    ( 35, 50, 63),
    'Psychedelia':          (292, 53, 58),
    'Industrial':           (216, 28, 49),
    'Reggae':               (145, 57, 48),
    'Blues':                (223, 58, 54),
    'Darkwave':             (248, 45, 44),
    'Spoken Word':          (200, 18, 60),
    # magenta (was 295, too close to Psychedelia)
    'Glitch Pop':           (305, 68, 60),
    'Hypnagogic Pop':       (310, 60, 63),
    # periwinkle (was 200, too close to Ambient/Spoken Word)
    'Ambient Pop':          (217, 44, 68),
    'Sampledelia':          (188, 55, 54),
    # indigo (was 278, too close to Classical)
    'Mashup':               (240, 52, 58),
    # pastel magenta/lilac (was 228, too close to Blues)
    'Vapor':                (300, 50, 70),
    'Field Recordings':     ( 28, 30, 48),
    # pastel teal (was 52, too close to warm cluster)
    'Easy Listening':       (182, 42, 67),
    'New Age':              (168, 40, 59),
    'Gospel':               ( 50, 60, 57),
    # soft lavender (was 48, too close to warm cluster)
    'CCM':                  (265, 45, 67),
    'Ska':                  (132, 52, 50),
    # wine-dark red (was 356, identical to Christmas)
    'Flamenco':             (348, 72, 44),
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
    # holly green (was 355, identical to Flamenco)
    'Christmas':            (128, 65, 46),
    # very soft blue-gray (was 170, too close to New Age)
    'ASMR':                 (190, 28, 68),
    'Musical Parody':       ( 60, 48, 62),
    'Musical Theatre & Entertainment': (45, 55, 63),
}
# fallback gray-blue for unmapped roots
_DEFAULT_HSL = (210, 20, 60)


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

    Reads the tree file rather than genre_relations: that table only holds
    relations where both endpoints are genres we stock, so it loses the
    intermediate links needed to walk a subgenre up to its real root.
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
        # max depth guard against cycles
        for _ in range(25):
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


GENRE_TREE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'genre_tree.txt')


def cmd_genres_refresh(args):
    """Recompute monthly genre profiles into monthly_genre_profile.

    Feeds the Taste Over Time heatmap on the home page. Only whole months are
    written: the current month is still accruing listens, so its blend would
    shift every time this ran.
    """
    tree_path = args.tree or GENRE_TREE_PATH
    if not os.path.exists(tree_path):
        console.print(f'[red]Genre tree not found: {tree_path}[/red]')
        sys.exit(1)

    now = datetime.now()
    with managed_db(args.db or DB_PATH) as conn:
        genre_root_map = _build_genre_root_map(tree_path)

        months = [
            (y, m) for y, m in conn.execute(
                'SELECT DISTINCT year, month FROM listens ORDER BY year, month'
            )
            if (y, m) < (now.year, now.month)
        ]
        console.print(f'  [dim]{len(months)} complete months '
                      f'(excluding {now.year}-{now.month:02d})[/dim]')

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

            # Each listen contributes 1.0, split across its genres, then split
            # again up the tree so a subgenre credits its root families.
            root_weights: dict[str, float] = {}
            for genres in listen_genres.values():
                genre_wt = 1.0 / len(genres)
                for gname in genres:
                    for root, rw in genre_root_map.get(gname, {gname: 1.0}).items():
                        root_weights[root] = root_weights.get(root, 0.0) + genre_wt * rw

            color, top = _blend_genres(root_weights)
            dominant   = top[0]['genre'] if top else None
            top_color  = _hsl_to_hex(*_TOP_GENRE_HSL.get(dominant, _DEFAULT_HSL)) if dominant else '#64748B'
            rows.append((year, month, listen_count, color, top_color, dominant,
                         json.dumps(top) if top else None))

        conn.execute('DELETE FROM monthly_genre_profile')
        conn.executemany(
            'INSERT INTO monthly_genre_profile '
            '(year, month, listen_count, color_hex, top_genre_color_hex, dominant_genre, genres_json) '
            'VALUES (?, ?, ?, ?, ?, ?, ?)',
            rows,
        )
    span = f'{rows[0][0]}-{rows[0][1]:02d} to {rows[-1][0]}-{rows[-1][1]:02d}' if rows else 'none'
    console.print(f'  [green]{len(rows)} months cached[/green]  [dim]{span}[/dim]')


def cmd_certs_refresh(args):
    """Recompute cert tiers for all artists based on all-time listen counts."""
    with managed_db(args.db or DB_PATH) as conn:
        # Counts main-artist plays via track_artists (per-track credit), not
        # release_artists (per-release credit) — an artist can be the main
        # credit on individual tracks (remixes, compilation cuts, features)
        # within a release primarily credited to someone else. stats.js and
        # artist.js count this way too;
        # release_artists undercounts artists like The Bloody Beetroots
        # (625 track-level plays vs. 447 release-level), misclassifying them
        # a tier low.
        rows = conn.execute('''
            SELECT a.id, a.name, COUNT(l.id) AS total
            FROM   artists a
            JOIN   track_artists ta ON ta.artist_id = a.id AND ta.role = 'main'
            JOIN   tracks t         ON t.id = ta.track_id AND (t.hidden IS NULL OR t.hidden = 0)
            JOIN   listens l        ON l.track_id = t.id
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


# ── cmd: stats refresh ───────────────────────────────────────────────────────
# Precomputes everything music/views/stats.js needs into `stats_cache`, plus
# the per-artist year-medal ranking artist.js needs into `artist_year_medals`.
# Replaces ~20 live aggregate queries per page load (~12s of blocked JS/WASM)
# with flat SELECTs against pre-baked rows.
# Every SQL query here is a direct port of the matching stats.js section —
# keep them in sync if a section's logic changes.

def _drill_artists(conn, extra_where, params=()):
    rows = conn.execute(f'''
        SELECT a.id, a.name, COALESCE(a.image_thumb_url, a.image_url) as image_url,
               COUNT(l.id) as total_listens, a.slug
        FROM artists a
        JOIN track_artists ta ON a.id = ta.artist_id AND ta.role = 'main'
        JOIN tracks t ON ta.track_id = t.id AND t.hidden = 0
        JOIN listens l ON t.id = l.track_id
        WHERE a.hidden = 0 {extra_where}
        GROUP BY a.id
        ORDER BY total_listens DESC
        LIMIT 4
    ''', params).fetchall()
    return [[r['id'], r['name'], r['image_url'], r['total_listens'], r['slug']] for r in rows]


def _drill_albums(conn, extra_where, params=()):
    rows = conn.execute(f'''
        SELECT r.id, r.title, COALESCE(r.album_art_thumb_url, r.album_art_url) as art_url,
               COUNT(l.id) as total_listens, r.slug
        FROM releases r
        JOIN tracks t ON t.release_id = r.id AND t.hidden = 0
        JOIN listens l ON l.track_id = t.id
        LEFT JOIN artists a ON a.id = r.primary_artist_id
        WHERE r.hidden = 0 AND (a.id IS NULL OR a.hidden = 0) {extra_where}
        GROUP BY r.id
        ORDER BY total_listens DESC
        LIMIT 4
    ''', params).fetchall()
    return [[r['id'], r['title'], r['art_url'], r['total_listens'], r['slug']] for r in rows]


def _drill(conn, kind, extra_where, params, n):
    """Skip the query entirely for a zero-listen row — trivially no top-4."""
    if not n:
        return []
    return _drill_artists(conn, extra_where, params) if kind == 'artist' \
        else _drill_albums(conn, extra_where, params)


def _breakdown_section(conn, rows, kind, where_fn):
    """rows: [(group_value, label, n), ...]. Returns the enriched list of
    {label, n, drill: [...]} dicts stats.js's _breakdownRows/_coloredRows
    expect, computing each row's drill-down eagerly."""
    return [
        {'label': label, 'n': n, 'drill': _drill(conn, kind, *where_fn(group_value), n)}
        for group_value, label, n in rows
    ]


def _stats_language(conn, cache, vlog):
    t0 = time.perf_counter()
    lang_rows = conn.execute('''
        SELECT t.language, COUNT(l.id) n
        FROM listens l JOIN tracks t ON l.track_id = t.id
        WHERE t.language IS NOT NULL
        GROUP BY t.language ORDER BY n DESC LIMIT 12
    ''').fetchall()
    cache['language'] = _breakdown_section(
        conn, [(r['language'], r['language'] or '?', r['n']) for r in lang_rows],
        'release', lambda lang: (' AND t.language = ?', (lang,)))
    vlog('language', cache['language'], t0)


def _stats_gender(conn, cache, vlog):
    t0 = time.perf_counter()
    gender_labels = {'Male': 'Male', 'Female': 'Female', 'Non-binary': 'Non-binary', 'Other': 'Other'}
    gender_rows = conn.execute('''
        SELECT a.gender, COUNT(l.id) n
        FROM listens l JOIN tracks t ON l.track_id = t.id
        JOIN track_artists ta ON ta.track_id = t.id AND ta.role = 'main'
        JOIN artists a ON a.id = ta.artist_id
        WHERE a.type = 'Person'
        GROUP BY a.gender ORDER BY n DESC
    ''').fetchall()
    cache['gender'] = _breakdown_section(
        conn, [(r['gender'], gender_labels.get(r['gender'], 'Unknown'), r['n']) for r in gender_rows],
        'artist', lambda g: (" AND a.type = 'Person' AND a.gender = ?", (g,)))
    vlog('gender', cache['gender'], t0)


def _stats_artist_type(conn, cache, vlog):
    t0 = time.perf_counter()
    artist_types = ['Person', 'Group', 'Orchestra', 'Choir', 'Character', 'Other']
    type_counts = dict(conn.execute('''
        SELECT a.type, COUNT(l.id) n
        FROM listens l JOIN tracks t ON l.track_id = t.id
        JOIN track_artists ta ON ta.track_id = t.id AND ta.role = 'main'
        JOIN artists a ON a.id = ta.artist_id
        WHERE a.type IS NOT NULL AND a.type != ''
        GROUP BY a.type
    ''').fetchall())
    all_types = {r[0] for r in conn.execute(
        "SELECT DISTINCT type FROM artists WHERE type IS NOT NULL AND type != ''")}
    cache['artistType'] = _breakdown_section(
        conn, [(t, t, type_counts.get(t, 0)) for t in artist_types if t in all_types],
        'artist', lambda t: (' AND a.type = ?', (t,)))
    vlog('artistType', cache['artistType'], t0)


def _stats_era(conn, cache, vlog):
    """Bucketed by release_year decade, not artist formed_year — formed_year
    is closer to a solo artist's birth year in MusicBrainz."""
    t0 = time.perf_counter()
    era_rows = conn.execute('''
        SELECT (r.release_year / 10) * 10 as decade, COUNT(l.id) n
        FROM listens l JOIN tracks t ON l.track_id = t.id
        JOIN releases r ON r.id = t.release_id
        WHERE r.release_year IS NOT NULL AND r.hidden = 0
        GROUP BY decade ORDER BY decade
    ''').fetchall()
    cache['era'] = _breakdown_section(
        conn, [(r['decade'], f"{r['decade']}s", r['n']) for r in era_rows],
        'release', lambda decade: (' AND (r.release_year / 10) * 10 = ?', (decade,)))
    vlog('era', cache['era'], t0)


def _stats_country(conn, cache, vlog):
    t0 = time.perf_counter()
    country_rows = conn.execute('''
        SELECT a.country, COUNT(l.id) n
        FROM listens l JOIN tracks t ON l.track_id = t.id
        JOIN track_artists ta ON ta.track_id = t.id AND ta.role = 'main'
        JOIN artists a ON a.id = ta.artist_id
        WHERE a.country IS NOT NULL AND a.country != ''
        GROUP BY a.country ORDER BY n DESC LIMIT 12
    ''').fetchall()
    cache['country'] = _breakdown_section(
        conn, [(r['country'], r['country'], r['n']) for r in country_rows],
        'artist', lambda code: (' AND a.country = ?', (code,)))
    vlog('country', cache['country'], t0)


def _stats_release_type(conn, cache, vlog):
    t0 = time.perf_counter()
    type_rows = conn.execute('''
        SELECT r.type, COUNT(l.id) n
        FROM listens l JOIN tracks t ON l.track_id = t.id
        JOIN releases r ON r.id = t.release_id
        WHERE r.type IS NOT NULL AND r.type != '' AND r.hidden = 0
        GROUP BY r.type ORDER BY n DESC
    ''').fetchall()
    cache['releaseType'] = _breakdown_section(
        conn, [(r['type'], r['type'].capitalize(), r['n']) for r in type_rows],
        'release', lambda t: (' AND r.type = ?', (t,)))
    vlog('releaseType', cache['releaseType'], t0)


def _stats_recency(conn, cache, vlog):
    t0 = time.perf_counter()
    recency_buckets = [
        ('Pre-release',    lambda g: g < 0,               ' AND r.release_year IS NOT NULL AND (l.year - r.release_year) < 0'),
        ('Same year',      lambda g: g == 0,              ' AND r.release_year IS NOT NULL AND (l.year - r.release_year) = 0'),
        ('1-2 years old',  lambda g: 1 <= g <= 2,         ' AND r.release_year IS NOT NULL AND (l.year - r.release_year) BETWEEN 1 AND 2'),
        ('3-5 years old',  lambda g: 3 <= g <= 5,         ' AND r.release_year IS NOT NULL AND (l.year - r.release_year) BETWEEN 3 AND 5'),
        ('6-10 years old', lambda g: 6 <= g <= 10,        ' AND r.release_year IS NOT NULL AND (l.year - r.release_year) BETWEEN 6 AND 10'),
        ('10+ years old',  lambda g: g > 10,              ' AND r.release_year IS NOT NULL AND (l.year - r.release_year) > 10'),
    ]
    gap_rows = conn.execute('''
        SELECT (l.year - r.release_year) as gap, COUNT(l.id) n
        FROM listens l JOIN tracks t ON l.track_id = t.id
        JOIN releases r ON r.id = t.release_id
        WHERE r.release_year IS NOT NULL AND r.hidden = 0
        GROUP BY gap
    ''').fetchall()
    bucket_totals = {label: 0 for label, _, _ in recency_buckets}
    for r in gap_rows:
        for label, test, _ in recency_buckets:
            if test(r['gap']):
                bucket_totals[label] += r['n']
                break
    where_by_label = {label: where for label, _, where in recency_buckets}
    cache['recency'] = _breakdown_section(
        conn, [(label, label, n) for label, n in bucket_totals.items() if n > 0],
        'release', lambda label: (where_by_label[label], ()))
    vlog('recency', cache['recency'], t0)


def _stats_explicit(conn, cache, vlog):
    t0 = time.perf_counter()
    explicit_rows = sorted(conn.execute('''
        SELECT t.is_explicit, COUNT(l.id) n
        FROM listens l JOIN tracks t ON l.track_id = t.id
        WHERE t.is_explicit IS NOT NULL
        GROUP BY t.is_explicit
    ''').fetchall(), key=lambda r: -r['is_explicit'])
    cache['explicit'] = _breakdown_section(
        conn, [(r['is_explicit'], 'Explicit' if r['is_explicit'] else 'Clean', r['n']) for r in explicit_rows],
        'artist', lambda is_explicit: (' AND t.is_explicit = ?', (is_explicit,)))
    vlog('explicit', cache['explicit'], t0)


def _stats_popularity(conn, cache, vlog):
    t0 = time.perf_counter()
    popularity_tiers = {
        'Mainstream (70+)': ' AND a.spotify_popularity >= 70',
        'Mid-tier (40-69)': ' AND a.spotify_popularity >= 40 AND a.spotify_popularity < 70',
        'Deep cuts (<40)':  ' AND a.spotify_popularity < 40',
    }
    pop_rows = conn.execute('''
        SELECT
          CASE WHEN a.spotify_popularity >= 70 THEN 'Mainstream (70+)'
               WHEN a.spotify_popularity >= 40 THEN 'Mid-tier (40-69)'
               ELSE 'Deep cuts (<40)' END as tier,
          COUNT(l.id) n
        FROM listens l JOIN tracks t ON l.track_id = t.id
        JOIN track_artists ta ON ta.track_id = t.id AND ta.role = 'main'
        JOIN artists a ON a.id = ta.artist_id
        WHERE a.spotify_popularity IS NOT NULL
        GROUP BY tier ORDER BY n DESC
    ''').fetchall()
    cache['popularity'] = _breakdown_section(
        conn, [(r['tier'], r['tier'], r['n']) for r in pop_rows],
        'artist', lambda tier: (' AND a.spotify_popularity IS NOT NULL' + popularity_tiers[tier], ()))
    vlog('popularity', cache['popularity'], t0)


def _stats_labels(conn, cache, vlog):
    """No drill-down — a label already implies its own releases, drilling
    in would just echo the section itself."""
    t0 = time.perf_counter()
    label_rows = conn.execute('''
        SELECT r.label, COUNT(l.id) n
        FROM listens l JOIN tracks t ON l.track_id = t.id
        JOIN releases r ON r.id = t.release_id
        WHERE r.label IS NOT NULL AND r.label != '' AND r.hidden = 0
        GROUP BY r.label ORDER BY n DESC LIMIT 12
    ''').fetchall()
    cache['labels'] = [{'label': r['label'], 'n': r['n']} for r in label_rows]
    vlog('labels', cache['labels'], t0)


def _stats_completion(conn, cache, vlog):
    """Grouped by same_song_key() so hearing any one edit/length variant
    (radio edit, extended mix, original mix, ...) of a track counts as
    having heard that song — a named remix stays its own distinct slot."""
    t0 = time.perf_counter()
    raw_rows = conn.execute('''
        SELECT r.id, r.title, COALESCE(r.album_art_thumb_url, r.album_art_url) as art_url,
               t.title as track_title,
               EXISTS (SELECT 1 FROM listens l WHERE l.track_id = t.id) as heard,
               (SELECT COUNT(*) FROM listens l WHERE l.track_id = t.id) as track_listens,
               r.slug
        FROM releases r
        JOIN tracks t ON t.release_id = r.id
        LEFT JOIN artists a ON a.id = r.primary_artist_id
        WHERE r.hidden = 0 AND (a.id IS NULL OR a.hidden = 0)
          AND t.hidden = 0 AND t.variant_section IS NULL
          AND (t.duration_ms IS NULL OR t.duration_ms >= 30000)
    ''').fetchall()
    by_release: dict = {}
    for r in raw_rows:
        entry = by_release.setdefault(
            r['id'], {'title': r['title'], 'art_url': r['art_url'], 'slug': r['slug'],
                      'groups': {}, 'total_listens': 0})
        key = same_song_key(r['track_title'])
        entry['groups'][key] = entry['groups'].get(key, False) or bool(r['heard'])
        entry['total_listens'] += r['track_listens']

    completion = []
    for rid, entry in by_release.items():
        total = len(entry['groups'])
        heard = sum(1 for v in entry['groups'].values() if v)
        if heard > 0 and heard < total and entry['total_listens'] > 0:
            completion.append({
                'id': rid, 'title': entry['title'], 'total': total, 'slug': entry['slug'],
                'heard': heard, 'listens': entry['total_listens'],
            })
    completion.sort(key=lambda c: c['listens'], reverse=True)
    cache['completion'] = completion[:8]
    vlog('completion', cache['completion'], t0)


def _stats_relistened(conn, cache, vlog):
    """One row per release, so a single album doesn't crowd out the rest
    of the list."""
    t0 = time.perf_counter()
    relistened_rows = conn.execute('''
        SELECT id, title, artist_name, art_url, release_id, total_listens, release_slug FROM (
            SELECT t.id, t.title, a.name as artist_name,
                   COALESCE(r.album_art_thumb_url, r.album_art_url) as art_url,
                   r.id as release_id, COUNT(l.id) as total_listens, r.slug as release_slug,
                   ROW_NUMBER() OVER (
                       PARTITION BY t.release_id ORDER BY COUNT(l.id) DESC
                   ) as release_rank
            FROM tracks t
            LEFT JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
            LEFT JOIN artists a ON ta.artist_id = a.id
            LEFT JOIN releases r ON t.release_id = r.id
            JOIN listens l ON t.id = l.track_id
            WHERE t.hidden = 0 AND (r.id IS NULL OR r.hidden = 0)
            GROUP BY t.id
        )
        WHERE release_rank = 1
        ORDER BY total_listens DESC LIMIT 8
    ''').fetchall()
    cache['relistened'] = [
        {'id': r['id'], 'title': r['title'], 'artist': r['artist_name'], 'art_url': r['art_url'],
         'release_id': r['release_id'], 'n': r['total_listens'], 'release_slug': r['release_slug']}
        for r in relistened_rows
    ]
    vlog('relistened', cache['relistened'], t0)


def _stats_vinyl(conn, cache, vlog):
    t0 = time.perf_counter()
    owned = conn.execute('''
        SELECT COUNT(*) FROM listens l JOIN tracks t ON l.track_id = t.id
        JOIN collection_items ci ON ci.release_id = t.release_id
    ''').fetchone()[0]
    total_listens_all = conn.execute(
        'SELECT COUNT(*) FROM listens l JOIN tracks t ON l.track_id = t.id').fetchone()[0]
    cache['vinyl'] = {'owned': owned, 'total': total_listens_all}
    vlog('vinyl', cache['vinyl'], t0)


def _stats_certified(conn, cache, vlog):
    t0 = time.perf_counter()
    cert_order = {'diamond': 0, 'platinum': 1, 'gold': 2}
    cert_rows = sorted(
        conn.execute("SELECT id, name, cert, slug FROM artists WHERE cert IS NOT NULL").fetchall(),
        key=lambda r: cert_order.get(r['cert'], 99))
    cache['cert'] = [{'id': r['id'], 'name': r['name'], 'cert': r['cert'], 'slug': r['slug']} for r in cert_rows]
    vlog('cert', cache['cert'], t0)


def _stats_nerd(conn, cache, vlog):
    """Stats for Nerds — currently live in views/home.js."""
    t0 = time.perf_counter()
    days_row = conn.execute('''
        SELECT COUNT(DISTINCT date(l.timestamp, 'unixepoch')) as active_days,
               CAST(julianday(date(MAX(l.timestamp), 'unixepoch'))
                    - julianday(date(MIN(l.timestamp), 'unixepoch')) + 1 AS INTEGER) as total_days,
               COUNT(l.id) as total_listens
        FROM listens l JOIN tracks t ON l.track_id = t.id WHERE t.hidden = 0
    ''').fetchone()
    time_row = conn.execute('''
        SELECT SUM(COALESCE(l.ms_played, t.duration_ms)) as total_ms
        FROM listens l JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
    ''').fetchone()
    peak_row = conn.execute('''
        SELECT strftime('%Y-%m', datetime(l.timestamp, 'unixepoch')) as ym, COUNT(l.id) as cnt
        FROM listens l JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
        GROUP BY ym ORDER BY cnt DESC LIMIT 1
    ''').fetchone()
    ohw_row = conn.execute('''
        WITH artist_counts AS (
            SELECT ta.artist_id, COUNT(l.id) as play_count
            FROM listens l
            JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
            JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
            JOIN artists a ON ta.artist_id = a.id AND a.hidden = 0
            GROUP BY ta.artist_id
        )
        SELECT SUM(CASE WHEN play_count = 1 THEN 1 ELSE 0 END) as ohw, COUNT(*) as total
        FROM artist_counts
    ''').fetchone()
    ey_rows = conn.execute('''
        WITH artist_years AS (
            SELECT ta.artist_id, strftime('%Y', datetime(l.timestamp, 'unixepoch')) AS yr
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
            WHERE t.hidden = 0
            GROUP BY ta.artist_id, yr
        ),
        total_years AS (
            SELECT COUNT(DISTINCT strftime('%Y', datetime(l.timestamp, 'unixepoch'))) AS n
            FROM listens l JOIN tracks t ON l.track_id = t.id WHERE t.hidden = 0
        ),
        every_year AS (
            SELECT artist_id, COUNT(DISTINCT yr) AS yrs FROM artist_years
            GROUP BY artist_id HAVING yrs = (SELECT n FROM total_years)
        )
        SELECT a.id, a.name, a.image_url, a.slug, (SELECT n FROM total_years) AS total_yrs
        FROM every_year ey JOIN artists a ON a.id = ey.artist_id
        WHERE a.hidden = 0 ORDER BY a.name
    ''').fetchall()
    edd_row = conn.execute('''
        WITH track_counts AS (
            SELECT l.track_id, COUNT(l.id) as play_count
            FROM listens l
            JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
            GROUP BY l.track_id
        ),
        ranked AS (
            SELECT play_count, ROW_NUMBER() OVER (ORDER BY play_count DESC) as rank
            FROM track_counts
        )
        SELECT MAX(rank) as eddington FROM ranked WHERE play_count >= rank
    ''').fetchone()
    cutover_row = conn.execute('''
        WITH artist_counts AS (
            SELECT ta.artist_id, COUNT(l.id) as play_count
            FROM listens l
            JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
            JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
            JOIN artists a ON ta.artist_id = a.id AND a.hidden = 0
            GROUP BY ta.artist_id
        ),
        ranked AS (
            SELECT play_count, ROW_NUMBER() OVER (ORDER BY play_count DESC) as rank
            FROM artist_counts
        )
        SELECT MAX(rank) as cutover FROM ranked WHERE play_count >= rank
    ''').fetchone()

    active = days_row['active_days'] or 0
    total_days = days_row['total_days'] or 0
    total_listens_n = days_row['total_listens'] or 0
    total_ms = time_row['total_ms'] or 0
    cache['nerd'] = {
        'active_days': active,
        'total_days': total_days,
        'active_pct': round(active / total_days * 100, 1) if total_days else 0,
        'avg_per_day': round(total_listens_n / active, 1) if active else 0,
        'total_hours': total_ms // 3600000,
        'peak_month': peak_row['ym'] if peak_row else None,
        'peak_month_count': peak_row['cnt'] if peak_row else 0,
        'one_hit_wonders': ohw_row['ohw'] or 0,
        'one_hit_wonders_total': ohw_row['total'] or 0,
        'every_year_artists': [{'id': r['id'], 'name': r['name'], 'img': r['image_url'], 'slug': r['slug']} for r in ey_rows],
        'every_year_total_years': ey_rows[0]['total_yrs'] if ey_rows else 0,
        'eddington': edd_row['eddington'] if edd_row else 0,
        'artist_cutover': cutover_row['cutover'] if cutover_row else 0,
    }
    vlog('nerd', [cache['nerd']], t0)


def _stats_genres_index(conn, cache, vlog):
    """One row per genre with plays > 0, matching genre.js's own per-genre
    queries — precomputed so the index page doesn't run the full
    release_genres/tracks/listens join once per genre on every load.

    track_plays pre-aggregates listens per track ONCE before joining to
    release_genres. Joining raw `listens` rows directly through
    release_genres -> tracks fans out multiplicatively (every listen
    re-matched per genre tag on the release), so pre-aggregating first is
    an order of magnitude cheaper for the same result.
    """
    t0 = time.perf_counter()
    genre_rows = conn.execute('''
        WITH track_plays AS (
            SELECT track_id, COUNT(*) as tp_count FROM listens GROUP BY track_id
        )
        SELECT g.aoty_id, g.name,
               COUNT(DISTINCT rg.release_id) as releases,
               COALESCE(SUM(tp.tp_count), 0) as total_plays
        FROM genres g
        JOIN release_genres rg ON g.aoty_id = rg.aoty_genre_id
        JOIN releases r ON r.id = rg.release_id AND r.hidden = 0
        JOIN tracks t ON rg.release_id = t.release_id AND t.hidden = 0
        LEFT JOIN track_plays tp ON tp.track_id = t.id
        GROUP BY g.aoty_id
        HAVING total_plays > 0
    ''').fetchall()
    # Releases average ~4 genre tags each, so summing each genre's own
    # `plays` double/triple-counts the same listen once per tag — that
    # sum is meaningful per-genre but not as a "total plays" headline
    # number. genresTotalListens is the actual distinct-listen count
    # (each scrobble counted once) for views/genres.js's subtitle.
    genres_total_listens = conn.execute('''
        SELECT COUNT(DISTINCT l.id)
        FROM listens l
        JOIN tracks t ON t.id = l.track_id AND t.hidden = 0
        JOIN release_genres rg ON rg.release_id = t.release_id
    ''').fetchone()[0]
    cache['genresIndex'] = [
        {'id': r['aoty_id'], 'name': r['name'], 'releases': r['releases'], 'plays': r['total_plays']}
        for r in genre_rows
    ]
    cache['genresTotalListens'] = genres_total_listens
    vlog('genresIndex', cache['genresIndex'], t0)


def _stats_canonical_lists(conn, cache, vlog):
    """Each list's full ranked entry set is embedded so the modal can render
    every album (heard or not) client-side with zero further queries —
    this table is small (≤ a few thousand rows per list) so shipping it
    whole is cheaper than round-tripping per click."""
    t0 = time.perf_counter()
    canon_lists = []
    for lst in conn.execute('SELECT * FROM canonical_lists ORDER BY name').fetchall():
        entries = conn.execute('''
            SELECT cle.rank, cle.artist_name, cle.album_title, cle.year, cle.release_id,
                   cle.position_label,
                   r.title as release_title, r.album_art_thumb_url, r.album_art_url,
                   r.primary_artist_id, r.release_year, r.slug as release_slug,
                   EXISTS (
                       SELECT 1 FROM tracks t JOIN listens l ON l.track_id = t.id
                       WHERE t.release_id = r.id
                   ) as heard,
                   (SELECT COUNT(*) FROM tracks t
                    WHERE t.release_id = r.id AND t.hidden = 0 AND t.variant_section IS NULL
                      AND (t.duration_ms IS NULL OR t.duration_ms >= 30000)
                   ) as total_tracks,
                   (SELECT COUNT(DISTINCT t.id) FROM tracks t JOIN listens l ON l.track_id = t.id
                    WHERE t.release_id = r.id AND t.hidden = 0 AND t.variant_section IS NULL
                      AND (t.duration_ms IS NULL OR t.duration_ms >= 30000)
                   ) as listened_tracks
            FROM canonical_list_entries cle
            LEFT JOIN releases r ON r.id = cle.release_id
            WHERE cle.list_id = ?
            ORDER BY cle.rank
        ''', (lst['id'],)).fetchall()
        heard_n = sum(1 for e in entries if e['heard'])
        matched_n = sum(1 for e in entries if e['release_id'])
        # Average per-album completion (tracks heard / tracks total),
        # over matched entries with a known tracklist — a texture stat
        # alongside heard_n, not a replacement for it: one play already
        # counts an album as heard, so this is deliberately a separate,
        # lower number that shows how much of each album beyond the
        # first play has actually landed.
        completions = [
            e['listened_tracks'] / e['total_tracks']
            for e in entries if e['release_id'] and e['total_tracks']
        ]
        avg_completion = round(sum(completions) / len(completions) * 100, 1) if completions else 0
        canon_lists.append({
            'id': lst['id'], 'name': lst['name'], 'short_name': lst['short_name'],
            'source_url': lst['source_url'], 'total': lst['total_count'],
            'heard': heard_n, 'matched': matched_n, 'avg_completion': avg_completion,
            'entries': [{
                'rank': e['rank'], 'artist': e['artist_name'], 'album': e['album_title'],
                'year': e['year'] or e['release_year'], 'release_id': e['release_id'],
                'release_slug': e['release_slug'],
                'position_label': e['position_label'],
                'title': e['release_title'] or e['album_title'],
                'art': e['album_art_thumb_url'] or e['album_art_url'],
                'primary_artist_id': e['primary_artist_id'],
                'heard': bool(e['heard']),
                'total_tracks': e['total_tracks'],
                'listened_tracks': e['listened_tracks'],
            } for e in entries],
        })
    cache['canonicalLists'] = canon_lists
    vlog('canonicalLists', canon_lists, t0)


def _stats_write_cache(conn, cache):
    now = int(time.time())
    conn.execute('DELETE FROM stats_cache')
    conn.executemany(
        'INSERT INTO stats_cache (key, value_json, updated_at) VALUES (?, ?, ?)',
        [(key, json.dumps(value), now) for key, value in cache.items()]
    )


def _refresh_track_stats(conn, vlog):
    """stat_avg_listen_ts, stat_total_plays, stat_first/last_listen_ts, stat_drift_days."""
    t0 = time.perf_counter()
    conn.execute('''
        UPDATE tracks SET stat_avg_listen_ts = NULL, stat_total_plays = NULL,
            stat_first_listen_ts = NULL, stat_last_listen_ts = NULL, stat_drift_days = NULL
    ''')
    track_stat_rows = conn.execute('''
        SELECT l.track_id,
               CAST(AVG(l.timestamp) AS INTEGER) as avg_ts,
               COUNT(*) as total_plays,
               MIN(l.timestamp) as first_ts,
               MAX(l.timestamp) as last_ts
        FROM listens l
        JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
        GROUP BY l.track_id
    ''').fetchall()
    conn.executemany(
        'UPDATE tracks SET stat_avg_listen_ts = ?, stat_total_plays = ?,'
        ' stat_first_listen_ts = ?, stat_last_listen_ts = ? WHERE id = ?',
        [(r['avg_ts'], r['total_plays'], r['first_ts'], r['last_ts'], r['track_id'])
         for r in track_stat_rows]
    )
    track_drift_rows = conn.execute('''
        SELECT track_id, AVG((ts - prev_ts) / 86400.0) as drift_days FROM (
            SELECT l.track_id, l.timestamp as ts,
                   LAG(l.timestamp) OVER (PARTITION BY l.track_id ORDER BY l.timestamp) as prev_ts
            FROM listens l
            JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
        )
        WHERE prev_ts IS NOT NULL
        GROUP BY track_id
        HAVING COUNT(*) >= 2
    ''').fetchall()
    conn.executemany(
        'UPDATE tracks SET stat_drift_days = ? WHERE id = ?',
        [(r['drift_days'], r['track_id']) for r in track_drift_rows]
    )
    vlog('track_stats', track_stat_rows, t0)


def _refresh_release_stats(conn, vlog):
    """stat_avg_listen_ts, stat_tracks_heard, stat_total_plays,
    stat_album_total_ms, stat_first/last_listen_ts, stat_drift_days."""
    t0 = time.perf_counter()
    conn.execute('''
        UPDATE releases SET stat_avg_listen_ts = NULL, stat_tracks_heard = NULL,
            stat_total_plays = NULL, stat_album_total_ms = NULL,
            stat_first_listen_ts = NULL, stat_last_listen_ts = NULL, stat_drift_days = NULL
    ''')
    release_stat_rows = conn.execute('''
        SELECT t.release_id,
               CAST(AVG(l.timestamp) AS INTEGER) as avg_ts,
               COUNT(DISTINCT t.id) as tracks_heard,
               COUNT(*) as total_plays,
               MIN(l.timestamp) as first_ts,
               MAX(l.timestamp) as last_ts
        FROM listens l
        JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
        JOIN releases r ON r.id = t.release_id AND r.hidden = 0
        GROUP BY t.release_id
    ''').fetchall()
    conn.executemany(
        'UPDATE releases SET stat_avg_listen_ts = ?, stat_tracks_heard = ?, stat_total_plays = ?,'
        ' stat_first_listen_ts = ?, stat_last_listen_ts = ? WHERE id = ?',
        [(r['avg_ts'], r['tracks_heard'], r['total_plays'], r['first_ts'], r['last_ts'], r['release_id'])
         for r in release_stat_rows]
    )
    album_ms_rows = conn.execute('''
        SELECT release_id, CAST(SUM(COALESCE(duration_ms, 0)) AS INTEGER) as total_ms
        FROM tracks WHERE hidden = 0 AND variant_section IS NULL
        GROUP BY release_id
    ''').fetchall()
    conn.executemany(
        'UPDATE releases SET stat_album_total_ms = ? WHERE id = ?',
        [(r['total_ms'], r['release_id']) for r in album_ms_rows]
    )
    release_drift_rows = conn.execute('''
        SELECT release_id, AVG((ts - prev_ts) / 86400.0) as drift_days FROM (
            SELECT t.release_id, l.timestamp as ts,
                   LAG(l.timestamp) OVER (PARTITION BY t.release_id ORDER BY l.timestamp) as prev_ts
            FROM listens l
            JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
            JOIN releases r ON r.id = t.release_id AND r.hidden = 0
        )
        WHERE prev_ts IS NOT NULL
        GROUP BY release_id
        HAVING COUNT(*) >= 2
    ''').fetchall()
    conn.executemany(
        'UPDATE releases SET stat_drift_days = ? WHERE id = ?',
        [(r['drift_days'], r['release_id']) for r in release_drift_rows]
    )
    vlog('release_stats', release_stat_rows, t0)


def _refresh_artist_stats(conn, vlog):
    """stat_avg_listen_ts, stat_unique_tracks, stat_total_plays,
    stat_total_releases, stat_first/last_listen_ts, stat_drift_days."""
    t0 = time.perf_counter()
    conn.execute('''
        UPDATE artists SET stat_avg_listen_ts = NULL, stat_unique_tracks = NULL,
            stat_total_plays = NULL, stat_total_releases = NULL,
            stat_first_listen_ts = NULL, stat_last_listen_ts = NULL, stat_drift_days = NULL
    ''')
    artist_stat_rows = conn.execute('''
        SELECT ta.artist_id,
               CAST(AVG(l.timestamp) AS INTEGER) as avg_ts,
               COUNT(*) as total_plays,
               MIN(l.timestamp) as first_ts,
               MAX(l.timestamp) as last_ts
        FROM listens l
        JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
        JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
        GROUP BY ta.artist_id
    ''').fetchall()
    conn.executemany(
        'UPDATE artists SET stat_avg_listen_ts = ?, stat_total_plays = ?,'
        ' stat_first_listen_ts = ?, stat_last_listen_ts = ? WHERE id = ?',
        [(r['avg_ts'], r['total_plays'], r['first_ts'], r['last_ts'], r['artist_id'])
         for r in artist_stat_rows]
    )
    # Catalog size (unique_tracks/total_releases) is independent of whether a
    # track has ever been listened to — matches views/artist.js's original
    # LEFT JOIN semantics (a track by this artist counts even with 0 plays).
    artist_catalog_rows = conn.execute('''
        SELECT ta.artist_id,
               COUNT(DISTINCT t.id) as unique_tracks,
               COUNT(DISTINCT t.release_id) as total_releases
        FROM track_artists ta
        JOIN tracks t ON t.id = ta.track_id AND t.hidden = 0 AND ta.role = 'main'
        GROUP BY ta.artist_id
    ''').fetchall()
    conn.executemany(
        'UPDATE artists SET stat_unique_tracks = ?, stat_total_releases = ? WHERE id = ?',
        [(r['unique_tracks'], r['total_releases'], r['artist_id']) for r in artist_catalog_rows]
    )
    artist_drift_rows = conn.execute('''
        SELECT artist_id, AVG((ts - prev_ts) / 86400.0) as drift_days FROM (
            SELECT ta.artist_id, l.timestamp as ts,
                   LAG(l.timestamp) OVER (PARTITION BY ta.artist_id ORDER BY l.timestamp) as prev_ts
            FROM listens l
            JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
            JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
        )
        WHERE prev_ts IS NOT NULL
        GROUP BY artist_id
        HAVING COUNT(*) >= 2
    ''').fetchall()
    conn.executemany(
        'UPDATE artists SET stat_drift_days = ? WHERE id = ?',
        [(r['drift_days'], r['artist_id']) for r in artist_drift_rows]
    )
    vlog('artist_stats', artist_stat_rows, t0)


def _refresh_year_medals(conn, vlog):
    """Artist year-medal ranking for artist.js — top 3 artists by play count
    per year, ties sharing a rank."""
    t0 = time.perf_counter()
    medal_rows = conn.execute('''
        SELECT ta.artist_id, l.year, COUNT(*) as plays
        FROM listens l
        JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
        JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
        GROUP BY ta.artist_id, l.year
    ''').fetchall()
    by_year = {}
    for r in medal_rows:
        by_year.setdefault(r['year'], []).append((r['artist_id'], r['plays']))
    conn.execute('DELETE FROM artist_year_medals')
    medal_inserts = []
    for year, artist_plays in by_year.items():
        ranked = sorted(artist_plays, key=lambda ap: -ap[1])
        rank = 0
        prev_plays = None
        for i, (artist_id, plays) in enumerate(ranked):
            if plays != prev_plays:
                rank = i + 1
                prev_plays = plays
            if rank > 3:
                break
            medal_inserts.append((artist_id, year, rank, plays))
    conn.executemany(
        'INSERT INTO artist_year_medals (artist_id, year, rank, plays) VALUES (?, ?, ?, ?)',
        medal_inserts
    )
    vlog('year_medals', medal_inserts, t0)
    return medal_inserts


def cmd_stats_refresh(args):
    """Precompute every stats.js section + artist.js's year-medal ranking."""
    verbose = getattr(args, 'verbose', False)

    def _vlog(label, value, t0):
        if not verbose:
            return
        n = len(value) if isinstance(value, list) else 1
        console.print(f'  [dim]✓[/dim] {label:<14} {n:>5}   [dim]{time.perf_counter() - t0:.2f}s[/dim]')

    t_total = time.perf_counter()
    with managed_db(args.db or DB_PATH) as conn:
        cache = {}
        if verbose:
            console.print('[bold]Refreshing stats cache...[/bold]')

        _stats_language(conn, cache, _vlog)
        _stats_gender(conn, cache, _vlog)
        _stats_artist_type(conn, cache, _vlog)
        _stats_era(conn, cache, _vlog)
        _stats_country(conn, cache, _vlog)
        _stats_release_type(conn, cache, _vlog)
        _stats_recency(conn, cache, _vlog)
        _stats_explicit(conn, cache, _vlog)
        _stats_popularity(conn, cache, _vlog)
        _stats_labels(conn, cache, _vlog)
        _stats_completion(conn, cache, _vlog)
        _stats_relistened(conn, cache, _vlog)
        _stats_vinyl(conn, cache, _vlog)
        _stats_certified(conn, cache, _vlog)
        _stats_nerd(conn, cache, _vlog)
        _stats_genres_index(conn, cache, _vlog)
        _stats_canonical_lists(conn, cache, _vlog)
        _stats_write_cache(conn, cache)

        _refresh_track_stats(conn, _vlog)
        _refresh_release_stats(conn, _vlog)
        _refresh_artist_stats(conn, _vlog)
        medal_inserts = _refresh_year_medals(conn, _vlog)

        conn.commit()

    console.print(f'[bold]Stats cache refreshed[/bold]  ({len(cache)} sections, {len(medal_inserts)} year-medals'
                  f'{f", {time.perf_counter() - t_total:.2f}s" if verbose else ""})')


def _print_fk_violations(fk: list, message: str, color: str = 'yellow') -> None:
    """Print an FK-violation count and its per-table breakdown."""
    by_table: dict = {}
    for row in fk:
        by_table[row[0]] = by_table.get(row[0], 0) + 1
    console.print(f'  [{color}]{len(fk)} foreign key violations {message}[/{color}]')
    for table, n in sorted(by_table.items(), key=lambda kv: -kv[1]):
        console.print(f'    [dim]{n:5}  {table}[/dim]')


def cmd_checkpoint(args):
    """Run the full publish pipeline: genres → certs → optimize → stats →
    wal-checkpoint → integrity → make_prod_db → gzip → jekyll build → verify.

    Run this after a batch of imports; skipping a step leaves the frontend
    serving a stale or truncated database.
    """
    db_path    = args.db or DB_PATH
    music_dir  = os.path.dirname(os.path.abspath(__file__))
    repo_root  = os.path.dirname(music_dir)
    site_dir   = os.path.join(repo_root, '_site')

    def _step(label):
        console.print(Rule(f'[bold]{label}[/bold]', style='bright_blue'))

    _step('1/10  genres refresh')
    cmd_genres_refresh(argparse.Namespace(db=db_path, tree=None))

    _step('2/10  certs refresh')
    cmd_certs_refresh(argparse.Namespace(db=db_path))

    # SQLite's query planner leans on table statistics gathered by ANALYZE,
    # and those go stale as rows are added/changed — a batch of imports
    # earlier in a session can leave it choosing a bad join order for
    # everything stats refresh runs next. Caught in practice: genresIndex
    # alone went from 38.8s to 0.29s after this, with the SQL unchanged —
    # SQLite had just started scanning tracks by a weak partial index
    # instead of the much more selective idx_tracks_release_id. Cheap
    # (well under a second) and safe to run unconditionally rather than
    # only when something feels slow, since there's no way to tell from
    # outside when the planner's estimates have drifted enough to matter.
    _step('3/10  optimize (refresh query planner statistics)')
    conn = open_db(db_path)
    try:
        conn.execute('PRAGMA optimize;')
    finally:
        conn.close()

    _step('4/10  stats refresh')
    cmd_stats_refresh(argparse.Namespace(db=db_path, verbose=False))

    _step('5/10  WAL checkpoint (TRUNCATE)')
    conn = open_db(db_path)
    try:
        result = conn.execute('PRAGMA wal_checkpoint(TRUNCATE);').fetchone()
        console.print(f'  {tuple(result)}')
    finally:
        conn.close()

    _step('6/10  integrity check')
    conn = open_db(db_path)
    try:
        result = conn.execute('PRAGMA integrity_check;').fetchone()
        ok = result[0] == 'ok'
        console.print(f'  [{"green" if ok else "red"}]{result[0]}[/{"green" if ok else "red"}]')
        if not ok:
            console.print('[red]Integrity check failed — aborting checkpoint.[/red]')
            sys.exit(1)

        # integrity_check only validates page structure, not references — a DB
        # with hundreds of orphaned rows passes it. Gate on referential
        # integrity too, or those orphans ship to production.
        fk = conn.execute('PRAGMA foreign_key_check;').fetchall()
        if fk:
            _print_fk_violations(fk, '— attempting auto-repair', 'yellow')

            # repair_integrity.py only fixes a known whitelist of orphan
            # patterns (e.g. release_genres left behind by a raw `DELETE FROM
            # releases` that bypassed the cascade) and refuses to report clean
            # if anything outside that whitelist remains — so retrying the
            # check after it runs is safe: either it actually cleaned up and
            # the checkpoint can proceed, or it couldn't and we still abort
            # with the original message instead of silently shipping orphans.
            repair_script = os.path.join(music_dir, 'repair_integrity.py')
            result = subprocess.run(
                [sys.executable, repair_script, '--db', db_path, '--no-backup'],
                capture_output=True, text=True,
            )
            for line in result.stdout.splitlines():
                console.print(f'    [dim]{line}[/dim]')
            if result.returncode != 0:
                # Surface *why* the repair script itself failed (crash,
                # permission error, disk full mid-backup) — without this,
                # a repair-script crash looked identical to "repair ran but
                # declined to fix anything," which sends anyone debugging it
                # to the wrong place entirely.
                console.print(f'  [red]repair_integrity.py exited {result.returncode}[/red]')
                for line in result.stderr.splitlines():
                    console.print(f'    [red]{line}[/red]')
            fk = conn.execute('PRAGMA foreign_key_check;').fetchall()

        if fk:
            _print_fk_violations(fk, 'remain after auto-repair', 'red')
            console.print('[red]Referential integrity check failed — aborting checkpoint.[/red]')
            console.print('[dim]Run: python repair_integrity.py --dry-run[/dim]')
            sys.exit(1)
        console.print('  [green]no foreign key violations[/green]')
    finally:
        conn.close()

    _step('7/10  make_prod_db.py')
    r = subprocess.run([sys.executable, os.path.join(music_dir, 'make_prod_db.py')],
                       cwd=music_dir)
    if r.returncode != 0:
        console.print('[red]make_prod_db.py failed — aborting checkpoint.[/red]')
        sys.exit(1)

    _step('8/10  gzip master_prod.sqlite')
    r = subprocess.run(['gzip', '-k', '-f', '-9', 'master_prod.sqlite'], cwd=music_dir)
    if r.returncode != 0:
        console.print('[red]gzip failed — aborting checkpoint.[/red]')
        sys.exit(1)

    if args.skip_jekyll:
        console.print('[dim]Skipping jekyll build (--skip-jekyll).[/dim]')
        return

    _step('9/10  jekyll build')
    r = subprocess.run(['bundle', 'exec', 'jekyll', 'build', '--destination', '_site'],
                       cwd=repo_root)
    if r.returncode != 0:
        console.print('[red]jekyll build failed — aborting checkpoint.[/red]')
        sys.exit(1)

    _step('10/10  verify gzip matches _site')
    src  = os.path.join(music_dir, 'master_prod.sqlite.gz')
    dest = os.path.join(site_dir, 'music', 'master_prod.sqlite.gz')
    r = subprocess.run(['cmp', src, dest])
    if r.returncode == 0:
        console.print('[bold green]✓ Checkpoint complete — gzip matches _site.[/bold green]')
    else:
        console.print('[red]gzip does NOT match _site — investigate before publishing.[/red]')
        sys.exit(1)


# ── main ──────────────────────────────────────────────────────────────────────


def cmd_track_variants_wrapper(args):
    """Dispatch to the interactive track-variants loop in mdb_cli."""
    include_linked = getattr(args, 'all', False)
    with managed_db(getattr(args, 'db', None) or DB_PATH) as conn:
        cmd_track_variants(conn, include_linked=include_linked)


# ── cmd: dedup ─────────────────────────────────────────────────────────────────

_EL_NAMES = {0: 'Wikipedia', 1: 'MusicBrainz', 2: 'Spotify', 3: 'Apple Music',
             4: 'Deezer', 5: 'Tidal', 6: 'Bandcamp', 7: 'Beatport',
             8: 'Genius', 9: 'Genius', 10: 'Discogs', 11: 'RateYourMusic', 12: 'Resident Advisor'}


def _dedup_find_groups(cur) -> list[list[dict]]:
    """Return groups of visible releases that share the same base title + artist,
    or the same release_group_mbid. Never groups across different artists or
    across releases MusicBrainz models as distinct types."""
    from mdb_strings import _base_title, normalize_text
    from collections import defaultdict
    rows = cur.execute('''
        SELECT r.id, r.title, r.primary_artist_id, a.name AS artist_name,
               r.release_date, r.date_source, r.mbid, r.spotify_id, r.apple_music_id,
               r.aoty_url, r.aoty_id, r.album_art_url, r.album_art_source,
               r.type, r.type_secondary, r.release_group_mbid, r.label, r.notes,
               r.total_tracks, r.aoty_score_critic, r.aoty_score_user,
               r.aoty_ratings_critic, r.aoty_ratings_user,
               (SELECT COUNT(*) FROM tracks t WHERE t.release_id = r.id) AS track_count,
               (SELECT COUNT(*) FROM listens l JOIN tracks t ON t.id = l.track_id
                WHERE t.release_id = r.id) AS listen_count
        FROM releases r LEFT JOIN artists a ON a.id = r.primary_artist_id
        WHERE r.hidden = 0
        ORDER BY a.name, r.title
    ''').fetchall()

    by_key: dict = defaultdict(list)
    by_rg: dict  = defaultdict(list)
    for row in rows:
        d = dict(row)
        key = (_base_title(d['title']).lower().strip(),
               normalize_text(d['artist_name'] or ''))
        by_key[key].append(d)
        if d['release_group_mbid']:
            by_rg[d['release_group_mbid']].append(d)

    seen: list = []
    groups: list = []
    for releases in list(by_key.values()) + list(by_rg.values()):
        if len(releases) < 2:
            continue
        # by_rg groups purely on a shared release_group_mbid, with no artist
        # check at all — bad MusicBrainz data (or an import bug) could in
        # principle put two different artists' releases in the same RG and
        # get silently offered as a "duplicate" to merge. by_key is already
        # artist-scoped and can't hit this; guard the by_rg path explicitly.
        artist_ids = {r['primary_artist_id'] for r in releases}
        if len(artist_ids) > 1:
            continue
        id_set = frozenset(r['id'] for r in releases)
        if id_set in seen:
            continue
        # Drop groups where releases differ by primary type AND by release-group MBID.
        # Different RG MBIDs mean MB explicitly models them as distinct entities
        # (e.g. "Circus" single vs "Circus" album) — don't merge those.
        rg_mbids = {r['release_group_mbid'] for r in releases if r['release_group_mbid']}
        types    = {r['type'] for r in releases if r['type']}
        if len(rg_mbids) > 1 and len(types) > 1:
            continue
        seen.append(id_set)
        groups.append(releases)
    return groups


def _dedup_load_tracks(cur, release_id: str) -> list[dict]:
    rows = cur.execute('''
        SELECT t.id, t.title, t.isrc, t.disc_number, t.track_number,
               t.duration_ms, t.tempo_bpm, t.musical_key, t.mix_name,
               t.spotify_id, t.mbid, t.release_id,
               COALESCE(COUNT(l.id), 0) AS listen_count
        FROM tracks t
        LEFT JOIN listens l ON l.track_id = t.id
        WHERE t.release_id = ? AND (t.hidden IS NULL OR t.hidden = 0)
        GROUP BY t.id
        ORDER BY t.disc_number, t.track_number, t.title
    ''', (release_id,)).fetchall()
    return [dict(r) for r in rows]


def _dedup_load_ext(cur, release_id: str) -> dict:
    rows = cur.execute(
        'SELECT service, link_value FROM external_links WHERE entity_type=1 AND entity_id=?',
        (release_id,),
    ).fetchall()
    return {r['service']: r['link_value'] for r in rows}


def _dedup_match_tracks(
    tracks_a: list, tracks_b: list
) -> tuple[list[tuple], list, list]:
    """Match tracks between two lists by ISRC then normalized title.
    Returns (matched_pairs, unmatched_a, unmatched_b).
    Each matched pair is (track_dict_from_a, track_dict_from_b).
    """
    matched: list[tuple] = []
    used_b: set = set()

    # Pass 1 — ISRC
    for ta in tracks_a:
        if not ta['isrc']:
            continue
        for tb in tracks_b:
            if tb['id'] in used_b:
                continue
            if tb['isrc'] and tb['isrc'].upper() == ta['isrc'].upper():
                matched.append((ta, tb))
                used_b.add(tb['id'])
                break

    matched_a = {ta['id'] for ta, _ in matched}

    # Pass 2 — normalized title
    for ta in tracks_a:
        if ta['id'] in matched_a:
            continue
        norm_a = normalize_text(ta['title'])
        for tb in tracks_b:
            if tb['id'] in used_b:
                continue
            if normalize_text(tb['title']) == norm_a:
                matched.append((ta, tb))
                used_b.add(tb['id'])
                matched_a.add(ta['id'])
                break

    unmatched_a = [t for t in tracks_a if t['id'] not in matched_a]
    unmatched_b = [t for t in tracks_b if t['id'] not in used_b]
    return matched, unmatched_a, unmatched_b


def _dedup_suggest_canonical(releases: list[dict]) -> int:
    """Return index of the release most suitable to be canonical."""
    # Score: listens × 4 + tracks × 2 + (has_mbid + has_sp + has_am + has_aoty) × 1
    def score(r):
        ids = (bool(r['mbid']) + bool(r['spotify_id']) + bool(r['apple_music_id'])
               + bool(r['aoty_url']))
        return r['listen_count'] * 4 + r['track_count'] * 2 + ids
    best = max(range(len(releases)), key=lambda i: score(releases[i]))
    return best


_DEDUP_UNIQUE_FIELDS = frozenset({'mbid', 'spotify_id', 'apple_music_id'})

_DEDUP_COALESCE_FIELDS = [
    'mbid', 'spotify_id', 'apple_music_id', 'aoty_id', 'aoty_url',
    'aoty_score_critic', 'aoty_score_user', 'aoty_ratings_critic',
    'aoty_ratings_user', 'album_art_url', 'album_art_source',
    'release_date', 'date_source', 'release_year',
    'release_group_mbid', 'type', 'type_secondary', 'label',
    'total_tracks', 'notes', 'spotify_popularity',
]


def _dedup_compute_merged(canonical: dict, losers: list[dict],
                          all_ext: list[dict]) -> tuple[dict, dict]:
    """Compute the merged release dict and merged external links from canonical + N losers.
    Returns (merged_release_dict, merged_ext_dict).
    Fields highlighted if they differ from canonical.
    """
    merged = dict(canonical)
    for loser in losers:
        for field in _DEDUP_COALESCE_FIELDS:
            cv, lv = merged.get(field), loser.get(field)
            if (cv is None or cv == '') and (lv is not None and lv != ''):
                merged[field] = lv
        # Date: prefer higher precision / higher priority
        if merged.get('release_date') and loser.get('release_date'):
            if _should_update_date(
                merged['release_date'], merged.get('date_source') or 'musicbrainz',
                loser['release_date'],  loser.get('date_source') or 'musicbrainz',
            ):
                merged['release_date'] = loser['release_date']
                merged['date_source']  = loser['date_source']
    # External links: union across all
    merged_ext = dict(all_ext[0])
    for ext in all_ext[1:]:
        for svc, val in ext.items():
            if svc not in merged_ext:
                merged_ext[svc] = val
    return merged, merged_ext


def _dedup_compute_updates(canonical: dict, loser: dict) -> dict:
    """Return the field→value dict that would be applied to canonical during 2-way merge."""
    merged, _ = _dedup_compute_merged(canonical, [loser], [{}, {}])
    return {k: v for k, v in merged.items()
            if k in _DEDUP_COALESCE_FIELDS and v != canonical.get(k)
            and (canonical.get(k) is None or canonical.get(k) == '')}


def _dedup_show_preview(releases: list[dict], all_tracks: list[list[dict]],
                        all_ext: list[dict], matched: list,
                        unmatched: list[list], sugg: int) -> None:
    """Show side-by-side comparison table + merged 'D' column at full console width."""
    labels = [chr(ord('A') + i) for i in range(len(releases))]
    total_listens = sum(r['listen_count'] for r in releases)
    artist = releases[0]['artist_name'] or 'unknown artist'

    canon = releases[sugg]
    losers = [r for i, r in enumerate(releases) if i != sugg]
    merged, merged_ext = _dedup_compute_merged(canon, losers, all_ext)

    console.print()
    console.print(Rule(
        f'[bold]{releases[0]["title"]}[/bold]  [dim]({artist})[/dim]'
        f'  [dim]{total_listens} listens[/dim]',
        style='bright_blue',
    ))

    # --- Attribute table ---
    t = Table(box=rbox.SIMPLE_HEAD, show_header=True, pad_edge=False,
              show_edge=False, expand=True)
    t.add_column('', style='dim', width=13, no_wrap=True)
    for label, r in zip(labels, releases):
        is_canon = (r is canon)
        hdr = (f'[{"bold cyan" if is_canon else "dim"}]{label}[/]'
               f'  [dim]{r["id"][:10]}…[/dim]'
               f'  [dim]{r["listen_count"]}L {r["track_count"]}T[/dim]')
        t.add_column(hdr, ratio=1, no_wrap=False, overflow='fold')
    t.add_column(
        f'[bold green]D[/bold green]  [dim]→ {canon["id"][:10]}…[/dim]',
        ratio=1, no_wrap=False, overflow='fold',
    )

    def yn(v): return '[green]✓[/green]' if v else '[dim]—[/dim]'

    def _changed(field, mval):
        """True if merged D value differs from canonical."""
        return mval not in (None, '') and mval != canon.get(field)

    def _d(field, mval):
        if _changed(field, mval):
            return f'[bold yellow]{mval}[/bold yellow]' if mval else '[dim]—[/dim]'
        return str(mval) if mval not in (None, '') else '[dim]—[/dim]'

    field_rows = [
        ('Title',
         lambda r, e: r['title'],
         merged.get('title')),
        ('Date',
         lambda r, e: f"{r['release_date'] or '—'}  [dim][{r['date_source'] or '?'}][/dim]",
         (f"{merged['release_date']}  [dim][{merged.get('date_source') or '?'}][/dim]"
          if merged.get('release_date') else '[dim]—[/dim]')),
        ('Type',
         lambda r, e: ' · '.join(filter(None, [r['type'], r['type_secondary']])) or '[dim]—[/dim]',
         ' · '.join(filter(None, [merged.get('type'), merged.get('type_secondary')])) or '[dim]—[/dim]'),
        ('MBID',
         lambda r, e: (r['mbid'][:16] if r['mbid'] else '[dim]—[/dim]'),
         _d('mbid', merged.get('mbid', '')[:16] if merged.get('mbid') else None)),
        ('Spotify',
         lambda r, e: yn(r['spotify_id']),
         yn(merged.get('spotify_id'))),
        ('Apple Music',
         lambda r, e: yn(r['apple_music_id']),
         yn(merged.get('apple_music_id'))),
        ('AOTY',
         lambda r, e: yn(r['aoty_url']),
         yn(merged.get('aoty_url'))),
        ('Wikipedia',
         lambda r, e: yn(e.get(0)),
         yn(merged_ext.get(0))),
        ('Beatport',
         lambda r, e: yn(e.get(7)),
         yn(merged_ext.get(7))),
        ('Bandcamp',
         lambda r, e: yn(e.get(6)),
         yn(merged_ext.get(6))),
        ('Art source',
         lambda r, e: r['album_art_source'] or '[dim]—[/dim]',
         _d('album_art_source', merged.get('album_art_source'))),
        ('Label',
         lambda r, e: r['label'] or '[dim]—[/dim]',
         _d('label', merged.get('label'))),
        ('AOTY score',
         lambda r, e: (
             f"{r['aoty_score_critic'] or '—'} / {r['aoty_score_user'] or '—'}"
             if (r['aoty_score_critic'] or r['aoty_score_user']) else '[dim]—[/dim]'),
         (f"{merged.get('aoty_score_critic') or '—'} / {merged.get('aoty_score_user') or '—'}"
          if (merged.get('aoty_score_critic') or merged.get('aoty_score_user')) else '[dim]—[/dim]')),
        ('RG MBID',
         lambda r, e: (r['release_group_mbid'][:16] if r['release_group_mbid'] else '[dim]—[/dim]'),
         _d('release_group_mbid',
            merged.get('release_group_mbid', '')[:16] if merged.get('release_group_mbid') else None)),
    ]
    for fname, fval, dval in field_rows:
        t.add_row(fname, *[fval(r, e) for r, e in zip(releases, all_ext)], dval)
    console.print(t)

    # --- Track section ---
    if len(releases) == 2 and (all_tracks[0] or all_tracks[1]):
        console.print()
        if matched:
            console.print(f'  [dim]Matched[/dim]  {len(matched)} tracks')
            for ta, tb in matched[:6]:
                how = 'ISRC' if (ta['isrc'] and ta['isrc'] == tb['isrc']) else 'title'
                tot = ta['listen_count'] + tb['listen_count']
                console.print(
                    f'    [dim]{ta["disc_number"]}:{ta["track_number"]:02d}[/dim]  '
                    f'{ta["title"]}  [dim]←[{how}]→  {tot}L[/dim]'
                )
            if len(matched) > 6:
                console.print(f'    [dim]… {len(matched) - 6} more[/dim]')
        for label, ulist in zip(labels, unmatched):
            if ulist:
                console.print(f'  [dim]Only in {label}[/dim]  {len(ulist)} tracks')
                for tr in ulist[:4]:
                    console.print(
                        f'    [dim]{tr["disc_number"]}:{tr["track_number"]:02d}[/dim]  '
                        f'{tr["title"]}  [dim]{tr["listen_count"]}L[/dim]'
                    )
                if len(ulist) > 4:
                    console.print(f'    [dim]… {len(ulist) - 4} more[/dim]')


def _dedup_show_merged(canonical: dict, loser: dict, updates: dict,
                       canon_tracks: list, loser_unmatched: list,
                       canon_idx: int, matched: list) -> None:
    """Kept for backwards compat — not used when preview already shows D column."""
    pass


def _dedup_merge(conn, canonical: dict, loser: dict,
                 matched: list[tuple], unmatched_loser: list,
                 canon_idx: int) -> None:
    """Merge loser into canonical. canonical/loser are full release dicts.
    matched:         [(track_a, track_b), …] — canon track is at canon_idx in each pair
    unmatched_loser: tracks from loser with no match in canonical
    """
    cur = conn.cursor()
    updates = _dedup_compute_updates(canonical, loser)

    # -- 1. COALESCE release fields (canonical wins; fill from loser where NULL) -
    if updates:
        # Clear UNIQUE fields from loser first to avoid constraint violations
        unique_to_clear = [f for f in _DEDUP_UNIQUE_FIELDS if f in updates]
        if unique_to_clear:
            cur.execute(
                f'UPDATE releases SET {", ".join(f + "=NULL" for f in unique_to_clear)} WHERE id=?',
                (loser['id'],),
            )
        cur.execute(
            f'UPDATE releases SET {", ".join(f"{k}=?" for k in updates)} WHERE id=?',
            [*updates.values(), canonical['id']],
        )

    # -- 2. Copy missing external links from loser to canonical --------------------
    ext_canon = _dedup_load_ext(cur, canonical['id'])
    ext_loser = _dedup_load_ext(cur, loser['id'])
    for svc, val in ext_loser.items():
        if svc not in ext_canon:
            cur.execute(
                'INSERT OR REPLACE INTO external_links'
                ' (entity_type, entity_id, service, link_value) VALUES (1,?,?,?)',
                (canonical['id'], svc, val),
            )

    # -- 3. Copy genres if canonical has none -------------------------------------
    if not cur.execute(
        'SELECT 1 FROM release_genres WHERE release_id=?', (canonical['id'],)
    ).fetchone():
        for row in cur.execute(
            'SELECT aoty_genre_id, is_primary FROM release_genres WHERE release_id=?',
            (loser['id'],),
        ).fetchall():
            cur.execute(
                'INSERT OR IGNORE INTO release_genres'
                ' (release_id, aoty_genre_id, is_primary) VALUES (?,?,?)',
                (canonical['id'], row['aoty_genre_id'], row['is_primary']),
            )

    # -- 4. Migrate listens + set canonical_track_id on matched pairs --------------
    # matched pairs: pair[canon_idx] = canonical track, pair[1-canon_idx] = loser track
    loser_idx_in_pair = 1 - canon_idx
    for pair in matched:
        canon_track = pair[canon_idx]
        loser_track = pair[loser_idx_in_pair]
        cur.execute('UPDATE listens SET track_id=? WHERE track_id=?',
                    (canon_track['id'], loser_track['id']))
        cur.execute('UPDATE tracks SET canonical_track_id=?, hidden=1 WHERE id=?',
                    (canon_track['id'], loser_track['id']))

    # -- 5. Move unmatched loser tracks to canonical release -----------------------
    for t in unmatched_loser:
        cur.execute('UPDATE tracks SET release_id=? WHERE id=?',
                    (canonical['id'], t['id']))

    # -- 6. Fix release_variants references ----------------------------------------
    # Case A: loser was the canonical — redirect its variants to the new canonical.
    # Can't UPDATE in bulk (would hit UNIQUE on existing rows); INSERT OR IGNORE + DELETE.
    loser_variants = [r[0] for r in cur.execute(
        'SELECT variant_id FROM release_variants WHERE canonical_id=?', (loser['id'],)
    ).fetchall()]
    for vid in loser_variants:
        if vid != canonical['id']:
            cur.execute(
                'INSERT OR IGNORE INTO release_variants (canonical_id, variant_id) VALUES (?,?)',
                (canonical['id'], vid),
            )
    cur.execute('DELETE FROM release_variants WHERE canonical_id=?', (loser['id'],))

    # Case B: loser was a variant of some other release — re-point to canonical.
    loser_parents = [r[0] for r in cur.execute(
        'SELECT canonical_id FROM release_variants WHERE variant_id=?', (loser['id'],)
    ).fetchall()]
    for cid in loser_parents:
        if cid != canonical['id']:
            cur.execute(
                'INSERT OR IGNORE INTO release_variants (canonical_id, variant_id) VALUES (?,?)',
                (cid, canonical['id']),
            )
    cur.execute('DELETE FROM release_variants WHERE variant_id=?', (loser['id'],))

    # Remove any self-referencing rows that may have appeared
    cur.execute('DELETE FROM release_variants WHERE canonical_id=? AND variant_id=?',
                (canonical['id'], canonical['id']))

    # -- 7. Hide loser ------------------------------------------------------------
    cur.execute('UPDATE releases SET hidden=1 WHERE id=?', (loser['id'],))
    conn.commit()


def cmd_doctor(args):
    """Read-only DB audit: surfaces backfill opportunities and data anomalies
    without writing anything. Each finding names the command that would fix
    it (enrich art/spotify-links/deezer-links, dedup --artist) rather than
    acting itself.
    """
    db_path = getattr(args, 'db', None) or DB_PATH
    as_json = getattr(args, 'json', False)
    report: dict = {}

    with managed_db(db_path) as conn:
        integrity = conn.execute('PRAGMA integrity_check').fetchone()[0]
        fk        = conn.execute('PRAGMA foreign_key_check').fetchall()
        report['integrity_check']    = integrity
        report['fk_violations']      = len(fk)

        report['upc_no_spotify'] = conn.execute('''
            SELECT COUNT(*) FROM releases
            WHERE hidden = 0 AND upc IS NOT NULL
              AND (spotify_id IS NULL OR spotify_id = '')
        ''').fetchone()[0]

        report['upc_no_deezer'] = conn.execute(f'''
            SELECT COUNT(*) FROM releases r
            WHERE r.hidden = 0 AND r.upc IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM external_links el
                  WHERE el.entity_type = {EL_RELEASE} AND el.service = {EL_SVC_DEEZER}
                    AND el.entity_id = r.id
              )
        ''').fetchone()[0]

        report['mbid_no_art'] = conn.execute('''
            SELECT COUNT(*) FROM releases
            WHERE hidden = 0 AND mbid IS NOT NULL
              AND (album_art_url IS NULL OR album_art_url = '')
        ''').fetchone()[0]

        # Same artist, same release_date, different titles, created within a
        # short window — the shape a majority-vote date collision leaves
        # behind. A legitimate same-day multi-release (e.g. several singles
        # dropped together) has the same shape, so this is a lead to check,
        # not a confirmed defect.
        date_collisions = conn.execute('''
            SELECT a.name, r1.title, r2.title, r1.release_date, r1.id, r2.id
            FROM releases r1 JOIN releases r2
              ON r1.primary_artist_id = r2.primary_artist_id
             AND r1.release_date = r2.release_date
             AND r1.id < r2.id
            LEFT JOIN artists a ON a.id = r1.primary_artist_id
            WHERE r1.hidden = 0 AND r2.hidden = 0
              AND r1.created_at IS NOT NULL AND r2.created_at IS NOT NULL
              AND ABS(r1.created_at - r2.created_at) < 10
            ORDER BY a.name
        ''').fetchall()
        report['date_collisions'] = [
            {'artist': r[0], 'title_a': r[1], 'title_b': r[2], 'date': r[3],
             'release_id_a': r[4], 'release_id_b': r[5]}
            for r in date_collisions
        ]

        # Same artist, same title, more than one visible release — a
        # duplicate that dedup would resolve, or a legitimate re-recording
        # that happens to share a title (verify by content before merging).
        dup_titles = conn.execute('''
            SELECT a.name, r.title, COUNT(*), GROUP_CONCAT(r.id, ',')
            FROM releases r LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE r.hidden = 0
            GROUP BY r.primary_artist_id, r.title
            HAVING COUNT(*) > 1
            ORDER BY a.name
        ''').fetchall()
        report['duplicate_titles'] = [
            {'artist': r[0], 'title': r[1], 'count': r[2], 'release_ids': r[3].split(',')}
            for r in dup_titles
        ]

    if as_json:
        print(json.dumps(report, indent=2))
        return

    console.print(f"  Integrity check:  [{'green' if integrity == 'ok' else 'red'}]{integrity}[/]")
    console.print(f"  FK violations:    [{'green' if not fk else 'red'}]{len(fk)}[/]")
    console.print(f"  UPC, no Spotify:  {report['upc_no_spotify']}  [dim](enrich spotify-links)[/dim]")
    console.print(f"  UPC, no Deezer:   {report['upc_no_deezer']}  [dim](enrich deezer-links)[/dim]")
    console.print(f"  MBID, no art:     {report['mbid_no_art']}  [dim](enrich art)[/dim]")

    console.rule(style='dim')
    console.print(f"  [bold]{len(report['duplicate_titles'])}[/bold] duplicate-title group(s)  [dim](dedup --artist)[/dim]")
    for d in report['duplicate_titles'][:20]:
        console.print(f"    {d['artist'] or '(no artist)'} — {d['title']!r}  ×{d['count']}")
    if len(report['duplicate_titles']) > 20:
        console.print(f"    [dim]… and {len(report['duplicate_titles']) - 20} more[/dim]")

    console.rule(style='dim')
    console.print(f"  [bold]{len(report['date_collisions'])}[/bold] same-day release pair(s) created within 10s of each other")
    for c in report['date_collisions'][:20]:
        console.print(f"    {c['artist'] or '(no artist)'} — {c['title_a']!r} / {c['title_b']!r}  ({c['date']})")
    if len(report['date_collisions']) > 20:
        console.print(f"    [dim]… and {len(report['date_collisions']) - 20} more[/dim]")


def cmd_dedup(args):
    """Find and resolve duplicate releases interactively.

    Every merge requires an explicit y/N confirmation per group; `--report`
    lists candidates without prompting or touching the DB. `--artist` scopes
    the search to one artist and is the recommended default for routine use
    — the artist-agnostic scan compares titles across the whole catalog and
    can turn up more candidates to review than are useful in one sitting.
    """
    db_path = getattr(args, 'db', None) or DB_PATH
    only_artist = (getattr(args, 'artist', None) or '').strip().lower()

    with managed_db(db_path) as conn:
        cur = conn.cursor()
        groups = _dedup_find_groups(cur)

    if not groups:
        console.print('[green]No duplicate groups found.[/green]')
        return

    # Optionally filter to a specific artist
    if only_artist:
        groups = [
            g for g in groups
            if any(normalize_text(r.get('artist_name') or '') == normalize_text(only_artist)
                   for r in g)
        ]

    console.print(
        f'  [bold]{len(groups)}[/bold] duplicate group(s)'
        + (f'  ·  [dim]filtering: {only_artist}[/dim]' if only_artist else '')
    )

    # Read-only mode: list candidate groups and exit — never enters the
    # interactive merge loop, never touches the DB. Safe for automation/CI
    # to run unattended, unlike the interactive mode (see Known Issues in
    # the discography-buildout skill for why that distinction matters).
    if getattr(args, 'report', False):
        for gi, group in enumerate(groups, 1):
            artist = group[0].get('artist_name') or '(no artist)'
            console.print(f'  [bold]{gi}.[/bold] {artist} — {group[0]["title"]!r}  [dim]({len(group)} releases)[/dim]')
            for r in group:
                console.print(f'      {r["id"]}  [dim]{r["type"] or "?"}[/dim]  tracks={r["track_count"]} listens={r["listen_count"]}')
        return

    merged = skipped = linked = 0

    for gi, group in enumerate(groups, 1):
        with managed_db(db_path) as conn:
            cur = conn.cursor()
            # Reload fresh (previous merges may have hidden some)
            live = []
            for r in group:
                row = cur.execute(
                    'SELECT hidden FROM releases WHERE id=?', (r['id'],)
                ).fetchone()
                if row and not row['hidden']:
                    live.append(r)
            if len(live) < 2:
                continue

            all_tracks = [_dedup_load_tracks(cur, r['id']) for r in live]
            all_ext    = [_dedup_load_ext(cur, r['id'])    for r in live]

        # For groups of exactly 2, compute track matches
        if len(live) == 2:
            matched, unmatched_a, unmatched_b = _dedup_match_tracks(
                all_tracks[0], all_tracks[1]
            )
            unmatched = [unmatched_a, unmatched_b]
        else:
            matched, unmatched = [], [[] for _ in live]

        sugg = _dedup_suggest_canonical(live)
        _dedup_show_preview(live, all_tracks, all_ext, matched, unmatched, sugg)

        labels = [chr(ord('A') + i) for i in range(len(live))]
        loser_label = labels[1 - sugg] if len(live) == 2 else '?'
        canon_label = labels[sugg]

        if len(live) == 2:
            hint = (f'  [dim]Suggestion: merge [bold]{loser_label}[/bold] → '
                    f'[bold]{canon_label}[/bold]'
                    f' (keep {canon_label})[/dim]')
            console.print(hint)
            console.print(
                f'  [bold]m[/bold] merge {loser_label}→{canon_label} '
                f'[dim]·[/dim]  [bold]r[/bold] reverse ({canon_label}→{loser_label}) '
                f'[dim]·[/dim]  [bold]v[/bold] link as variants  '
                f'[dim]·[/dim]  [bold]s[/bold] skip  '
                f'[dim]·[/dim]  [bold]q[/bold] quit'
            )
        else:
            console.print(
                f'  [dim]Suggestion: keep [bold]{canon_label}[/bold] as canonical[/dim]'
            )
            console.print(
                f'  [bold]m[/bold] merge all → {canon_label} '
                f'[dim]·[/dim]  [bold]v[/bold] link as variants  '
                f'[dim]·[/dim]  [bold]s[/bold] skip  '
                f'[dim]·[/dim]  [bold]q[/bold] quit'
            )

        try:
            ch = console.input(f'  Action [{gi}/{len(groups)}]: ').strip().lower()
        except (EOFError, KeyboardInterrupt):
            break

        if ch == 'q':
            break
        elif ch in ('m', 'r'):
            if len(live) == 2:
                canon_idx = sugg if ch == 'm' else (1 - sugg)
            else:
                # 'r' not available for N-way
                canon_idx = sugg
            canonical  = live[canon_idx]
            loser_list = [r for i, r in enumerate(live) if i != canon_idx]
            try:
                confirm = console.input(
                    f'  [bold]Confirm: merge {len(loser_list)} release(s) → '
                    f'{labels[canon_idx]}?[/bold] [y/N]: '
                ).strip().lower()
            except (EOFError, KeyboardInterrupt):
                break
            if confirm != 'y':
                skipped += 1
                continue
            with managed_db(db_path) as conn:
                for i, loser in enumerate(loser_list):
                    loser_idx_in_live = live.index(loser)
                    if len(live) == 2:
                        pair_matched = matched
                        loser_unmatched = unmatched[loser_idx_in_live]
                        pair_canon_idx = canon_idx
                    else:
                        # For N-way: pair-match each loser against canonical on the fly.
                        # _dedup_match_tracks(canonical, loser) always returns pairs as
                        # (canonical_track, loser_track), so the canonical side is index 0.
                        pair_matched, _, loser_unmatched = _dedup_match_tracks(
                            all_tracks[canon_idx], all_tracks[loser_idx_in_live]
                        )
                        pair_canon_idx = 0
                    _dedup_merge(conn, canonical, loser, pair_matched, loser_unmatched, pair_canon_idx)
                    console.print(
                        f'  [green]✓[/green]  Merged [dim]{loser["id"][:12]}…[/dim] → '
                        f'[bold]{canonical["title"]}[/bold]'
                        + (f'  ({len(pair_matched)} track pairs, '
                           f'{len(loser_unmatched)} moved)' if pair_matched else '')
                    )
            merged += 1
        elif ch == 'v':
            # Link as variants (no track merge)
            with managed_db(db_path) as conn:
                canon_r = live[sugg]
                others  = [r for i, r in enumerate(live) if i != sugg]
                _write_variant_links(
                    conn,
                    canon_r['id'],
                    [(r['id'], r['title'], i) for i, r in enumerate(others)],
                )
            console.print(
                f'  [cyan]→[/cyan]  Linked as variants under '
                f'[bold]{live[sugg]["title"]}[/bold]'
            )
            linked += 1
        else:
            skipped += 1

    console.print()
    parts = []
    if merged:  parts.append(f'[green]{merged} merged[/green]')
    if linked:  parts.append(f'[cyan]{linked} linked[/cyan]')
    if skipped: parts.append(f'[dim]{skipped} skipped[/dim]')
    console.print('  ' + '  ·  '.join(parts) if parts else '[dim]Done.[/dim]')


def cmd_link_discogs(args):
    """Associate a DB release with a Discogs release ID."""
    db_path = getattr(args, 'db', None) or DB_PATH
    with managed_db(db_path) as conn:
        # Resolve release
        release = None
        raw = args.release
        # Try internal ID
        release = conn.execute('SELECT id, title FROM releases WHERE id=?', [raw]).fetchone()
        # Try Spotify ID
        if not release:
            release = conn.execute('SELECT id, title FROM releases WHERE spotify_id=?', [raw]).fetchone()
        # Try MB UUID
        if not release and is_valid_mbid(raw):
            release = conn.execute('SELECT id, title FROM releases WHERE mbid=?', [raw]).fetchone()
        if not release:
            console.print(f'[red]Release not found:[/red] {raw}')
            return

        # Check if already linked to a different Discogs ID
        existing = conn.execute(
            'SELECT link_value FROM external_links WHERE entity_type=? AND entity_id=? AND service=?',
            [EL_RELEASE, release['id'], EL_SVC_DISCOGS]
        ).fetchone()
        if existing and existing[0] != str(args.discogs_id):
            console.print(f'[yellow]Warning:[/yellow] replacing existing Discogs ID {existing[0]} → {args.discogs_id}')

        link_discogs(conn, release['id'], args.discogs_id)
        console.print(
            f'  [green]✓[/green]  [bold]{release["title"]}[/bold]  '
            f'→  Discogs [dim]https://www.discogs.com/release/{args.discogs_id}[/dim]'
        )


def _collection_match_release(conn: sqlite3.Connection, artist: str, title: str) -> list:
    """Candidate DB releases for one Discogs collection item, via the same
    fuzzy artist+title matcher scrobble sync uses."""
    return db_search_releases(conn, artist, title)


def _collection_resolve_interactive(conn: sqlite3.Connection, artist: str, title: str,
                                     candidates: list) -> 'str | None':
    """Prompt for the correct release when a Discogs item has 0 or >1 candidates.

    Accepts a numbered pick, a pasted release ID/URL (resolved the same way
    import_album_unified resolves import targets), or 's' to skip (leaves
    the item queued — collection_items.release_id is NOT NULL, so a skipped
    item is simply not written this run).
    """
    console.print(f'\n  [bold]{artist} — {title}[/bold]')
    if not candidates:
        console.print('  [dim]no DB candidates found[/dim]')
    options = [f"{c['title']} — {c['artist_name']} ({c.get('release_date') or '?'})" for c in candidates]
    value, quit_, _, _ = _prompt_choice(
        'Pick the matching release, paste a URL/ID to import, or [s]kip',
        options, allow_hide=True,
    )
    if quit_:
        raise KeyboardInterrupt
    if value in options:
        return candidates[options.index(value)]['id']
    if value and value not in ('none',):
        try:
            rid, _, _, _ = import_album_unified(DB_PATH, value, auto=True)
            return rid
        except Exception as e:
            console.print(f'  [red]import failed:[/red] {e}')
    return None


def cmd_collection_sync_discogs(args):
    """Sync the physical collection directly from the Discogs API.

    Every Discogs item gets a collection_items row, whether or not it
    resolves to a release: linked items carry release_id; everything else
    carries unresolved_reason ('non_music' | 'ambiguous' | 'unresolved') so
    its discogs_instance_id is permanently in the "already seen" set —
    otherwise an item that never resolves (a DJ control record, an obscure
    12" absent from every source) gets rediscovered and reprocessed on
    every single future sync, forever.

    Additive by design: a page-1 count check short-circuits the whole sync
    when nothing changed (one API call), and any remaining run only does the
    expensive per-item work (DB matching, a condition-fields API call) for
    discogs_instance_ids not already in collection_items. --force reprocesses
    everything anyway (e.g. after the DB gained releases that could resolve a
    previously-ambiguous item).

    --json implies --auto: there's no terminal to prompt against once output
    is a single machine-readable payload, so ambiguous items are always
    queued rather than interactively resolved in that mode.
    """
    from mdb_apis import DiscogsClient
    from mdb_collection import (
        CollectionItem, CollectionMedia, CollectionIdentifier,
        discogs_tags_to_coarse, is_non_music_item, upsert_collection_item,
    )

    db_path = getattr(args, 'db', None) or DB_PATH
    force = getattr(args, 'force', False)
    as_json = getattr(args, 'json', False)
    auto = args.auto or as_json
    dc = DiscogsClient.from_env()
    identity = dc.get_identity()
    username = identity['username']

    def emit(msg):
        if not as_json:
            console.print(msg)

    with managed_db(db_path) as conn:
        local_count = conn.execute('SELECT COUNT(*) FROM collection_items').fetchone()[0]
        existing_instance_ids = {
            r[0] for r in conn.execute('SELECT discogs_instance_id FROM collection_items').fetchall()
        }

    if not force:
        remote_count = dc.get_collection_item_count(username)
        if remote_count == local_count:
            if as_json:
                print(json.dumps({'up_to_date': True, 'local_count': local_count}, indent=2))
            else:
                console.print(f'  [green]Up to date[/green] — {local_count} items, no change on Discogs')
            return
        emit(f'  Discogs has {remote_count} items, {local_count} synced locally — checking for changes')

    emit(f'Fetching collection for [bold]{username}[/bold]...')
    raw_items = dc.get_collection_items(username)
    emit(f'  {len(raw_items)} item(s) in collection\n')

    remote_instance_ids = {str(r['instance_id']) for r in raw_items}
    removed = existing_instance_ids - remote_instance_ids
    if removed and not as_json:
        console.print(f'  [yellow]{len(removed)} item(s)[/yellow] previously synced are no longer in the '
                       f'Discogs collection (not auto-removed — review with collection_item_id in '
                       f'discogs_instance_id {sorted(removed)})\n')

    already_synced = 0
    if not force:
        new_items = [r for r in raw_items if str(r['instance_id']) not in existing_instance_ids]
        already_synced = len(raw_items) - len(new_items)
        emit(f'  {already_synced} already synced, {len(new_items)} new\n')
        raw_items = new_items

    linked = skipped = non_music = unresolved = errored = 0
    queued_items: list = []
    errored_instance_ids: list = []
    with managed_db(db_path) as conn:
        try:
            for raw in raw_items:
                bi = raw['basic_information']
                artist_name = (bi.get('artists') or [{}])[0].get('name', '')
                title = bi['title']
                folder_id = raw.get('folder_id', 0)
                genres, styles = bi.get('genres', []), bi.get('styles', [])

                release_id = None
                reason = None
                candidate_ids: list = []

                if is_non_music_item(genres, styles):
                    reason = 'non_music'
                    non_music += 1
                    emit(f'  [dim]non-music, tracked only:[/dim] {artist_name} — {title}')
                else:
                    candidates = _collection_match_release(conn, artist_name, title)
                    if len(candidates) == 1:
                        release_id = candidates[0]['id']
                        linked += 1
                    elif auto:
                        candidate_ids = [c['id'] for c in candidates]
                        reason = 'ambiguous' if candidates else 'unresolved'
                        if candidates:
                            queued_items.append({
                                'discogs_release_id': raw['id'],
                                'discogs_instance_id': raw['instance_id'],
                                'artist': artist_name,
                                'title': title,
                                'candidates': [
                                    {'release_id': c['id'], 'title': c['title'], 'artist_name': c['artist_name']}
                                    for c in candidates
                                ],
                            })
                            emit(f'  [dim]queued (needs review):[/dim] {artist_name} — {title}')
                        else:
                            unresolved += 1
                            emit(f'  [dim]unresolved, tracked only:[/dim] {artist_name} — {title}')
                    else:
                        release_id = _collection_resolve_interactive(conn, artist_name, title, candidates)
                        if not release_id:
                            skipped += 1
                            candidate_ids = [c['id'] for c in candidates]
                            reason = 'ambiguous' if candidates else 'unresolved'

                try:
                    fields = dc.get_collection_instance_fields(
                        username, folder_id, raw['id'], raw['instance_id'])
                except SourceError as e:
                    errored += 1
                    errored_instance_ids.append(str(raw['instance_id']))
                    emit(f'  [red]API error, skipped for this run:[/red] {artist_name} — {title} ({e})')
                    continue

                item = CollectionItem(
                    discogs_release_id=str(raw['id']),
                    discogs_instance_id=str(raw['instance_id']),
                    release_id=release_id,
                    catalog_number=(bi.get('labels') or [{}])[0].get('catno'),
                    label=(bi.get('labels') or [{}])[0].get('name'),
                    date_added=raw.get('date_added'),
                    media_condition=fields.get(1),
                    sleeve_condition=fields.get(2),
                    notes=fields.get(3),
                    discogs_folder=str(folder_id),
                    discogs_genres=genres + styles,
                    coarse_genre=discogs_tags_to_coarse(genres, styles),
                    media=[CollectionMedia.from_discogs_format(f) for f in bi.get('formats', [])],
                    identifiers=[],
                    hidden=is_non_music_item(genres, styles),
                    unresolved_reason=reason,
                    candidate_release_ids=candidate_ids,
                )
                upsert_collection_item(conn, item)
                conn.commit()
        except KeyboardInterrupt:
            emit('\n[dim]stopped — progress so far is saved[/dim]')

        conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')

    if as_json:
        print(json.dumps({
            'up_to_date': False,
            'already_synced': already_synced,
            'processed': len(raw_items),
            'linked': linked,
            'queued': queued_items,
            'non_music': non_music,
            'unresolved': unresolved,
            'skipped': skipped,
            'errored': errored,
            'errored_instance_ids': errored_instance_ids,
            'removed_instance_ids': sorted(removed),
        }, indent=2))
        return

    console.rule(style='dim')
    console.print(
        f'  linked [green]{linked}[/green]  ·  '
        f'queued for review [yellow]{len(queued_items)}[/yellow]  ·  '
        f'non-music [dim]{non_music}[/dim]  ·  '
        f'unresolved [dim]{unresolved}[/dim]  ·  '
        f'skipped [dim]{skipped}[/dim]  ·  '
        f'errored [dim]{errored}[/dim]'
    )


def cmd_collection_set_image(args):
    """Assign a specific pressing photo URL to one owned copy of a release.

    coloredvinylrecords.com's own image-number suffixes don't correspond to
    album numbering (confirmed by inspection — e.g. RTJ1 vs RTJ4 aren't
    -1.png/-4.png), so there's no way to guess the right URL automatically;
    matching is manual, this just applies the result of that lookup.
    """
    with managed_db(getattr(args, 'db', None) or DB_PATH) as conn:
        release = _resolve_release(conn, args.release)
        if not release:
            console.print(f'[red]Release not found:[/red] {args.release}')
            sys.exit(1)

        rows = conn.execute('''
            SELECT cim.id AS media_id, ci.id AS collection_item_id, cim.raw_text, cim.color_primary
            FROM collection_item_media cim
            JOIN collection_items ci ON ci.id = cim.collection_item_id
            LEFT JOIN collection_item_releases cir ON cir.collection_item_id = ci.id
            WHERE cim.medium = ? AND (ci.release_id = ? OR cir.release_id = ?)
        ''', (args.medium, release['id'], release['id'])).fetchall()

        if not rows:
            console.print(f'[red]No owned {args.medium} copy found for[/red] {release["title"]}')
            sys.exit(1)

        if len(rows) > 1 and not args.collection_item_id:
            console.print(f'[yellow]{len(rows)} owned {args.medium} copies of[/yellow] {release["title"]} '
                          f'— pass --collection-item-id to pick one:')
            for r in rows:
                console.print(f'  {r["collection_item_id"]}  {r["color_primary"] or ""}  {r["raw_text"]}')
            sys.exit(1)

        if args.collection_item_id:
            rows = [r for r in rows if r['collection_item_id'] == args.collection_item_id]
            if not rows:
                console.print(f'[red]No {args.medium} media row for collection_item[/red] {args.collection_item_id} '
                              f'on {release["title"]}')
                sys.exit(1)

        conn.execute('UPDATE collection_item_media SET image_url = ? WHERE id = ?',
                     (args.url, rows[0]['media_id']))
        conn.commit()
        console.print(f'[green]Set image[/green] on {release["title"]} (collection_item {rows[0]["collection_item_id"]})')


_SHOW_TABLES = {
    'release': ('releases', 'title'),
    'artist':  ('artists', 'name'),
    'track':   ('tracks', 'title'),
}


def _show_find_entity(conn: sqlite3.Connection, key: str) -> 'tuple[str, sqlite3.Row] | tuple[None, None]':
    """Look up key as an ID across releases/artists/tracks. Returns (kind, row) or (None, None)."""
    for kind, (table, _) in _SHOW_TABLES.items():
        row = conn.execute(f'SELECT * FROM {table} WHERE id = ?', [key]).fetchone()
        if row:
            return kind, row
    return None, None


def _show_to_epoch(value) -> 'int | None':
    """Coerce created_at/updated_at to a Unix timestamp.

    Most rows store an int, but a few legacy rows store an ISO datetime
    string instead — normalize both to int so sorting/formatting don't break.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    for fmt in ('%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S'):
        try:
            return int(datetime.strptime(value, fmt).timestamp())
        except ValueError:
            continue
    return None


def _check_substring_boost(key: str, other_key: str, min_len: int = 4) -> bool:
    """True if key/other_key should get a substring-match confidence boost.

    Guards against two false-positive modes:
    - Degenerate ascii_key() output on non-Latin names (e.g. 'MØ', all-CJK/
      Hangul/Greek/Hebrew titles can normalize to a single character or empty
      string), which would otherwise substring-match almost everything.
    - Short-but-valid queries ('Sia', 'GTA') incidentally appearing inside many
      unrelated longer names ('Asia', 'Fantasia', 'Persia') — require the
      shorter string to cover at least half the longer one, not just appear
      anywhere in it, so a short exact-name query only boosts near-length
      matches (aliases, stylization drift) rather than any superstring.
    """
    if not key or not other_key:
        return False
    if key == other_key:
        return True
    if len(key) < min_len or len(other_key) < min_len:
        return False
    shorter, longer = sorted((key, other_key), key=len)
    if shorter not in longer:
        return False
    return len(shorter) / len(longer) >= 0.5


def _check_find_artists(conn: sqlite3.Connection, query: str, threshold: float = 0.72) -> list:
    """Fuzzy-match query against artists.name and artist_aliases.alias.

    Returns a list of dicts: {id, name, score, matched_on, alias} sorted by score desc,
    deduped by artist id (best match per artist wins). Checks both the raw table and
    the alias table so a search for an aliased/former/native-script name still surfaces
    the canonical artist row.
    """
    key = _norm(query)
    if not key:
        return []
    candidates = {}

    for aid, name in conn.execute('SELECT id, name FROM artists').fetchall():
        name_key = _norm(name)
        score = difflib.SequenceMatcher(None, key, name_key).ratio()
        if _check_substring_boost(key, name_key):
            score = max(score, 0.9)
        if score >= threshold:
            candidates[aid] = {'id': aid, 'name': name, 'score': score,
                                'matched_on': 'name', 'alias': None}

    for aid, alias, name in conn.execute(
        'SELECT aa.artist_id, aa.alias, a.name FROM artist_aliases aa '
        'JOIN artists a ON a.id = aa.artist_id'
    ).fetchall():
        alias_key = _norm(alias)
        score = difflib.SequenceMatcher(None, key, alias_key).ratio()
        if _check_substring_boost(key, alias_key):
            score = max(score, 0.9)
        if score >= threshold and (aid not in candidates or score > candidates[aid]['score']):
            candidates[aid] = {'id': aid, 'name': name, 'score': score,
                                'matched_on': 'alias', 'alias': alias}

    return sorted(candidates.values(), key=lambda c: -c['score'])


def _check_find_releases(conn: sqlite3.Connection, query: str, artist_id: 'str | None' = None,
                         threshold: float = 0.72) -> list:
    """Fuzzy-match query against releases.title, optionally scoped to one artist.

    Returns a list of dicts: {id, title, artist_name, score} sorted by score desc.
    """
    key = _norm(query)
    if not key:
        return []
    sql = ('SELECT r.id, r.title, a.name, r.hidden FROM releases r '
           'JOIN artists a ON a.id = r.primary_artist_id')
    params = []
    if artist_id:
        sql += ' WHERE r.primary_artist_id = ?'
        params.append(artist_id)
    results = []
    for rid, title, artist_name, hidden in conn.execute(sql, params).fetchall():
        rkey = _norm(title)
        score = difflib.SequenceMatcher(None, key, rkey).ratio()
        if _check_substring_boost(key, rkey):
            score = max(score, 0.9)
        if score >= threshold:
            results.append({'id': rid, 'title': title, 'artist_name': artist_name,
                            'score': score, 'hidden': bool(hidden)})
    return sorted(results, key=lambda r: -r['score'])


def cmd_check(args):
    """Fuzzy-check whether an artist (and optionally an album) already exists.

    Checks artists.name AND artist_aliases in one shot, then (if --album is given)
    checks that artist's releases for a title match, catching stylization/
    punctuation drift so real duplicates surface before an import is attempted.
    """
    with managed_db(args.db or DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        threshold = args.threshold
        artists = _check_find_artists(conn, args.artist, threshold)

        if args.json:
            out = {'artist_query': args.artist, 'artist_matches': artists}
            if args.album:
                out['album_query'] = args.album
                out['album_matches'] = (
                    _check_find_releases(conn, args.album, artists[0]['id'], threshold) if artists
                    else _check_find_releases(conn, args.album, threshold=threshold)
                )
            print(json.dumps(out, indent=2, ensure_ascii=False))
            return

        if not artists:
            console.print(f'[green]No existing artist matches[/green] for {args.artist!r} — safe to import as new.')
        else:
            console.print(f'[yellow]Possible existing artist(s)[/yellow] for {args.artist!r}:')
            for a in artists:
                via = f"  [dim](via alias {a['alias']!r})[/dim]" if a['matched_on'] == 'alias' else ''
                console.print(f"  {a['score']*100:5.1f}%  {a['id']}  {a['name']}{via}")

        if args.album:
            scoped_id = artists[0]['id'] if artists else None
            releases = _check_find_releases(conn, args.album, scoped_id, threshold)
            if not releases:
                console.print(f'[green]No existing release matches[/green] for {args.album!r} — safe to import as new.')
            else:
                console.print(f'[yellow]Possible existing release(s)[/yellow] for {args.album!r}:')
                for r in releases:
                    hidden_tag = '  [dim](hidden)[/dim]' if r['hidden'] else ''
                    console.print(f"  {r['score']*100:5.1f}%  {r['id']}  {r['artist_name']} — {r['title']}{hidden_tag}")


_AUDIT_MEANINGFUL_ETI_WORDS: frozenset = frozenset({
    'remix', 'refix', 'rework', 'bootleg', 'mashup', 'live', 'acoustic',
    'demo', 'instrumental', 'a cappella', 'reprise', 'cover', 'flip',
    'rerecorded',
})


def _audit_variant_word(text: 'str | None') -> 'str | None':
    """Return a normalized variant descriptor (remix name, 'live', 'acoustic', ...)
    extracted from a title's ETI, or None if the title carries no MEANINGFUL
    qualifier (a marker of a genuinely different recording).

    Deliberately excludes cosmetic/production tags — "2015 Remaster",
    "Radio Edit", "Extended Version", "Explicit", disc-locale markers, etc.
    Those describe the SAME recording in a different master/edit and are
    overwhelmingly what a naive raw-vs-matched ETI diff flags (noise this
    heuristic exists specifically to filter out). What actually indicates a
    different recording — the failure mode behind every real mismatch found
    in practice (remix-vs-original, live-vs-studio, instrumental-vs-vocal) —
    is one of _AUDIT_MEANINGFUL_ETI_WORDS. Reuses parse_track_title so this
    stays consistent with how titles are classified during import.
    """
    if not text:
        return None
    eti = parse_track_title(text).eti
    if not eti:
        return None
    words = normalize_text(eti.strip('()'))
    if not any(w in words for w in _AUDIT_MEANINGFUL_ETI_WORDS):
        return None
    return words


_AUDIT_ARTIST_SPLIT_RE = re.compile(r'\s*(?:&|/|,|\bx\b|\bvs\.?\b|\band\b)\s*', re.IGNORECASE)


def _audit_split_artist(raw_artist: str, known_artist_names: set) -> list:
    """Split a compound raw artist string ("JAY-Z & Kanye West") into parts.

    Scrobble sources frequently record a collaboration's full billing as one
    raw_artist_name string even though each artist is credited on the track
    individually. Splitting lets the artist heuristic check "is any of these
    names credited" instead of false-flagging every legitimate collab.

    Only splits when the raw string as a WHOLE isn't itself a known artist
    name/alias — plenty of real acts are named "X and Y" or "A & B" or
    contain a bare "x" (Tegan and Sara, Above & Beyond, TOMORROW X TOGETHER),
    and splitting those would shred a real single-artist name into garbage.
    """
    if normalize_text(raw_artist) in known_artist_names:
        return [raw_artist]
    parts = [p.strip() for p in _AUDIT_ARTIST_SPLIT_RE.split(raw_artist) if p.strip()]
    return parts or [raw_artist]


def cmd_audit_matches(args):
    """Flag listens whose track_id points somewhere plausibly wrong.

    Three targeted heuristics (deliberately narrow — a blunt raw-vs-matched
    title diff drowns in cosmetic noise: censoring, romanization, medley
    slashes, bonus-track suffixes). Each one catches a distinct failure mode
    seen in practice when a raw scrobble title has no exact track to match
    and the fuzzy matcher falls back to the closest available title:

      variant   — raw title names a specific remix/live/acoustic/etc. cut
                  that differs from (or is absent from) the matched track's
                  own qualifier. Catches "matched to the wrong edit."
      feat      — raw title's "feat. X" doesn't appear among the matched
                  track's credited artists (any role). Catches "matched to
                  a same-titled track with different featured artists."
      artist    — raw scrobble artist isn't credited on the matched track
                  at all (checked against name + all aliases). Catches
                  "matched to a completely different song."

    Scope with --artist / --release-id; otherwise scans everything, which is
    slow on a large catalog — prefer scoping right after a batch of imports.
    """
    db_path = args.db or DB_PATH
    with managed_db(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        where = ["l.track_id IS NOT NULL"]
        params: list = []

        artist_filter_id = None
        if args.artist:
            row = resolve_artist(conn, args.artist)
            if not row:
                console.print(f'[red]Artist not found:[/red] {args.artist}')
                sys.exit(1)
            artist_filter_id = row['id']
            where.append('''t.id IN (
                SELECT track_id FROM track_artists WHERE artist_id = ?
            )''')
            params.append(artist_filter_id)

        if args.release_id:
            where.append('t.release_id = ?')
            params.append(args.release_id)

        if args.since:
            # _parse_user_date returns a normalized YYYY / YYYY-MM / YYYY-MM-DD
            # *string*, not a timestamp — pad partial dates to the 1st and
            # convert to a UTC epoch second to compare against listens.timestamp,
            # which is always stored as UTC epoch (see sync.py).
            since_str = _parse_user_date(args.since)
            if since_str is None:
                console.print(f'[red]Could not parse --since date:[/red] {args.since}')
                sys.exit(1)
            parts = since_str.split('-')
            padded = parts + ['01'] * (3 - len(parts))
            since_dt = datetime(int(padded[0]), int(padded[1]), int(padded[2]), tzinfo=timezone.utc)
            where.append('l.timestamp >= ?')
            params.append(int(since_dt.timestamp()))

        rows = cur.execute(f'''
            SELECT l.id as listen_id, l.raw_artist_name, l.raw_track_name,
                   l.raw_album_name, l.track_id, t.title as track_title,
                   t.mix_name, r.id as release_id, r.title as release_title,
                   r.type_secondary as release_type_secondary
            FROM listens l
            JOIN tracks t ON t.id = l.track_id
            JOIN releases r ON r.id = t.release_id
            WHERE {' AND '.join(where)}
        ''', params).fetchall()

        if not args.json:
            console.print(f'[dim]Auditing {len(rows):,} matched listen(s)…[/dim]')

        # Preload credited-artist names (+aliases) per track, and per-release
        # track lists (for a friendlier "did you mean" hint on artist flags).
        track_ids = list({r['track_id'] for r in rows})
        credited_by_track: dict[str, set] = {}
        if track_ids:
            for i in range(0, len(track_ids), 500):
                chunk = track_ids[i:i + 500]
                ph = ','.join('?' for _ in chunk)
                for tid, aname, alias in cur.execute(f'''
                    SELECT ta.track_id, a.name, aa.alias
                    FROM track_artists ta
                    JOIN artists a ON a.id = ta.artist_id
                    LEFT JOIN artist_aliases aa ON aa.artist_id = a.id
                    WHERE ta.track_id IN ({ph})
                ''', chunk):
                    s = credited_by_track.setdefault(tid, set())
                    s.add(normalize_text(aname))
                    if alias:
                        s.add(normalize_text(alias))

        # Every known artist name/alias in the catalog — used to recognize
        # when a raw_artist_name that LOOKS like a compound billing ("Fitz
        # and the Tantrums") is actually one real act's own name, so the
        # artist heuristic below doesn't shred it into nonsense parts.
        known_artist_names = {normalize_text(n) for (n,) in cur.execute('SELECT name FROM artists')}
        known_artist_names |= {normalize_text(a) for (a,) in cur.execute('SELECT alias FROM artist_aliases')}

        findings = {'variant': [], 'feat': [], 'artist': []}
        # dedup identical (category, raw, matched) triples
        seen_keys = set()

        for row in rows:
            raw_track  = row['raw_track_name'] or ''
            raw_artist = row['raw_artist_name'] or ''
            matched_title = row['track_title'] or ''
            credited = credited_by_track.get(row['track_id'], set())

            # -- artist heuristic --------------------------------------------
            # Split compound billings ("JAY-Z & Kanye West") and require NONE
            # of the parts to be credited before flagging — a legitimate
            # collab where each artist is credited individually shouldn't
            # trip this just because the raw string names both at once.
            raw_artist_parts = _audit_split_artist(raw_artist, known_artist_names) if raw_artist else []
            if raw_artist_parts and not any(normalize_text(p) in credited for p in raw_artist_parts):
                key = ('artist', normalize_text(raw_artist), row['track_id'])
                if key not in seen_keys:
                    seen_keys.add(key)
                    findings['artist'].append(row)

            # -- variant heuristic ---------------------------------------------
            # Only flag when the RAW title names a meaningful variant
            # (remix/live/acoustic/instrumental/etc.) that the matched track
            # itself carries no trace of — that asymmetry is what "matched to
            # the wrong edit because the right one was never imported" looks
            # like. The reverse (matched track happens to be a live/remix
            # version but the raw title doesn't say so) is usually benign —
            # it's often the only version of that title actually in the
            # catalog — so it's deliberately not flagged here.
            #
            # Special case: raw title says "Live" but the release itself is
            # already tagged type_secondary='live' (e.g. At Folsom Prison) —
            # every track on it is inherently a live recording, so a bare
            # "- Live" suffix on the raw scrobble isn't evidence of a wrong
            # match, just redundant labeling. Only suppress the 'live' marker
            # in that case; other qualifiers still apply.
            raw_variant = _audit_variant_word(raw_track)
            if raw_variant == 'live' and row['release_type_secondary'] == 'live':
                raw_variant = None
            matched_variant = _audit_variant_word(matched_title) or \
                (normalize_text(row['mix_name']) if row['mix_name'] else None)
            if raw_variant and raw_variant != matched_variant:
                key = ('variant', normalize_text(raw_track), row['track_id'])
                if key not in seen_keys:
                    seen_keys.add(key)
                    findings['variant'].append(row)

            # -- feat heuristic --------------------------------------------
            # Flag only when NONE of the extracted feat names are credited,
            # not "any single one missing" — parse_track_title splits on
            # "and", so a solo artist name containing "and" (e.g. "Christine
            # and the Queens") gets split into pieces that individually miss,
            # even though the credit itself is correct. Requiring zero
            # overlap avoids that false positive while still catching a
            # track matched to a same-titled release with wholly different
            # featured artists.
            feat_artists = parse_track_title(raw_track).feat_artists
            if feat_artists and not any(normalize_text(f) in credited for f in feat_artists):
                key = ('feat', normalize_text(raw_track), row['track_id'])
                if key not in seen_keys:
                    seen_keys.add(key)
                    findings['feat'].append(row)

        total = sum(len(v) for v in findings.values())

        if args.json:
            out = {
                cat: [dict(r) for r in rs]
                for cat, rs in findings.items()
            }
            print(json.dumps(out, indent=2, ensure_ascii=False))
            return

        if total == 0:
            console.print('[green]No suspicious matches found.[/green]')
            return

        labels = {
            'artist':  'Raw artist not credited on matched track',
            'variant': 'Variant/remix qualifier mismatch',
            'feat':    'Raw "feat." artist not credited on matched track',
        }
        for cat, rs in findings.items():
            if not rs:
                continue
            console.print(f'\n[yellow]{labels[cat]}[/yellow]  ({len(rs)})')
            for r in rs[:args.limit]:
                console.print(
                    f"  [dim]{r['raw_artist_name']!r} — {r['raw_track_name']!r}[/dim]"
                    f"  →  [bold]{r['track_title']}[/bold]"
                    f"  [dim]({r['release_title']}, track {r['track_id']})[/dim]"
                )
            if len(rs) > args.limit:
                console.print(f'  [dim]… and {len(rs) - args.limit} more (raise --limit)[/dim]')

        console.print(f'\n[dim]{total} suspicious match(es) across {len(rows):,} audited.[/dim]')


def cmd_show(args):
    """Print every column of a release/artist/track row, by ID."""
    with managed_db(args.db or DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        kind, row = _show_find_entity(conn, args.id)
        if row is None:
            console.print(f'[red]No release, artist, or track found with id {args.id!r}[/red]')
            return

        data = dict(row)
        if args.json:
            print(json.dumps(data, indent=2, ensure_ascii=False))
            return

        label = data.get('title') or data.get('name') or ''
        t = Table(box=rbox.SIMPLE_HEAD, show_header=False, pad_edge=False, show_edge=False)
        t.add_column('field', style='dim', no_wrap=True)
        t.add_column('value', overflow='fold')
        for field, value in data.items():
            if field in ('created_at', 'updated_at'):
                epoch = _show_to_epoch(value)
                if epoch:
                    value = f'{value}  ({datetime.fromtimestamp(epoch).strftime("%Y-%m-%d %H:%M:%S")})'
            t.add_row(field, '' if value is None else str(value))
        console.print(Rule(f'[bold]{kind}[/bold]  [dim]{label}[/dim]', style='bright_blue'))
        console.print(t)


def cmd_show_recent(args):
    """List the most recently created/edited releases, artists, and tracks."""
    with managed_db(args.db or DB_PATH) as conn:
        rows = []
        for kind, (table, name_col) in _SHOW_TABLES.items():
            for r in conn.execute(
                f'SELECT id, {name_col} AS name, created_at, updated_at FROM {table} '
                f'WHERE updated_at IS NOT NULL ORDER BY updated_at DESC LIMIT ?',
                [args.limit],
            ).fetchall():
                rows.append({
                    'id': r[0], 'name': r[1], 'type': kind,
                    'created_at': _show_to_epoch(r[2]), 'updated_at': _show_to_epoch(r[3]),
                })
        rows.sort(key=lambda r: r['updated_at'] or 0, reverse=True)
        rows = rows[:args.limit]

        if args.json:
            print(json.dumps(rows, indent=2, ensure_ascii=False))
            return

        t = Table(box=rbox.SIMPLE_HEAD, show_header=True, pad_edge=False, show_edge=False)
        t.add_column('id', style='dim', no_wrap=True)
        t.add_column('name', overflow='fold')
        t.add_column('type', no_wrap=True)
        t.add_column('edited', no_wrap=True)
        for r in rows:
            is_new = r['created_at'] and r['updated_at'] and abs(r['updated_at'] - r['created_at']) < 5
            edited_str = datetime.fromtimestamp(r['updated_at']).strftime('%Y-%m-%d %H:%M:%S') if r['updated_at'] else ''
            tag = '[green]created[/green]' if is_new else '[yellow]edited[/yellow]'
            t.add_row(r['id'], r['name'] or '', f'{r["type"]}  {tag}', edited_str)
        console.print(t)
        console.print(f'[dim]{len(rows)} most recently touched entities[/dim]')


def cmd_admin_pin(args):
    db_path = getattr(args, 'db', None) or DB_PATH
    pin = getpass.getpass('New PIN: ')
    confirm = getpass.getpass('Confirm PIN: ')
    if pin != confirm:
        console.print('[red]PINs do not match.[/red]')
        return
    salt = os.urandom(16).hex()
    h = hashlib.pbkdf2_hmac('sha256', pin.encode(), salt.encode(), 100_000).hex()
    conn = open_db(db_path)
    conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("INSERT OR REPLACE INTO settings (key,value) VALUES ('admin_pin_salt',?)", (salt,))
    conn.execute("INSERT OR REPLACE INTO settings (key,value) VALUES ('admin_pin_hash',?)", (h,))
    conn.commit()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    console.print('[green]Admin PIN set.[/green]')


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(
        prog='mdb',
        description='Music database import and enrichment tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest='cmd', required=True)

    # import
    p = sub.add_parser('import', help='Import a release from any source URL')
    p.add_argument('albums', nargs='+', metavar='ALBUM',
                   help='URL (Spotify/MB/Beatport/Apple Music/Bandcamp) or batch file')
    p.add_argument('--no-mb',       action='store_true', help='Skip MusicBrainz lookup')
    p.add_argument('--no-aoty',     action='store_true', help='Skip AOTY enrichment')
    p.add_argument('--no-wiki',     action='store_true', help='Skip Wikipedia date lookup')
    p.add_argument('--no-gtin',     action='store_true', help='Skip GTIN cross-platform discovery')
    p.add_argument('--no-variants', action='store_true', help='Skip MB release-group variant selection')
    p.add_argument('--auto',        action='store_true', help='Apply enrichment without prompting')
    p.add_argument('--json',        action='store_true',
                   help='Also print a final JSON summary line '
                        '({"release_id","title","artist","date","warnings"})')
    p.add_argument('--db',          metavar='PATH',      help='Path to master.sqlite')
    p.set_defaults(func=cmd_import)

    # discography
    p_disc = sub.add_parser('discography', help='Import a full discography from a YAML or wikitext file')
    p_disc.add_argument('discography', metavar='FILE',
                        help='YAML file with album_title + article (Wikipedia URL) per entry, '
                             'or --wikitext dump')
    p_disc.add_argument('--wikitext', action='store_true',
                        help='FILE is a wikitext dump (Wikipedia "Discography" sections), '
                             'one artist per `%%%%%% Artist Name` block, instead of YAML')
    p_disc.add_argument('--sections', metavar='LIST', default='studio albums,extended plays,eps',
                        help='Comma-separated heading names to pull rows from '
                             '(--wikitext only; default: %(default)r)')
    p_disc.add_argument('--artist',   metavar='NAME', default='',
                        help='Artist name hint for MB title-search fallback')
    p_disc.add_argument('--no-aoty',  action='store_true', help='Skip AOTY enrichment')
    p_disc.add_argument('--no-wiki',  action='store_true', help='Skip Wikipedia date lookup')
    p_disc.add_argument('--db',       metavar='PATH',      help='Path to master.sqlite')
    p_disc.set_defaults(func=cmd_discography)

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
    p_aoty.add_argument('--auto',    action='store_true', help='Auto-accept without prompting')
    p_aoty.add_argument('--force',   action='store_true', help='Re-process and overwrite even if already enriched')
    p_aoty.add_argument('--verbose', action='store_true', help='Debug scraping output')
    p_aoty.set_defaults(func=cmd_enrich_aoty)

    p_dates = es.add_parser('dates', help='Look up release dates via Wikipedia + MusicBrainz')
    _add_filter_args(p_dates)
    p_dates.add_argument('--force',   action='store_true', help='Overwrite existing dates')
    p_dates.add_argument('--verbose', action='store_true', help='Debug output')
    p_dates.set_defaults(func=cmd_enrich_dates)

    p_tracks = es.add_parser('tracks', help='Fetch track MBIDs from MusicBrainz')
    _add_filter_args(p_tracks)
    p_tracks.add_argument('--force', action='store_true', help='Re-fetch even if MBID already present')
    p_tracks.add_argument('--missing-tracks', dest='missing_tracks', action='store_true',
                          help='Re-fetch full tracklist for MB releases that have 0 track rows')
    p_tracks.set_defaults(func=cmd_enrich_tracks)

    p_audio = es.add_parser('audio', help='Fetch Spotify audio features (BPM, energy, etc.)')
    _add_filter_args(p_audio)
    p_audio.add_argument('--force', action='store_true', help='Re-fetch even if already populated')

    p_deezer = es.add_parser('deezer-links', help='Backfill Deezer external links via UPC lookup')
    _add_filter_args(p_deezer)
    p_deezer.set_defaults(func=cmd_enrich_deezer_links)
    p_audio.set_defaults(func=cmd_enrich_audio)

    p_apple = es.add_parser('apple-links', help='Backfill Apple Music IDs via UPC lookup')
    _add_filter_args(p_apple)
    p_apple.set_defaults(func=cmd_enrich_apple_links)

    p_apple_verify = es.add_parser('apple-verify',
        help='Strictly re-check apple_music_id against Apple\'s own title+artist (flags only, no rewrites)')
    _add_filter_args(p_apple_verify)
    p_apple_verify.set_defaults(func=cmd_enrich_apple_verify)

    p_apple_review = es.add_parser('apple-review',
        help='Interactively resolve releases apple-verify flagged, via keypress + inline chafa previews')
    p_apple_review.add_argument('--limit', type=int, help='Review at most N releases this session')
    p_apple_review.add_argument('--no-preview', action='store_true',
                                 help='Skip chafa image rendering — text-only, much faster to page through')
    p_apple_review.add_argument('--db', metavar='PATH', help='Path to master.sqlite')
    p_apple_review.set_defaults(func=cmd_enrich_apple_review)

    p_sp_links = es.add_parser('spotify-links', help='Backfill Spotify album IDs via UPC lookup')
    _add_filter_args(p_sp_links)
    p_sp_links.set_defaults(func=cmd_enrich_spotify_links)

    p_thumbs = es.add_parser('thumbnails', help='Backfill small album art thumbnails from Spotify')
    _add_filter_args(p_thumbs)
    p_thumbs.add_argument('--force', action='store_true', help='Re-fetch even if a thumb is already set')
    p_thumbs.set_defaults(func=cmd_enrich_thumbnails)

    p_art_dims = es.add_parser('art-dims',
        help='Backfill real pixel dimensions for album art (detects low-res art without re-downloading everything each time)')
    _add_filter_args(p_art_dims)
    p_art_dims.add_argument('--force', action='store_true', help='Re-check even if dimensions are already stored')
    p_art_dims.set_defaults(func=cmd_enrich_art_dims)

    p_art_verify = es.add_parser('art-verify',
        help='Perceptual-hash check that thumb and large art are the same photo at different sizes (read-only)')
    _add_filter_args(p_art_verify)
    p_art_verify.add_argument('--list-id', metavar='ID', help='Scope to one canonical list (e.g. apple-music-100)')
    p_art_verify.add_argument('--threshold', type=int, default=2,
                               help='Max Hamming distance before flagging a mismatch (default: 2)')
    p_art_verify.add_argument('--json', action='store_true',
                               help='Also print a final JSON summary line '
                                    '({"checked","same_asset","mismatches","errors"})')
    p_art_verify.set_defaults(func=cmd_enrich_art_verify)

    p_desc = es.add_parser('descriptions', help="Scrape Apple Music 'About this Album' editorial notes")
    _add_filter_args(p_desc)
    p_desc.add_argument('--force', action='store_true', help='Re-fetch even if editorial_note already present')
    p_desc.set_defaults(func=cmd_enrich_descriptions)

    p_artists_enrich = es.add_parser('artists', help='Fetch artist metadata from MusicBrainz')
    _add_filter_args(p_artists_enrich)
    p_artists_enrich.add_argument('--force', action='store_true', help='Re-fetch even if already populated')
    p_artists_enrich.add_argument('--spotify', action='store_true',
                                  help='Also fetch Spotify photo/followers/popularity for artists missing them '
                                       '(one command instead of the manual search+curl+UPDATE loop)')
    p_artists_enrich.set_defaults(func=cmd_enrich_artists)

    p_art = es.add_parser('art', help='Fill in or replace album art (CAA → Spotify → manual URL)')
    _add_filter_args(p_art)
    p_art.add_argument('--force',       action='store_true', help='Re-process releases that already have art')
    p_art.add_argument('--interactive', action='store_true', help='Prompt for each release instead of auto-applying')
    p_art.set_defaults(func=cmd_enrich_art)

    p_soundtracks = es.add_parser('soundtracks', help='Tag soundtrack releases with source type, region, and language')
    _add_filter_args(p_soundtracks)
    p_soundtracks.add_argument('--force', action='store_true', help='Re-prompt releases already fully tagged')
    p_soundtracks.set_defaults(func=cmd_enrich_soundtracks_wrapper)

    p_popularity = es.add_parser('popularity', help='Refresh Spotify popularity snapshots for artists, releases, and tracks')
    _add_filter_args(p_popularity)
    p_popularity.add_argument('--force', action='store_true', help='Re-fetch even if already populated')
    p_popularity.set_defaults(func=cmd_enrich_popularity)

    p_sp_tracks = es.add_parser('spotify-tracks',
                                 help='Backfill missing track-level Spotify IDs via ISRC match')
    _add_filter_args(p_sp_tracks)
    p_sp_tracks.set_defaults(func=cmd_enrich_spotify_tracks)

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
    p_del.add_argument('--purge',  action='store_true',
                       help='Hard-delete listen rows instead of unlinking them')
    p_del.add_argument('-y', '--yes', action='store_true',
                       help='Skip confirmation prompt')
    p_del.add_argument('--db', metavar='PATH')
    p_del.set_defaults(func=cmd_delete)

    # artist
    p_artist = sub.add_parser('artist', help='Manage artist metadata')
    as_      = p_artist.add_subparsers(dest='artist_cmd', required=True)
    p_img    = as_.add_parser('images', help='Bulk update artist profile images from CSV')
    p_img.add_argument('csv_file', metavar='CSV',
                       help='CSV with columns: artist_name, profile_image_url')
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
    p_ra_add.add_argument('--source', default='manual')
    p_ra_add.add_argument('--type', dest='type', default='dsp',
                          choices=['transliteration', 'unicode', 'native_script', 'translation', 'dsp'],
                          help='Alias type (default: dsp)')
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

    p_discogs = ls_.add_parser('discogs',
                                help='Associate a DB release with its Discogs release ID')
    p_discogs.add_argument('release', metavar='RELEASE',
                           help='DB release ID, Spotify URL/ID, or MusicBrainz UUID')
    p_discogs.add_argument('discogs_id', metavar='DISCOGS_ID', type=int,
                           help='Discogs release integer ID (from the release URL)')
    p_discogs.add_argument('--db', metavar='PATH')
    p_discogs.set_defaults(func=cmd_link_discogs)

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
    p_al_add.add_argument('--source', default='manual')
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
    p_genres = sub.add_parser('genres', help='Manage monthly genre profiles')
    gs_      = p_genres.add_subparsers(dest='genres_cmd', required=True)
    p_g_ref  = gs_.add_parser('refresh', help='Recompute monthly genre profiles (Taste Over Time)')
    p_g_ref.add_argument('--tree', metavar='PATH', help='Tab-indented genre tree (default: music/genre_tree.txt)')
    p_g_ref.add_argument('--db',   metavar='PATH', help='Path to master.sqlite')
    p_g_ref.set_defaults(func=cmd_genres_refresh)

    p_certs  = sub.add_parser('certs', help='Manage certification tiers')
    cs_      = p_certs.add_subparsers(dest='certs_cmd', required=True)
    p_c_ref  = cs_.add_parser('refresh', help='Recompute gold/platinum/diamond tiers for all artists')
    p_c_ref.add_argument('--db', metavar='PATH', help='Path to master.sqlite')
    p_c_ref.set_defaults(func=cmd_certs_refresh)

    # stats
    p_stats  = sub.add_parser('stats', help='Manage precomputed stats cache')
    ss_      = p_stats.add_subparsers(dest='stats_cmd', required=True)
    p_s_ref  = ss_.add_parser('refresh', help='Recompute the stats.js cache + artist year-medals')
    p_s_ref.add_argument('--db', metavar='PATH', help='Path to master.sqlite')
    p_s_ref.add_argument('--verbose', action='store_true', help='Print per-section row counts + timings')
    p_s_ref.set_defaults(func=cmd_stats_refresh)

    # genre-relations
    p_gr = sub.add_parser('genre-relations', help='Populate genre parent/child relations from tree file')
    p_gr.add_argument('--tree', metavar='PATH', help='Path to tab-indented genre tree file')
    p_gr.add_argument('--db',   metavar='PATH', help='Path to master.sqlite')
    p_gr.set_defaults(func=cmd_genre_relations)

    p_grs = sub.add_parser('genre-relations-sync',
                            help="Rebuild genre parent/child relations from AOTY's live genre pages")
    p_grs.add_argument('--limit', type=int, metavar='N', help='Only scrape the first N genres (testing)')
    p_grs.add_argument('--db',    metavar='PATH', help='Path to master.sqlite')
    p_grs.set_defaults(func=cmd_genre_relations_sync)

    # list (canonical lists: RS500, AFI-style "top N albums" trackers)
    p_list = sub.add_parser('list', help='Manage canonical album lists (RS500, etc.)')
    ls2_    = p_list.add_subparsers(dest='list_cmd', required=True)

    p_l_import = ls2_.add_parser('import-csv', help='Create/refresh a canonical list from a ranked CSV')
    p_l_import.add_argument('--id', required=True, metavar='SLUG', help="List id, e.g. 'rs500-2020'")
    p_l_import.add_argument('--name', required=True, metavar='NAME', help='Full display name')
    p_l_import.add_argument('--short-name', dest='short_name', metavar='NAME', help='Compact label for tight UI')
    p_l_import.add_argument('--source-url', dest='source_url', metavar='URL')
    p_l_import.add_argument('--csv', required=True, metavar='PATH', help='CSV or .json file path')
    p_l_import.add_argument('--rank-col', metavar='COL', help='CSV column holding the rank (CSV only, required for CSV)')
    p_l_import.add_argument('--artist-col', default='Artist', metavar='COL')
    p_l_import.add_argument('--album-col', default='Album', metavar='COL')
    p_l_import.add_argument('--year-col', default='Year', metavar='COL')
    p_l_import.add_argument('--db', metavar='PATH', help='Path to master.sqlite')
    p_l_import.set_defaults(func=cmd_list_import_csv)

    p_l_match = ls2_.add_parser('match', help='Match list entries to existing releases (read-only)')
    p_l_match.add_argument('--id', required=True, metavar='SLUG')
    p_l_match.add_argument('--force', action='store_true', help='Re-check already-matched entries too')
    p_l_match.add_argument('--db', metavar='PATH', help='Path to master.sqlite')
    p_l_match.set_defaults(func=cmd_list_match)

    p_l_status = ls2_.add_parser('status', help='Print completion summary for one or all lists')
    p_l_status.add_argument('--id', metavar='SLUG', help='Limit to one list (default: all)')
    p_l_status.add_argument('--db', metavar='PATH', help='Path to master.sqlite')
    p_l_status.set_defaults(func=cmd_list_status)

    p_doctor = sub.add_parser('doctor', help='Read-only DB audit — backfill opportunities and data anomalies')
    p_doctor.add_argument('--json', action='store_true', help='Emit findings as JSON')
    p_doctor.add_argument('--db',   metavar='PATH', help='Path to master.sqlite')
    p_doctor.set_defaults(func=cmd_doctor)

    p_dedup = sub.add_parser('dedup', help='Find and resolve duplicate releases interactively')
    p_dedup.add_argument('--artist', metavar='NAME', help='Limit to a specific artist')
    p_dedup.add_argument('--report', action='store_true',
        help='List candidate duplicate groups and exit — read-only, no prompts, safe for automation/CI')
    p_dedup.add_argument('--db',     metavar='PATH', help='Path to master.sqlite')
    p_dedup.set_defaults(func=cmd_dedup)

    p_coll = sub.add_parser('collection', help='Manage physical media collection')
    cs_    = p_coll.add_subparsers(dest='collection_cmd', required=True)

    p_coll_sync = cs_.add_parser('sync-discogs',
        help='Sync the physical collection directly from the Discogs API')
    p_coll_sync.add_argument('--auto', action='store_true',
        help='Skip and queue ambiguous items instead of prompting interactively')
    p_coll_sync.add_argument('--force', action='store_true',
        help='Reprocess every item, not just ones new since the last sync')
    p_coll_sync.add_argument('--json', action='store_true',
        help='Emit a structured summary instead of console output; implies --auto')
    p_coll_sync.add_argument('--db', metavar='PATH')
    p_coll_sync.set_defaults(func=cmd_collection_sync_discogs)

    p_coll_img = cs_.add_parser('set-image',
        help='Assign a pressing photo URL to one owned copy of a release')
    p_coll_img.add_argument('release', metavar='RELEASE', help='Release ID, Spotify/MB ID, or title')
    p_coll_img.add_argument('url', metavar='URL', help='Pressing photo URL (hotlinked, not downloaded)')
    p_coll_img.add_argument('--medium', default='vinyl', help='Media type to update (default: vinyl)')
    p_coll_img.add_argument('--collection-item-id', type=int, metavar='ID',
        help='Disambiguate when a release has more than one owned copy')
    p_coll_img.add_argument('--db', metavar='PATH')
    p_coll_img.set_defaults(func=cmd_collection_set_image)

    p_adminpin = sub.add_parser('admin-pin', help='Set or reset the admin view PIN')
    p_adminpin.add_argument('--db', metavar='PATH', help='Path to master.sqlite')
    p_adminpin.set_defaults(func=cmd_admin_pin)

    p_checkpoint = sub.add_parser('checkpoint', help='Run the full publish pipeline (certs, stats, wal-checkpoint, integrity, prod-db, gzip, jekyll build, verify)')
    p_checkpoint.add_argument('--skip-jekyll', action='store_true',
                              help='Stop after gzip; skip jekyll build + _site verification')
    p_checkpoint.add_argument('--db', metavar='PATH')
    p_checkpoint.set_defaults(func=cmd_checkpoint)

    p_check = sub.add_parser('check', help='Fuzzy-check whether an artist/album already exists before importing')
    p_check.add_argument('artist', metavar='ARTIST', help='Artist name to check (matches name + aliases)')
    p_check.add_argument('album', metavar='ALBUM', nargs='?', help='Optional album/release title to check')
    p_check.add_argument('--threshold', type=float, default=0.72,
                         help='Minimum fuzzy-match score 0-1 to report a candidate (default: 0.72)')
    p_check.add_argument('--json', action='store_true', help='Print as JSON instead of a table')
    p_check.add_argument('--db', metavar='PATH')
    p_check.set_defaults(func=cmd_check)

    p_audit = sub.add_parser('audit', help='Audit existing data for likely mistakes')
    audit_sub = p_audit.add_subparsers(dest='audit_cmd', required=True)

    p_audit_matches = audit_sub.add_parser(
        'matches',
        help='Flag listens likely matched to the wrong track (variant/feat/artist mismatches)'
    )
    p_audit_matches.add_argument('--artist', metavar='NAME_OR_ID',
                                  help='Scope to one artist (by name, slug, ULID, or Spotify ID)')
    p_audit_matches.add_argument('--release-id', metavar='ID', help='Scope to one release')
    p_audit_matches.add_argument('--since', metavar='DATE',
                                  help='Only audit listens on/after this date (e.g. 2026-07-01)')
    p_audit_matches.add_argument('--limit', type=int, default=20,
                                  help='Max example rows to print per category (default: 20)')
    p_audit_matches.add_argument('--json', action='store_true', help='Print as JSON instead of a report')
    p_audit_matches.add_argument('--db', metavar='PATH')
    p_audit_matches.set_defaults(func=cmd_audit_matches)

    p_show = sub.add_parser('show', help='Print a release/artist/track row, or --recent for recently touched entities')
    p_show.add_argument('id', nargs='?', help='Release, artist, or track ID (omit with --recent)')
    p_show.add_argument('--recent', action='store_true', help='List the most recently created/edited entities')
    p_show.add_argument('--limit', type=int, default=50, help='With --recent: max rows to show (default: 50)')
    p_show.add_argument('--json', action='store_true', help='Print as JSON instead of a table')
    p_show.add_argument('--db', metavar='PATH')
    p_show.set_defaults(func=lambda a: cmd_show_recent(a) if a.recent else cmd_show(a))

    args = parser.parse_args()

    # Configure logging once, for every subcommand — the scrapers log warnings
    # that would otherwise go nowhere.
    logging.basicConfig(
        level=logging.DEBUG if getattr(args, 'verbose', False) else logging.WARNING,
        format='  [%(levelname)s] %(message)s',
    )

    if getattr(args, 'cmd', None) == 'show' and not args.recent and not args.id:
        p_show.error('the following arguments are required: id (or pass --recent)')
    try:
        args.func(args)
    except KeyboardInterrupt:
        console.print('\n[dim]Interrupted.[/dim]')
        sys.exit(0)


if __name__ == '__main__':
    main()
