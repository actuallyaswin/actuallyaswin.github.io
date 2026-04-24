"""mdb_ops — Database infrastructure for master.sqlite."""

import difflib
import os
import random
import re
import sqlite3
import time
import unicodedata
from contextlib import contextmanager

from mdb_strings import (
    resolve_title,
    ascii_key,
    normalize_text,
    normalize_upc,
    parse_track_title as _parse_track_title,
    _should_update_date,
    is_soundtrack_title,
)

# ── Paths ─────────────────────────────────────────────────────────────────────

_DIR    = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_DIR, 'master.sqlite')

# ── external_links enum constants ─────────────────────────────────────────────

EL_ARTIST  = 0  # entity_type: artist
EL_RELEASE = 1  # entity_type: release

EL_SVC_WIKIPEDIA   = 0
EL_SVC_MUSICBRAINZ = 1
EL_SVC_SPOTIFY     = 2
EL_SVC_APPLE_MUSIC = 3
EL_SVC_DEEZER      = 4
EL_SVC_TIDAL       = 5
EL_SVC_BANDCAMP    = 6
EL_SVC_BEATPORT    = 7

# ── ULID / slug ────────────────────────────────────────────────────────────────

_ULID_CHARS = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'


def new_ulid() -> str:
    t = int(time.time() * 1000)
    ts = ''
    for _ in range(10):
        ts = _ULID_CHARS[t % 32] + ts
        t //= 32
    return ts + ''.join(random.choices(_ULID_CHARS, k=16))


def slugify(text: str) -> str:
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    text = re.sub(r"'", '', text.lower())
    return re.sub(r'[^a-z0-9]+', '-', text).strip('-')


def unique_slug(base: str, existing: set) -> str:
    slug, n = base, 2
    while slug in existing:
        slug = f'{base}-{n}'
        n += 1
    return slug


# ── .env ──────────────────────────────────────────────────────────────────────

def load_dotenv() -> None:
    """Load key=value pairs from music/.env into os.environ (no-op if absent)."""
    try:
        with open(os.path.join(_DIR, '.env')) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, _, v = line.partition('=')
                v = v.strip().strip('"\'')
                for marker in (' #', ' //'):
                    idx = v.find(marker)
                    if idx != -1:
                        v = v[:idx].strip().strip('"\'')
                os.environ.setdefault(k.strip(), v)
    except FileNotFoundError:
        pass


# ── Schema ─────────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS artists (
    id             TEXT PRIMARY KEY,
    slug           TEXT UNIQUE,
    name           TEXT NOT NULL,
    sort_name      TEXT,
    aliases        TEXT,
    spotify_id     TEXT UNIQUE,
    mbid           TEXT UNIQUE,
    lastfm_url     TEXT,
    image_url      TEXT,
    image_source   TEXT,
    image_position TEXT,
    hero_image_url TEXT,
    country        TEXT,
    formed_year    INTEGER,
    disbanded_year INTEGER,
    bio            TEXT,
    hidden         INTEGER NOT NULL DEFAULT 0,
    notes          TEXT,
    created_at     INTEGER,
    updated_at     INTEGER,
    aoty_id        INTEGER,
    aoty_url       TEXT,
    wikipedia_url  TEXT,
    type           TEXT,
    gender         TEXT,
    disambiguation TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS artists_aoty_id ON artists(aoty_id) WHERE aoty_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS artists_slug    ON artists(slug)    WHERE slug    IS NOT NULL;
CREATE TABLE IF NOT EXISTS artist_aliases (
    artist_id  TEXT NOT NULL REFERENCES artists(id) ON DELETE CASCADE,
    alias      TEXT NOT NULL,
    alias_type TEXT CHECK(alias_type IN ('past_name', 'native_script', 'common')) NOT NULL DEFAULT 'common',
    language   TEXT,
    source     TEXT CHECK(source IN ('musicbrainz', 'lastfm', 'manual')) NOT NULL DEFAULT 'manual',
    sort_order INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (artist_id, alias)
);
CREATE INDEX IF NOT EXISTS artist_aliases_lower ON artist_aliases (lower(alias));
CREATE TABLE IF NOT EXISTS artist_relations (
    from_artist_id TEXT NOT NULL REFERENCES artists(id) ON DELETE CASCADE,
    to_artist_id   TEXT NOT NULL REFERENCES artists(id) ON DELETE CASCADE,
    relation_type  TEXT NOT NULL CHECK(relation_type IN ('member', 'collaboration', 'side_project')),
    source         TEXT CHECK(source IN ('musicbrainz', 'manual')) NOT NULL DEFAULT 'manual',
    PRIMARY KEY (from_artist_id, to_artist_id, relation_type)
);
CREATE TABLE IF NOT EXISTS releases (
    id                 TEXT PRIMARY KEY,
    slug               TEXT NOT NULL,
    title              TEXT NOT NULL,
    primary_artist_id  TEXT REFERENCES artists(id),
    type               TEXT,
    type_secondary     TEXT,
    release_date       TEXT,
    release_year       INTEGER,
    date_source        TEXT,
    label              TEXT,
    spotify_id         TEXT UNIQUE,
    mbid               TEXT UNIQUE,
    release_group_mbid TEXT,
    apple_music_id     TEXT,
    aoty_id            INTEGER UNIQUE,
    aoty_url           TEXT,
    wikipedia_url      TEXT,
    album_art_url      TEXT,
    album_art_source   TEXT,
    album_art_position TEXT,
    total_tracks       INTEGER,
    aoty_score_critic   INTEGER,
    aoty_score_user     REAL,
    aoty_ratings_critic INTEGER,
    aoty_ratings_user   INTEGER,
    hidden             INTEGER NOT NULL DEFAULT 0,
    notes              TEXT,
    created_at         INTEGER,
    updated_at         INTEGER,
    UNIQUE (primary_artist_id, slug)
);
CREATE TABLE IF NOT EXISTS release_aliases (
    release_id     TEXT NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
    alias          TEXT NOT NULL,
    is_definitive  INTEGER NOT NULL DEFAULT 0,
    source         TEXT CHECK(source IN ('musicbrainz', 'manual')) NOT NULL DEFAULT 'manual',
    PRIMARY KEY (release_id, alias)
);
CREATE INDEX IF NOT EXISTS release_aliases_lower ON release_aliases (lower(alias));
CREATE TABLE IF NOT EXISTS release_artists (
    release_id TEXT NOT NULL REFERENCES releases(id),
    artist_id  TEXT NOT NULL REFERENCES artists(id),
    role       TEXT NOT NULL DEFAULT 'main',
    PRIMARY KEY (release_id, artist_id)
);
CREATE TABLE IF NOT EXISTS tracks (
    id           TEXT PRIMARY KEY,
    title        TEXT NOT NULL,
    release_id   TEXT REFERENCES releases(id),
    track_number INTEGER,
    disc_number  INTEGER NOT NULL DEFAULT 1,
    duration_ms  INTEGER,
    is_explicit  INTEGER NOT NULL DEFAULT 0,
    spotify_id   TEXT UNIQUE,
    mbid         TEXT UNIQUE,
    isrc         TEXT,
    tempo_bpm    REAL,
    audio_features TEXT,
    mix_name     TEXT,
    musical_key  TEXT,
    beatport_genre TEXT,
    beatport_sub_genre TEXT,
    canonical_track_id  TEXT REFERENCES tracks(id),
    track_variant_type  TEXT,
    hidden       INTEGER NOT NULL DEFAULT 0,
    notes        TEXT,
    created_at   INTEGER,
    updated_at   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_tracks_canonical
    ON tracks(canonical_track_id)
    WHERE canonical_track_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS track_artists (
    track_id  TEXT NOT NULL REFERENCES tracks(id),
    artist_id TEXT NOT NULL REFERENCES artists(id),
    role      TEXT NOT NULL DEFAULT 'main',
    PRIMARY KEY (track_id, artist_id, role)
);
CREATE TABLE IF NOT EXISTS genres (
    aoty_id INTEGER PRIMARY KEY,
    name    TEXT NOT NULL UNIQUE,
    slug    TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS release_genres (
    release_id    TEXT    NOT NULL REFERENCES releases(id),
    aoty_genre_id INTEGER NOT NULL REFERENCES genres(aoty_id),
    is_primary    INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (release_id, aoty_genre_id)
);
CREATE TABLE IF NOT EXISTS genre_relations (
    parent_aoty_id INTEGER NOT NULL REFERENCES genres(aoty_id),
    child_aoty_id  INTEGER NOT NULL REFERENCES genres(aoty_id),
    PRIMARY KEY (parent_aoty_id, child_aoty_id)
);
CREATE TABLE IF NOT EXISTS monthly_genre_profile (
    year               INTEGER NOT NULL,
    month              INTEGER NOT NULL,
    listen_count       INTEGER NOT NULL DEFAULT 0,
    color_hex          TEXT    NOT NULL DEFAULT '#64748B',
    top_genre_color_hex TEXT   NOT NULL DEFAULT '#64748B',
    dominant_genre     TEXT,
    genres_json        TEXT,
    PRIMARY KEY (year, month)
);
CREATE TABLE IF NOT EXISTS listens (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       INTEGER NOT NULL,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    track_id        TEXT REFERENCES tracks(id),
    raw_track_name  TEXT,
    raw_artist_name TEXT,
    raw_album_name  TEXT,
    raw_source_id   TEXT,
    source          TEXT NOT NULL DEFAULT 'lastfm'
);
CREATE TABLE IF NOT EXISTS legacy_track_map (
    lastfm_id    TEXT PRIMARY KEY,
    track_id     TEXT REFERENCES tracks(id),
    match_method TEXT,
    confidence   REAL
);
CREATE TABLE IF NOT EXISTS release_variants (
    canonical_id TEXT NOT NULL REFERENCES releases(id),
    variant_id   TEXT NOT NULL REFERENCES releases(id),
    variant_type TEXT,
    sort_order   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (canonical_id, variant_id)
);
CREATE TABLE IF NOT EXISTS release_sources (
    compilation_id TEXT NOT NULL REFERENCES releases(id),
    source_id      TEXT NOT NULL REFERENCES releases(id),
    disc_number    INTEGER,
    PRIMARY KEY (compilation_id, source_id)
);
CREATE TABLE IF NOT EXISTS artist_members (
    group_artist_id  TEXT NOT NULL REFERENCES artists(id) ON DELETE CASCADE,
    member_artist_id TEXT NOT NULL REFERENCES artists(id) ON DELETE CASCADE,
    sort_order       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (group_artist_id, member_artist_id)
);
CREATE TABLE IF NOT EXISTS external_links (
    entity_type INTEGER NOT NULL,  -- 0=artist, 1=release
    entity_id   TEXT    NOT NULL,  -- ULID
    service     INTEGER NOT NULL,  -- 0=wikipedia 1=musicbrainz 2=spotify 3=apple_music 4=deezer 5=tidal 6=bandcamp 7=beatport
    link_value  TEXT    NOT NULL,  -- Wikipedia: page_id str; Bandcamp: full URL; others: platform ID/slug
    PRIMARY KEY (entity_type, entity_id, service)
);
CREATE TABLE IF NOT EXISTS release_soundtrack_meta (
    release_id        TEXT PRIMARY KEY REFERENCES releases(id),
    source_type       TEXT CHECK(source_type IN ('film','video_game','tv_series','musical','podcast','other')),
    industry_region   TEXT,  -- ISO 3166-1 alpha-2 (US, IN, GB, JP, ES, ...)
    original_language TEXT   -- ISO 639-1 (en, hi, ta, es, ja, ...)
);
"""


def open_db(path=None) -> sqlite3.Connection:
    conn = sqlite3.connect(path or DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON')
    conn.execute('PRAGMA journal_mode = WAL')
    conn.execute('PRAGMA synchronous = NORMAL')
    conn.execute('PRAGMA busy_timeout = 10000')
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    # Additive column migrations run before executescript so that any
    # indexes in SCHEMA that reference new columns find them already present.
    for ddl in [
        "ALTER TABLE releases ADD COLUMN release_group_mbid TEXT",
        "ALTER TABLE releases ADD COLUMN wikipedia_url TEXT",
        "ALTER TABLE releases ADD COLUMN date_source TEXT",
        "ALTER TABLE releases ADD COLUMN aoty_score_critic INTEGER",
        "ALTER TABLE releases ADD COLUMN aoty_score_user REAL",
        "ALTER TABLE releases ADD COLUMN aoty_ratings_critic INTEGER",
        "ALTER TABLE releases ADD COLUMN aoty_ratings_user INTEGER",
        "ALTER TABLE tracks ADD COLUMN tempo_bpm REAL",
        "ALTER TABLE tracks ADD COLUMN audio_features TEXT",
        "ALTER TABLE artists ADD COLUMN mbid TEXT",
        "ALTER TABLE artists ADD COLUMN aoty_id INTEGER",
        "ALTER TABLE artists ADD COLUMN aoty_url TEXT",
        "ALTER TABLE artists ADD COLUMN wikipedia_url TEXT",
        "ALTER TABLE artists ADD COLUMN slug TEXT",
        "ALTER TABLE artists ADD COLUMN type TEXT",
        "ALTER TABLE artists ADD COLUMN gender TEXT",
        "ALTER TABLE artists ADD COLUMN disambiguation TEXT",
        "ALTER TABLE artists ADD COLUMN is_supergroup INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE artists ADD COLUMN mb_attempted INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE artists ADD COLUMN cert TEXT",
        "ALTER TABLE release_aliases ADD COLUMN is_definitive INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE artist_aliases ADD COLUMN alias_type TEXT NOT NULL DEFAULT 'common'",
        "ALTER TABLE artist_aliases ADD COLUMN language TEXT",
        "ALTER TABLE artist_aliases ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE tracks ADD COLUMN canonical_track_id TEXT REFERENCES tracks(id)",
        "ALTER TABLE tracks ADD COLUMN track_variant_type TEXT",
        "ALTER TABLE artists ADD COLUMN spotify_popularity INTEGER",
        "ALTER TABLE artists ADD COLUMN spotify_followers INTEGER",
        "ALTER TABLE releases ADD COLUMN spotify_popularity INTEGER",
        "ALTER TABLE tracks ADD COLUMN spotify_popularity INTEGER",
        "ALTER TABLE tracks ADD COLUMN mix_name TEXT",
        "ALTER TABLE tracks ADD COLUMN musical_key TEXT",
        "ALTER TABLE tracks ADD COLUMN beatport_genre TEXT",
        "ALTER TABLE tracks ADD COLUMN beatport_sub_genre TEXT",
        # Listens columns added by sync.py in earlier versions — centralised here
        "ALTER TABLE listens ADD COLUMN ms_played INTEGER",
        "ALTER TABLE listens ADD COLUMN skipped INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE releases ADD COLUMN upc TEXT",
    ]:
        try:
            conn.execute(ddl)
        except Exception:
            pass  # column already exists

    # Create genre_relations if it doesn't exist yet
    conn.execute('''
        CREATE TABLE IF NOT EXISTS genre_relations (
            parent_aoty_id INTEGER NOT NULL REFERENCES genres(aoty_id),
            child_aoty_id  INTEGER NOT NULL REFERENCES genres(aoty_id),
            PRIMARY KEY (parent_aoty_id, child_aoty_id)
        )
    ''')
    conn.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS listens_ts_src
        ON listens(timestamp, raw_source_id)
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS monthly_genre_profile (
            year               INTEGER NOT NULL,
            month              INTEGER NOT NULL,
            listen_count       INTEGER NOT NULL DEFAULT 0,
            color_hex          TEXT    NOT NULL DEFAULT '#64748B',
            top_genre_color_hex TEXT   NOT NULL DEFAULT '#64748B',
            dominant_genre     TEXT,
            genres_json        TEXT,
            PRIMARY KEY (year, month)
        )
    ''')
    conn.commit()

    # Migrate genres to use aoty_id as primary key (drop old auto-increment id)
    rg_cols = {row[1] for row in conn.execute('PRAGMA table_info(release_genres)').fetchall()}
    if 'genre_id' in rg_cols and 'aoty_genre_id' not in rg_cols:
        conn.executescript("""
            PRAGMA foreign_keys = OFF;
            CREATE TABLE genres_new (
                aoty_id INTEGER PRIMARY KEY,
                name    TEXT NOT NULL UNIQUE,
                slug    TEXT NOT NULL UNIQUE
            );
            INSERT OR IGNORE INTO genres_new (aoty_id, name, slug)
                SELECT aoty_id, name, slug FROM genres WHERE aoty_id IS NOT NULL;
            CREATE TABLE release_genres_new (
                release_id    TEXT    NOT NULL REFERENCES releases(id),
                aoty_genre_id INTEGER NOT NULL REFERENCES genres_new(aoty_id),
                is_primary    INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (release_id, aoty_genre_id)
            );
            INSERT OR IGNORE INTO release_genres_new (release_id, aoty_genre_id, is_primary)
                SELECT rg.release_id, g.aoty_id, rg.is_primary
                FROM release_genres rg
                JOIN genres g ON rg.genre_id = g.id
                WHERE g.aoty_id IS NOT NULL;
            DROP TABLE release_genres;
            DROP TABLE genres;
            ALTER TABLE genres_new RENAME TO genres;
            ALTER TABLE release_genres_new RENAME TO release_genres;
            PRAGMA foreign_keys = ON;
        """)
        conn.commit()

    # Migrate release_variants: drop CHECK constraint on variant_type so it can
    # store comma-separated multi-type values like 'anniversary,remix'.
    rv_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='release_variants'"
    ).fetchone()
    if rv_row and 'CHECK' in rv_row[0]:
        conn.executescript("""
            PRAGMA foreign_keys = OFF;
            CREATE TABLE release_variants_new (
                canonical_id TEXT NOT NULL REFERENCES releases(id),
                variant_id   TEXT NOT NULL REFERENCES releases(id),
                variant_type TEXT,
                sort_order   INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (canonical_id, variant_id)
            );
            INSERT INTO release_variants_new SELECT * FROM release_variants;
            DROP TABLE release_variants;
            ALTER TABLE release_variants_new RENAME TO release_variants;
            PRAGMA foreign_keys = ON;
        """)
        conn.commit()

    conn.executescript(SCHEMA)
    conn.commit()


def upsert_external_link(conn: sqlite3.Connection,
                         entity_type: int, entity_id: str,
                         service: int, link_value: str) -> None:
    """Insert or update a single external link row."""
    conn.execute(
        'INSERT INTO external_links (entity_type, entity_id, service, link_value)'
        ' VALUES (?, ?, ?, ?)'
        ' ON CONFLICT(entity_type, entity_id, service)'
        ' DO UPDATE SET link_value = excluded.link_value',
        (entity_type, entity_id, service, link_value),
    )


# ── Import helpers ─────────────────────────────────────────────────────────────

def _best_image(images: list) -> 'str | None':
    if not images:
        return None
    return sorted(images, key=lambda i: (i.get('height') or 0) * (i.get('width') or 0),
                  reverse=True)[0]['url']


def _sp_type(album: dict) -> str:
    return {'album': 'album', 'single': 'single', 'compilation': 'compilation'}.get(
        album.get('album_type', '').lower(), 'album')


_VARIOUS_ARTISTS_SPOTIFY_ID = '0LyfQWJT6nXafLPZqxe9Of'


def _is_various_artists(sp_artist: dict) -> bool:
    return (sp_artist.get('id') == _VARIOUS_ARTISTS_SPOTIFY_ID
            or sp_artist.get('name', '').lower() == 'various artists')


def upsert_artist(cur, sp_artist: dict) -> 'tuple[str, bool]':
    """Insert or update an artist from Spotify data. Returns (artist_id, created)."""
    sid = sp_artist['id']
    row = cur.execute('SELECT id FROM artists WHERE spotify_id = ?', (sid,)).fetchone()
    now = int(time.time())
    if row:
        cur.execute(
            'UPDATE artists SET name = ?, image_url = ?, image_source = ?,'
            ' spotify_popularity = ?, spotify_followers = ?, updated_at = ?'
            ' WHERE id = ?',
            (sp_artist['name'], _best_image(sp_artist.get('images', [])), 'spotify',
             sp_artist.get('popularity'), sp_artist.get('followers', {}).get('total'),
             now, row[0])
        )
        return row[0], False
    base     = slugify(sp_artist['name'])
    existing = {r[0] for r in cur.execute('SELECT slug FROM artists WHERE slug IS NOT NULL').fetchall()}
    slug     = unique_slug(base, existing)
    aid      = new_ulid()
    cur.execute(
        'INSERT INTO artists (id, slug, name, spotify_id, image_url, image_source,'
        ' spotify_popularity, spotify_followers, created_at, updated_at)'
        ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (aid, slug, sp_artist['name'], sid,
         _best_image(sp_artist.get('images', [])), 'spotify',
         sp_artist.get('popularity'), sp_artist.get('followers', {}).get('total'),
         now, now)
    )
    return aid, True


def upsert_release(cur, sp_album: dict, primary_artist_id: str) -> 'tuple[str, bool]':
    """Insert or update a release from Spotify album data. Returns (release_id, created)."""
    sid          = sp_album['id']
    row          = cur.execute(
        'SELECT id, release_date, date_source FROM releases WHERE spotify_id = ?', (sid,)
    ).fetchone()
    now          = int(time.time())
    release_date = sp_album.get('release_date', '')
    precision    = sp_album.get('release_date_precision', 'year')
    if precision == 'month':
        release_date = release_date[:7]
    elif precision != 'day':
        release_date = release_date[:4]
    release_year   = int(release_date[:4]) if release_date else None
    art            = _best_image(sp_album.get('images', []))
    type_secondary = 'soundtrack' if is_soundtrack_title(sp_album['name']) else None

    if row:
        update_date = _should_update_date(row['release_date'], row['date_source'],
                                          release_date, 'spotify')
        date_fields = ', release_date = ?, release_year = ?, date_source = ?' if update_date else ''
        date_vals   = (release_date, release_year, 'spotify') if update_date else ()
        cur.execute(
            f'UPDATE releases SET title = ?, primary_artist_id = ?, type = ?,'
            f' type_secondary = COALESCE(type_secondary, ?){date_fields},'
            f' label = ?, album_art_url = ?, album_art_source = ?, total_tracks = ?,'
            f' spotify_popularity = ?, updated_at = ? WHERE id = ?',
            (sp_album['name'], primary_artist_id, _sp_type(sp_album),
             type_secondary, *date_vals,
             sp_album.get('label'), art, 'spotify',
             sp_album.get('total_tracks'), sp_album.get('popularity'), now, row['id'])
        )
        return row['id'], False

    base       = slugify(sp_album['name'])
    existing   = {r[0] for r in cur.execute(
        'SELECT slug FROM releases WHERE primary_artist_id = ?', (primary_artist_id,)).fetchall()}
    slug       = unique_slug(base, existing)
    release_id = new_ulid()
    cur.execute(
        'INSERT INTO releases (id, slug, title, primary_artist_id, type, type_secondary,'
        ' release_date, release_year, label, spotify_id, album_art_url, album_art_source,'
        ' total_tracks, spotify_popularity, date_source, created_at, updated_at)'
        ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (release_id, slug, sp_album['name'], primary_artist_id, _sp_type(sp_album),
         type_secondary, release_date, release_year, sp_album.get('label'), sid, art, 'spotify',
         sp_album.get('total_tracks'), sp_album.get('popularity'), 'spotify', now, now)
    )
    return release_id, True


def upsert_tracks(cur, release_id: str, full_tracks: list,
                  artist_map: dict, mb_by_isrc: dict, mb_by_title: dict) -> 'tuple[int, int]':
    """Upsert tracks from Spotify full-track objects enriched with MB MBID dicts.
    Returns (created_count, updated_count)."""
    created = updated = 0
    now     = int(time.time())
    for sp in full_tracks:
        if sp is None:
            continue
        sid        = sp['id']
        isrc       = (sp.get('external_ids') or {}).get('isrc')
        track_mbid = mb_by_isrc.get(isrc) if isrc else None
        if not track_mbid:
            track_mbid = mb_by_title.get(ascii_key(sp.get('name', '')))
        row = cur.execute('SELECT id, title FROM tracks WHERE spotify_id = ?', (sid,)).fetchone()
        if row:
            track_id, existing_title = row[0], row[1]
            title_to_store = resolve_title(sp['name'], existing_title)
            try:
                cur.execute(
                    'UPDATE tracks SET release_id = ?, title = ?, track_number = ?,'
                    ' disc_number = ?, duration_ms = ?, is_explicit = ?, isrc = ?,'
                    ' mbid = ?, spotify_popularity = ?, updated_at = ? WHERE id = ?',
                    (release_id, title_to_store, sp.get('track_number'), sp.get('disc_number', 1),
                     sp.get('duration_ms'), 1 if sp.get('explicit') else 0,
                     isrc, track_mbid, sp.get('popularity'), now, track_id)
                )
            except sqlite3.IntegrityError:
                # MBID already assigned to a different track — update without mbid
                cur.execute(
                    'UPDATE tracks SET release_id = ?, title = ?, track_number = ?,'
                    ' disc_number = ?, duration_ms = ?, is_explicit = ?, isrc = ?,'
                    ' spotify_popularity = ?, updated_at = ? WHERE id = ?',
                    (release_id, title_to_store, sp.get('track_number'), sp.get('disc_number', 1),
                     sp.get('duration_ms'), 1 if sp.get('explicit') else 0,
                     isrc, sp.get('popularity'), now, track_id)
                )
            updated += 1
        else:
            track_id       = new_ulid()
            title_to_store = resolve_title(sp['name'])
            try:
                cur.execute(
                    'INSERT INTO tracks (id, title, release_id, track_number, disc_number,'
                    ' duration_ms, is_explicit, spotify_id, mbid, isrc,'
                    ' spotify_popularity, created_at, updated_at)'
                    ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (track_id, title_to_store, release_id, sp.get('track_number'),
                     sp.get('disc_number', 1), sp.get('duration_ms'),
                     1 if sp.get('explicit') else 0, sid, track_mbid, isrc,
                     sp.get('popularity'), now, now)
                )
            except sqlite3.IntegrityError:
                # MBID collision — insert without mbid
                cur.execute(
                    'INSERT INTO tracks (id, title, release_id, track_number, disc_number,'
                    ' duration_ms, is_explicit, spotify_id, isrc,'
                    ' spotify_popularity, created_at, updated_at)'
                    ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (track_id, title_to_store, release_id, sp.get('track_number'),
                     sp.get('disc_number', 1), sp.get('duration_ms'),
                     1 if sp.get('explicit') else 0, sid, isrc,
                     sp.get('popularity'), now, now)
                )
            created += 1
        cur.execute('DELETE FROM track_artists WHERE track_id = ?', (track_id,))
        for i, a in enumerate(sp.get('artists', [])):
            aid = artist_map.get(a['id'])
            if aid:
                try:
                    cur.execute(
                        'INSERT INTO track_artists (track_id, artist_id, role) VALUES (?, ?, ?)',
                        (track_id, aid, 'main' if i == 0 else 'featured')
                    )
                except sqlite3.IntegrityError:
                    pass
    return created, updated


# ── MusicBrainz upsert helpers ─────────────────────────────────────────────────

def upsert_artist_mb(cur, mb_artist: dict) -> 'tuple[str, bool]':
    """Insert or update an artist from a MusicBrainz artist-credit 'artist' sub-dict.
    Returns (artist_id, created)."""
    mbid = (mb_artist.get('id') or '').strip()
    name = (mb_artist.get('name') or '').strip()
    now  = int(time.time())

    # Match by MBID first (most reliable)
    if mbid:
        row = cur.execute('SELECT id FROM artists WHERE mbid = ?', (mbid,)).fetchone()
        if row:
            cur.execute('UPDATE artists SET name = ?, updated_at = ? WHERE id = ?',
                        (name, now, row[0]))
            return row[0], False

    # Match by name — artist may exist from a prior Spotify import with no MBID yet
    row = cur.execute(
        'SELECT id FROM artists WHERE lower(name) = lower(?)', (name,)
    ).fetchone()
    if row:
        if mbid:
            try:
                cur.execute('UPDATE artists SET mbid = ?, updated_at = ? WHERE id = ?',
                            (mbid, now, row[0]))
            except sqlite3.IntegrityError:
                pass  # MBID already on a different row — leave it
        return row[0], False

    # New artist
    base     = slugify(name)
    existing = {r[0] for r in cur.execute(
        'SELECT slug FROM artists WHERE slug IS NOT NULL').fetchall()}
    slug = unique_slug(base, existing)
    aid  = new_ulid()
    cur.execute(
        'INSERT INTO artists (id, slug, name, mbid, created_at, updated_at)'
        ' VALUES (?, ?, ?, ?, ?, ?)',
        (aid, slug, name, mbid or None, now, now),
    )
    return aid, True


def upsert_release_mb(cur, mb_data: dict, primary_artist_id: 'str | None',
                      image_url: 'str | None' = None) -> 'tuple[str, bool]':
    """Insert or update a release from a MusicBrainz release data dict.
    Returns (release_id, created)."""
    mbid      = (mb_data.get('id') or '').strip()
    title     = (mb_data.get('title') or '').strip()
    date_raw  = (mb_data.get('date') or '').strip()
    rel_year  = int(date_raw[:4]) if date_raw and date_raw[:4].isdigit() else None
    rg        = mb_data.get('release-group') or {}
    rg_mbid   = rg.get('id')
    rg_type   = (rg.get('primary-type') or '').lower() or 'album'
    rg_secondary_types = [s.lower() for s in (rg.get('secondary-types') or [])]
    # Derive type_secondary from MB release group, then fall back to title detection
    _known_secondary = {'compilation', 'soundtrack', 'live', 'remix', 'dj-mix',
                        'mixtape', 'demo', 'spokenword', 'interview', 'audiobook',
                        'audio drama', 'field recording'}
    type_secondary = next((s for s in rg_secondary_types if s in _known_secondary), None)
    if type_secondary is None and is_soundtrack_title(title):
        type_secondary = 'soundtrack'
    now       = int(time.time())

    label = ''
    for info in (mb_data.get('label-info') or []):
        lbl = (info.get('label') or {}).get('name', '')
        if lbl:
            label = lbl
            break

    barcode = (mb_data.get('barcode') or '').strip()
    upc = normalize_upc(barcode) if barcode else None

    total_tracks = sum(
        len(m.get('tracks') or []) for m in (mb_data.get('media') or [])
    )

    row = cur.execute(
        'SELECT id, release_date, date_source, album_art_url FROM releases WHERE mbid = ?',
        (mbid,),
    ).fetchone()

    if row:
        update_date = _should_update_date(row['release_date'], row['date_source'],
                                          date_raw, 'musicbrainz')
        date_fields = ', release_date = ?, release_year = ?, date_source = ?' if update_date else ''
        date_vals   = (date_raw, rel_year, 'musicbrainz') if update_date else ()
        cur.execute(
            f'UPDATE releases SET title = ?, primary_artist_id = ?, type = ?,'
            f' type_secondary = COALESCE(type_secondary, ?){date_fields},'
            f' label = ?, release_group_mbid = ?, total_tracks = ?,'
            f' upc = COALESCE(upc, ?), updated_at = ?'
            f' WHERE id = ?',
            (title, primary_artist_id, rg_type, type_secondary, *date_vals,
             label or None, rg_mbid, total_tracks, upc, now, row['id']),
        )
        if image_url and not row['album_art_url']:
            cur.execute(
                'UPDATE releases SET album_art_url = ?, album_art_source = ? WHERE id = ?',
                (image_url, 'coverartarchive', row['id']),
            )
        return row['id'], False

    base     = slugify(title)
    existing = {r[0] for r in cur.execute(
        'SELECT slug FROM releases WHERE primary_artist_id IS ?',
        (primary_artist_id,),
    ).fetchall()}
    slug       = unique_slug(base, existing)
    release_id = new_ulid()
    cur.execute(
        'INSERT INTO releases (id, slug, title, primary_artist_id, type, type_secondary,'
        ' release_date, release_year, label, mbid, release_group_mbid, album_art_url,'
        ' album_art_source, total_tracks, upc, date_source, created_at, updated_at)'
        ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (release_id, slug, title, primary_artist_id, rg_type, type_secondary,
         date_raw or None, rel_year, label or None,
         mbid or None, rg_mbid,
         image_url, 'coverartarchive' if image_url else None,
         total_tracks, upc, 'musicbrainz' if date_raw else None,
         now, now),
    )
    return release_id, True


def upsert_release_beatport(cur, beatport_id: int, title: str,
                            primary_artist_id: 'str | None',
                            date_raw: str, label: str,
                            image_url: 'str | None',
                            total_tracks: int,
                            album_type: str) -> 'tuple[str, bool]':
    """Insert or update a release from Beatport data.
    Identifies existing releases via the external_links table (EL_SVC_BEATPORT).
    Returns (release_id, created)."""
    now = int(time.time())

    row = cur.execute(
        'SELECT el.entity_id FROM external_links el'
        ' WHERE el.entity_type = ? AND el.service = ? AND el.link_value = ?',
        (EL_RELEASE, EL_SVC_BEATPORT, str(beatport_id)),
    ).fetchone()

    if row:
        release_id  = row[0]
        date_fields = ''
        date_vals: tuple = ()
        if date_raw:
            rel_year    = int(date_raw[:4]) if date_raw[:4].isdigit() else None
            date_fields = ', release_date = ?, release_year = ?, date_source = ?'
            date_vals   = (date_raw, rel_year, 'beatport')
        cur.execute(
            f'UPDATE releases SET title = ?, primary_artist_id = ?, type = ?{date_fields},'
            f' label = ?, total_tracks = ?, updated_at = ? WHERE id = ?',
            (title, primary_artist_id, album_type, *date_vals,
             label or None, total_tracks, now, release_id),
        )
        if image_url:
            cur.execute(
                'UPDATE releases SET album_art_url = COALESCE(album_art_url, ?),'
                ' album_art_source = COALESCE(album_art_source, ?) WHERE id = ?',
                (image_url, 'beatport', release_id),
            )
        return release_id, False

    rel_year   = int(date_raw[:4]) if date_raw and date_raw[:4].isdigit() else None
    base       = slugify(title)
    existing   = {r[0] for r in cur.execute(
        'SELECT slug FROM releases WHERE primary_artist_id IS ?', (primary_artist_id,)
    ).fetchall()}
    slug       = unique_slug(base, existing)
    release_id = new_ulid()
    cur.execute(
        'INSERT INTO releases (id, slug, title, primary_artist_id, type,'
        ' release_date, release_year, label, album_art_url, album_art_source,'
        ' total_tracks, date_source, created_at, updated_at)'
        ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (release_id, slug, title, primary_artist_id, album_type,
         date_raw or None, rel_year, label or None,
         image_url, 'beatport' if image_url else None,
         total_tracks, 'beatport' if date_raw else None,
         now, now),
    )
    return release_id, True


def upsert_tracks_mb(cur, release_id: str, mb_tracks: list,
                     artist_map: dict, *,
                     no_release_reassign: bool = False) -> 'tuple[int, int]':
    """Upsert tracks from a MusicBrainzRelease's .tracks list.
    Each track dict has: name, duration_ms, _mb_recording_id, _isrcs,
    _disc_number, _track_number, _artist_credit.
    artist_map: {mb_artist_id: our_artist_id}.
    no_release_reassign: if True, skip (don't move) tracks already owned by
      a different release — avoids ping-pong when called from --missing-tracks.
    Returns (created_count, updated_count)."""
    created = updated = 0
    now     = int(time.time())

    for i, t in enumerate(mb_tracks):
        rec_id  = t.get('_mb_recording_id')
        isrcs   = t.get('_isrcs') or []
        isrc    = isrcs[0] if isrcs else None
        title   = t.get('name', '')
        dur     = t.get('duration_ms')
        disc    = t.get('_disc_number') or 1
        tr_num  = t.get('_track_number') or (i + 1)

        row = None
        if rec_id:
            row = cur.execute(
                'SELECT id, title, release_id FROM tracks WHERE mbid = ?', (rec_id,)
            ).fetchone()
        if not row and isrc:
            row = cur.execute(
                'SELECT id, title, release_id FROM tracks WHERE isrc = ?', (isrc,)
            ).fetchone()

        if row:
            # Skip tracks owned by a different release when reassignment is forbidden
            if no_release_reassign and row[2] and row[2] != release_id:
                continue
            track_id       = row[0]
            title_to_store = resolve_title(title, row[1])
            cur.execute(
                'UPDATE tracks SET release_id = ?, title = ?, track_number = ?,'
                ' disc_number = ?, duration_ms = ?, isrc = ?, mbid = ?, updated_at = ?'
                ' WHERE id = ?',
                (release_id, title_to_store, tr_num, disc, dur,
                 isrc, rec_id, now, track_id),
            )
            updated += 1
        else:
            track_id       = new_ulid()
            title_to_store = resolve_title(title)
            cur.execute(
                'INSERT INTO tracks (id, title, release_id, track_number, disc_number,'
                ' duration_ms, mbid, isrc, created_at, updated_at)'
                ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (track_id, title_to_store, release_id, tr_num, disc,
                 dur, rec_id, isrc, now, now),
            )
            created += 1

        cur.execute('DELETE FROM track_artists WHERE track_id = ?', (track_id,))
        for j, credit in enumerate(t.get('_artist_credit') or []):
            if not isinstance(credit, dict) or 'artist' not in credit:
                continue
            our_id = artist_map.get(credit['artist'].get('id', ''))
            if our_id:
                try:
                    cur.execute(
                        'INSERT INTO track_artists (track_id, artist_id, role)'
                        ' VALUES (?, ?, ?)',
                        (track_id, our_id, 'main' if j == 0 else 'featured'),
                    )
                except sqlite3.IntegrityError:
                    pass

    return created, updated


def populate_genre_relations(conn: sqlite3.Connection, tree_path: str) -> tuple[int, int]:
    """Parse a tab-indented genre tree file and populate genre_relations.

    Each line's indentation level determines its depth; the immediate parent
    is the nearest ancestor at depth-1.  A genre can appear multiple times
    under different parents — each occurrence creates a distinct relation row.

    Only inserts relations where both parent and child exist in the genres table.
    Returns (inserted, skipped_unknown) counts.
    """
    # Build name → aoty_id lookup from DB
    name_to_id = {
        row[0]: row[1]
        for row in conn.execute('SELECT name, aoty_id FROM genres').fetchall()
    }

    # Parse tree into (depth, name) pairs
    entries = []
    with open(tree_path, encoding='utf-8') as f:
        for line in f:
            stripped = line.rstrip('\n')
            if not stripped.strip():
                continue
            depth = len(stripped) - len(stripped.lstrip('\t'))
            name  = stripped.strip()
            entries.append((depth, name))

    # Walk entries, maintaining a parent-stack keyed by depth
    inserted       = 0
    skipped        = 0
    parent_stack   = {}  # depth → name

    for depth, name in entries:
        parent_stack[depth] = name
        # Remove any stale deeper entries
        for d in list(parent_stack):
            if d > depth:
                del parent_stack[d]

        if depth == 0:
            continue  # top-level genre — no parent to link

        parent_name = parent_stack.get(depth - 1)
        if not parent_name:
            continue

        parent_id = name_to_id.get(parent_name)
        child_id  = name_to_id.get(name)

        if parent_id is None or child_id is None:
            skipped += 1
            continue

        try:
            conn.execute(
                'INSERT OR IGNORE INTO genre_relations (parent_aoty_id, child_aoty_id)'
                ' VALUES (?, ?)',
                (parent_id, child_id),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    return inserted, skipped


# ── AOTY / Wikipedia DB persistence ───────────────────────────────────────────

def save_aoty_data(conn: sqlite3.Connection, release_id: str, aoty_url: str, data: dict,
                   force: bool = False,
                   overwrite_date: bool = False, overwrite_type: bool = False) -> None:
    """Persist AOTY scrape data for a release.

    Pass force=True to overwrite all existing values, or use the granular
    overwrite_date / overwrite_type booleans for partial control.
    """
    now      = int(time.time())
    existing = conn.execute(
        'SELECT release_date, type, date_source FROM releases WHERE id = ?', (release_id,)
    ).fetchone()
    ex_date   = existing['release_date'] if existing else None
    ex_type   = existing['type']         if existing else None
    ex_source = existing['date_source']  if existing else None

    updates = {'aoty_url': aoty_url, 'updated_at': now}
    if data['release_date'] and (force or overwrite_date
            or _should_update_date(ex_date, ex_source, data['release_date'], 'aoty')):
        updates['release_date'] = data['release_date']
        updates['release_year'] = data['release_year']
        updates['date_source']  = 'aoty'
    if data['type'] and (force or overwrite_type or not ex_type):
        updates['type'] = data['type']
        if data['type_secondary'] is not None:
            updates['type_secondary'] = data['type_secondary']
    if data['score_critic']   is not None: updates['aoty_score_critic']   = data['score_critic']
    if data['score_user']     is not None: updates['aoty_score_user']     = data['score_user']
    if data['ratings_critic'] is not None: updates['aoty_ratings_critic'] = data['ratings_critic']
    if data['ratings_user']   is not None: updates['aoty_ratings_user']   = data['ratings_user']

    if updates:
        set_clause = ', '.join(f'{k} = ?' for k in updates)
        conn.execute(f'UPDATE releases SET {set_clause} WHERE id = ?',
                     (*updates.values(), release_id))

    for aoty_id, name, slug, is_primary in data['genres']:
        conn.execute(
            'INSERT INTO genres (aoty_id, name, slug) VALUES (?, ?, ?)'
            ' ON CONFLICT(aoty_id) DO UPDATE SET name = excluded.name, slug = excluded.slug',
            (aoty_id, name, slug)
        )
        conn.execute(
            'INSERT INTO release_genres (release_id, aoty_genre_id, is_primary) VALUES (?, ?, ?)'
            ' ON CONFLICT(release_id, aoty_genre_id) DO UPDATE SET is_primary = excluded.is_primary',
            (release_id, aoty_id, int(is_primary))
        )

    conn.commit()


def save_release_date(conn: sqlite3.Connection, release_id: str, date_str: str,
                      wiki_page_id: 'int | None' = None, source: str = 'musicbrainz') -> bool:
    """Write a date to releases, respecting precision and source priority.

    Returns True if the date was actually written, False if it was skipped
    because an existing date took priority.  source='manual' always wins.
    """
    if source != 'manual':
        row = conn.execute(
            'SELECT release_date, date_source FROM releases WHERE id = ?', (release_id,)
        ).fetchone()
        if row and not _should_update_date(row['release_date'], row['date_source'], date_str, source):
            return False
    year    = int(date_str[:4]) if date_str else None
    updates = {'release_date': date_str, 'release_year': year,
               'date_source': source, 'updated_at': int(time.time())}
    set_clause = ', '.join(f'{k} = ?' for k in updates)
    conn.execute(f'UPDATE releases SET {set_clause} WHERE id = ?', (*updates.values(), release_id))
    if wiki_page_id:
        upsert_external_link(conn, EL_RELEASE, release_id, EL_SVC_WIKIPEDIA, str(wiki_page_id))
    conn.commit()
    return True


# ── Listen matching ────────────────────────────────────────────────────────────

def bulk_rematch(conn: sqlite3.Connection) -> int:
    """Match unresolved listens to catalog tracks via MBID.

    raw_source_id holds the track MBID for ~76% of scrobbles; the rest use
    artist|||track keys which require name-based matching.
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
                         raw_artist: str | None = None,
                         raw_album: str | None = None) -> int:
    """Name-based listen matching for tracks without MBIDs.

    Scopes to listens whose raw_artist_name matches the release's artists (or
    any known alias), then joins on normalised track title using three phases:
      Phase 1 (SQL): apostrophe variants, feat. credits, mix suffixes.
      Phase 2 (Python): ETI-normalised clean-title comparison.
      Phase 3 (Python): fuzzy ascii_key ratio (threshold 0.85).

    raw_artist and raw_album are optional seeds for the filter. When raw_album
    is None the album-name filter is omitted so all unmatched listens by the
    matching artists are considered — useful for forced single-release matching.

    Returns the count of newly matched listens.
    """
    if not release_ids:
        return 0

    # SQLite's built-in lower() is ASCII-only (e.g. 'ROSALÍA' → 'rosalÍa').
    # Register a Python-backed function so Unicode lowercasing works correctly.
    conn.create_function('_ulower', 1, lambda s: s.lower() if s else '')

    rph = ','.join('?' * len(release_ids))

    # Collect canonical artist names + aliases. Include both Unicode-lowercased
    # and ASCII-normalized (accent-stripped) forms so that e.g. "ROSALIA" and
    # "ROSALÍA" both match scrobbles regardless of how the user typed the name.
    def _name_forms(s: str):
        return {s.lower(), normalize_text(s)}

    artist_name_set: set = set()
    if raw_artist is not None:
        artist_name_set |= _name_forms(raw_artist)
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
            artist_name_set |= _name_forms(r['name'])
        if r['alias']:
            artist_name_set |= _name_forms(r['alias'])

    if not artist_name_set:
        return 0  # no artists found for this release

    aph = ','.join('?' * len(artist_name_set))
    artist_names = list(artist_name_set)

    # Album filter — omitted in forced-match mode (raw_album is None)
    album_clause = "AND lower(raw_album_name) = lower(?)" if raw_album is not None else ""
    album_params = [raw_album] if raw_album is not None else []

    def _n(col):
        return f"REPLACE(REPLACE(lower({col}), char(8217), char(39)), '&', ' and ')"

    def _bare(col):
        nc = _n(col)
        return (f"CASE WHEN instr({nc}, ' (feat.') > 0"
                f" THEN trim(substr({col}, 1, instr({nc}, ' (feat.') - 1))"
                f" ELSE {col} END")

    def _strip_feat(col):
        """Strip any featured-artist credit from col (all formats: 'featuring X',
        ' feat. X' without parens, '(feat. X)' — covers raw scrobble variations).
        Uses _n(col) for both position lookup AND substr to avoid offset drift
        when '&' expansion changes string length."""
        nc = _n(col)
        bare_nc = (f"CASE WHEN instr({nc}, ' (feat.') > 0"
                   f" THEN trim(substr({nc}, 1, instr({nc}, ' (feat.') - 1))"
                   f" ELSE {nc} END")
        return (f"CASE "
                f"WHEN instr({nc}, ' featuring ') > 0 "
                f"THEN trim(substr({nc}, 1, instr({nc}, ' featuring ') - 1)) "
                f"WHEN instr({nc}, ' feat. ') > 0 "
                f"THEN trim(substr({nc}, 1, instr({nc}, ' feat. ') - 1)) "
                f"ELSE {bare_nc} END")

    def _match(db_col, raw_col):
        return (f"({_n(db_col)} = {_n(raw_col)}"
                f" OR {_n(_bare(db_col))} = {_n(raw_col)}"
                f" OR {_n(db_col)} = {_n(_strip_feat(raw_col))}"
                f")")

    title_match = _match('t.title', 'listens.raw_track_name')

    cur = conn.execute(f'''
        UPDATE listens
        SET track_id = (
            SELECT t.id FROM tracks t
            WHERE  t.release_id IN ({rph})
              AND  {title_match}
            LIMIT 1
        )
        WHERE track_id IS NULL
          AND _ulower(raw_artist_name) IN ({aph})
          {album_clause}
          AND raw_track_name IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM tracks t
              WHERE  t.release_id IN ({rph})
                AND  {title_match}
          )
    ''', [*release_ids, *artist_names, *album_params, *release_ids])
    conn.commit()
    sql_matched = cur.rowcount

    # Phase 2: ETI-normalised matching (normalize both sides to MB parenthetical format)
    db_tracks = conn.execute(
        f'SELECT id, title FROM tracks WHERE release_id IN ({rph})', release_ids
    ).fetchall()

    def _mb_key(title: str) -> str:
        """ascii_key of a title normalized to MB parenthetical ETI format.
        'My Melody - TEED Club Mix' → 'My Melody (TEED club mix)' → ascii_key.
        Strips MB '(With X)' collaborator credits — scrobbles never include them."""
        import re as _re
        title = _re.sub(r'\s*\([Ww]ith [^)]+\)', '', title).strip()
        title = _re.sub(r'\s*\(from\s+["\'].*?["\']\)', '', title, flags=_re.IGNORECASE).strip()
        r = _parse_track_title(title)
        full = r.clean_title
        if r.feat_artists:
            full += f" (feat. {', '.join(r.feat_artists)})"
        if r.eti:
            full += f" ({r.eti})"
        return ascii_key(full)

    def _clean_key(title: str) -> str:
        """ascii_key of clean title only — strips feat. artists and ETI.
        Fallback for when the DB stores 'Suit & Tie' but the scrobble says
        'Suit & Tie featuring JAY Z'."""
        import re as _re
        title = _re.sub(r'\s*\([Ww]ith [^)]+\)', '', title).strip()
        title = _re.sub(r'\s*\(from\s+["\'].*?["\']\)', '', title, flags=_re.IGNORECASE).strip()
        r = _parse_track_title(title)
        return ascii_key(r.clean_title)

    eti_map = {}
    clean_map = {}
    for t in db_tracks:
        k = _mb_key(t['title'])
        if k:  # skip non-ASCII-only titles (empty key = unsafe catch-all)
            eti_map.setdefault(k, t['id'])
        ck = _clean_key(t['title'])
        if ck:
            clean_map.setdefault(ck, t['id'])

    unmatched = conn.execute(f'''
        SELECT id, raw_track_name FROM listens
        WHERE  track_id IS NULL
          AND  _ulower(raw_artist_name) IN ({aph})
          {album_clause}
          AND  raw_track_name IS NOT NULL
    ''', [*artist_names, *album_params]).fetchall()

    eti_matched = 0
    for listen in unmatched:
        track_id = (eti_map.get(_mb_key(listen['raw_track_name']))
                    or clean_map.get(_clean_key(listen['raw_track_name'])))
        if track_id:
            conn.execute('UPDATE listens SET track_id = ? WHERE id = ?',
                         [track_id, listen['id']])
            eti_matched += 1
    if eti_matched:
        conn.commit()

    # Phase 3: fuzzy ascii_key matching (full MB-normalized title, not stripped clean title)
    _FUZZY_THRESHOLD = 85  # rapidfuzz uses 0-100 scale
    fuzzy_map = {}
    for t in db_tracks:
        k = _mb_key(t['title'])
        if k:  # skip non-ASCII-only titles
            fuzzy_map.setdefault(k, t['id'])

    still_unmatched = conn.execute(f'''
        SELECT id, raw_track_name FROM listens
        WHERE  track_id IS NULL
          AND  _ulower(raw_artist_name) IN ({aph})
          {album_clause}
          AND  raw_track_name IS NOT NULL
    ''', [*artist_names, *album_params]).fetchall()

    fuzzy_matched = 0
    if still_unmatched and fuzzy_map:
        try:
            from rapidfuzz import process as _rfprocess, fuzz as _rffuzz
            _use_rapidfuzz = True
        except ImportError:
            _use_rapidfuzz = False

        fuzzy_keys = list(fuzzy_map.keys())
        for listen in still_unmatched:
            raw_key = _mb_key(listen['raw_track_name'])
            if not raw_key:
                continue
            if _use_rapidfuzz:
                result = _rfprocess.extractOne(
                    raw_key, fuzzy_keys,
                    scorer=_rffuzz.ratio,
                    score_cutoff=_FUZZY_THRESHOLD,
                )
                if result:
                    best_id = fuzzy_map[result[0]]
                    conn.execute('UPDATE listens SET track_id = ? WHERE id = ?',
                                 [best_id, listen['id']])
                    fuzzy_matched += 1
            else:
                best_ratio, best_id = 0.0, None
                for db_key, tid in fuzzy_map.items():
                    ratio = difflib.SequenceMatcher(None, raw_key, db_key).ratio() * 100
                    if ratio > best_ratio:
                        best_ratio, best_id = ratio, tid
                if best_ratio >= _FUZZY_THRESHOLD and best_id:
                    conn.execute('UPDATE listens SET track_id = ? WHERE id = ?',
                                 [best_id, listen['id']])
                    fuzzy_matched += 1
    if fuzzy_matched:
        conn.commit()

    return sql_matched + eti_matched + fuzzy_matched


# ── Track variant linking ──────────────────────────────────────────────────────

def link_track_variant(conn: sqlite3.Connection, canonical_id: str,
                       variant_id: str, variant_type: 'str | None') -> int:
    """
    Declare variant_id as a variant of canonical_id.

    - Moves listens from variant to canonical.
    - Sets variant.hidden = 1, canonical_track_id, track_variant_type.

    Returns number of listens moved.

    Invariant: canonical_id must not itself have a canonical_track_id (no chains).
    """
    row = conn.execute(
        'SELECT canonical_track_id FROM tracks WHERE id = ?', (canonical_id,)
    ).fetchone()
    if row and row[0]:
        raise ValueError(
            f'canonical track {canonical_id} already points to {row[0]} — no chaining allowed'
        )

    moved = conn.execute(
        'UPDATE listens SET track_id = ? WHERE track_id = ?',
        (canonical_id, variant_id),
    ).rowcount

    now = int(time.time())
    conn.execute(
        '''UPDATE tracks
              SET hidden = 1,
                  canonical_track_id = ?,
                  track_variant_type = ?,
                  updated_at = ?
            WHERE id = ?''',
        (canonical_id, variant_type, now, variant_id),
    )
    return moved


def db_search_releases(conn: sqlite3.Connection, artist: str, album: str) -> list:
    """Search the catalog for releases matching raw scrobble artist/album strings.

    Uses ascii_key comparison (strips all punctuation + lowercase) so hyphen
    variants, apostrophe differences, and accent variations don't block a hit.
    Returns a list of row dicts.
    """
    key_album  = ascii_key(album)
    key_artist = ascii_key(artist)
    rows = conn.execute('''
        SELECT r.id, r.title, r.release_date, r.type, r.type_secondary,
               a.name                                                AS artist_name,
               COUNT(t.id)                                          AS track_count,
               SUM(CASE WHEN t.is_explicit = 1 THEN 1 ELSE 0 END)  AS explicit_count
        FROM   releases r
        LEFT JOIN artists  a  ON a.id  = r.primary_artist_id
        LEFT JOIN tracks   t  ON t.release_id = r.id AND t.hidden = 0
        WHERE  r.hidden = 0
        GROUP  BY r.id
    ''').fetchall()

    results = []
    for row in rows:
        row = dict(row)
        row_key = ascii_key(row['title'])
        if row_key == key_album:
            results.append(row)
        elif key_album in row_key or row_key in key_album:
            if row['artist_name'] and key_artist in ascii_key(row['artist_name']):
                results.append(row)
    return results


def resolve_artist(conn: sqlite3.Connection, key: str) -> 'sqlite3.Row | None':
    """Look up an artist by internal ID, slug, Spotify ID, or name (case-insensitive).

    Tries exact matches in order: ULID → slug → Spotify ID → lowercase name.
    Returns a sqlite3.Row with at minimum (id, name), or None if not found.
    """
    return (
        conn.execute('SELECT id, name FROM artists WHERE id = ?',                 [key]).fetchone() or
        conn.execute('SELECT id, name FROM artists WHERE slug = ?',               [key]).fetchone() or
        conn.execute('SELECT id, name FROM artists WHERE spotify_id = ?',         [key]).fetchone() or
        conn.execute('SELECT id, name FROM artists WHERE lower(name) = lower(?)', [key]).fetchone()
    )


@contextmanager
def managed_db(db_path: str):
    """Open DB, run init_schema, yield conn, always close on exit.

    Use this for any command that needs a connection:

        with managed_db(args.db or DB_PATH) as conn:
            ...

    For interactive commands that want to print a summary even on Ctrl+C, wrap
    the whole block in ``try/except KeyboardInterrupt`` outside the ``with``:

        updated = skipped = 0
        try:
            with managed_db(args.db or DB_PATH) as conn:
                ...
        except KeyboardInterrupt:
            pass
        console.rule(style='dim')
        console.print(f'  [dim]Updated: {updated} · Skipped: {skipped}[/dim]')
    """
    conn = open_db(db_path)
    init_schema(conn)
    try:
        yield conn
    finally:
        conn.close()
