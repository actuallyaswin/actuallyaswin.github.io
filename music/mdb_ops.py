"""
mdb_ops — Database infrastructure for master.sqlite.

Provides: schema DDL, open_db, init_schema, ULID/slug generation,
.env loading, and the core upsert functions (artist, release, tracks).

Dependency order: mdb_strings → mdb_ops (no circular imports)
"""

import os
import random
import re
import sqlite3
import time
import unicodedata

from mdb_strings import resolve_title, ascii_key as _norm, _should_update_date

# ── Paths ─────────────────────────────────────────────────────────────────────

_DIR    = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_DIR, 'master.sqlite')

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
    hidden       INTEGER NOT NULL DEFAULT 0,
    notes        TEXT,
    created_at   INTEGER,
    updated_at   INTEGER
);
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
"""


def open_db(path=None) -> sqlite3.Connection:
    conn = sqlite3.connect(path or DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON')
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
        "ALTER TABLE release_aliases ADD COLUMN is_definitive INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE artist_aliases ADD COLUMN alias_type TEXT NOT NULL DEFAULT 'common'",
        "ALTER TABLE artist_aliases ADD COLUMN language TEXT",
        "ALTER TABLE artist_aliases ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 0",
    ]:
        try:
            conn.execute(ddl)
        except Exception:
            pass  # column already exists

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
            'UPDATE artists SET name = ?, image_url = ?, image_source = ?, updated_at = ?'
            ' WHERE id = ?',
            (sp_artist['name'], _best_image(sp_artist.get('images', [])), 'spotify', now, row[0])
        )
        return row[0], False
    base     = slugify(sp_artist['name'])
    existing = {r[0] for r in cur.execute('SELECT slug FROM artists WHERE slug IS NOT NULL').fetchall()}
    slug     = unique_slug(base, existing)
    aid      = new_ulid()
    cur.execute(
        'INSERT INTO artists (id, slug, name, spotify_id, image_url, image_source,'
        ' created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (aid, slug, sp_artist['name'], sid,
         _best_image(sp_artist.get('images', [])), 'spotify', now, now)
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
    release_year = int(release_date[:4]) if release_date else None
    art          = _best_image(sp_album.get('images', []))

    if row:
        update_date = _should_update_date(row['release_date'], row['date_source'],
                                          release_date, 'spotify')
        date_fields = ', release_date = ?, release_year = ?, date_source = ?' if update_date else ''
        date_vals   = (release_date, release_year, 'spotify') if update_date else ()
        cur.execute(
            f'UPDATE releases SET title = ?, primary_artist_id = ?, type = ?{date_fields},'
            f' label = ?, album_art_url = ?, album_art_source = ?, total_tracks = ?,'
            f' updated_at = ? WHERE id = ?',
            (sp_album['name'], primary_artist_id, _sp_type(sp_album),
             *date_vals,
             sp_album.get('label'), art, 'spotify',
             sp_album.get('total_tracks'), now, row['id'])
        )
        return row['id'], False

    base       = slugify(sp_album['name'])
    existing   = {r[0] for r in cur.execute(
        'SELECT slug FROM releases WHERE primary_artist_id = ?', (primary_artist_id,)).fetchall()}
    slug       = unique_slug(base, existing)
    release_id = new_ulid()
    cur.execute(
        'INSERT INTO releases (id, slug, title, primary_artist_id, type, release_date,'
        ' release_year, label, spotify_id, album_art_url, album_art_source, total_tracks,'
        ' date_source, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (release_id, slug, sp_album['name'], primary_artist_id, _sp_type(sp_album),
         release_date, release_year, sp_album.get('label'), sid, art, 'spotify',
         sp_album.get('total_tracks'), 'spotify', now, now)
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
            track_mbid = mb_by_title.get(_norm(sp.get('name', '')))
        row = cur.execute('SELECT id, title FROM tracks WHERE spotify_id = ?', (sid,)).fetchone()
        if row:
            track_id, existing_title = row[0], row[1]
            title_to_store = resolve_title(sp['name'], existing_title)
            try:
                cur.execute(
                    'UPDATE tracks SET release_id = ?, title = ?, track_number = ?,'
                    ' disc_number = ?, duration_ms = ?, is_explicit = ?, isrc = ?,'
                    ' mbid = ?, updated_at = ? WHERE id = ?',
                    (release_id, title_to_store, sp.get('track_number'), sp.get('disc_number', 1),
                     sp.get('duration_ms'), 1 if sp.get('explicit') else 0,
                     isrc, track_mbid, now, track_id)
                )
            except sqlite3.IntegrityError:
                # MBID already assigned to a different track — update without mbid
                cur.execute(
                    'UPDATE tracks SET release_id = ?, title = ?, track_number = ?,'
                    ' disc_number = ?, duration_ms = ?, is_explicit = ?, isrc = ?,'
                    ' updated_at = ? WHERE id = ?',
                    (release_id, title_to_store, sp.get('track_number'), sp.get('disc_number', 1),
                     sp.get('duration_ms'), 1 if sp.get('explicit') else 0,
                     isrc, now, track_id)
                )
            updated += 1
        else:
            track_id       = new_ulid()
            title_to_store = resolve_title(sp['name'])
            try:
                cur.execute(
                    'INSERT INTO tracks (id, title, release_id, track_number, disc_number,'
                    ' duration_ms, is_explicit, spotify_id, mbid, isrc, created_at, updated_at)'
                    ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (track_id, title_to_store, release_id, sp.get('track_number'),
                     sp.get('disc_number', 1), sp.get('duration_ms'),
                     1 if sp.get('explicit') else 0, sid, track_mbid, isrc, now, now)
                )
            except sqlite3.IntegrityError:
                # MBID collision — insert without mbid
                cur.execute(
                    'INSERT INTO tracks (id, title, release_id, track_number, disc_number,'
                    ' duration_ms, is_explicit, spotify_id, isrc, created_at, updated_at)'
                    ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (track_id, title_to_store, release_id, sp.get('track_number'),
                     sp.get('disc_number', 1), sp.get('duration_ms'),
                     1 if sp.get('explicit') else 0, sid, isrc, now, now)
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
