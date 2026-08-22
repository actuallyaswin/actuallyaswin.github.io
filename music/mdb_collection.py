"""mdb_collection — physical collection sync from Discogs and query helpers.

Discogs exposes vinyl color/weight/packaging only as free-text per format
block (e.g. "White, 180 Gram", "Blue Translucent"), never as structured
fields. The parsers below extract what they reliably can; anything left over
is kept verbatim in raw_text for manual review rather than discarded.
"""
from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass, field

BASE_COLORS = [
    'off-white', 'dark red', 'dark green', 'dark blue',
    'black', 'white', 'red', 'blue', 'green', 'yellow', 'purple', 'pink',
    'orange', 'clear', 'silver', 'gold', 'gray', 'grey', 'brown', 'magenta',
    'cyan', 'violet', 'maroon', 'teal', 'turquoise', 'bronze', 'copper',
    'lavender', 'olive', 'peach', 'cream',
]
COLOR_EFFECTS = [
    'translucent', 'transparent', 'marbled', 'marble', 'splatter', 'swirl',
    'split', 'tie-dye', 'sparkle', 'glow', 'smoke', 'metallic', 'haze', 'burst',
]

_WEIGHT_RE = re.compile(r'(\d{2,3})\s*(?:g|gram|gr)\b', re.IGNORECASE)
_BRACKET_RE = re.compile(r'\[[^\]]+\]')
_GATEFOLD_RE = re.compile(r'\bgatefold\b', re.IGNORECASE)


def parse_weight(text: str) -> 'int | None':
    """Extract a gram weight from Discogs format text, e.g. '180 Gram' -> 180."""
    m = _WEIGHT_RE.search(text)
    return int(m.group(1)) if m else None


def parse_packaging(text: str, descriptors: list) -> 'str | None':
    """Return 'gatefold' if stated; None if unstated (not necessarily standard)."""
    hay = text + ' ' + ' '.join(descriptors)
    return 'gatefold' if _GATEFOLD_RE.search(hay) else None


def parse_colors(text: str) -> 'tuple[str | None, str | None]':
    """Extract (base_color, effect) from Discogs format text.

    Strips bracketed nicknames ("[Cherry Pie]") and weight/gatefold noise
    before matching, since those otherwise collide with color words.
    Returns (None, None) when text carries no recognizable color.
    """
    clean = _BRACKET_RE.sub('', text)
    clean = _GATEFOLD_RE.sub('', clean)
    clean = _WEIGHT_RE.sub('', clean)
    low = clean.lower()
    color = next((c for c in BASE_COLORS if re.search(r'\b' + re.escape(c) + r'\b', low)), None)
    effect = next((e for e in COLOR_EFFECTS if re.search(r'\b' + re.escape(e) + r'\b', low)), None)
    return color, effect


# Discogs genre/style tag -> coarse label, and the priority order used to
# pick one when a release carries several. Absorbed from the archived
# collection_genres.py (scratch/) — same mapping, live import path.
DISCOGS_TO_COARSE = {
    'hip hop': 'Hip-Hop / Rap', 'rap': 'Hip-Hop / Rap',
    'electronic': 'Electronic', 'house': 'Electronic', 'techno': 'Electronic',
    'ambient': 'Electronic', 'idm': 'Electronic', 'drum n bass': 'Electronic',
    'drum & bass': 'Electronic', 'downtempo': 'Electronic', 'breakbeat': 'Electronic',
    'electro': 'Electronic',
    'rock': 'Rock / Alternative', 'alternative rock': 'Rock / Alternative',
    'indie rock': 'Rock / Alternative', 'post-rock': 'Rock / Alternative',
    'punk': 'Rock / Alternative', 'new wave': 'Rock / Alternative',
    'grunge': 'Rock / Alternative', 'metal': 'Rock / Alternative',
    'funk': 'Funk / Soul', 'soul': 'Funk / Soul', 'r&b': 'Funk / Soul',
    'disco': 'Funk / Soul', 'gospel': 'Funk / Soul', 'rhythm & blues': 'Funk / Soul',
    'jazz': 'Jazz', 'blues': 'Jazz',
    'pop': 'Pop', 'synth-pop': 'Pop', 'indie pop': 'Pop', 'dream pop': 'Pop',
    'classical': 'Classical / Orchestral', 'orchestral': 'Classical / Orchestral',
    'contemporary': 'Classical / Orchestral', 'modern': 'Classical / Orchestral',
    'experimental': 'Experimental', 'noise': 'Experimental',
    'abstract': 'Experimental', 'avant-garde': 'Experimental',
    'soundtrack': 'Soundtrack', 'score': 'Soundtrack', 'theme': 'Soundtrack',
}
COARSE_PRIORITY = [
    'Soundtrack', 'Hip-Hop / Rap', 'Electronic', 'Rock / Alternative',
    'Funk / Soul', 'Jazz', 'Pop', 'Classical / Orchestral', 'Experimental',
]


def discogs_tags_to_coarse(genres: list, styles: list) -> str:
    """Map Discogs genre + style tags to a single coarse label."""
    all_tags = [t.lower() for t in (genres or []) + (styles or [])]
    for coarse in COARSE_PRIORITY:
        if any(DISCOGS_TO_COARSE.get(tag) == coarse for tag in all_tags):
            return coarse
    return 'Other'


# Discogs styles under genre=Non-Music that mean "not a music recording at
# all" (DJ hardware, spoken-word/audiobook) rather than "recorded audio the
# owner wants tracked" (comedy, spoken poetry set to music, etc.) — the
# latter stays visible.
_NON_MUSIC_HIDDEN_STYLES = {'technical', 'dialogue', 'audiobook', 'movie effects'}


def is_non_music_item(genres: list, styles: list) -> bool:
    """True if styles indicate the physical item isn't a music recording
    (DJ control vinyl, spoken-word audiobook) and should stay out of any
    music-collection display, distinct from Discogs' broader Non-Music genre
    tag which also covers comedy albums that are real audio content."""
    style_set = {s.lower() for s in (styles or [])}
    return bool(style_set & _NON_MUSIC_HIDDEN_STYLES)


def format_to_coarse(descriptors: list, folder: str = '') -> str:
    """Derive album/ep/single/soundtrack from Discogs format descriptors + folder."""
    if 'soundtrack' in folder.lower():
        return 'soundtrack'
    desc = {d.lower() for d in descriptors}
    if 'ep' in desc:
        return 'ep'
    if 'single' in desc:
        return 'single'
    return 'album'


def medium_from_format_name(name: str) -> str:
    """Map a Discogs formats[].name to our medium enum."""
    low = name.lower()
    if 'cd' in low or 'sacd' in low:
        return 'cd'
    if 'cass' in low:
        return 'cassette'
    if low == 'vinyl':
        return 'vinyl'
    return 'other'


@dataclass
class CollectionMedia:
    medium: str
    format_coarse: 'str | None'
    disc_count: 'int | None'
    descriptors: list
    weight_g: 'int | None'
    color_primary: 'str | None'
    color_effect: 'str | None'
    packaging: 'str | None'
    pressing_plant: 'str | None'
    raw_text: str
    # Bare-disc photo URL for this specific pressing (hotlinked, e.g. from
    # coloredvinylrecords.com) -- not derived from Discogs data, set separately.
    image_url: 'str | None' = None

    @classmethod
    def from_discogs_format(cls, fmt: dict, folder: str = '') -> 'CollectionMedia':
        text = fmt.get('text') or ''
        descriptors = fmt.get('descriptions', [])
        color, effect = parse_colors(text)
        return cls(
            medium=medium_from_format_name(fmt['name']),
            format_coarse=format_to_coarse(descriptors, folder),
            disc_count=int(fmt['qty']) if fmt.get('qty', '').isdigit() else None,
            descriptors=descriptors,
            weight_g=parse_weight(text),
            color_primary=color,
            color_effect=effect,
            packaging=parse_packaging(text, descriptors),
            pressing_plant=None,
            raw_text=text,
        )


@dataclass
class CollectionIdentifier:
    id_type: str
    value: str
    description: 'str | None'


@dataclass
class CollectionItem:
    discogs_release_id: str
    discogs_instance_id: str
    release_id: 'str | None'
    catalog_number: 'str | None'
    label: 'str | None'
    date_added: 'str | None'
    media_condition: 'str | None' = None
    sleeve_condition: 'str | None' = None
    notes: 'str | None' = None
    discogs_folder: 'str | None' = None
    discogs_genres: list = field(default_factory=list)
    coarse_genre: 'str | None' = None
    media: list = field(default_factory=list)          # list[CollectionMedia]
    identifiers: list = field(default_factory=list)     # list[CollectionIdentifier]
    # Additional release_ids for one physical item spanning multiple DB
    # releases (a box set bundling two separately-cataloged albums).
    # release_id above stays the primary pointer; every id (primary + extra)
    # is also written to collection_item_releases for uniform querying.
    extra_release_ids: list = field(default_factory=list)
    # Excludes the item from SPA display without deleting collection data —
    # for physical media that isn't a music release (DJ control vinyl,
    # spoken-word/audio-drama pressings) but is still worth tracking as owned.
    hidden: bool = False
    # Set only when release_id is None — the reason this Discogs item hasn't
    # been linked yet, so a sync doesn't keep re-fetching and re-matching it
    # forever. 'non_music': no release should exist (DJ control vinyl).
    # 'ambiguous': candidate_release_ids holds every plausible match, pending
    # a human/agent pick. 'unresolved': no candidate found anywhere.
    unresolved_reason: 'str | None' = None
    candidate_release_ids: list = field(default_factory=list)


class Collection:
    """Read-side query helper over collection_items/_media/_identifiers.

    A thin wrapper over plain SQL, not an ORM — every method returns rows
    (as dicts) rather than hydrating full objects, since the query surface
    here is small and hydration would just add indirection.
    """

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def _rows(self, sql: str, params: tuple = ()) -> list:
        return [dict(r) for r in self._conn.execute(sql, params).fetchall()]

    def counts_by_medium(self) -> dict:
        """{'vinyl': N, 'cd': N, ...} — an item counts once per medium it has.
        Excludes hidden and not-yet-linked items."""
        rows = self._rows('''
            SELECT cim.medium, COUNT(DISTINCT cim.collection_item_id) AS n
            FROM collection_item_media cim
            JOIN collection_items ci ON ci.id = cim.collection_item_id
            WHERE ci.hidden = 0 AND ci.release_id IS NOT NULL
            GROUP BY cim.medium
        ''')
        return {r['medium']: r['n'] for r in rows}

    def colored_vinyl(self) -> list:
        """Vinyl media with a confirmed non-black color (excludes unstated)."""
        return self._rows('''
            SELECT ci.id AS collection_item_id, r.title, r.primary_artist_id,
                   cim.color_primary, cim.color_effect, cim.weight_g
            FROM collection_item_media cim
            JOIN collection_items ci ON ci.id = cim.collection_item_id
            JOIN releases r ON r.id = ci.release_id
            WHERE ci.hidden = 0 AND cim.medium = 'vinyl'
              AND cim.color_primary IS NOT NULL AND cim.color_primary != 'black'
        ''')

    def by_weight(self, grams: int) -> list:
        return self._rows('''
            SELECT ci.id AS collection_item_id, r.title, cim.weight_g
            FROM collection_item_media cim
            JOIN collection_items ci ON ci.id = cim.collection_item_id
            JOIN releases r ON r.id = ci.release_id
            WHERE ci.hidden = 0 AND cim.weight_g = ?
        ''', (grams,))

    def needs_backfill(self) -> dict:
        """Rows with an unresolved attribute, grouped by what's missing."""
        return {
            'weight_g':   self._rows("SELECT collection_item_id, medium, raw_text FROM collection_item_media WHERE medium='vinyl' AND weight_g IS NULL"),
            'color':      self._rows("SELECT collection_item_id, medium, raw_text FROM collection_item_media WHERE medium='vinyl' AND color_primary IS NULL AND raw_text != ''"),
            'packaging':  self._rows("SELECT collection_item_id, medium, raw_text FROM collection_item_media WHERE medium='vinyl' AND packaging IS NULL"),
            'no_release': self._rows("SELECT id, discogs_release_id FROM collection_items WHERE release_id IS NULL"),
        }

    def unresolved_items(self, reason: 'str | None' = None) -> list:
        """Items with no release_id, optionally filtered to one unresolved_reason
        ('non_music' | 'ambiguous' | 'unresolved')."""
        sql = ('SELECT id, discogs_release_id, discogs_instance_id, unresolved_reason, '
               'candidate_release_ids FROM collection_items WHERE release_id IS NULL')
        params: tuple = ()
        if reason:
            sql += ' AND unresolved_reason = ?'
            params = (reason,)
        return self._rows(sql, params)

    def unresolved_raw_text(self) -> list:
        """raw_text that carries neither a parsed color nor a weight — candidates
        for parser improvement or a one-off manual color assignment."""
        return self._rows('''
            SELECT collection_item_id, raw_text FROM collection_item_media
            WHERE medium='vinyl' AND raw_text != ''
              AND color_primary IS NULL AND weight_g IS NULL
        ''')

    def multi_release_items(self) -> list:
        """Collection items spanning more than one release (box sets)."""
        return self._rows('''
            SELECT ci.id AS collection_item_id, r.title, r.id AS release_id
            FROM collection_item_releases cir
            JOIN collection_items ci ON ci.id = cir.collection_item_id
            JOIN releases r ON r.id = cir.release_id
            WHERE cir.collection_item_id IN (
                SELECT collection_item_id FROM collection_item_releases
                GROUP BY collection_item_id HAVING COUNT(*) > 1
            )
            ORDER BY ci.id
        ''')


def upsert_collection_item(conn: sqlite3.Connection, item: CollectionItem) -> int:
    """Insert or replace one collection item and its media/identifier rows.

    Keyed on discogs_instance_id (unique per physical copy owned, even if two
    copies of the same release are both in the collection).
    """
    now = int(time.time())
    row = conn.execute(
        'SELECT id FROM collection_items WHERE discogs_instance_id = ?',
        (item.discogs_instance_id,),
    ).fetchone()

    fields = dict(
        discogs_release_id=item.discogs_release_id,
        discogs_instance_id=item.discogs_instance_id,
        release_id=item.release_id,
        unresolved_reason=item.unresolved_reason,
        candidate_release_ids=json.dumps(item.candidate_release_ids) if item.candidate_release_ids else None,
        catalog_number=item.catalog_number,
        label=item.label,
        date_added=item.date_added,
        media_condition=item.media_condition,
        sleeve_condition=item.sleeve_condition,
        notes=item.notes,
        discogs_folder=item.discogs_folder,
        discogs_genres=json.dumps(item.discogs_genres),
        coarse_genre=item.coarse_genre,
        hidden=int(item.hidden),
        updated_at=now,
    )

    if row:
        item_id = row['id']
        conn.execute(
            f'UPDATE collection_items SET {", ".join(f"{k}=?" for k in fields)} WHERE id=?',
            [*fields.values(), item_id],
        )
        conn.execute('DELETE FROM collection_item_media WHERE collection_item_id=?', (item_id,))
        conn.execute('DELETE FROM collection_item_identifiers WHERE collection_item_id=?', (item_id,))
        conn.execute('DELETE FROM collection_item_releases WHERE collection_item_id=?', (item_id,))
    else:
        fields['created_at'] = now
        cols = ', '.join(fields)
        qs = ', '.join('?' for _ in fields)
        cur = conn.execute(f'INSERT INTO collection_items ({cols}) VALUES ({qs})', list(fields.values()))
        item_id = cur.lastrowid

    for m in item.media:
        conn.execute('''
            INSERT INTO collection_item_media
                (collection_item_id, medium, format_coarse, disc_count, descriptors,
                 weight_g, color_primary, color_effect, packaging, pressing_plant, raw_text, image_url)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        ''', (item_id, m.medium, m.format_coarse, m.disc_count, json.dumps(m.descriptors),
              m.weight_g, m.color_primary, m.color_effect, m.packaging, m.pressing_plant, m.raw_text, m.image_url))

    for ident in item.identifiers:
        conn.execute('''
            INSERT INTO collection_item_identifiers (collection_item_id, id_type, value, description)
            VALUES (?,?,?,?)
        ''', (item_id, ident.id_type, ident.value, ident.description))

    for release_id in [item.release_id, *item.extra_release_ids]:
        if release_id:
            conn.execute('''
                INSERT OR IGNORE INTO collection_item_releases (collection_item_id, release_id)
                VALUES (?,?)
            ''', (item_id, release_id))

    return item_id
