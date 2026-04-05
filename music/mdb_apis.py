"""mdb_apis — HTTP clients for Spotify and MusicBrainz."""

__all__ = [
    'RateLimiter',
    'SpotifyClient',
    'SpotifyRelease',
    'MusicBrainzRelease',
    'compare_releases',
    'MB_API', 'MB_UA', 'SP_TOKEN', 'SP_BASE',
    'CAA_API',
    'AOTY_SEARCH', 'AOTY_UA', 'AOTY_RETRY',
    'AOTY_AHEAD', 'DATES_AHEAD',
    '_extract_mbid',
    'caa_fetch_front_image_url',
    'mb_find_release', 'mb_fetch_recording_ids',
    'mb_fetch_release_data', 'mb_fetch_artist_data',
]

import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from mdb_strings import ascii_key as _norm, extract_mbid as _extract_mbid_or_none

log = logging.getLogger(__name__)

# ── API constants ──────────────────────────────────────────────────────────────

MB_API  = 'https://musicbrainz.org/ws/2'
MB_UA   = 'aswin-music-browser/1.0 (personal project)'
SP_TOKEN = 'https://accounts.spotify.com/api/token'
SP_BASE  = 'https://api.spotify.com/v1'
CAA_API  = 'https://coverartarchive.org'

AOTY_SEARCH   = 'https://www.albumoftheyear.org/search/'
AOTY_UA       = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'

MB_INTERVAL   = 1.1
WIKI_INTERVAL = 0.5
AOTY_INTERVAL = 3.0
AOTY_RETRY    = 15.0
AOTY_AHEAD    = 2
DATES_AHEAD   = 5

# ── Rate limiter ───────────────────────────────────────────────────────────────

class RateLimiter:
    def __init__(self, interval: float):
        self._lock     = threading.Lock()
        self._last     = 0.0
        self._interval = interval

    def wait(self) -> None:
        with self._lock:
            rem = self._interval - (time.monotonic() - self._last)
            if rem > 0:
                time.sleep(rem)
            self._last = time.monotonic()


_mb_lim   = RateLimiter(MB_INTERVAL)
_wiki_lim = RateLimiter(WIKI_INTERVAL)
_aoty_lim = RateLimiter(AOTY_INTERVAL)


# ── Spotify client ─────────────────────────────────────────────────────────────

class SpotifyClient:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id     = client_id
        self.client_secret = client_secret
        self._token        = None
        self._expiry       = 0.0

    def _ensure_token(self) -> None:
        if time.time() < self._expiry - 60:
            return
        creds = f'{self.client_id}:{self.client_secret}'
        req = urllib.request.Request(
            SP_TOKEN,
            data=urllib.parse.urlencode({'grant_type': 'client_credentials'}).encode(),
            headers={
                'Authorization': 'Basic ' + __import__('base64').b64encode(creds.encode()).decode(),
                'Content-Type':  'application/x-www-form-urlencoded',
            },
        )
        with urllib.request.urlopen(req) as r:
            d = json.loads(r.read())
        self._token  = d['access_token']
        self._expiry = time.time() + d['expires_in']

    def get(self, path: str, params: dict = None) -> dict:
        self._ensure_token()
        url = SP_BASE + path
        if params:
            url += '?' + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={'Authorization': f'Bearer {self._token}'})
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    def get_album(self, album_id: str) -> dict:
        album  = self.get(f'/albums/{album_id}')
        tracks = list(album['tracks']['items'])
        nxt    = album['tracks'].get('next')
        while nxt:
            self._ensure_token()
            req = urllib.request.Request(nxt, headers={'Authorization': f'Bearer {self._token}'})
            with urllib.request.urlopen(req) as r:
                page = json.loads(r.read())
            tracks.extend(page['items'])
            nxt = page.get('next')
        album['_all_tracks'] = tracks
        return album

    def get_tracks_batch(self, ids: list) -> list:
        out = []
        for i in range(0, len(ids), 50):
            out.extend(self.get('/tracks', {'ids': ','.join(ids[i:i+50])})['tracks'])
        return out

    def get_artists_batch(self, ids: list) -> list:
        out, seen = [], set()
        unique = [x for x in ids if x not in seen and not seen.add(x)]
        for i in range(0, len(unique), 50):
            out.extend(self.get('/artists', {'ids': ','.join(unique[i:i+50])})['artists'])
        return out

    def get_audio_features_batch(self, ids: list) -> list:
        """Fetch audio features for up to 100 track IDs per call."""
        out = []
        for i in range(0, len(ids), 100):
            chunk = ids[i:i+100]
            data  = self.get('/audio-features', {'ids': ','.join(chunk)})
            out.extend(data.get('audio_features') or [])
        return out


# ── SpotifyRelease ─────────────────────────────────────────────────────────────

_SP_ID_RE    = re.compile(r'[A-Za-z0-9]{22}')
_EDITION_RE  = re.compile(
    r'\b(deluxe|anniversary|expanded|special|bonus|remaster(?:ed)?|reissue)\b',
    re.IGNORECASE,
)
_FEAT_RE = re.compile(
    r'\s*[\(\[]\s*feat\..*?[\)\]]'   # (feat. ...) or [feat. ...]
    r'|\s+[-–—]\s*feat\b.*$',        # - feat. ... or – feat. ...
    re.IGNORECASE,
)
_TRACK_VERSION_RE = re.compile(
    r'\s*[\(\[]\s*(?:\d{4}\s+)?remaster(?:ed)?\s*[\)\]]'           # (Remastered) / (2023 Remastered)
    r'|\s*[\(\[]\s*\d+\s*(?:th|st|nd|rd)\s+anniversary\b[^\)\]]*[\)\]]'  # (10th Anniversary ...)
    r'|\s+[-–—]\s*remaster(?:ed)?\b.*$',                           # - Remastered / – Remastered
    re.IGNORECASE,
)


def _bare_track_title(title: str) -> str:
    """Strip feat. credits and remaster/anniversary version tags for cross-edition comparison."""
    t = _FEAT_RE.sub('', title)
    t = _TRACK_VERSION_RE.sub('', t)
    return t.strip()


# ── MBID extraction ────────────────────────────────────────────────────────────

def _extract_mbid(s: str) -> str:
    """Extract a MusicBrainz UUID from a URL or bare UUID string. Raises on failure."""
    result = _extract_mbid_or_none(s)
    if result is None:
        raise ValueError(f'Cannot parse MBID from: {s!r}')
    return result


# ── Cover Art Archive ──────────────────────────────────────────────────────────

def caa_fetch_front_image_url(release_mbid: str) -> 'str | None':
    """Fetch the front cover art URL from Cover Art Archive for a release MBID.
    Uses the 'large' thumbnail when available (still high-res, faster than original).
    Returns None on 404 or if no Front image is found."""
    req = urllib.request.Request(
        f'{CAA_API}/release/{release_mbid}',
        headers={'User-Agent': MB_UA, 'Accept': 'application/json'},
    )
    _mb_lim.wait()
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    for image in data.get('images', []):
        if 'Front' in (image.get('types') or []):
            thumbs = image.get('thumbnails') or {}
            return thumbs.get('large') or thumbs.get('small') or image.get('image')
    return None


# ── MusicBrainzRelease ────────────────────────────────────────────────────────

class MusicBrainzRelease:
    """
    Lazy-loading wrapper around a MusicBrainz release.

    Mirrors SpotifyRelease's interface (name, artist, year, date, tracks,
    track_count, total_ms, label, album_type, canonical_score) so both can
    be passed to compare_releases and the mdb import machinery.

    The .tracks list matches SpotifyRelease shape: dicts with 'name' and
    'duration_ms'.  MB-specific import fields are prefixed '_mb_' on each
    track dict and consumed by import_album_from_mb in mdb.py.
    """

    def __init__(self, mbid: str):
        from mdb_strings import is_valid_mbid
        if not is_valid_mbid(mbid):
            raise ValueError(f'Not a valid MBID: {mbid!r}')
        self.id  = mbid.lower()
        self.url = f'https://musicbrainz.org/release/{self.id}'
        self._data: 'dict | None' = None

    # -- lazy helpers --

    def _ensure_full(self) -> None:
        if self._data is not None:
            return
        data = _mb_get(f'/release/{self.id}', {
            'inc': 'recordings isrcs artists release-groups labels media',
        })
        # Build flat track list once and cache it
        tracks = []
        for medium in (data.get('media') or []):
            disc_num = medium.get('position') or 1
            for t in (medium.get('tracks') or []):
                rec    = t.get('recording') or {}
                length = rec.get('length') or t.get('length')
                tracks.append({
                    'name':              rec.get('title') or t.get('title', ''),
                    'duration_ms':       length,
                    # MB extras for import
                    '_mb_recording_id':  rec.get('id'),
                    '_isrcs':            list(rec.get('isrcs') or []),
                    '_disc_number':      disc_num,
                    '_track_number':     t.get('position'),
                    '_artist_credit':    rec.get('artist-credit') or t.get('artist-credit') or [],
                })
        data['_track_list'] = tracks
        self._data = data

    # -- properties --

    @property
    def name(self) -> str:
        self._ensure_full()
        return (self._data.get('title') or '').strip()

    @property
    def artist(self) -> str:
        """Primary artist name (first credit)."""
        self._ensure_full()
        for credit in (self._data.get('artist-credit') or []):
            if isinstance(credit, dict) and 'artist' in credit:
                return credit['artist'].get('name', '')
        return ''

    @property
    def year(self) -> str:
        return (self.date or '')[:4]

    @property
    def date(self) -> str:
        self._ensure_full()
        return (self._data.get('date') or '').strip()

    @property
    def tracks(self) -> list:
        self._ensure_full()
        return self._data['_track_list']

    @property
    def track_count(self) -> int:
        return len(self.tracks)

    @property
    def explicit_count(self) -> int:
        return 0  # MB does not carry explicit flags

    @property
    def total_ms(self) -> int:
        return sum(t.get('duration_ms') or 0 for t in self.tracks)

    @property
    def label(self) -> str:
        self._ensure_full()
        for info in (self._data.get('label-info') or []):
            lbl = (info.get('label') or {}).get('name', '')
            if lbl:
                return lbl
        return ''

    @property
    def album_type(self) -> str:
        self._ensure_full()
        rg = self._data.get('release-group') or {}
        return (rg.get('primary-type') or '').lower()

    @property
    def release_group_mbid(self) -> 'str | None':
        self._ensure_full()
        return (self._data.get('release-group') or {}).get('id')

    def canonical_score(self) -> tuple:
        """Lower = more canonical: earliest full date, fewest tracks, no edition words."""
        d    = self.date
        prec = (3 if (len(d) == 10 and not d.endswith('-01-01'))
                else (2 if len(d) == 7 else 1))
        return (-prec, d, self.track_count,
                1 if _EDITION_RE.search(self.name) else 0,
                0)  # explicit_count always 0 for MB releases
_sp_client_singleton: 'SpotifyClient | None' = None


def _get_sp_client() -> SpotifyClient:
    """Return a module-level SpotifyClient, lazily initialised from env/.env."""
    global _sp_client_singleton
    if _sp_client_singleton is None:
        from mdb_ops import load_dotenv
        load_dotenv()
        cid = os.environ.get('SPOTIFY_CLIENT_ID')
        csc = os.environ.get('SPOTIFY_CLIENT_SECRET')
        if not (cid and csc):
            raise RuntimeError('SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET not set')
        _sp_client_singleton = SpotifyClient(cid, csc)
    return _sp_client_singleton


def _extract_sp_id(s: str) -> str:
    m = _SP_ID_RE.search(s)
    if not m:
        raise ValueError(f'Cannot parse Spotify ID from: {s!r}')
    return m.group()


class SpotifyRelease:
    """
    Lazy-loading wrapper around a Spotify album.

    Constructed from a URL or bare ID.  Optionally seeded with partial data
    (name, artists, release_date) from a search result to avoid extra fetches
    for display.  Full track/label data is fetched on first access of any
    property that requires it (tracks, label, explicit_count, etc.).
    """

    def __init__(self, id_or_url: str, *,
                 client: 'SpotifyClient | None' = None,
                 _seed: 'dict | None' = None):
        self.id      = _extract_sp_id(id_or_url)
        self.url     = f'https://open.spotify.com/album/{self.id}'
        self._cli    = client
        self._data   = _seed   # may be partial (no _all_tracks key)

    @classmethod
    def from_search_item(cls, item: dict,
                         client: 'SpotifyClient | None' = None) -> 'SpotifyRelease':
        """Construct cheaply from a Spotify search-result item (no track fetch)."""
        return cls(item['id'], client=client, _seed={
            'id':           item['id'],
            'name':         item.get('name', ''),
            'artists':      item.get('artists', []),
            'release_date': item.get('release_date', ''),
            'album_type':   item.get('album_type', ''),
        })

    # -- lazy helpers --

    def _client(self) -> SpotifyClient:
        if self._cli is None:
            self._cli = _get_sp_client()
        return self._cli

    def _ensure_full(self) -> None:
        if self._data is not None and '_all_tracks' in self._data:
            return
        full = self._client().get_album(self.id)
        self._data = {**(self._data or {}), **full}

    # -- cheap properties (available from search seed) --

    @property
    def name(self) -> str:
        return (self._data or {}).get('name', '')

    @property
    def artist(self) -> str:
        artists = (self._data or {}).get('artists') or []
        if artists and isinstance(artists[0], dict):
            return artists[0].get('name', '')
        return ''

    @property
    def year(self) -> str:
        return ((self._data or {}).get('release_date') or '')[:4]

    # -- full properties (trigger network fetch) --

    @property
    def date(self) -> str:
        self._ensure_full()
        return (self._data.get('release_date') or '').strip()

    @property
    def tracks(self) -> list:
        self._ensure_full()
        return self._data['_all_tracks']

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
        self._ensure_full()
        return (self._data.get('label') or '').strip()

    @property
    def album_type(self) -> str:
        self._ensure_full()
        return (self._data.get('album_type') or '').lower()

    def canonical_score(self) -> tuple:
        """Lower = more canonical: earliest full date, fewest tracks, no edition words."""
        d    = self.date
        prec = (3 if (len(d) == 10 and not d.endswith('-01-01'))
                else (2 if len(d) == 7 else 1))
        return (-prec, d, self.track_count,
                1 if _EDITION_RE.search(self.name) else 0,
                -self.explicit_count)


# ── compare_releases ───────────────────────────────────────────────────────────

def compare_releases(*releases: SpotifyRelease) -> dict:
    """
    Compare two or more SpotifyRelease objects.  Fetches full data as needed.
    Returns a structured dict consumed by render_diff in mdb_cli.
    """
    track_lists = [
        [(t['name'], t.get('duration_ms') or 0) for t in r.tracks]
        for r in releases
    ]
    # Normalise titles for set operations: strip feat. credits so that
    # "(feat. Lloyd)" and "- feat. Lloyd" variants of the same track are treated
    # as identical.  Raw names are kept in track_lists for display/dur_diffs.
    norm_lists = [
        [(_bare_track_title(t).lower(), d) for t, d in tl]
        for tl in track_lists
    ]
    same_titles = len({tuple(t for t, _ in nl) for nl in norm_lists}) == 1

    dur_diffs = []
    if same_titles:
        n = len(track_lists[0])
        for i in range(n):
            durs = [tl[i][1] for tl in track_lists if i < len(tl)]
            if max(durs) - min(durs) > 2000:
                dur_diffs.append((
                    i + 1,
                    track_lists[0][i][0],
                    [tl[i][1] if i < len(tl) else 0 for tl in track_lists],
                ))

    all_title_sets = [set(t for t, _ in nl) for nl in norm_lists]

    union_set     = all_title_sets[0].union(*all_title_sets[1:])
    shared_titles = sorted(all_title_sets[0].intersection(*all_title_sets[1:]))
    similarity    = len(shared_titles) / len(union_set) if union_set else 0.0

    unique_per = []
    if not same_titles:
        for i, (r, titles) in enumerate(zip(releases, all_title_sets)):
            others = set().union(*(all_title_sets[j]
                                   for j in range(len(releases)) if j != i))
            unique = sorted(titles - others)
            if unique:
                unique_per.append((r.id, unique))

    ranked   = sorted(releases, key=lambda r: r.canonical_score())
    canon    = ranked[0]
    can_date = canon.date
    reasons  = {}
    for alt in ranked[1:]:
        rs = []
        if alt.date and can_date and alt.date > can_date:
            rs.append(f'later release ({alt.date} > {can_date})')
        elif alt.date == can_date:
            rs.append('same date — likely re-upload')
        if alt.track_count > canon.track_count:
            rs.append(f'{alt.track_count - canon.track_count} bonus track(s)')
        if _EDITION_RE.search(alt.name) and not _EDITION_RE.search(canon.name):
            words = _EDITION_RE.findall(alt.name)
            rs.append(f'edition qualifier ({", ".join(words)})')
        if same_titles and alt.date != can_date:
            rs.append('same tracklist — re-upload artifact')
        reasons[alt.id] = rs or ['lower canonical score']

    return {
        'releases':      list(releases),
        'similarity':    similarity,      # Jaccard over track title sets
        'shared_titles': shared_titles,   # titles present in all releases
        'same_titles':   same_titles,
        'dur_diffs':     dur_diffs,       # [(pos, title, [ms, ...]), ...]
        'unique_per':    unique_per,      # [(sp_id, [title, ...]), ...]
        'ranked':        ranked,
        'canonical':     canon,
        'reasons':       reasons,         # {sp_id: [reason, ...]} for non-canonical
    }


# ── MusicBrainz low-level ──────────────────────────────────────────────────────

def _mb_get(path: str, params: dict = None) -> dict:
    p = {'fmt': 'json', **(params or {})}
    req = urllib.request.Request(
        f'{MB_API}{path}?' + urllib.parse.urlencode(p),
        headers={'User-Agent': MB_UA},
    )
    _mb_lim.wait()
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def _mb_get_safe(path: str, params: dict = None) -> 'dict | None':
    try:
        return _mb_get(path, params)
    except Exception as e:
        log.debug('MB error %s: %s', path, e)
        return None


# ── MusicBrainz query functions ────────────────────────────────────────────────

def mb_find_release(title: str, artist: str, track_count: int, year: int) -> 'tuple[str | None, int]':
    """Search MB for a release matching title/artist/track_count/year.
    Returns (mbid, score) or (None, 0)."""
    try:
        data = _mb_get('/release/', {
            'query': f'release:"{title}" AND artist:"{artist}"',
            'limit': 5,
        })
    except Exception:
        return None, 0
    best_id, best = None, 0.0
    for r in data.get('releases', []):
        mb_s   = r.get('score', 0)
        r_cnt  = sum(m.get('track-count', 0) for m in r.get('media', []))
        cnt_s  = max(0.0, 1.0 - abs(r_cnt - track_count) / max(track_count, 1)) * 100
        r_year = int((r.get('date') or '0')[:4] or '0')
        yr_s   = 100 if r_year == year else (50 if abs(r_year - year) <= 1 else 0)
        score  = mb_s * 0.65 + cnt_s * 0.25 + yr_s * 0.10
        if score > best:
            best, best_id = score, r['id']
    return (best_id, round(best)) if best_id and best >= 65 else (None, 0)


def mb_fetch_recording_ids(release_mbid: str) -> 'tuple[dict, dict, str | None]':
    """Return (by_isrc, by_title, release_group_mbid) for a release MBID."""
    try:
        data = _mb_get(f'/release/{release_mbid}', {'inc': 'recordings isrcs release-groups'})
    except Exception:
        return {}, {}, None
    by_isrc, by_title = {}, {}
    for medium in data.get('media', []):
        for track in medium.get('tracks', []):
            rec    = track.get('recording', {})
            rec_id = rec.get('id')
            if not rec_id:
                continue
            for isrc in rec.get('isrcs', []):
                by_isrc[isrc] = rec_id
            norm = _norm(rec.get('title') or track.get('title', ''))
            if norm:
                by_title[norm] = rec_id
    rg_mbid = (data.get('release-group') or {}).get('id')
    return by_isrc, by_title, rg_mbid


def mb_fetch_release_data(mbid: str) -> 'tuple[str, str, str | None]':
    """Return (release_date, rg_first_date, wikipedia_url) for a release MBID."""
    data = _mb_get_safe(f'/release/{mbid}', {'inc': 'release-groups'})
    if not data:
        return None, None, None
    release_date = (data.get('date') or '').strip()
    rg           = data.get('release-group') or {}
    rg_mbid      = rg.get('id')
    rg_first, wiki_url = '', None
    if rg_mbid:
        rg_data = _mb_get_safe(f'release-group/{rg_mbid}', {'inc': 'url-rels'})
        if rg_data:
            rg_first = (rg_data.get('first-release-date') or '').strip()
            for rel in rg_data.get('relations') or []:
                url = (rel.get('url') or {}).get('resource', '')
                if 'wikipedia.org/wiki/' in url:
                    wiki_url = url
                    break
    return release_date, rg_first, wiki_url


def mb_fetch_artist_data(mbid: str) -> dict:
    """Fetch artist metadata from MusicBrainz.
    Returns dict with type, gender, country, formed_year, disbanded_year,
    sort_name, disambiguation, and wikipedia_url."""
    data = _mb_get_safe(f'/artist/{mbid}', {'inc': 'url-rels'})
    if not data:
        return {}
    out = {}
    for key in ('type', 'gender', 'country', 'sort-name', 'disambiguation'):
        val = data.get(key)
        if val:
            out[key.replace('-', '_')] = val
    span  = data.get('life-span') or {}
    begin = (span.get('begin') or '')[:4]
    end   = (span.get('end')   or '')[:4]
    if begin.isdigit():
        out['formed_year']    = int(begin)
    if end.isdigit():
        out['disbanded_year'] = int(end)
    for rel in data.get('relations') or []:
        url = (rel.get('url') or {}).get('resource', '')
        if 'wikipedia.org/wiki/' in url:
            out['wikipedia_url'] = url
            break
    return out
