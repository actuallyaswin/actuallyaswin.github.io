"""mdb_apis — HTTP clients for Spotify and MusicBrainz."""

__all__ = [
    'RateLimiter',
    'MetadataRelease',
    'SpotifyClient',
    'SpotifyRelease',
    'MusicBrainzRelease',
    'BeatportRelease',
    'ItunesRelease',
    'BandcampRelease',
    'DeezerRelease',
    'compare_releases',
    'MB_API', 'MB_UA', 'SP_TOKEN', 'SP_BASE',
    'CAA_API',
    'AOTY_SEARCH', 'AOTY_UA', 'AOTY_RETRY',
    'AOTY_AHEAD', 'DATES_AHEAD',
    '_extract_mbid',
    'caa_fetch_front_image_url',
    'mb_find_release', 'mb_fetch_recording_ids',
    'mb_fetch_release_data', 'mb_fetch_artist_data',
    'mb_fetch_release_group_releases',
    'mb_find_release_group',
    'mb_canonical_score', 'mb_release_reasons',
    'mb_rg_from_wiki_url',
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
from typing import Protocol, runtime_checkable

from mdb_strings import ascii_key as _norm, extract_mbid as _extract_mbid_or_none

log = logging.getLogger(__name__)


# ── Shared release interface ───────────────────────────────────────────────────

@runtime_checkable
class MetadataRelease(Protocol):
    """Structural protocol shared by all provider release classes.

    Every class that implements this interface can be passed to compare_releases()
    and used as a source in ReleaseMerge without any explicit inheritance.
    """
    @property
    def name(self) -> str: ...
    @property
    def artist(self) -> str: ...
    @property
    def year(self) -> str: ...
    @property
    def date(self) -> str: ...
    @property
    def tracks(self) -> list: ...
    @property
    def track_count(self) -> int: ...
    @property
    def explicit_count(self) -> int: ...
    @property
    def total_ms(self) -> int: ...
    @property
    def label(self) -> str: ...
    @property
    def album_type(self) -> str: ...
    def canonical_score(self) -> tuple: ...
    def _ensure_full(self) -> None: ...

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


def _http_get(url: str, *, headers: dict = None, lim: 'RateLimiter | None' = None,
              timeout: int = 10) -> bytes:
    """Make a GET request and return the raw response bytes.

    Centralises urllib boilerplate used across all provider classes.
    Applies the rate limiter before opening the connection (so the limiter
    fires even when the caller caches the result and skips the call).
    """
    if lim:
        lim.wait()
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _http_get_json(url: str, *, headers: dict = None, lim: 'RateLimiter | None' = None,
                   timeout: int = 10) -> dict:
    """GET + JSON decode."""
    return json.loads(_http_get(url, headers=headers, lim=lim, timeout=timeout))


_mb_lim   = RateLimiter(MB_INTERVAL)
_wiki_lim = RateLimiter(WIKI_INTERVAL)
_aoty_lim = RateLimiter(AOTY_INTERVAL)


# ── Spotify client ─────────────────────────────────────────────────────────────

_SP_TOKEN_CACHE = os.path.expanduser('~/.cache/mdb/spotify_token.json')


class SpotifyClient:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id     = client_id
        self.client_secret = client_secret
        self._token        = None
        self._expiry       = 0.0
        self._load_cached_token()

    def _load_cached_token(self) -> None:
        """Read a previously-saved token so successive mdb invocations skip the fetch."""
        try:
            with open(_SP_TOKEN_CACHE) as f:
                d = json.load(f)
            if d.get('client_id') == self.client_id and time.time() < d.get('expiry', 0) - 60:
                self._token  = d['access_token']
                self._expiry = d['expiry']
        except (FileNotFoundError, json.JSONDecodeError, KeyError, ValueError):
            pass

    def _ensure_token(self) -> None:
        if self._token and time.time() < self._expiry - 60:
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
        try:
            os.makedirs(os.path.dirname(_SP_TOKEN_CACHE), exist_ok=True)
            with open(_SP_TOKEN_CACHE, 'w') as f:
                json.dump({'client_id': self.client_id, 'access_token': self._token,
                           'expiry': self._expiry}, f)
        except OSError:
            pass  # cache write failure is non-fatal

    def get(self, path: str, params: dict = None) -> dict:
        self._ensure_token()
        url = SP_BASE + path
        if params:
            url += '?' + urllib.parse.urlencode(params)
        return _http_get_json(url, headers={'Authorization': f'Bearer {self._token}'})

    def get_album(self, album_id: str) -> dict:
        album  = self.get(f'/albums/{album_id}')
        tracks = list(album['tracks']['items'])
        nxt    = album['tracks'].get('next')
        while nxt:
            self._ensure_token()
            page = _http_get_json(nxt, headers={'Authorization': f'Bearer {self._token}'})
            tracks.extend(page['items'])
            nxt = page.get('next')
        album['_all_tracks'] = tracks
        return album

    def get_tracks_batch(self, ids: list) -> list:
        out = []
        for i in range(0, len(ids), 50):
            out.extend(self.get('/tracks', {'ids': ','.join(ids[i:i+50])})['tracks'])
        return out

    def get_albums_batch(self, ids: list) -> list:
        out = []
        for i in range(0, len(ids), 20):
            out.extend(self.get('/albums', {'ids': ','.join(ids[i:i+20])})['albums'])
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
    try:
        data = _http_get_json(
            f'{CAA_API}/release/{release_mbid}',
            headers={'User-Agent': MB_UA, 'Accept': 'application/json'},
            lim=_mb_lim,
        )
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


# ── BeatportRelease ────────────────────────────────────────────────────────────

BP_UA         = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                 ' (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
BP_INTERVAL   = 2.0
_bp_lim       = RateLimiter(BP_INTERVAL)
_BP_RELEASE_RE = re.compile(r'beatport\.com/release/([^/?#]+)/(\d+)', re.IGNORECASE)


class BeatportRelease:
    """
    Wrapper around a Beatport release page, scraping the embedded __NEXT_DATA__ JSON.

    Mirrors the MusicBrainzRelease / SpotifyRelease interface so it can be
    passed to import_album_from_beatport.

    .tracks is a list of dicts with:
      name, duration_ms, _isrcs, _disc_number, _track_number, _artist_credit,
      _bpm, _mix_name, _genre, _sub_genre, _key, _key_camelot, _label_name,
      _remixers, _track_id
    where _artist_credit matches the MB format: [{'artist': {'id': str, 'name': str, 'slug': str}}]
    and duration_ms is populated from length_ms in the __NEXT_DATA__ JSON.
    """

    def __init__(self, url_or_id: str):
        m = _BP_RELEASE_RE.search(str(url_or_id))
        if m:
            self._slug     = m.group(1)
            self.beatport_id = int(m.group(2))
        else:
            try:
                self.beatport_id = int(url_or_id)
                self._slug       = 'x'
            except (ValueError, TypeError):
                raise ValueError(f'Cannot parse Beatport release ID from: {url_or_id!r}')
        self.url   = f'https://www.beatport.com/release/{self._slug}/{self.beatport_id}'
        self._data: 'dict | None' = None

    def _ensure_full(self) -> None:
        if self._data is not None:
            return
        html = _http_get(self.url, headers={'User-Agent': BP_UA}, lim=_bp_lim,
                         timeout=15).decode('utf-8', errors='replace')

        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html
        )
        if not m:
            raise RuntimeError(f'No __NEXT_DATA__ found at {self.url}')

        full_data  = json.loads(m.group(1))
        page_props = full_data.get('props', {}).get('pageProps', {})
        queries    = (page_props.get('dehydratedState') or {}).get('queries', [])

        # -- Release metadata: pageProps.release is the most direct path (Harmony approach)
        release_info: dict = page_props.get('release') or {}
        if not release_info:
            # Fallback: scan queries for the one containing 'upc'
            for q in queries:
                data = (q.get('state') or {}).get('data') or {}
                if isinstance(data, dict) and 'upc' in data:
                    release_info = data
                    break
        if not release_info:
            raise RuntimeError(f'Release metadata not found in __NEXT_DATA__ at {self.url}')

        # -- Track data: Harmony uses queries[1]; scan all queries as fallback
        raw_tracks: list = []
        candidates = ([queries[1]] if len(queries) > 1 else []) + \
                     [q for i, q in enumerate(queries) if i != 1]
        for q in candidates:
            data = (q.get('state') or {}).get('data') or {}
            if not isinstance(data, dict):
                continue
            results = data.get('results') or []
            if results and isinstance(results[0], dict) and 'bpm' in results[0]:
                raw_tracks = results
                break

        # -- Track ordering: match by URL against release.tracks (Harmony approach)
        # release.tracks is a list of API URL strings in descending track-ID order;
        # reversing gives ascending (track 1 first) order.
        release_track_urls = list(reversed(release_info.get('tracks') or []))
        track_by_url = {t.get('url'): t for t in raw_tracks if t.get('url')}

        ordered_tracks = [track_by_url[url] for url in release_track_urls if url in track_by_url]
        if not ordered_tracks and raw_tracks:
            # Fallback: reversed order (handles older responses without url field)
            ordered_tracks = list(reversed(raw_tracks))

        tracks = []
        for i, t in enumerate(ordered_tracks):
            mix_name  = (t.get('mix_name') or '').strip()
            base_name = (t.get('name') or '').strip()
            full_name = f'{base_name} ({mix_name})' if mix_name else base_name

            artist_credit = [
                {'artist': {'id': str(a.get('id', '')), 'name': a.get('name', ''), 'slug': a.get('slug', '')}}
                for a in (t.get('artists') or [])
            ]
            remixer_credit = [
                {'artist': {'id': str(r.get('id', '')), 'name': r.get('name', ''), 'slug': r.get('slug', '')}}
                for r in (t.get('remixers') or [])
            ]
            genre_obj     = t.get('genre')     or {}
            sub_genre_obj = t.get('sub_genre') or {}
            key_obj       = t.get('key')        or {}
            label_obj     = t.get('label')      or {}

            # Camelot Wheel key notation (e.g. "8A" for A minor, "9B" for F major)
            camelot_num = key_obj.get('camelot_number', '')
            camelot_let = key_obj.get('camelot_letter', '')
            camelot_key = f'{camelot_num}{camelot_let}' if camelot_num and camelot_let else None

            tracks.append({
                'name':           full_name,
                'duration_ms':    t.get('length_ms'),  # ms duration, always present in __NEXT_DATA__
                '_isrcs':         [t['isrc']] if t.get('isrc') else [],
                '_disc_number':   1,
                '_track_number':  i + 1,
                '_artist_credit': artist_credit,
                '_bpm':           t.get('bpm'),
                '_mix_name':      mix_name,
                '_genre':         genre_obj.get('name', ''),
                '_sub_genre':     sub_genre_obj.get('name', '') if sub_genre_obj else '',
                '_key':           key_obj.get('name', ''),
                '_key_camelot':   camelot_key,
                '_label_name':    label_obj.get('name', ''),
                '_remixers':      remixer_credit,
                '_track_id':      t.get('id'),  # Beatport numeric track ID
            })

        release_info['results']     = raw_tracks   # keep raw results accessible
        release_info['_track_list'] = tracks
        self._data = release_info

    @property
    def name(self) -> str:
        self._ensure_full()
        return (self._data.get('name') or '').strip()

    @property
    def artist(self) -> str:
        """First artist name from the first track's credits."""
        self._ensure_full()
        for t in self._data.get('_track_list', []):
            for credit in (t.get('_artist_credit') or []):
                name = (credit.get('artist') or {}).get('name', '')
                if name:
                    return name
        return ''

    @property
    def year(self) -> str:
        return (self.date or '')[:4]

    @property
    def date(self) -> str:
        self._ensure_full()
        return (
            self._data.get('new_release_date') or
            self._data.get('publish_date') or
            ''
        ).strip()

    @property
    def tracks(self) -> list:
        self._ensure_full()
        return self._data['_track_list']

    @property
    def track_count(self) -> int:
        return len(self.tracks)

    @property
    def explicit_count(self) -> int:
        return 0  # Beatport does not expose explicit flags

    @property
    def total_ms(self) -> int:
        return sum(t.get('duration_ms') or 0 for t in self.tracks)

    @property
    def label(self) -> str:
        self._ensure_full()
        return ((self._data.get('label') or {}).get('name') or '').strip()

    @property
    def album_type(self) -> str:
        n = self.track_count
        if n == 1:
            return 'single'
        if n <= 4:
            return 'ep'
        return 'album'

    @property
    def image_url(self) -> 'str | None':
        self._ensure_full()
        img = self._data.get('image') or {}
        uri = img.get('uri')
        if uri:
            return uri
        dyn = img.get('dynamic_uri', '')
        if dyn:
            return dyn.replace('{w}x{h}', '1400x1400')
        return None

    def canonical_score(self) -> tuple:
        d    = self.date
        prec = (3 if (len(d) == 10 and not d.endswith('-01-01'))
                else (2 if len(d) == 7 else 1))
        return (-prec, d, self.track_count,
                1 if _EDITION_RE.search(self.name) else 0,
                0)


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

def compare_releases(*releases: MetadataRelease) -> dict:
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
    url = f'{MB_API}{path}?' + urllib.parse.urlencode(p)
    return _http_get_json(url, headers={'User-Agent': MB_UA}, lim=_mb_lim)


def _mb_get_safe(path: str, params: dict = None) -> 'dict | None':
    try:
        return _mb_get(path, params)
    except Exception as e:
        log.debug('MB error %s: %s', path, e)
        return None


# ── MusicBrainz query functions ────────────────────────────────────────────────

def mb_rg_from_wiki_url(wiki_url: str) -> 'str | None':
    """Resolve a Wikipedia article URL to a MusicBrainz release-group MBID.

    MB stores Wikipedia URL relations on release groups.  We look up the URL
    entity, then follow the release-group relation back to its MBID.
    Returns the MBID string or None if not found.
    """
    # Normalise URL: ensure https, strip anchors/query, decode %xx
    url = re.sub(r'^http:', 'https:', wiki_url.split('#')[0].split('?')[0])
    data = _mb_get_safe('/url', {'resource': url, 'inc': 'release-group-rels'})
    if not data:
        return None
    for rel in (data.get('relations') or []):
        if rel.get('target-type') == 'release-group':
            rg = rel.get('release-group') or {}
            if rg.get('id'):
                return rg['id']
    return None


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


def mb_find_release_group(title: str, artist: str, year: int = 0) -> 'str | None':
    """Search MB for a release group matching title/artist.
    Returns the release-group MBID of the best match, or None."""
    try:
        query = f'releasegroup:"{title}"'
        if artist:
            query += f' AND artist:"{artist}"'
        data = _mb_get('/release-group', {'query': query, 'limit': 5})
    except Exception:
        return None
    for rg in (data.get('release-groups') or []):
        score = int(rg.get('score') or 0)
        if score < 80:
            continue
        # Optionally verify the year against first-release-date
        if year:
            rg_year = int((rg.get('first-release-date') or '0')[:4] or '0')
            if rg_year and abs(rg_year - year) > 1:
                continue
        return rg['id']
    return None


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


def mb_fetch_release_group_releases(rg_mbid: str) -> list:
    """Fetch all releases in a MusicBrainz release group, including track counts.

    Uses the release browse endpoint (/release?release-group=…&inc=media) so
    each release stub includes its media/track-count data.
    Returns a list sorted by date ascending (oldest first).  Empty list on error.
    """
    data = _mb_get_safe('/release', {
        'release-group': rg_mbid,
        'inc': 'media',
        'limit': 100,
    })
    if not data:
        return []
    releases = data.get('releases') or []
    return sorted(releases, key=lambda r: (r.get('date') or '9999'))


def mb_canonical_score(r: dict) -> tuple:
    """Sortable score for an MB release dict — lower = more canonical.

    Criteria (in order):
      1. Clean title (no edition qualifiers)
      2. Official status
      3. Worldwide release (country == 'XW' preferred)
      4. Earliest date
      5. Smaller track count (no bonus-disc inflation)
    """
    from mdb_strings import detect_variant_type
    title   = r.get('title') or ''
    status  = (r.get('status') or '').lower()
    country = r.get('country') or ''
    date    = r.get('date') or '9999'
    n       = sum(m.get('track-count', 0) for m in (r.get('media') or []))

    has_edition = 1 if detect_variant_type(title) is not None else 0
    not_official = 0 if status == 'official' else 1
    not_worldwide = 0 if country == 'XW' else (1 if country else 2)
    return (has_edition, not_official, not_worldwide, date, n)


def mb_release_reasons(candidates: list, canonical: dict) -> dict:
    """Return {mbid: [reason, ...]} explaining why each candidate is non-canonical.

    candidates: list of MB release dicts (the non-canonical ones)
    canonical:  the MB release dict scored as canonical
    """
    from mdb_strings import detect_variant_type
    can_date = canonical.get('date') or ''
    can_n    = sum(m.get('track-count', 0) for m in (canonical.get('media') or []))
    can_status = (canonical.get('status') or '').lower()

    reasons = {}
    for r in candidates:
        mbid  = r.get('id') or ''
        title = r.get('title') or ''
        date  = r.get('date') or ''
        n     = sum(m.get('track-count', 0) for m in (r.get('media') or []))
        status  = (r.get('status') or '').lower()
        country = r.get('country') or ''

        rs = []
        if date and can_date and date > can_date:
            rs.append(f'later release ({date} > {can_date})')
        elif date and can_date and date == can_date and n == can_n:
            rs.append('same date and tracklist — re-upload artifact')
        if n > can_n:
            rs.append(f'{n - can_n} bonus track(s)')
        vtype = detect_variant_type(title)
        if vtype and not detect_variant_type(canonical.get('title') or ''):
            rs.append(f'edition qualifier ({vtype})')
        if status and status != 'official' and can_status == 'official':
            rs.append(f'status: {status}')
        can_country = canonical.get('country') or ''
        if country and country != 'XW' and can_country == 'XW':
            rs.append(f'regional pressing ({country})')
        reasons[mbid] = rs
    return reasons


# ── iTunes / Apple Music ───────────────────────────────────────────────────────

ITUNES_LOOKUP = 'https://itunes.apple.com/lookup'
ITUNES_INTERVAL = 3.0
_itunes_lim = RateLimiter(ITUNES_INTERVAL)
_ITUNES_ID_RE = re.compile(
    r'(?:music\.apple\.com/[a-z]{2}/album/[^/]+/|itunes\.apple\.com/[a-z]{2}/album/[^/]+/)(\d+)'
    r'|/(\d{7,12})$',   # bare numeric ID (7-12 digits, distinct from Beatport's shorter IDs)
    re.IGNORECASE,
)
_ITUNES_BARE_RE = re.compile(r'^\d{7,12}$')


class ItunesRelease:
    """
    Lazy-loading wrapper around an Apple Music / iTunes album.

    Uses the public iTunes Lookup API (no authentication required).

    Constructs from a music.apple.com URL, an itunes.apple.com URL, or a bare
    iTunes numeric collection ID (7-12 digits).

    Cover art: artworkUrl100 with '100x100bb' replaced by '3000x3000bb'.
    """

    def __init__(self, id_or_url: str):
        m = _ITUNES_ID_RE.search(str(id_or_url))
        if m:
            self.itunes_id = m.group(1) or m.group(2)
        elif _ITUNES_BARE_RE.match(str(id_or_url).strip()):
            self.itunes_id = str(id_or_url).strip()
        else:
            raise ValueError(f'Cannot parse iTunes collection ID from: {id_or_url!r}')
        self.url   = f'https://music.apple.com/album/{self.itunes_id}'
        self._data: 'dict | None' = None

    def _ensure_full(self) -> None:
        if self._data is not None:
            return
        url = (f'{ITUNES_LOOKUP}?id={self.itunes_id}&entity=song'
               f'&limit=200&country=US')
        raw = _http_get_json(url, headers={'User-Agent': MB_UA}, lim=_itunes_lim)

        results = raw.get('results') or []
        album = None
        tracks = []
        for item in results:
            if item.get('wrapperType') == 'collection':
                album = item
            elif item.get('wrapperType') == 'track' and item.get('kind') == 'song':
                tracks.append(item)

        if not album:
            raise RuntimeError(f'No album found in iTunes response for id {self.itunes_id}')

        # Build normalised track list
        track_list = []
        for t in sorted(tracks, key=lambda x: (x.get('discNumber', 1), x.get('trackNumber', 0))):
            track_list.append({
                'name':         t.get('trackName', ''),
                'duration_ms':  t.get('trackTimeMillis'),
                '_disc_number': t.get('discNumber', 1),
                '_track_number': t.get('trackNumber', 0),
                '_is_explicit': t.get('trackExplicitness') == 'explicit',
            })

        album['_track_list'] = track_list
        self._data = album

    # -- properties --

    @property
    def name(self) -> str:
        self._ensure_full()
        return (self._data.get('collectionName') or '').strip()

    @property
    def artist(self) -> str:
        self._ensure_full()
        return (self._data.get('artistName') or '').strip()

    @property
    def year(self) -> str:
        return (self.date or '')[:4]

    @property
    def date(self) -> str:
        self._ensure_full()
        raw = (self._data.get('releaseDate') or '').strip()
        return raw[:10] if raw else ''  # YYYY-MM-DDTHH:MM:SS → YYYY-MM-DD

    @property
    def tracks(self) -> list:
        self._ensure_full()
        return self._data['_track_list']

    @property
    def track_count(self) -> int:
        self._ensure_full()
        return self._data.get('trackCount') or len(self.tracks)

    @property
    def label(self) -> str:
        self._ensure_full()
        return ''  # Not exposed by iTunes Lookup API

    @property
    def album_type(self) -> str:
        self._ensure_full()
        kind = (self._data.get('collectionType') or '').lower()
        if kind == 'album':
            return 'album'
        if kind == 'single':
            return 'single'
        if kind == 'ep':
            return 'ep'
        n = self.track_count
        return 'single' if n == 1 else ('ep' if n <= 4 else 'album')

    @property
    def image_url(self) -> 'str | None':
        """3000×3000 cover art via URL parameter replacement."""
        self._ensure_full()
        art = (self._data.get('artworkUrl100') or '').strip()
        if not art:
            return None
        # Replace standard size suffix with 3000×3000
        art = re.sub(r'\b\d+x\d+bb\b', '3000x3000bb', art)
        art = re.sub(r'\b100x100\b', '3000x3000', art)
        return art

    @property
    def apple_music_id(self) -> str:
        return self.itunes_id

    def canonical_score(self) -> tuple:
        d    = self.date
        prec = (3 if len(d) == 10 else (2 if len(d) == 7 else 1))
        return (-prec, d, self.track_count, 0, 0)


# ── Bandcamp ────────────────────────────────────────────────────────────────────

BC_UA       = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
               ' (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
BC_INTERVAL = 2.0
_bc_lim     = RateLimiter(BC_INTERVAL)
_BC_URL_RE  = re.compile(r'https?://[^./]+\.bandcamp\.com/album/[^/?#]+', re.IGNORECASE)
_BC_ART_RE  = re.compile(r'"art_id"\s*:\s*(\d+)')


class BandcampRelease:
    """
    Lazy-loading wrapper around a Bandcamp album page.

    Scrapes the `data-tralbum` JSON embedded in the album HTML.

    Bandcamp is URL-input only — no GTIN-based auto-discovery.
    Main contributions: high-res cover art (3000px), sometimes UPC, credits text.
    """

    def __init__(self, url: str):
        if not _BC_URL_RE.match(str(url)):
            raise ValueError(f'Not a Bandcamp album URL: {url!r}')
        self.url   = url
        self._data: 'dict | None' = None

    def _ensure_full(self) -> None:
        if self._data is not None:
            return

        import html as _html_mod

        raw_html = _http_get(self.url, headers={'User-Agent': BC_UA}, lim=_bc_lim,
                             timeout=15).decode('utf-8', errors='replace')

        # Strategy 1: data-tralbum attribute (most Bandcamp pages)
        m = re.search(r'\bdata-tralbum="([^"]+)"', raw_html)
        if m:
            tralbum = json.loads(_html_mod.unescape(m.group(1)))
            band_m  = re.search(r'\bdata-band="([^"]+)"', raw_html)
            band    = json.loads(_html_mod.unescape(band_m.group(1))) if band_m else {}
            self._data = self._normalise({'tralbum': tralbum, 'band': band})
            return

        # Strategy 2: <script>var TralbumData = {...};</script>
        m = re.search(r'TralbumData\s*=\s*(\{.*?\})\s*;?\s*\n', raw_html, re.DOTALL)
        if m:
            tralbum = json.loads(m.group(1))
            self._data = self._normalise({'tralbum': tralbum, 'band': {}})
            return

        raise RuntimeError(f'Could not extract tralbum JSON from Bandcamp page: {self.url}')

    def _normalise(self, page: dict) -> dict:
        """Flatten the nested tralbum/band structure into a flat _data dict."""
        tralbum = page.get('tralbum') or {}
        current = tralbum.get('current') or {}
        band    = page.get('band') or {}
        tracks  = []
        for t in (tralbum.get('trackinfo') or []):
            dur_s = t.get('duration') or 0.0
            tracks.append({
                'name':          t.get('title', ''),
                'duration_ms':   int(dur_s * 1000) if dur_s else None,
                '_track_number': t.get('track_num'),
                '_disc_number':  1,
            })

        # Release date: try 'current.release_date' (e.g. "30 Apr 2025 00:00:00 GMT")
        release_date = _parse_bc_date(current.get('release_date') or
                                      tralbum.get('album_release_date') or '')

        # Cover art URL: https://f4.bcbits.com/img/a{art_id}_0.jpg
        art_id  = tralbum.get('art_id') or current.get('art_id')
        art_url = f'https://f4.bcbits.com/img/a{art_id}_0.jpg' if art_id else None

        return {
            'title':        current.get('title') or '',
            'artist':       tralbum.get('artist') or current.get('artist') or band.get('name') or '',
            'band_name':    band.get('name') or '',
            'release_date': release_date,
            'upc':          current.get('upc') or None,
            'credits':      current.get('credits') or None,
            'about':        current.get('about') or None,
            'item_type':    tralbum.get('item_type') or 'album',
            'art_url':      art_url,
            '_track_list':  tracks,
        }

    # -- properties --

    @property
    def name(self) -> str:
        self._ensure_full()
        return (self._data.get('title') or '').strip()

    @property
    def artist(self) -> str:
        self._ensure_full()
        return (self._data.get('artist') or '').strip()

    @property
    def year(self) -> str:
        return (self.date or '')[:4]

    @property
    def date(self) -> str:
        self._ensure_full()
        return self._data.get('release_date') or ''

    @property
    def tracks(self) -> list:
        self._ensure_full()
        return self._data['_track_list']

    @property
    def track_count(self) -> int:
        return len(self.tracks)

    @property
    def label(self) -> str:
        return ''  # Not reliably extractable without band/artist disambiguation

    @property
    def upc(self) -> 'str | None':
        self._ensure_full()
        return self._data.get('upc')

    @property
    def credits_text(self) -> 'str | None':
        self._ensure_full()
        return self._data.get('credits')

    @property
    def image_url(self) -> 'str | None':
        self._ensure_full()
        return self._data.get('art_url')

    @property
    def album_type(self) -> str:
        n = self.track_count
        return 'single' if n == 1 else ('ep' if n <= 4 else 'album')

    def canonical_score(self) -> tuple:
        d    = self.date
        prec = (3 if len(d) == 10 else (2 if len(d) == 7 else 1))
        return (-prec, d, self.track_count, 0, 0)


def _parse_bc_date(raw: str) -> str:
    """Parse a Bandcamp release date string to YYYY-MM-DD.

    Handles formats like:
      "30 Apr 2025 00:00:00 GMT"  → "2025-04-30"
      "2025-04-30"                → "2025-04-30"
      "April 30, 2025"            → "2025-04-30"
    """
    if not raw:
        return ''
    raw = raw.strip()
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', raw):
        return raw
    from mdb_strings import MONTHS
    # "30 Apr 2025 ..."
    m = re.match(r'(\d{1,2})\s+(\w{3,})\s+(\d{4})', raw, re.IGNORECASE)
    if m:
        day, mon, year = m.group(1), m.group(2).lower(), m.group(3)
        if mon in MONTHS:
            return f'{year}-{MONTHS[mon]}-{day.zfill(2)}'
    # "April 30, 2025"
    m = re.match(r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', raw, re.IGNORECASE)
    if m:
        mon, day, year = m.group(1).lower(), m.group(2), m.group(3)
        if mon in MONTHS:
            return f'{year}-{MONTHS[mon]}-{day.zfill(2)}'
    # Just return whatever year we can find
    m = re.search(r'\b(\d{4})\b', raw)
    return m.group(1) if m else ''


# ── Deezer ─────────────────────────────────────────────────────────────────────

DZ_API      = 'https://api.deezer.com'
DZ_INTERVAL = 1.0
_dz_lim     = RateLimiter(DZ_INTERVAL)
_DZ_URL_RE  = re.compile(r'deezer\.com/(?:[a-z]{2}/)?album/(\d+)', re.IGNORECASE)


class DeezerRelease:
    """
    Lazy-loading wrapper around a Deezer album.

    Uses the public Deezer API — no authentication required.
    Main contributions: UPC (for GTIN broadcast), ISRCs, release date, label,
    and 1000×1000 cover art.  Track durations are in whole seconds (lower
    precision than Spotify); they serve only as a last-resort fallback.
    """

    def __init__(self, url_or_id: str):
        m = _DZ_URL_RE.search(str(url_or_id))
        if m:
            self.deezer_id = m.group(1)
        elif re.match(r'^\d+$', str(url_or_id).strip()):
            self.deezer_id = str(url_or_id).strip()
        else:
            raise ValueError(f'Cannot parse Deezer album ID from: {url_or_id!r}')
        self.url   = f'https://www.deezer.com/album/{self.deezer_id}'
        self._data: 'dict | None' = None

    def _ensure_full(self) -> None:
        if self._data is not None:
            return
        album = _http_get_json(f'{DZ_API}/album/{self.deezer_id}', lim=_dz_lim)
        if album.get('error'):
            raise RuntimeError(f'Deezer API error for album {self.deezer_id}: {album["error"]}')

        tracks_resp = _http_get_json(
            f'{DZ_API}/album/{self.deezer_id}/tracks?limit=200', lim=_dz_lim
        )
        track_list = []
        for i, t in enumerate(tracks_resp.get('data') or []):
            dur_s = t.get('duration') or 0
            track_list.append({
                'name':             t.get('title', ''),
                'duration_ms':      dur_s * 1000 if dur_s else None,  # seconds → ms
                '_isrcs':           [t['isrc']] if t.get('isrc') else [],
                '_disc_number':     t.get('disk_number', 1),
                '_track_number':    t.get('track_position', i + 1),
                '_artist_credit':   [
                    {'artist': {'id': str(c.get('id', '')), 'name': c.get('name', '')}}
                    for c in (t.get('contributors') or [])
                ],
            })

        album['_track_list'] = track_list
        self._data = album

    # -- properties --

    @property
    def name(self) -> str:
        self._ensure_full()
        return (self._data.get('title') or '').strip()

    @property
    def artist(self) -> str:
        self._ensure_full()
        return (self._data.get('artist') or {}).get('name', '')

    @property
    def year(self) -> str:
        return (self.date or '')[:4]

    @property
    def date(self) -> str:
        self._ensure_full()
        return (self._data.get('release_date') or '').strip()

    @property
    def tracks(self) -> list:
        self._ensure_full()
        return self._data['_track_list']

    @property
    def track_count(self) -> int:
        self._ensure_full()
        return self._data.get('nb_tracks') or len(self.tracks)

    @property
    def explicit_count(self) -> int:
        return 0

    @property
    def total_ms(self) -> int:
        return sum(t.get('duration_ms') or 0 for t in self.tracks)

    @property
    def label(self) -> str:
        self._ensure_full()
        return (self._data.get('label') or '').strip()

    @property
    def upc(self) -> 'str | None':
        self._ensure_full()
        return self._data.get('upc') or None

    @property
    def album_type(self) -> str:
        self._ensure_full()
        rt = (self._data.get('record_type') or '').lower()
        return rt if rt in ('album', 'ep', 'single') else 'album'

    @property
    def image_url(self) -> 'str | None':
        """1000×1000 cover art (cover_xl)."""
        self._ensure_full()
        return self._data.get('cover_xl') or self._data.get('cover_big') or None

    def canonical_score(self) -> tuple:
        d    = self.date
        prec = (3 if len(d) == 10 else (2 if len(d) == 7 else 1))
        return (-prec, d, self.track_count, 0, 0)
