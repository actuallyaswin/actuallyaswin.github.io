"""
mdb_apis — HTTP clients for Spotify and MusicBrainz.

Provides: RateLimiter, SpotifyClient, MB fetch helpers, MB query functions.
Rate limiter instances (_mb_lim, _wiki_lim, _aoty_lim) are exported for use
by mdb_websources.

Dependency order: mdb_strings → mdb_ops → mdb_apis
"""

import json
import logging
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from mdb_strings import ascii_key as _norm

log = logging.getLogger(__name__)

# ── API constants ──────────────────────────────────────────────────────────────

MB_API  = 'https://musicbrainz.org/ws/2'
MB_UA   = 'aswin-music-browser/1.0 (personal project)'
SP_TOKEN = 'https://accounts.spotify.com/api/token'
SP_BASE  = 'https://api.spotify.com/v1'

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
