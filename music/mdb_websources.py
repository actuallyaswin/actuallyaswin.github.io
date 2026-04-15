"""mdb_websources — Web scrapers for AOTY and Wikipedia.

DB persistence (save_aoty_data, save_release_date) lives in mdb_ops.
"""

import difflib
import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request

try:
    import requests
    from bs4 import BeautifulSoup
    _SCRAPING_AVAILABLE = True
except ImportError:
    _SCRAPING_AVAILABLE = False

from mdb_strings import MONTHS, _should_update_date
from mdb_apis import (
    _aoty_lim, _wiki_lim,
    AOTY_SEARCH, AOTY_UA, AOTY_RETRY, MB_UA,
    mb_fetch_release_data,
)

log = logging.getLogger(__name__)

# ── AOTY type mapping ──────────────────────────────────────────────────────────

AOTY_TYPE_MAP = {
    'LP':          ('album',  None),
    'EP':          ('ep',     None),
    'Single':      ('single', None),
    'Mixtape':     ('album',  'mixtape'),
    'Soundtrack':  ('album',  'soundtrack'),
    'Compilation': ('album',  'compilation'),
    'Live Album':  ('album',  'live'),
    'Demo':        ('other',  None),
    'Bootleg':     ('other',  'bootleg'),
    'Remix':       (None,     'remix'),    # ambiguous primary — sub-prompt fires
    'Reissue':     ('album',  'reissue'),
    'Remaster':    ('album',  'remaster'),
}

# ── AOTY helpers ───────────────────────────────────────────────────────────────

_RE_DASH  = re.compile(
    r'\s*[-–—]\s*(?:(?:Official|Original|Complete|Full|Deluxe|Special|Expanded|Anniversary|Remastered?)\s+)*'
    r'(?:Soundtrack|Score|OST|Edition|Version|Release|Remaster(?:ed)?)\b.*$', re.IGNORECASE)
_RE_PAREN = re.compile(
    r'\s*\((?:Deluxe|Special|Expanded|Anniversary|Remaster(?:ed)?|Bonus\s+Track|Explicit)[^)]*\)\s*$',
    re.IGNORECASE)
_RE_TYPE  = re.compile(r'\s+(?:EP|LP|OST|Soundtrack|Mixtape|Compilation)\s*$', re.IGNORECASE)
_RE_PUNCT = re.compile(r'[\s!?:,;.\-–—]+$')


def _clean_aoty_title(title: str) -> str:
    t = _RE_DASH.sub('', title)
    t = _RE_PAREN.sub('', t)
    t = _RE_TYPE.sub('', t)
    t = _RE_PUNCT.sub('', t)
    return t.strip() or title.strip()


def _aoty_get(url: str, **kw):
    _aoty_lim.wait()
    r = requests.get(url, headers={'User-Agent': AOTY_UA}, timeout=15, **kw)
    if r.status_code == 403:
        log.debug('AOTY 403 — waiting %.0fs', AOTY_RETRY)
        time.sleep(AOTY_RETRY)
        _aoty_lim.wait()
        r = requests.get(url, headers={'User-Agent': AOTY_UA}, timeout=15, **kw)
    r.raise_for_status()
    return r


def find_aoty_url(release_name: str, artist_name: str) -> 'str | None':
    query = f'{_clean_aoty_title(release_name)} {artist_name or ""}'.strip()
    try:
        r = _aoty_get(AOTY_SEARCH, params={'q': query, 'type': 'albums'})
    except Exception as e:
        log.debug('AOTY search error: %s', e)
        return None
    soup       = BeautifulSoup(r.text, 'html.parser')
    candidates = soup.find_all('a', href=re.compile(r'^/album/'))[:8]
    if not candidates:
        return None

    target = _clean_aoty_title(release_name).lower()

    def _score(a):
        text = a.get_text(strip=True)
        if not text:
            slug = re.sub(r'^\d+-', '', a['href'].split('/')[-1].replace('.php', ''))
            text = slug.replace('-', ' ')
        return difflib.SequenceMatcher(None, _clean_aoty_title(text).lower(), target).ratio()

    best = max(candidates, key=_score)
    return 'https://www.albumoftheyear.org' + best['href']


def _empty_aoty() -> dict:
    return {'genres': [], 'release_date': None, 'release_year': None,
            'aoty_type': None, 'type': None, 'type_secondary': None,
            'score_critic': None, 'score_user': None,
            'ratings_critic': None, 'ratings_user': None}


def _parse_aoty_date(text: str) -> 'str | None':
    t = text.strip()
    m = re.fullmatch(r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', t, re.IGNORECASE)
    if m and m.group(1).lower() in MONTHS:
        return f'{m.group(3)}-{MONTHS[m.group(1).lower()]}-{m.group(2).zfill(2)}'
    m = re.fullmatch(r'(\w+)\s+(\d{4})', t, re.IGNORECASE)
    if m and m.group(1).lower() in MONTHS:
        return f'{m.group(2)}-{MONTHS[m.group(1).lower()]}'
    m = re.fullmatch(r'(\d{4})', t)
    return m.group(1) if m else None


def scrape_aoty_page(url: str) -> dict:
    try:
        r = _aoty_get(url)
    except Exception as e:
        log.debug('AOTY fetch error: %s', e)
        return _empty_aoty()
    soup = BeautifulSoup(r.text, 'html.parser')
    rows = soup.find_all('div', class_='detailRow')

    genre_row = date_row = type_row = None
    for row in rows:
        span = row.find('span')
        lbl  = span.get_text(strip=True).lstrip('/').strip().lower() if span else ''
        if 'genre' in lbl:
            genre_row = row
        elif 'release date' in lbl or lbl == 'date':
            date_row = row
        elif lbl in ('type', 'format'):
            type_row = row

    data = _empty_aoty()

    if genre_row:
        secondary_ids = set()
        for div in genre_row.find_all('div', class_='secondary'):
            a_tag = (div.parent if div.parent and div.parent.name == 'a'
                     else div.find_previous_sibling('a'))
            if a_tag:
                m2 = re.match(r'^/genre/(\d+)-', a_tag.get('href', ''))
                if m2:
                    secondary_ids.add(int(m2.group(1)))
        genres, seen = [], set()
        for a in genre_row.find_all('a', href=re.compile(r'^/genre/\d+-')):
            m = re.match(r'^/genre/(\d+)-([^/]+)/', a['href'])
            if not m:
                continue
            gid = int(m.group(1))
            if gid in seen:
                continue
            seen.add(gid)
            slug = m.group(2)
            name = a.get_text(strip=True)
            if not name:
                sib  = a.find_next_sibling('div', class_='secondary')
                name = sib.get_text(strip=True) if sib else slug.replace('-', ' ').title()
            genres.append((gid, name, slug, gid not in secondary_ids))
        data['genres'] = genres

    if date_row:
        raw = re.sub(r'/\s*release\s*date', '',
                     date_row.get_text(separator=' ', strip=True), flags=re.IGNORECASE).strip()
        d = _parse_aoty_date(raw)
        if d:
            data['release_date'] = d
            data['release_year'] = int(d[:4])

    if type_row:
        raw  = re.sub(r'/\s*(type|format)\s*$', '',
                      type_row.get_text(separator=' ', strip=True), flags=re.IGNORECASE).strip()
        pri, sec           = AOTY_TYPE_MAP.get(raw, (None, None))
        data['aoty_type']  = raw
        data['type']       = pri
        data['type_secondary'] = sec

    def _parse_score_int(el):
        if not el: return None
        m = re.search(r'\d+', el.get_text(strip=True))
        return int(m.group()) if m else None

    def _parse_score_float(el):
        if not el: return None
        m = re.search(r'\d+\.?\d*', el.get_text(strip=True))
        return float(m.group()) if m else None

    def _parse_rating_count(el):
        if not el: return None
        m = re.search(r'([\d,]+)', el.get_text(strip=True))
        return int(m.group(1).replace(',', '')) if m else None

    critic_box = soup.find('div', class_=re.compile(r'\balbumCriticScore\b'))
    if critic_box:
        data['score_critic'] = _parse_score_int(critic_box.find('a') or critic_box)
        count_el = critic_box.find_next(class_=re.compile(r'\bnumReviews\b|\bcriticReviewCount\b'))
        if count_el:
            data['ratings_critic'] = _parse_rating_count(count_el)

    user_box = soup.find('div', class_=re.compile(r'\balbumUserScore\b'))
    if user_box:
        data['score_user'] = _parse_score_float(user_box.find('a') or user_box)
        count_el = user_box.find_next(class_=re.compile(r'\bnumRatings\b|\buserRatingCount\b'))
        if count_el:
            data['ratings_user'] = _parse_rating_count(count_el)

    return data


def fetch_aoty_data(release_name: str, artist_name: str,
                    cached_url: 'str | None' = None) -> 'tuple[str | None, dict]':
    url = cached_url or find_aoty_url(release_name, artist_name)
    if not url:
        return None, _empty_aoty()
    return url, scrape_aoty_page(url)


def _has_aoty(data: dict) -> bool:
    return bool(data['genres'] or data['release_date'] or data['type']
                or data['score_critic'] is not None or data['score_user'] is not None)


def _fmt_aoty(data: dict) -> str:
    parts = []
    if data['aoty_type']:
        t = data['type'] or '?'
        if data['type_secondary']:
            t += f' + {data["type_secondary"]}'
        if not data['type']:
            t = '? (ambiguous — Album/EP/Single)'
        parts.append(f'  type:    {data["aoty_type"]}  →  {t}')
    if data['release_date']:
        parts.append(f'  date:    {data["release_date"]}')
    if data['score_critic'] is not None:
        rc = f'  ({data["ratings_critic"]} reviews)' if data['ratings_critic'] else ''
        parts.append(f'  critic:  {data["score_critic"]}/100{rc}')
    if data['score_user'] is not None:
        ru = f'  ({data["ratings_user"]} ratings)' if data['ratings_user'] else ''
        parts.append(f'  user:    {data["score_user"]:.2f}/10{ru}')
    primary   = [(i, n) for i, n, s, p in data['genres'] if p]
    secondary = [(i, n) for i, n, s, p in data['genres'] if not p]
    if primary:
        parts.append('  genres:  ' + ', '.join(f'{n} (#{i})' for i, n in primary))
    if secondary:
        parts.append('  2nd:     ' + ', '.join(f'{n} (#{i})' for i, n in secondary))
    return '\n'.join(parts)


# NOTE: save_aoty_data and save_release_date live in mdb_ops (not here) to
# keep scraping logic separate from DB write logic.

# ── Wikipedia ──────────────────────────────────────────────────────────────────

def _wiki_get_html(wiki_url: 'str | None' = None, page_id: 'int | None' = None) -> 'str | None':
    if page_id:
        params = {'action': 'parse', 'pageid': page_id,
                  'prop': 'text', 'section': 0, 'format': 'json'}
    else:
        title  = urllib.parse.unquote((wiki_url or '').split('/wiki/')[-1])
        params = {'action': 'parse', 'page': title,
                  'prop': 'text', 'section': 0, 'format': 'json'}
    api_url = 'https://en.wikipedia.org/w/api.php?' + urllib.parse.urlencode(params)
    _wiki_lim.wait()
    req = urllib.request.Request(api_url, headers={'User-Agent': MB_UA})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            d = json.loads(r.read())
        return (d.get('parse') or {}).get('text', {}).get('*')
    except Exception as e:
        log.debug('Wikipedia error: %s', e)
        return None


def _wiki_url_to_id(wiki_url: str) -> 'int | None':
    """Resolve a Wikipedia URL to its permanent integer page ID."""
    title   = urllib.parse.unquote(wiki_url.split('/wiki/')[-1])
    api_url = ('https://en.wikipedia.org/w/api.php?'
               + urllib.parse.urlencode({
                   'action': 'query', 'titles': title,
                   'redirects': '1', 'indexpageids': '1', 'format': 'json',
               }))
    _wiki_lim.wait()
    req = urllib.request.Request(api_url, headers={'User-Agent': MB_UA})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            d = json.loads(r.read())
        ids = d.get('query', {}).get('pageids', [])
        pid = int(ids[0]) if ids else None
        return pid if pid and pid > 0 else None
    except Exception as e:
        log.debug('Wikipedia ID lookup error: %s', e)
        return None


def _date_from_cell(cell_html: str) -> 'str | None':
    plain = re.sub(r'<[^>]+>', ' ', cell_html)
    m = re.search(r'class="[^"]*bday[^"]*"[^>]*>(\d{4}-\d{2}-\d{2})<', cell_html)
    if m: return m.group(1)
    m = re.search(r'datetime="(\d{4}-\d{2}-\d{2})"', cell_html)
    if m: return m.group(1)
    m = re.search(r'\b(\d{4}-\d{2}-\d{2})\b', cell_html)
    if m: return m.group(1)
    _mon = '|'.join(MONTHS)
    m = re.search(rf'\b({_mon})\s+(\d{{1,2}}),?\s+(\d{{4}})\b', plain, re.IGNORECASE)
    if m: return f'{m.group(3)}-{MONTHS[m.group(1).lower()]}-{m.group(2).zfill(2)}'
    m = re.search(rf'\b(\d{{1,2}})\s+({_mon})\s+(\d{{4}})\b', plain, re.IGNORECASE)
    if m: return f'{m.group(3)}-{MONTHS[m.group(2).lower()]}-{m.group(1).zfill(2)}'
    m = re.search(rf'\b({_mon})\s+(\d{{4}})\b', plain, re.IGNORECASE)
    if m: return f'{m.group(2)}-{MONTHS[m.group(1).lower()]}'
    m = re.search(r'\b(\d{4}-\d{2})\b', cell_html)
    return m.group(1) if m else None


def fetch_wikipedia_date(wiki_url: 'str | None' = None,
                         page_id: 'int | None' = None) -> 'str | None':
    html = _wiki_get_html(wiki_url=wiki_url, page_id=page_id)
    if not html:
        return None
    pattern = re.compile(
        r'<th[^>]*class="[^"]*infobox-label[^"]*"[^>]*>.*?Released.*?</th>\s*'
        r'<td[^>]*class="[^"]*infobox-data[^"]*"[^>]*>(.*?)</td>',
        re.DOTALL | re.IGNORECASE,
    )
    m = pattern.search(html)
    return _date_from_cell(m.group(1)) if m else None


def _gemini_find_wiki_article(
    artist: str,
    title: str,
    year: 'str | None' = None,
    release_type: 'str | None' = None,
) -> 'str | None':
    """Ask Gemini for the Wikipedia article title for a specific music release.

    Returns:
      'Article Title'  — Gemini found a specific article (use it)
      ''               — Gemini ran and confirmed no article exists (don't keyword-search)
      None             — Gemini unavailable (no API key / error) → fall back to search_wikipedia
    """
    import os, json as _json, re as _re, time as _time
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        return None
    try:
        from google import genai
        from google.genai import types as genai_types
    except ImportError:
        return None

    year_str = f' ({year})' if year else ''
    type_str = f' [{release_type}]' if release_type else ''

    prompt = (
        'You are a music metadata assistant. '
        'Return the exact Wikipedia article title for this specific music release, '
        'or "NONE" if no Wikipedia article exists for it.\n\n'
        f'Artist: {artist or "Unknown"}\n'
        f'Release: {title}{year_str}{type_str}\n\n'
        'Rules:\n'
        '- Return the article for THIS SPECIFIC RELEASE (album/EP/single), '
        'not the artist biography page.\n'
        '- If the artist or title matches a non-music entity (TV show, film, person, '
        'place, etc.) do NOT return that entity\'s article — return "NONE".\n'
        '- Return "NONE" if you are not highly confident the article exists.\n'
        '- Respond with ONLY JSON: {"article": "Title"} or {"article": "NONE"}'
    )

    _MAX_RETRIES = 3
    for attempt in range(_MAX_RETRIES):
        try:
            client   = genai.Client(api_key=api_key)
            response = client.models.generate_content(
                model='gemini-2.5-flash-lite',
                contents=prompt,
                config=genai_types.GenerateContentConfig(
                    response_mime_type='application/json',
                ),
            )
            data    = _json.loads(response.text)
            article = (data.get('article') or '').strip()
            if not article or article.upper() == 'NONE':
                return ''
            return article
        except Exception as exc:
            msg = str(exc)
            m   = _re.search(r"'retryDelay':\s*'(\d+(?:\.\d+)?)s'", msg)
            wait = float(m.group(1)) + 1 if m else 2 ** (attempt + 1)
            if attempt < _MAX_RETRIES - 1 and ('429' in msg or '503' in msg):
                _time.sleep(wait)
            else:
                log.debug('Gemini wiki lookup failed: %s', exc)
                return None
    return None



    """Return (page_id, date). page_id is the permanent Wikipedia integer page ID."""
    query = f'{release_name} {artist_name}'.strip() if artist_name else release_name
    url   = ('https://en.wikipedia.org/w/api.php?'
             + urllib.parse.urlencode({
                 'action': 'query', 'list': 'search',
                 'srsearch': query, 'srlimit': 5, 'format': 'json',
             }))
    _wiki_lim.wait()
    req = urllib.request.Request(url, headers={'User-Agent': MB_UA})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
    except Exception:
        return None, None
    for hit in (data.get('query') or {}).get('search') or []:
        pid  = hit.get('pageid')
        date = fetch_wikipedia_date(page_id=pid) if pid else None
        if date:
            return pid, date
    return None, None


def fetch_date_candidates(mbid: str, release_name: str = None,
                          artist_name: str = None,
                          release_year: str = None,
                          release_type: str = None) -> 'tuple[list, int | None]':
    """Return (candidates, wiki_page_id). Each candidate is {'date', 'source', 'notes'}.

    Wikipedia article discovery order:
      1. MB url-rels  — curated, most reliable
      2. Gemini       — semantic disambiguation (requires GEMINI_API_KEY)
      3. search_wikipedia — keyword fallback (only when Gemini unavailable)
    """
    release_date, rg_first, wiki_url = mb_fetch_release_data(mbid)
    mb_dates, seen = [], set()
    for date, label in [(rg_first, 'MusicBrainz (release group)'),
                        (release_date, 'MusicBrainz (this release)')]:
        if date and date not in seen:
            seen.add(date)
            mb_dates.append({'date': date, 'source': label, 'notes': ''})

    wiki_page_id = None
    wiki_date    = None
    if wiki_url:
        # MB provided a curated Wikipedia link — use it directly
        wiki_page_id = _wiki_url_to_id(wiki_url)
        if wiki_page_id:
            wiki_date = fetch_wikipedia_date(page_id=wiki_page_id)
    elif release_name:
        # Try Gemini first for semantic disambiguation
        article = _gemini_find_wiki_article(
            artist_name or '', release_name, release_year, release_type
        )
        if article is None:
            # Gemini unavailable → keyword search fallback
            wiki_page_id, wiki_date = search_wikipedia(release_name, artist_name)
        elif article:
            # Gemini returned an article title — resolve to page_id
            # _wiki_url_to_id also works with bare titles (no /wiki/ prefix)
            wiki_page_id = _wiki_url_to_id(article)
            if wiki_page_id:
                wiki_date = fetch_wikipedia_date(page_id=wiki_page_id)
        # else article == '' → Gemini confirmed no article; don't keyword-search

    candidates = []
    if wiki_date:
        notes_url = f'https://en.wikipedia.org/wiki/?curid={wiki_page_id}' if wiki_page_id else ''
        candidates.append({'date': wiki_date, 'source': 'Wikipedia ★', 'notes': notes_url})
    for c in mb_dates:
        if c['date'] not in {x['date'] for x in candidates}:
            candidates.append(c)
    return candidates, wiki_page_id

# NOTE: _save_date / save_release_date lives in mdb_ops.
