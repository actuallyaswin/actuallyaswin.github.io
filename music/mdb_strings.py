"""
mdb_strings — Pure string utilities: MB title case, ETI formatting,
MBID validation, text normalization, and variant-type detection.

TrackParseResult fields:
  .clean_title   — title with feat groups + dash-ETI stripped and MB title-cased
  .feat_artists  — list of featured artist name strings (from "feat." / "ft." groups)
  .eti           — formatted ETI string such as "(Soulchild remix)", or None

Design notes:
  Remixers stay in ETI (never extracted to feat_artists) to avoid bare-title collision.
  Dash-suffix ETI (e.g. "Song - X Remix") is converted to parens before classification.
  Follows MusicBrainz Style / Language / English and Style / Titles guidelines.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import List, Optional


# -- MB English title case -----------------------------------------------------

_ARTICLES:    frozenset[str] = frozenset({'a', 'an', 'the'})
_COORD_CONJ:  frozenset[str] = frozenset({'and', 'but', 'or', 'nor'})
_SHORT_PREPS: frozenset[str] = frozenset({
    'as', 'at', 'by', 'for', 'in', 'of', 'on', 'to',
    'cum', 'mid', 'off', 'per', 'qua', 're', 'up', 'via',
})
_LOWERCASE_WORDS = _ARTICLES | _COORD_CONJ | _SHORT_PREPS

# Regex to strip leading/trailing non-alphanumeric from a word
_WORD_STRIP_RE = re.compile(r'^([^A-Za-z0-9]*)(.*?)([^A-Za-z0-9]*)$', re.DOTALL)
# Apostrophe compound: o'clock, n'sync, rock'n'roll ...
_APOS_RE = re.compile(r"^([A-Za-z]+)(['''])([A-Za-z].*)$")
# Contraction suffixes that should stay lowercase after an apostrophe
# (e.g. "she's")
_CONTRACTION_SUFFIXES: frozenset[str] = frozenset({
    's', 't', 'm', 'd', 've', 're', 'll', 'nt',
})


def _tc_word(word: str, force_cap: bool, is_last: bool) -> str:
    """Apply MB English title-case rules to a single token (no hyphens, no spaces)."""
    if not word:
        return word

    m = _WORD_STRIP_RE.match(word)
    if not m:
        return word
    prefix, core, suffix = m.group(1), m.group(2), m.group(3)
    if not core:
        return word

    # Apostrophe compounds: O'Clock/O'Brien rule — capitalise both parts.
    # Exception: common contraction suffixes (s, t, m, d, ve, re, ll, nt)
    # stay lowercase, e.g. "she's" → "She's".
    apos = _APOS_RE.match(core)
    if apos:
        p1, apos_char, p2 = apos.group(1), apos.group(2), apos.group(3)
        if p2.lower() in _CONTRACTION_SUFFIXES:
            cap = p1[0].upper() + p1[1:].lower() + apos_char + p2.lower()
        else:
            cap = p1[0].upper() + p1[1:].lower() + apos_char + p2[0].upper() + p2[1:].lower()
        return prefix + cap + suffix

    lower = core.lower()
    if force_cap or is_last or lower not in _LOWERCASE_WORDS:
        cap = core[0].upper() + core[1:].lower()
    else:
        cap = lower
    return prefix + cap + suffix


def _tc_token(token: str, force_cap: bool, is_last: bool) -> str:
    """Apply MB English title-case rules to one whitespace-delimited token."""
    if not token:
        return token

    # Preserve all-uppercase alphabetic tokens ≥ 2 chars (acronyms: BBC, DARE, IV)
    alpha = re.sub(r'[^A-Za-z]', '', token)
    if len(alpha) >= 2 and alpha == alpha.upper():
        return token

    # Hyphenated compound: each part treated as an independent word
    if '-' in token:
        parts = token.split('-')
        out: list[str] = []
        for j, part in enumerate(parts):
            # First part inherits force_cap; subsequent parts always capitalised per MB rules
            part_force = force_cap if j == 0 else True
            part_last  = is_last and (j == len(parts) - 1)
            out.append(_tc_word(part, force_cap=part_force, is_last=part_last))
        return '-'.join(out)

    # Slash-joined compound within a single token (e.g. "Light/White" in "White Light/White Heat")
    # Note: "Title 1 / Title 2" has spaces and splits into separate whitespace tokens, so this
    # branch only fires for non-spaced slashes like in "White Light/White Heat".
    if '/' in token:
        parts = token.split('/')
        out = []
        for j, part in enumerate(parts):
            part_force = force_cap if j == 0 else True
            part_last  = is_last and (j == len(parts) - 1)
            out.append(_tc_word(part, force_cap=part_force, is_last=part_last))
        return '/'.join(out)

    return _tc_word(token, force_cap=force_cap, is_last=is_last)


def mb_guess_case_english(title: str) -> str:
    """
    Apply MusicBrainz English title-case rules to *title*.

    Rules (per MB Style/Language/English):
    - First and last word always capitalised.
    - All-uppercase alphabetic tokens preserved as-is (acronyms/initialisms).
    - Articles, coordinate conjunctions, short prepositions (≤ 3 chars) are
      lowercased between first/last.
    - Hyphenated compounds: each part treated as an independent title word.
    - Major punctuation (:, ?, !, em/en dash) resets "first word" capitalisation
      for the immediately following token.

    Known limitation: prepositions used as adverbs or verb particles
    (e.g. "Plug In Baby") are lowercased by this function,
    which differs from the MB guideline. Manual correction is required.
    """
    if not title:
        return title
    tokens = title.split()
    if not tokens:
        return title

    result: list[str] = []
    # first token always capitalised
    force_cap = True

    for i, token in enumerate(tokens):
        is_last = (i == len(tokens) - 1)
        result.append(_tc_token(token, force_cap=force_cap, is_last=is_last))

        # Major punctuation that resets capitalisation for the next token
        bare = token.rstrip('"\'')
        if bare and bare[-1] in ':?!':
            force_cap = True
        elif bare in ('—', '–') or (bare and bare[-1] in '—–'):
            force_cap = True
        else:
            force_cap = False

    return ' '.join(result)


# -- ETI (Extra Title Information) normalization -------------------------------

# Descriptor words that should be lowercased when they appear as trailing
# words in ETI content.  Name prefixes before these are preserved as-is.
_ETI_DESCRIPTORS: frozenset[str] = frozenset({
    'remix', 'refix', 'rework', 'bootleg', 'mashup',
    'mix', 'edit', 'version', 'ver',
    'live', 'acoustic', 'demo', 'session', 'performance', 'recording',
    'instrumental', 'vocal', 'a cappella',
    'remaster', 'remastered',
    'reprise', 'cover', 'flip',
    'radio', 'extended', 'club', 'dub',
    'edition',
    'original', 'alternate', 'alternative',
    'mono', 'stereo',
    'clean', 'explicit',
    'arrangement', 'interlude', 'intro', 'outro',
})

# Regex: trailing ETI descriptor word, optionally followed by a 4-digit year
# Matches: "Remastered", "Remastered 2011", "Live at X" — but not bare years
_ETI_END_RE = re.compile(
    r'\b(?:' +
    '|'.join(re.escape(w) for w in sorted(_ETI_DESCRIPTORS, key=len, reverse=True)) +
    r')\s*(?:\d{4})?\s*$',
    re.IGNORECASE,
)


def _normalize_eti_content(content: str) -> str:
    """
    Lowercase trailing ETI descriptor words; preserve the name prefix as-is.

    Example: "Soulchild Remix" → "Soulchild remix". If every word is a
    descriptor (e.g. "Radio Edit"), there's no name prefix, so it stays
    as originally cased.
    """
    tokens = content.split()
    if not tokens:
        return content

    # Scan right-to-left: collect consecutive descriptor words
    desc_start = len(tokens)
    for i in range(len(tokens) - 1, -1, -1):
        clean = tokens[i].lower().strip('.,)(\'\"')
        if clean in _ETI_DESCRIPTORS:
            desc_start = i
        else:
            break

    return ' '.join(
        tok.lower() if idx >= desc_start else tok
        for idx, tok in enumerate(tokens)
    )


def format_eti(content: str) -> str:
    """
    Wrap ETI content in parentheses following MusicBrainz style.

    - Strips outer parentheses if already present.
    - Lowercases trailing descriptor words (remix, refix, mix, edit, version…).
    - Name prefix before the descriptors is preserved as-is.

    Example: format_eti("Soulchild Remix") → "(Soulchild remix)"
    """
    s = content.strip()
    if s.startswith('(') and s.endswith(')'):
        s = s[1:-1].strip()
    return f'({_normalize_eti_content(s)})'


# -- Featured-artist splitting --------------------------------------------------

_FEAT_PREFIX_RE = re.compile(
    r'^(?:feat(?:uring)?\.?|ft\.?)\s+',
    re.IGNORECASE,
)

# Bare/dash-inline feat. clause with no surrounding parentheses at all, e.g.
# "Song ft. Artist" or "Song - feat. Artist" (common in Last.fm scrobbles,
# which don't follow Spotify's "(feat. X)" convention). Requires a word
# boundary before ft./feat. and an immediately-following alphanumeric so
# "6000 Ft." (a unit, not a feature credit) doesn't false-positive.
_INLINE_FEAT_RE = re.compile(
    r'\s*-?\s*\b(?:feat(?:uring)?\.?|ft\.?)\s*(?=[A-Za-z0-9])',
    re.IGNORECASE,
)

# A trailing [bracket] tag that's pure clean/explicit-edition noise (this DB
# tracks one canonical recording, not clean vs. explicit as distinct listens)
# rather than a genuine attribution/remix credit like "[Deadmeat]" (a real
# subtitle, e.g. Steve Aoki's "Come With Me (Deadmeat)").
_NOISE_BRACKET_RE = re.compile(
    r'\s*[;,]?\s*\[\s*(?:\w+\s+)*?(?:clean|explicit)(?:\s+version|\s+edition)?\s*\]\s*$',
    re.IGNORECASE,
)

# A [bracket] group remaining in the head after feat.-clause extraction —
# real title content (e.g. "Come With Me [Deadmeat]"), not noise. Normalized
# to parens so the main parser's trailing-paren classification handles it.
_HEAD_BRACKET_RE = re.compile(r'^(.*?)\s*\[([^\]]+)\]\s*$')

# Indian-cinema scrobble noise: a trailing "(From "Film")" source-attribution
# group, and/or a trailing bare-language tag like "(Tamil)"/"[TELUGU]". Either
# can trail the other, so both patterns are applied twice.
_FROM_FILM_RE = re.compile(r'\s*[\(\[]from\s+["\'].*?["\'][\)\]]', re.IGNORECASE)
_LANG_TAG_RE = re.compile(
    r'\s*[\(\[](?:tamil|telugu|hindi|kannada|malayalam)[\)\]]\s*$',
    re.IGNORECASE,
)


def strip_scrobble_source_noise(title: str) -> str:
    """Strip Indian-cinema scrobble noise: '(From "Film")' and/or a trailing
    bare-language tag like '(Tamil)' / '[TELUGU]'. Order-independent — either
    can trail the other in real scrobble data.
    """
    title = _LANG_TAG_RE.sub('', title).strip()
    title = _FROM_FILM_RE.sub('', title).strip()
    title = _LANG_TAG_RE.sub('', title).strip()
    return title


def _extract_inline_feat(title: str) -> 'tuple[str, list[str]]':
    """Convert a bare/dash-inline feat. clause to (head, feat_artists).

    Strips a trailing clean/explicit-edition [bracket] tag first (noise, not
    content). A [bracket] group still present in the head afterward is real
    title content and is normalized to parens rather than discarded.
    """
    if '(' in title:
        return title, []
    stripped = _NOISE_BRACKET_RE.sub('', title).rstrip(' ;,')
    m = _INLINE_FEAT_RE.search(stripped)
    if not m:
        return title, []
    head = stripped[:m.start()].rstrip(' ;,-').rstrip()
    tail = stripped[m.end():].strip()
    if not head or not tail:
        return title, []
    bm = _HEAD_BRACKET_RE.match(head)
    if bm:
        head = f'{bm.group(1).rstrip()} ({bm.group(2)})'
    return head, _split_feat_group(tail)


def _split_feat_group(group: str) -> List[str]:
    """
    Split a featured-artists string into individual artist names.

    Splits on ' and ', ' & ', ' / ' (conjunction/slash delimiters) first,
    then on commas, e.g. "Mos Def and Bobby Womack" → ["Mos Def", "Bobby Womack"].
    """
    # Split on word-level conjunction/slash separators before commas
    parts = re.split(r'\s+and\s+|\s*&\s*|\s*/\s*', group, flags=re.IGNORECASE)
    artists: list[str] = []
    for part in parts:
        for sub in part.split(','):
            sub = sub.strip()
            if sub:
                artists.append(sub)
    return artists


# -- Dash-suffix ETI detection --------------------------------------------------

# "Title - ETI Content"  (space-dash-space separator)
_DASH_SEP_RE = re.compile(r'^(.*?)\s+-\s+(.+)$')

# Trailing paren group (for iterative extraction)
_TRAILING_PAREN_RE = re.compile(r'\s*\(([^)]*)\)\s*$')


# -- Main parser -----------------------------------------------------------------

@dataclass
class TrackParseResult:
    """Result of :func:`parse_track_title`."""
    clean_title:  str
    feat_artists: List[str]       = field(default_factory=list)
    eti:          Optional[str]   = None


def parse_track_title(title: str) -> TrackParseResult:
    """
    Parse a Spotify-style track title into clean title, featured artists and ETI.

    Example: "Stylo (feat. Mos Def and Bobby Womack)" →
    clean="Stylo", feat=["Mos Def", "Bobby Womack"]. Dash-suffix descriptors
    like "19-2000 - Soulchild Remix" are converted to ETI parens before
    classification, giving clean="19-2000", eti="(Soulchild remix)".

    IMPORTANT: Remixers are *not* extracted to feat_artists — they remain in
    the ETI string to avoid a bare-title collision with the original recording.
    Only "feat." / "ft." / "featuring" groups are extracted to feat_artists.
    """
    rest = title.strip()
    feat_artists: list[str] = []
    eti_parts:    list[str] = []

    # Bare/dash-inline feat. clause with no parentheses at all, e.g.
    # "Song ft. Artist" → rest="Song", feat_artists=["Artist"]. Common in
    # Last.fm scrobbles; the dash-suffix ETI conversion below still runs
    # afterward so a trailing descriptor like "Remix" in "Song - Remix;
    # feat. Artist" is still correctly classified as ETI, not swallowed
    # into the feat group.
    rest, inline_feat = _extract_inline_feat(rest)
    feat_artists.extend(inline_feat)

    # Convert dash-suffix ETI to parentheses first, e.g. "Song - X Remix"
    # → rest="Song", eti_parts=["(X remix)"]. Only applies when the
    # suffix ends with a recognised descriptor word.
    dm = _DASH_SEP_RE.match(rest)
    if dm:
        suffix_candidate = dm.group(2)
        if _ETI_END_RE.search(suffix_candidate):
            rest = dm.group(1).rstrip()
            eti_parts.append(format_eti(suffix_candidate))

    # Extract all trailing parenthetical groups (right to left), collected
    # in left-to-right order for classification below.
    trailing: list[str] = []
    while True:
        m = _TRAILING_PAREN_RE.search(rest)
        if not m:
            break
        trailing.insert(0, m.group(1))
        rest = rest[:m.start()].rstrip()

    # Classify each paren group.
    for inner in trailing:
        feat_m = _FEAT_PREFIX_RE.match(inner)
        if feat_m:
            # Featured-artist group → extract names
            group = inner[feat_m.end():]
            feat_artists.extend(_split_feat_group(group))
        elif _ETI_END_RE.search(inner):
            # ETI group (ends with a descriptor word)
            eti_parts.append(format_eti(inner))
        else:
            # Not ETI — re-attach as part of the title (subtitle, alternate name, etc.)
            rest = f'{rest} ({inner})'

    clean = mb_guess_case_english(rest.strip())
    eti   = ' '.join(eti_parts) if eti_parts else None

    return TrackParseResult(clean_title=clean, feat_artists=feat_artists, eti=eti)


# -- Title resolution — preserve manual capitalisation corrections -------------

def resolve_title(incoming_raw: str, existing_db: Optional[str] = None) -> str:
    """
    Return the title to store, respecting manual capitalisation corrections.

    Algorithm:
    1. Sanitize *incoming_raw* via :func:`parse_track_title` to get a canonical title
       (clean title + ETI if present).
    2. If *existing_db* is set and differs from the sanitized title *only in
       capitalisation* (case-insensitive equal), the existing DB value is returned
       unchanged — it represents a deliberate human correction (e.g. "Plug In Baby"
       vs the auto-sanitized "Plug in Baby").
    3. Otherwise the sanitized title is returned (new track, or a genuine title change).

    Example: resolve_title("plug in baby", "Plug In Baby") → "Plug In Baby" (defer to DB),
    but resolve_title("different title", "Old Name") → "Different Title" (genuine change).
    """
    parsed    = parse_track_title(incoming_raw)
    sanitized = parsed.clean_title
    if parsed.eti:
        sanitized = f'{sanitized} {parsed.eti}'
    if existing_db is not None and existing_db.lower() == sanitized.lower():
        return existing_db
    return sanitized

# -- MBID validation ----------------------------------------------------------

_RE_MBID   = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
_ZERO_MBID = '00000000-0000-0000-0000-000000000000'


def is_valid_mbid(s) -> bool:
    """True if *s* is a well-formed, non-zero MusicBrainz UUID."""
    if not s or not isinstance(s, str):
        return False
    s = s.strip()
    return bool(_RE_MBID.match(s)) and s != _ZERO_MBID


# -- Text normalization -------------------------------------------------------

# Smart/curly punctuation → ASCII equivalents (NFKD won't map these)
_SMART_PUNCT = str.maketrans({
    # LEFT SINGLE QUOTATION MARK  '
    '\u2018': "'",
    # RIGHT SINGLE QUOTATION MARK '
    '\u2019': "'",
    # SINGLE LOW-9 QUOTATION MARK ‚
    '\u201a': "'",
    # LEFT DOUBLE QUOTATION MARK  "
    '\u201c': '"',
    # RIGHT DOUBLE QUOTATION MARK "
    '\u201d': '"',
    # DOUBLE LOW-9 QUOTATION MARK „
    '\u201e': '"',
    # HORIZONTAL ELLIPSIS         …
    '\u2026': '...',
    # EN DASH                     –
    '\u2013': '-',
    # EM DASH                     —
    '\u2014': '-',
    # MINUS SIGN                  −
    '\u2212': '-',
    # Latin-Extended letters NFKD does NOT decompose (they are not a base
    # letter + combining mark, they are their own codepoints), so without an
    # explicit mapping they'd survive normalization and silently fail to
    # match a plain-ASCII scrobble of the same name. Covers the common cases
    # likely to appear in artist or track names; not an exhaustive
    # Unicode-confusables table.
    '\u0141': 'L', '\u0142': 'l',
    '\u0110': 'D', '\u0111': 'd',
    '\u00d8': 'O', '\u00f8': 'o',
    '\u00de': 'Th', '\u00fe': 'th',
    '\u0126': 'H', '\u0127': 'h',
    '\u014a': 'N', '\u014b': 'n',
    '\u0166': 'T', '\u0167': 't',
})


def _fold_accents(s: str) -> str:
    """Shared normalization core for normalize_text/ascii_key: apply the
    smart-punct/non-decomposable-letter translation table, NFKD-decompose,
    then strip the resulting combining marks. Split out so the two callers
    cannot drift out of sync with each other."""
    s = str(s or '').translate(_SMART_PUNCT)
    s = unicodedata.normalize('NFKD', s)
    return ''.join(c for c in s if not unicodedata.combining(c))


def normalize_text(s: str) -> str:
    """Lowercase, normalize punctuation, strip combining accents (NFKD), collapse whitespace.

    Handles smart/curly quotes, en/em dashes, and ellipsis explicitly (NFKD does not map
    these to ASCII equivalents). Useful for dedup and matching.

    Examples:
      normalize_text("Shakespeare\u2019s") → "shakespeare's"
      normalize_text("Motörhead")          → "motorhead"
    """
    if not s:
        return ''
    s = _fold_accents(str(s))
    return re.sub(r'\s+', ' ', s).strip().lower()


def ascii_key(s: str) -> str:
    """Lowercase ASCII matching key: strips non-[a-z0-9] to spaces, collapses whitespace.

    Runs NFKD decomposition first so accented Latin chars resolve to their base letter
    ('Motörhead' → 'motorhead'). Characters with no ASCII equivalent (symbols, emoji,
    non-Latin scripts) are stripped to spaces; a pure-symbol title returns ''.
    '&' is normalised to 'and' before stripping so 'Butterflies & Hurricanes' and
    'Butterflies and Hurricanes' produce the same key.

    Used to build lookup dicts for fuzzy title matching (e.g. MusicBrainz track lookups).
    """
    t = re.sub(r'&', ' and ', str(s or ''))
    t = _fold_accents(t)
    t = re.sub(r'[^a-z0-9 ]', ' ', t.lower())
    return re.sub(r'\s+', ' ', t).strip()


# -- Variant-type detection ---------------------------------------------------

_RE_RERECORDED   = re.compile(r"\bRe-?Recorded\b|Taylor'?s\s+Version|\bRedux\b", re.IGNORECASE)
_RE_REMIX        = re.compile(r'\bRemix(?:ed)?\b', re.IGNORECASE)
_RE_LIVE         = re.compile(r'\bLive\s+(?:Edition|Version|Recording|Album|At\b)', re.IGNORECASE)
_RE_INSTRUMENTAL = re.compile(r'\bInstrumental(?:\s+(?:Version|Edition))?\b', re.IGNORECASE)
_RE_EXPLICIT     = re.compile(r'\bExplicit(?:\s+(?:Version|Edition))?\b', re.IGNORECASE)
_RE_CLEAN        = re.compile(r'\bClean(?:\s+(?:Version|Edition))?\b', re.IGNORECASE)
_RE_ANNIVERSARY  = re.compile(r'\b\d+(?:th|st|nd|rd)\s+Anniversary(?:\s+Edition)?\b', re.IGNORECASE)
_RE_REMASTER     = re.compile(
    r'\b(?:\d{4}\s+)?Remaster(?:ed)?\b'
    r'|\bHalf-?Speed\s+Master(?:ed)?\b'
    r'|\bAudiophile(?:\s+Edition)?\b',
    re.IGNORECASE)
_RE_REISSUE      = re.compile(r'\bReissue\b', re.IGNORECASE)
_RE_DELUXE       = re.compile(
    r'\b(?:Deluxe|Expanded|Extended|Platinum|Ultimate|Complete|Collector.?s?)\b'
    r'(?:\s+(?:Edition|Version|Release|Collection))?\b'
    r'|\bBonus\s+Tracks?\b',
    re.IGNORECASE)
_RE_SPECIAL      = re.compile(
    r'\b(?:Special|Limited)\s+(?:Edition|Version|Release)\b'
    r'|\bRecord\s+Store\s+Day\b',
    re.IGNORECASE)
_RE_MONO_STEREO  = re.compile(r'\b(Mono|Stereo)\s*(?:Version|Mix|Edition)?\b', re.IGNORECASE)
_RE_BOX_SET      = re.compile(r'\bBox\s+Set\b', re.IGNORECASE)
_RE_REGIONAL     = re.compile(
    r'\b(?:Japanese?|UK|US|International|European?)\s+(?:Edition|Version|Release|Import)\b',
    re.IGNORECASE)

_VARIANT_CHECKS = [
    (_RE_RERECORDED,   'rerecorded'),
    (_RE_REMIX,        'remix'),
    (_RE_LIVE,         'live'),
    (_RE_INSTRUMENTAL, 'instrumental'),
    (_RE_EXPLICIT,     'explicit'),
    (_RE_CLEAN,        'clean'),
    (_RE_ANNIVERSARY,  'anniversary'),
    (_RE_REMASTER,     'remaster'),
    (_RE_REISSUE,      'reissue'),
    (_RE_DELUXE,       'deluxe'),
    (_RE_SPECIAL,      'special'),
    (_RE_BOX_SET,      'box_set'),
    (_RE_REGIONAL,     'regional'),
]

VARIANT_TYPES: frozenset[str] = frozenset(vt for _, vt in _VARIANT_CHECKS) | {'mono', 'stereo'}


def detect_variant_type(title: str) -> 'str | None':
    """Return the first matching variant type string, or None."""
    for pattern, vtype in _VARIANT_CHECKS:
        if pattern.search(title):
            return vtype
    m = _RE_MONO_STEREO.search(title)
    return m.group(1).lower() if m else None


def detect_variant_types(title: str) -> list:
    """Return all matching variant type strings from the title."""
    found = []
    for pattern, vtype in _VARIANT_CHECKS:
        if pattern.search(title):
            found.append(vtype)
    m = _RE_MONO_STEREO.search(title)
    if m:
        v = m.group(1).lower()
        if v not in found:
            found.append(v)
    return found


def detect_variant_label(title: str) -> str:
    """Extract a human-readable variant label from a release title.

    Returns the content of the first non-remix parenthetical/bracketed suffix,
    or the trailing diff from _base_title. Falls back to the full title.

    Example: "Hold Your Colour (Deluxe)" → "Deluxe"
    """
    m = re.search(r'[\(\[]((?!.*\bremix\b)[^\)\]]+)[\)\]]', title, re.I)
    if m:
        return m.group(1).strip()
    base = _base_title(title)
    if base and base != title:
        diff = title[len(base):].strip().lstrip('-–—: ')
        if diff:
            return diff
    return title


_GENERIC_EDIT_TERMS = frozenset({
    'radio edit', 'radio version', 'radio mix',
    'extended mix', 'extended', 'extended version',
    'original mix', 'original version', 'original',
    'club mix', 'dub mix', 'album mix', 'album version',
    'single version', 'single mix',
})

_EDIT_SUFFIX_RE = re.compile(r'\s*[\(\[]([^\(\)\[\]]+)[\)\]]\s*$')


def _is_generic_edit(inner: str) -> bool:
    if inner in _GENERIC_EDIT_TERMS:
        return True
    # 'original club mix' / 'original dub mix' etc. — 'original' as a plain
    # prefix on another generic term, not a compound term of its own.
    stripped = re.sub(r'^original\s+', '', inner)
    return stripped != inner and stripped in _GENERIC_EDIT_TERMS


def same_song_key(title: str) -> str:
    """Group track titles that are just edit/length cuts of one recording
    (radio edit, extended mix, original mix, club mix, ...) so hearing any
    one variant counts as having heard the song for completion purposes.

    A remix or edit credited to a specific artist/DJ — '(X Remix)',
    '(Simmons & Christopher Extended)' — is a distinct musical work and is
    never folded in here, even if it also carries a length qualifier.
    """
    m = _EDIT_SUFFIX_RE.search(title)
    if not m or not _is_generic_edit(m.group(1).strip().lower()):
        return title.strip().lower()
    return title[:m.start()].strip().lower()


def _base_title(title: str) -> str:
    """Strip edition/variant qualifiers for grouping similar releases/tracks.

    Remix-containing qualifiers are intentionally preserved — remixes are
    distinct artistic works, not variants of the same recording.

    Bare 'version' is intentionally NOT a keyword — '(Classixx version)',
    '(DJ Name version)', etc. are artist-credited variants and should be
    treated like remixes.  Only specific known-safe 'X version' compounds
    are stripped (album, single, radio, original).  Keyword-prefixed cases
    like '(Remastered Version)' are handled by the 'remaster' keyword plus
    the trailing [^\\)\\]]* absorber.
    """
    # Strip parenthetical/bracketed qualifiers that don't involve remixes.
    # 'live' is intentionally absent here — handled separately below because
    # bare 'live' is too broad and would wrongly strip "(Live Without You)".
    t = re.sub(
        r'\s*[\(\[](?![^\)\]]*\bremix\b)[^\)\]]*'
        r'(?:edition|(?:album|single|radio|original)\s+version|'
        r'(?:\d{4}|single|album)\s+edit|'
        r'remaster(?:ed)?|deluxe|expanded|extended|unmixed|bonus|'
        r'explicit|clean|instrumental|reissue|anniversary|special|limited|'
        r'box\s*set|regional|mono|stereo|sound\s*tracks?|\bost\b|radio.?\s*edit|'
        r'score\b|'
        r'music\s+(?:from|for)\s+(?:the\s+)?(?:motion\s+picture|film|movie))'
        r'[^\)\]]*[\)\]]',
        '', title, flags=re.I)
    # Live performance qualifiers: (live), (live at X), (Album live), etc.
    # Requires 'live' to be standalone, followed by at/in/from/recording/version,
    # or followed by a dash — so "(Live Without You)" is correctly preserved.
    t = re.sub(
        r'\s*[\(\[](?![^\)\]]*\bremix\b)'
        r'(?:[^\)\]]*\s)?live'
        r'(?:\s+(?:at|in|from|recording|version)\b[^\)\]]*|\s*[-–][^\)\]]*|\s*)'
        r'[\)\]]',
        '', t, flags=re.I)
    # Strip bare (original) — lone "original" means the canonical version
    t = re.sub(r'\s*[\(\[]\s*original\s*[\)\]]', '', t, flags=re.I)
    # Strip (With Artist) / (with Artist) guest credits, but not
    # (With a/an/the ...) where "with" starts a title phrase.
    t = re.sub(r'\s*[\(\[]\s*with\s+(?!(?:a |an |the ))[^\)\]]+[\)\]]', '', t, flags=re.I)
    t = re.sub(r'\s+original\s+sound\s*tracks?$', '', t, flags=re.I)
    # Strip colon-separated soundtrack/score ETI trailing the source work title.
    # Pattern: ": <soundtrack/score descriptor> [anything]"
    # The trailing colon left behind is cleaned up by the punctuation strip below.
    t = re.sub(
        r'\s*:\s*(?:'
        r'music\s+(?:from|for)\s+(?:the\s+)?(?:original\s+)?(?:motion\s+picture|film|movie)(?:\s+sound\s*tracks?)?'
        r'|original\s+(?:motion\s+picture\s+)?(?:sound\s*tracks?|score)(?:\s+from\s+.*)?'
        # bare ": Soundtrack" (e.g. "UNDERTALE: Soundtrack")
        r'|sound\s*tracks?'
        r').*$',
        '', t, flags=re.I)
    # Bare trailing "Soundtrack", e.g. "Sonic The Hedgehog 1&2 Soundtrack" ->
    # "Sonic The Hedgehog 1&2". Official/Records/CD name a specific release
    # rather than a bare tag, so those are excluded.
    t = re.sub(r'(?<!\bofficial)(?<!\brecords)(?<!\bcd)\s+sound\s*tracks?$', '', t, flags=re.I)
    # Strip colon-separated general edition qualifiers (e.g. "Settle: Special Edition")
    t = re.sub(
        r'\s*:\s*(?!.*\bremix\b)'
        r'(?:deluxe|expanded|extended|special|limited|bonus\s+tracks?|'
        r'(?:\d{4}\s+)?remaster(?:ed)?|anniversary|.*edition|.*version).*$',
        '', t, flags=re.I)
    # Strip bare trailing qualifiers (after dash/em-dash) that don't involve remixes
    t = re.sub(
        r'\s*[-–]\s*(?!.*\bremix\b)'
        r'(?:deluxe|expanded|extended|unmixed|(?:\d{4}\s+)?remaster(?:ed)?|bonus tracks?|'
        r'.*edition|(?:album|single|radio|original)\s+version|'
        r'\d{4}\s+edit|instrumental|explicit|clean|reissue|live|'
        r'special|limited|original\s+sound(?:track)?|ost|radio.?\s*edit).*$',
        '', t, flags=re.I)
    t = re.sub(r'\s*[-–:,]\s*$', '', t)
    return t.strip() or title


_SOUNDTRACK_PAT = re.compile(
    r'(?:original\s+(?:motion\s+picture\s+)?sound\s*tracks?'
    r'|music\s+(?:from|for)\s+(?:the\s+)?(?:motion\s+picture|film|movie)'
    r'|[\(\[]\s*(?:ost|sound\s*tracks?)\s*[\)\]])',
    re.I,
)


def is_soundtrack_title(title: str) -> bool:
    """Return True if the title contains soundtrack keywords (OMPS, OST, etc.)."""
    return bool(_SOUNDTRACK_PAT.search(title))


# -- Release type lists -------------------------------------------------------

_PRIMARY_TYPES = ['album', 'ep', 'single', 'broadcast', 'other']

_SECONDARY_TYPES = [
    'none', 'compilation', 'soundtrack', 'live', 'remix', 'dj-mix',
    'mixtape', 'demo', 'spokenword', 'interview',
    'audiobook', 'audio drama', 'field recording',
]

_EDITION_TYPES = [
    'none', 'reissue', 'remaster', 'deluxe', 'anniversary', 'special',
    'instrumental', 'box_set', 'explicit', 'clean', 'rerecorded',
    'mono', 'stereo', 'regional',
]


# -- Date helpers -------------------------------------------------------------

MONTHS: dict[str, str] = {
    'january': '01', 'february': '02', 'march': '03', 'april': '04',
    'may': '05', 'june': '06', 'july': '07', 'august': '08',
    'september': '09', 'october': '10', 'november': '11', 'december': '12',
    'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
    'jun': '06', 'jul': '07', 'aug': '08', 'sep': '09',
    'oct': '10', 'nov': '11', 'dec': '12',
}

_SOURCE_PRIORITY: dict[str, int] = {
    'manual': 5, 'wikipedia': 4, 'aoty': 3, 'musicbrainz': 2, 'spotify': 1, 'beatport': 1,
}


def _date_prec(date_str: str) -> int:
    """Effective precision: 0=none, 1=year-only, 2=year+month, 3=full.
    Spotify uses Jan 1 as a placeholder when only the year is known — treat
    YYYY-01-01 as year-only precision (1), not full precision (3)."""
    if not date_str:
        return 0
    if re.fullmatch(r'\d{4}', date_str):
        return 1
    if re.fullmatch(r'\d{4}-\d{2}', date_str):
        return 2
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', date_str):
        return 1 if date_str.endswith('-01-01') else 3
    return 0


def _should_update_date(existing: str, ex_source: str, new: str, new_source: str) -> bool:
    """True if new_date should replace existing_date.
    Prefers higher precision first; breaks ties by source priority."""
    ep = _date_prec(existing)
    np = _date_prec(new)
    if np == 0:
        # new date unusable
        return False
    if ep == 0:
        # nothing stored yet
        return True
    if np > ep:
        # strictly better precision
        return True
    if np < ep:
        # would downgrade precision
        return False
    # equal precision — prefer higher-priority source
    return _SOURCE_PRIORITY.get(new_source, 0) > _SOURCE_PRIORITY.get(ex_source, 0)


def _parse_user_date(text: str) -> 'str | None':
    """Parse a user-typed date string into YYYY, YYYY-MM, or YYYY-MM-DD.
    Returns None if the input can't be parsed."""
    t = text.strip()
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', t):
        return t
    if re.fullmatch(r'\d{4}-\d{2}', t):
        return t
    if re.fullmatch(r'\d{4}', t):
        return t
    m = re.fullmatch(r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', t, re.IGNORECASE)
    if m and m.group(1).lower() in MONTHS:
        return f'{m.group(3)}-{MONTHS[m.group(1).lower()]}-{m.group(2).zfill(2)}'
    m = re.fullmatch(r'(\d{1,2})\s+(\w+)\s+(\d{4})', t, re.IGNORECASE)
    if m and m.group(2).lower() in MONTHS:
        return f'{m.group(3)}-{MONTHS[m.group(2).lower()]}-{m.group(1).zfill(2)}'
    m = re.fullmatch(r'(\w+)\s+(\d{4})', t, re.IGNORECASE)
    if m and m.group(1).lower() in MONTHS:
        return f'{m.group(2)}-{MONTHS[m.group(1).lower()]}'
    return None


# -- UPC / GTIN normalization ------------------------------------------------

def normalize_upc(upc: 'str | None') -> 'str | None':
    """Normalize a UPC/GTIN to 13-digit EAN-13 canonical form.

    Spotify returns 13 digits; MusicBrainz often omits the leading zero and
    returns 12 digits (UPC-A). Both represent the same barcode — canonicalize
    to EAN-13 by zero-padding 12-digit values.

    Valid input lengths: 8 (EAN-8), 12 (UPC-A → padded to 13), 13 (EAN-13),
    14 (GTIN-14). Returns None if empty or not a recognized GTIN length.
    """
    if not upc:
        return None
    digits = re.sub(r'\D', '', str(upc))
    if len(digits) == 12:
        # UPC-A → EAN-13
        digits = '0' + digits
    if len(digits) == 14 and digits[0] == '0':
        # GTIN-14 indicator-0 → EAN-13
        digits = digits[1:]
    if len(digits) in (8, 13, 14):
        return digits
    # not a recognized GTIN length
    return None


def beatport_is_catalog_addition(bp_data: dict) -> bool:
    """Return True if Beatport's publish_date is a catalog availability date,
    not the original release date.

    Detection: if the gap between encoded_date (when Beatport processed the
    release internally) and publish_date (Beatport's listed release date) exceeds
    30 days, the release was likely added to Beatport long after the original release.

    Example: Settle (The Remixes) — released 2013-06-11, added to Beatport
    catalog 2013-12-16 (encoded_date 2021-09-09). Gap = 2824 days → catalog addition.
    """
    encoded_str = (bp_data.get('encoded_date') or '')[:10]
    publish_str = (bp_data.get('publish_date') or '')[:10]
    if not encoded_str or not publish_str:
        return False
    try:
        from datetime import date
        encoded = date.fromisoformat(encoded_str)
        publish = date.fromisoformat(publish_str)
        return abs((encoded - publish).days) > 30
    except ValueError:
        return False


# -- URL / ID extraction ------------------------------------------------------

_RE_MB_URL = re.compile(
    r'musicbrainz\.org/release/'
    r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})',
    re.IGNORECASE,
)
_RE_SP_ALBUM_URL = re.compile(
    r'spotify\.com/album/([A-Za-z0-9]+)',
    re.IGNORECASE,
)


def extract_mbid(s: str) -> 'str | None':
    """Extract a MusicBrainz UUID from a MB URL or bare UUID string.
    Returns the UUID (lowercased) or None if not found."""
    if not s:
        return None
    m = _RE_MB_URL.search(s)
    if m:
        return m.group(1).lower()
    stripped = s.strip()
    if is_valid_mbid(stripped):
        return stripped.lower()
    return None


def extract_spotify_id(s: str) -> 'str | None':
    """Extract a Spotify album ID from a URL, or return the string itself if
    it looks like a bare ID (alphanumeric, no slashes).  Returns None if empty."""
    if not s:
        return None
    m = _RE_SP_ALBUM_URL.search(s)
    if m:
        return m.group(1)
    stripped = s.strip()
    if re.fullmatch(r'[A-Za-z0-9]+', stripped):
        return stripped
    return None


# -- Wikipedia discography wikitext -------------------------------------------

_WIKI_HEADING_RE   = re.compile(r'^(=+)\s*(.*?)\s*=+$')
_WIKI_REF_RE       = re.compile(r'<ref[^>]*/>|<ref[^>]*>.*?</ref>', re.S)
_WIKI_COMMENT_RE   = re.compile(r'<!--.*?-->', re.S)
_WIKI_BR_RE        = re.compile(r'<br\s*/?>', re.I)
_WIKI_ROW_RE       = re.compile(r'^!\s*(scope="row"|\{\{nobold\|)')
_WIKI_LINK_PIPE_RE = re.compile(r'\[\[([^\]|]+)\|([^\]]+)\]\]')
_WIKI_LINK_RE      = re.compile(r'\[\[([^\]]+)\]\]')
_WIKI_ITALIC_RE    = re.compile(r"''+([^']+?)''+")
_WIKI_MEMBER_OF_RE = re.compile(r'as an?\s+(?:member|part)\s+of\s+\[\[([^\]|]+)', re.I)
_WIKI_RELEASED_RE  = re.compile(r'^\*\s*Released:\s*(.+)$', re.I)


def parse_wikitext_discographies(
    text: str, sections: 'tuple[str, ...]' = ('studio albums', 'extended plays', 'eps'),
) -> list:
    """Parse one or more Wikipedia "Discography" sections into import entries.

    Input is plain wikitext, one artist per block, each block introduced by a
    line of the form `%%% Artist Name`. Within each block, only the row
    headers (`! scope="row"| ...`) under headings matching *sections* are
    read — chart data, citations, and prose are ignored.

    A nested heading like `===Studio albums as a member of [[Tin Machine]]===`
    reassigns those rows to the named artist instead of the block's own name,
    so band credits land on the band rather than the member.

    Returns a list of dicts shaped like `cmd_discography`'s YAML entries:
    `{'artist': str, 'album_title': str, 'article': wikipedia_url, 'release_date': str|None}`.
    release_date is the raw "Released: ..." bullet text from the same table
    cell (e.g. "April 7, 1978") — this lets the MB title-search fallback
    filter by year instead of grabbing the first same-titled release it
    finds, which for a prolific artist is often the wrong one.
    """
    entries = []
    artist_blocks = re.split(r'^%%%\s*(.+)$', text, flags=re.M)[1:]

    for i in range(0, len(artist_blocks), 2):
        artist = artist_blocks[i].strip()
        body   = artist_blocks[i + 1]
        lines  = body.split('\n')

        capturing = False
        cap_level = None
        override_artist = None
        override_level  = None

        idx = 0
        while idx < len(lines):
            line = lines[idx]
            m = _WIKI_HEADING_RE.match(line.strip())
            if m:
                level, heading = len(m.group(1)), m.group(2)
                if capturing and level <= cap_level:
                    capturing = False
                if not capturing:
                    if heading.lower() in sections:
                        capturing, cap_level = True, level
                        override_artist = override_level = None
                    idx += 1
                    continue
                member = _WIKI_MEMBER_OF_RE.search(heading)
                if member:
                    override_artist, override_level = member.group(1).strip(), level
                elif override_artist and level <= override_level:
                    override_artist = override_level = None
                idx += 1
                continue

            if not capturing or not _WIKI_ROW_RE.match(line):
                idx += 1
                continue

            clean = _WIKI_BR_RE.sub(' ', _WIKI_REF_RE.sub('', _WIKI_COMMENT_RE.sub('', line)))
            m = _WIKI_LINK_PIPE_RE.search(clean)
            if m:
                target, display = m.group(1).strip(), m.group(2).strip()
            else:
                m = _WIKI_LINK_RE.search(clean)
                if m:
                    target = display = m.group(1).strip()
                else:
                    m = _WIKI_ITALIC_RE.search(clean)
                    if not m:
                        idx += 1
                        continue
                    target = display = m.group(1).strip()

            # Display text can itself carry markup, e.g. [[X|''X'' ("nickname")]]
            display = display.replace("''", '').strip()
            display = re.sub(r'\s*\("[^"]*"\)\s*$', '', display).strip()

            # The "Released: ..." bullet lives in the same table cell, on one
            # of the lines between this row header and the next one.
            release_date = None
            j = idx + 1
            while j < len(lines):
                nxt = lines[j]
                if _WIKI_ROW_RE.match(nxt) or _WIKI_HEADING_RE.match(nxt.strip()) or nxt.strip() == '|-':
                    break
                rm = _WIKI_RELEASED_RE.match(_WIKI_REF_RE.sub('', _WIKI_COMMENT_RE.sub('', nxt)).strip())
                if rm:
                    release_date = rm.group(1).strip().rstrip('.')
                    break
                j += 1

            wiki_url = 'https://en.wikipedia.org/wiki/' + target.replace(' ', '_')
            entries.append({
                'artist':      override_artist or artist,
                'album_title': display,
                'article':     wiki_url,
                'release_date': release_date,
            })
            idx += 1

    return entries
