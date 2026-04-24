"""mdb_cli — Rich UI helpers for the mdb interactive CLI."""

__all__ = [
    'render_diff',
    '_fmt_dur', '_trunc',
    '_format_mb_type', '_print_member',
    '_aoty_prompt', '_dates_prompt', '_prompt_choice',
    'cmd_track_variants',
    'cmd_enrich_soundtracks',
]

import logging

from rich import box
from rich.console import Console
from rich.table   import Table

from mdb_apis     import compare_releases
from mdb_strings  import detect_variant_types, _parse_user_date, normalize_text, _base_title
from mdb_websources import _has_aoty, _fmt_aoty

console = Console(width=80, highlight=False)
log = logging.getLogger(__name__)


# ── Formatting utilities ───────────────────────────────────────────────────────

def _fmt_dur(ms: 'int | None') -> str:
    """Format duration in milliseconds as 'm:ss'."""
    if not ms:
        return '?:??'
    s = ms // 1000
    return f'{s // 60}:{s % 60:02d}'


def _trunc(text: str, n: int) -> str:
    """Truncate text to n chars, appending '…' if needed."""
    return text if len(text) <= n else text[:n-1] + '…'


# ── Release / member display ───────────────────────────────────────────────────

def _format_mb_type(m: dict) -> str:
    """Format type + type_secondary MB-style, e.g. 'Album · Instrumental'."""
    t = (m.get('type') or '').capitalize()
    s = (m.get('type_secondary') or '').capitalize()
    return f'{t} · {s}' if t and s else t or s or '?'


def _print_member(i: int, m: dict) -> None:
    """Print one group member line with track info, MB type, and IDs."""
    already    = (f'  [dim](→ {m["existing_canonical_id"]})[/dim]'
                  if m['existing_canonical_id'] else '')
    vtypes     = detect_variant_types(m['title'])
    vtype_str  = '  ' + ' '.join(f'[yellow]{t}[/yellow]' for t in vtypes) if vtypes else ''
    track_info = f'  [dim]{m["track_count"]} tracks'
    if m['explicit_count']:
        track_info += f', {m["explicit_count"]} explicit'
    track_info += '[/dim]'
    mb_type  = _format_mb_type(m)
    id_parts = []
    if m.get('spotify_id'):
        id_parts.append(f'sp:{m["spotify_id"]}')
    if m.get('mbid'):
        id_parts.append(f'mb:{m["mbid"]}')
    id_str = f'  [dim cyan]{" · ".join(id_parts)}[/dim cyan]' if id_parts else ''
    console.print(
        f'  [bold]{i}.[/bold]  [bold]{m["title"]}[/bold]'
        f'  [dim]{m["release_date"] or "?"}[/dim]'
        f'  [dim]{mb_type}[/dim]'
        f'{track_info}{vtype_str}{already}'
    )
    if id_str:
        console.print(f'       {id_str}')


# ── Interactive prompts ────────────────────────────────────────────────────────

def _aoty_prompt(release_name: str, artist_name: 'str | None',
                 aoty_url: 'str | None', data: dict) -> 'tuple[str, str | None, dict | None]':
    """Interactive AOTY data review. Returns ('save'|'url'|'skip'|'quit', url, data)."""
    has = _has_aoty(data)
    console.print()
    console.print(f'  [bold]{release_name}[/bold]  [dim]{artist_name or "Unknown"}[/dim]')
    if aoty_url:
        console.print(f'  [dim]{aoty_url}[/dim]')
    if has:
        print(_fmt_aoty(data))
    else:
        print('  No data found.')

    prompt = '  Accept? [Y/n/u=url/s=skip/q=quit]: ' if has else '  [u=url/s=skip/q=quit]: '
    try:
        ans = input(prompt).strip().lower()
    except EOFError:
        return 'quit', None, None

    if ans in ('q', 'quit'):
        return 'quit', None, None
    if ans in ('s', 'skip', 'n', 'no') or (ans == '' and not has):
        return 'skip', None, None
    if ans in ('u', 'url') or ans.startswith('http'):
        url = ans if ans.startswith('http') else input('  AOTY URL: ').strip()
        return 'url', url, None

    # Accepted — clarify ambiguous type
    if data.get('type_secondary') and not data.get('type'):
        print(f'  "{data["aoty_type"]}" — pick primary type:')
        print('    1) album   2) ep   3) single   0) skip type')
        sub  = input('  Choice [1]: ').strip()
        data = dict(data)
        data['type'] = {'2': 'ep', '3': 'single', '0': None}.get(sub, 'album')
        if sub == '0':
            data['type_secondary'] = None

    return 'save', aoty_url, data


def _dates_prompt(candidates: list, release_name: str,
                  artist_name: 'str | None', current_year: 'int | None') -> 'str | None':
    """Interactive date selection. Returns chosen date string, None to skip, or 'QUIT'."""
    console.print()
    console.print(f'  [bold]{release_name}[/bold]  '
                  f'[dim]{artist_name or "Unknown"}, year: {current_year or "?"}[/dim]')
    if len(candidates) == 1:
        c      = candidates[0]
        suffix = f'  ({c["notes"]})' if c['notes'] else ''
        print(f'  Found: {c["date"]:12s}  [{c["source"]}]{suffix}')
        try:
            ans = input('  Accept? [Y/n/s=skip/q=quit] or type a date: ').strip().lower()
        except EOFError:
            return 'QUIT'
        if ans in ('q', 'quit'): return 'QUIT'
        if ans in ('n', 'no', 's', 'skip'): return None
        parsed = _parse_user_date(ans)
        return parsed if parsed else c['date']

    print('  Multiple dates found:')
    for i, c in enumerate(candidates, 1):
        suffix = f'  ({c["notes"]})' if c['notes'] else ''
        print(f'  [{i}] {c["date"]:12s}  {c["source"]}{suffix}')
    while True:
        try:
            ans = input(f'  Pick [1-{len(candidates)}], s=skip, q=quit, or type a date: ').strip().lower()
        except EOFError:
            return 'QUIT'
        if ans in ('q', 'quit'): return 'QUIT'
        if ans in ('s', 'skip', ''): return None
        if ans.isdigit() and 1 <= int(ans) <= len(candidates):
            return candidates[int(ans) - 1]['date']
        parsed = _parse_user_date(ans)
        if parsed:
            confirm = input(f'  Use "{parsed}"? [Y/n] ').strip().lower()
            if confirm not in ('n', 'no'):
                return parsed
        print('  Invalid input, try again.')


def _prompt_choice(label: str, options: list, current=None,
                   allow_hide: bool = False, allow_back: bool = False,
                   multi: bool = False):
    """
    Display a numbered choice list and return the chosen value.

    Single-select (multi=False):
      Enter with no input keeps `current` (shown as default).
      Returns (value, quit, hide, back).

    Multi-select (multi=True):
      `current` is a list of currently-selected values.
      Input is space/comma-separated numbers.
      Enter keeps current selection.
      Returns (list_of_values, quit, hide, back).
    """
    if multi:
        current_list = current if isinstance(current, list) else ([current] if current else ['none'])
        console.print(f'\n  [bold]{label}[/bold]  [dim](space-separated numbers for multi-select)[/dim]')
        cols, col_w = 3, 18
        for row_start in range(0, len(options), cols):
            row   = options[row_start:row_start + cols]
            parts = []
            for j, opt in enumerate(row, row_start):
                marker = '*' if opt in current_list else ' '
                parts.append(f'[dim]{marker}[{j}][/dim] {opt:<{col_w}}')
            console.print('  ' + '  '.join(parts))
        current_str = ','.join(current_list)
        hide_hint  = '  \\[h]ide' if allow_hide else ''
        back_hint  = '  \\[b]ack' if allow_back else ''
        console.print(f'  [dim]Enter=keep ({current_str}){hide_hint}{back_hint}  q=quit:[/dim] ', end='')
        raw = input().strip().lower()
        if raw == 'q':
            return None, True, False, False
        if allow_back and raw == 'b':
            return None, False, False, True
        if allow_hide and raw == 'h':
            return None, False, True, False
        if not raw:
            return current_list, False, False, False
        tokens = raw.replace(',', ' ').split()
        chosen = []
        for tok in tokens:
            if tok.isdigit():
                idx = int(tok)
                if 0 <= idx < len(options):
                    chosen.append(options[idx])
        if not chosen:
            return current_list, False, False, False
        # If 'none' is explicitly chosen alongside others, keep only 'none'
        if 'none' in chosen and len(chosen) > 1:
            chosen = ['none']
        return chosen, False, False, False

    default = current if current and current in options else options[0]
    console.print(f'\n  [bold]{label}[/bold]')
    cols, col_w = 3, 18
    for row_start in range(0, len(options), cols):
        row   = options[row_start:row_start + cols]
        parts = []
        for j, opt in enumerate(row, row_start):
            marker = '*' if opt == default else ' '
            parts.append(f'[dim]{marker}[{j}][/dim] {opt:<{col_w}}')
        console.print('  ' + '  '.join(parts))
    hide_hint = '  \\[h]ide' if allow_hide else ''
    back_hint = '  \\[b]ack' if allow_back else ''
    console.print(f'  [dim]Enter=keep ({default}){hide_hint}{back_hint}  q=quit:[/dim] ', end='')
    raw = input().strip().lower()
    if raw == 'q':
        return None, True, False, False
    if allow_back and raw == 'b':
        return None, False, False, True
    if allow_hide and raw == 'h':
        return None, False, True, False
    if raw == '' or not raw.isdigit():
        return default, False, False, False
    idx = int(raw)
    if 0 <= idx < len(options):
        return options[idx], False, False, False
    return default, False, False, False


# ── Diff rendering ─────────────────────────────────────────────────────────────

def render_diff(*releases: 'SpotifyRelease', compact: bool = False) -> None:
    """
    Render a diff of two or more SpotifyRelease objects.

    compact=False (default): full per-album tracklists + comparison table +
        tracklist diff + canonical recommendation.  Used by `mdb diff`.
    compact=True: comparison table + tracklist diff + recommendation only.
        Used by `sync match` inline [d]iff.

    Display mode is driven by Jaccard similarity over track title sets:
      similarity == 0   → unrelated albums: table + warning only
      0 < s < 0.7       → partial overlap: table + shared/unique tracks, no canonical
      similarity >= 0.7 → variant mode: full diff + canonical recommendation
    """
    result        = compare_releases(*releases)
    similarity    = result['similarity']
    shared_titles = result['shared_titles']
    same_titles   = result['same_titles']
    dur_diffs     = result['dur_diffs']
    unique_per    = result['unique_per']
    ranked        = result['ranked']
    canon         = result['canonical']
    reasons       = result['reasons']

    # -- artist mismatch warning -----------------------------------------------
    artists = [r.artist for r in releases]
    if len(set(artists)) > 1:
        console.print(
            f'  [yellow]Warning: different artists — '
            + '  vs  '.join(f'[bold]{a}[/bold]' for a in artists)
            + '[/yellow]'
        )

    # -- full tracklists (variant mode, non-compact only) ----------------------
    if not compact and similarity >= 0.7:
        for r in releases:
            console.print()
            console.print(
                f'[bold]{r.name}[/bold]  '
                f'[dim]{r.artist}  ·  {r.date}  ·  {r.album_type.capitalize()}  '
                f'{r.track_count} tracks  ·  {_fmt_dur(r.total_ms)}  ·  '
                f'label: {r.label}  id: {r.id}[/dim]'
            )
            console.print('─' * 78)
            for i, t in enumerate(r.tracks, 1):
                feat = [a['name'] for a in t.get('artists', []) if a['name'] != r.artist]
                feat_str = f'  [dim](feat. {", ".join(feat)})[/dim]' if feat else ''
                exp_str  = '  [yellow]E[/yellow]' if t.get('explicit') else ''
                dur      = _fmt_dur(t.get('duration_ms'))
                console.print(
                    f'  [dim]{i:2}.[/dim]  {t["name"]}{feat_str}{exp_str}'
                    f'  [dim]{dur}[/dim]'
                )
            console.print()

    # -- comparison table ------------------------------------------------------
    # For unrelated albums don't yellow the title/artist rows — it's noise.
    # For variant mode, suppress edition-words row in compact display.
    tbl = Table(box=box.SIMPLE_HEAD, show_header=True, header_style='bold')
    tbl.add_column('', style='dim')
    for r in releases:
        tbl.add_column(r.id[:12] + '…', no_wrap=True)

    rows_meta = [
        ('Title',         lambda r: r.name,                              similarity >= 0.7),
        ('Artist',        lambda r: r.artist,                            similarity >= 0.7),
        ('Release date',  lambda r: r.date or '?',                       True),
        ('Type',          lambda r: r.album_type.capitalize() or '?',    True),
        ('Tracks',        lambda r: str(r.track_count),                  True),
        ('Explicit',      lambda r: str(r.explicit_count),               True),
        ('Duration',      lambda r: _fmt_dur(r.total_ms),                True),
        ('Label',         lambda r: r.label or '?',                      True),
        ('Edition words', lambda r: ', '.join(detect_variant_types(r.name)) or '—',
                          similarity >= 0.7 and not compact),
    ]
    for label, fn, highlight in rows_meta:
        vals  = [fn(r) for r in releases]
        style = 'yellow' if highlight and len(set(vals)) > 1 else ''
        tbl.add_row(label, *[f'[{style}]{v}[/{style}]' if style else v for v in vals])
    console.print(tbl)

    # -- unrelated: bail after table -------------------------------------------
    if similarity == 0:
        console.print(
            '  [red]No shared tracks — these appear to be different albums, '
            'not variants.[/red]'
        )
        console.print()
        return

    # -- partial overlap: show shared + unique, no canonical -------------------
    if similarity < 0.7:
        pct = int(similarity * 100)
        total_unique = len(shared_titles) + sum(len(t) for _, t in unique_per)
        console.print(
            f'  [yellow]{pct}% track overlap '
            f'({len(shared_titles)} shared / {total_unique} total unique) '
            f'— likely different albums, not variants.[/yellow]'
        )
        if shared_titles:
            console.print(f'  [dim]Shared tracks ({len(shared_titles)}):[/dim]')
            for t in shared_titles:
                console.print(f'    = {t}')
        for sp_id, titles in unique_per:
            console.print(f'  [bold]Only in {sp_id[:12]}…:[/bold]')
            for t in titles:
                console.print(f'    + {t}')
        console.print()
        return

    # -- variant mode: tracklist diff + canonical recommendation ---------------
    if same_titles:
        console.print('  [dim]Track titles are identical across all editions.[/dim]')
        if dur_diffs:
            console.print(f'  [yellow]{len(dur_diffs)} track(s) differ in duration (>2s):[/yellow]')
            for pos, name, durs_ms in dur_diffs:
                dur_str = '  vs  '.join(_fmt_dur(d) for d in durs_ms)
                console.print(f'    {pos:2}. {name}  —  {dur_str}')
        else:
            console.print('  [dim]All track durations are byte-for-byte identical.[/dim]')
    else:
        for sp_id, titles in unique_per:
            console.print(f'  [bold]Tracks only in {sp_id[:12]}…:[/bold]')
            for t in titles:
                console.print(f'    + {t}')
    console.print()

    console.print(
        f'  [green]·  Canonical:[/green]  [bold]{canon.name}[/bold]  '
        f'[dim]({canon.id[:12]}…)  {canon.date}  {canon.track_count} tracks[/dim]'
    )
    for alt in ranked[1:]:
        rs = [r for r in (reasons.get(alt.id) or []) if r != 'lower canonical score']
        reason_str = ('; '.join(rs)) if rs else ''
        console.print(
            f'  [red]·  Hide:[/red]  [dim]{alt.name}  ({alt.id[:12]}…)  '
            f'{alt.date or "?"}  {alt.track_count} tracks[/dim]'
            + (f'  — {reason_str}' if reason_str else '')
        )
    console.print()


# ── cmd: track variants ────────────────────────────────────────────────────────

# shorthand key → stored variant_type value
_TRACK_VARIANT_KEYS = {
    'r': 'radio_edit',
    'e': 'extended',
    'm': 'remaster',
    'd': 'demo',
    'c': 'clean',
    'i': 'instrumental',
    'a': 'acoustic',
    'l': 'live',
    'o': 'original_mix',
    'x': 'other',
}

_TRACK_VARIANT_HINT = (
    r'\[r]adio_edit \[e]xtended \[m]aster \[d]emo \[c]lean '
    r'\[i]nstrumental \[a]coustic \[l]ive \[o]riginal_mix \[x]other'
)


def _find_track_variant_groups(conn, include_linked: bool = False) -> list:
    """
    Return candidate groups of tracks that look like variant versions of the same recording.

    Each group is a list of dicts:
        { id, title, release_id, release_title, release_type, artist_name,
          isrc, listen_count, canonical_track_id }

    Detection passes (deduplicated):
      1. Same ISRC across different tracks (strongest signal).
      2. Same release_id + same _base_title().lower() (tracks on the same release).
      3. Same release_group_mbid on parent release + same clean title (across releases).
    """
    # Fetch all visible tracks with context
    rows = conn.execute('''
        SELECT t.id, t.title, t.release_id, t.isrc, t.canonical_track_id,
               r.title  AS release_title,
               r.type   AS release_type,
               r.release_group_mbid,
               a.name   AS artist_name,
               COUNT(l.id) AS listen_count
        FROM   tracks  t
        JOIN   releases r ON r.id = t.release_id
        JOIN   artists  a ON a.id = r.primary_artist_id
        LEFT JOIN listens l ON l.track_id = t.id
        WHERE  t.hidden = 0
        GROUP  BY t.id
        ORDER  BY a.name, r.release_date, t.track_number
    ''').fetchall()

    rows = [dict(r) for r in rows]

    # Build indexes
    by_isrc       = {}   # isrc → [row]
    by_release    = {}   # (release_id, base_title) → [row]
    by_rg_title   = {}   # (release_group_mbid, base_title) → [row]

    for row in rows:
        isrc = row['isrc']
        if isrc:
            by_isrc.setdefault(isrc, []).append(row)

        bt = normalize_text(_base_title(row['title']))
        by_release.setdefault((row['release_id'], bt), []).append(row)

        rg = row['release_group_mbid']
        if rg:
            by_rg_title.setdefault((rg, bt), []).append(row)

    # Already-linked track ids (has canonical_track_id set)
    linked_ids = {r['id'] for r in rows if r['canonical_track_id']}

    seen_sets = []
    groups    = []

    def _add(members):
        ids = frozenset(m['id'] for m in members)
        if len(ids) < 2:
            return
        for s in seen_sets:
            if s == ids:
                return
        seen_sets.append(ids)
        if not include_linked and ids.issubset(linked_ids | {m['id'] for m in members
                                                              if m['canonical_track_id']}):
            return
        # Skip if all members already have canonical_track_id set
        if not include_linked and all(m['canonical_track_id'] for m in members):
            return
        groups.append(list(members))

    # Pass 1: same ISRC (must be different track rows to be interesting)
    for isrc, members in by_isrc.items():
        if len(members) >= 2:
            ids = {m['id'] for m in members}
            if len(ids) >= 2:
                _add(members)

    # Pass 2: same release + same base title
    for (_rid, _bt), members in by_release.items():
        if len(members) >= 2:
            _add(members)

    # Pass 3: same release group + same base title across releases
    for (_rg, _bt), members in by_rg_title.items():
        if len(members) >= 2:
            ids = {m['id'] for m in members}
            if len(ids) >= 2:
                _add(members)

    # Sort groups by total listen count descending (most impactful first)
    groups.sort(key=lambda g: sum(m['listen_count'] for m in g), reverse=True)
    return groups


def cmd_track_variants(conn, include_linked: bool = False) -> None:
    """
    Interactive loop for declaring track variant relationships.

    For each candidate group:
      - User picks the canonical track (by number).
      - For each non-canonical track, user picks the variant type (or [h]ide-only).
      - Changes are committed immediately after each group.
    """
    from mdb_ops import link_track_variant

    groups = _find_track_variant_groups(conn, include_linked=include_linked)

    if not groups:
        console.print('  [dim]No unlinked track variant groups found.[/dim]')
        return

    console.print(
        f'  Found [bold]{len(groups)}[/bold] candidate group(s).  '
        f'[dim]\\[s]kip  \\[q]uit[/dim]\n'
    )

    saved    = 0
    gi       = 0
    quit_all = False

    while gi < len(groups) and not quit_all:
        group = groups[gi]
        gi   += 1
        console.rule(f'[dim]Group {gi}/{len(groups)}[/dim]', style='dim')

        # Sort: put clean titles first, variant-titled last; break ties by listen count desc
        group.sort(key=lambda m: (
            1 if detect_variant_types(m['title']) else 0,
            -(m['listen_count']),
        ))

        artist_name = group[0]['artist_name']
        base        = _base_title(group[0]['title'])
        console.print(f'  [bold]{base}[/bold]  ·  {artist_name}')
        console.print()

        for i, m in enumerate(group, 1):
            vtypes    = detect_variant_types(m['title'])
            vtype_str = '  ' + ' '.join(f'[yellow]{t}[/yellow]' for t in vtypes) if vtypes else ''
            canon_str = (f'  [dim](→ {m["canonical_track_id"][:14]}…)[/dim]'
                         if m['canonical_track_id'] else '')
            console.print(
                f'  [bold]{i}.[/bold]  [bold]{m["title"]}[/bold]'
                f'  [dim]{m["listen_count"]} listens[/dim]'
                f'  [dim cyan]db:{m["id"][:14]}…[/dim cyan]'
                f'  [dim]\[{m["release_type"] or "?"}] {m["release_title"]}[/dim]'
                f'{vtype_str}{canon_str}'
            )

        console.print()
        console.print(
            '  [bold]Canonical?[/bold]  '
            '[dim]number / \\[s]kip / \\[q]uit:[/dim] ',
            end='',
        )
        try:
            raw = input().strip()
        except (EOFError, KeyboardInterrupt):
            console.print('\n  [yellow]Interrupted.[/yellow]')
            break

        rl = raw.lower()
        if rl == 'q':
            quit_all = True
            break
        if rl in ('s', ''):
            console.print('  [dim]Skipped.[/dim]\n')
            continue
        if not raw.isdigit() or not (1 <= int(raw) <= len(group)):
            console.print(f'  [red]Enter 1–{len(group)}, s, or q.[/red]')
            gi -= 1  # retry same group
            continue

        canonical = group[int(raw) - 1]
        variants  = [m for m in group if m['id'] != canonical['id']]

        group_moved = 0
        group_ok    = True

        for m in variants:
            console.print(
                f'\n  [dim]{m["title"]}[/dim]  →  type?  '
                f'[dim]{_TRACK_VARIANT_HINT} / \\[h]ide-only / \\[s]kip:[/dim] ',
                end='',
            )
            try:
                ans = input().strip().lower()
            except (EOFError, KeyboardInterrupt):
                console.print('\n  [yellow]Interrupted.[/yellow]')
                quit_all = True
                group_ok = False
                break

            if ans == 'q':
                quit_all = True
                group_ok = False
                break
            if ans == 's':
                console.print('  [dim]Skipped variant.[/dim]')
                continue

            if ans == 'h':
                # hide-only: no variant record, just hide the track + move listens
                variant_type = None
            else:
                variant_type = _TRACK_VARIANT_KEYS.get(ans)
                if variant_type is None:
                    console.print(f'  [red]Unknown key "{ans}" — skipping.[/red]')
                    continue

            try:
                moved = link_track_variant(conn, canonical['id'], m['id'], variant_type)
                conn.commit()
                group_moved += moved
                saved += 1
                type_label = variant_type or 'hidden'
                console.print(
                    f'  [green]✓[/green]  moved {moved} listens, hidden'
                    f'  [dim]({type_label})[/dim]'
                )
            except ValueError as e:
                console.print(f'  [red]{e}[/red]')

        if group_ok and group_moved:
            console.print(f'  [dim]Group done — {group_moved} listens total moved.[/dim]')
        console.print()

    console.print(f'[bold]Done.[/bold]  {saved} variant(s) linked.')


# ── cmd: enrich soundtracks ───────────────────────────────────────────────────

_SOURCE_TYPES     = ['film', 'video_game', 'tv_series', 'musical', 'podcast', 'other']
_INDUSTRY_REGIONS = ['US', 'IN', 'GB', 'JP', 'ES', 'FR', 'KR', 'HK', 'AU', 'DE', 'IT', 'MX']
_LANGUAGES        = ['en', 'hi', 'ta', 'te', 'ml', 'ja', 'ko', 'es', 'fr', 'de', 'zh', 'pt']
_GEMINI_MODEL     = 'gemini-2.5-flash-lite'

# Nicknames shown next to codes at the prompt (purely cosmetic)
_REGION_LABELS = {
    'US': 'Hollywood', 'IN': 'India', 'GB': 'UK', 'JP': 'Japan',
    'KR': 'Korea',     'HK': 'Hong Kong', 'FR': 'France',
}
_LANG_LABELS = {
    'en': 'English', 'hi': 'Hindi', 'ta': 'Tamil', 'te': 'Telugu',
    'ml': 'Malayalam', 'ja': 'Japanese', 'ko': 'Korean',
    'es': 'Spanish', 'fr': 'French', 'de': 'German', 'zh': 'Chinese',
}


def _gemini_soundtrack_meta(title: str, artist: str) -> 'tuple[str|None, str|None, str|None]':
    """Ask Gemini to classify a soundtrack release into (source_type, industry_region, original_language).

    Requires GEMINI_API_KEY env var and the google-genai package.
    Returns (src, reg, lng, raw_json_str) — all None on any failure.
    """
    import os, json
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        return None, None, None, None
    try:
        from google import genai
        from google.genai import types as genai_types
    except ImportError:
        console.print('  [yellow]google-genai not installed — run: pip install google-genai[/yellow]')
        return None, None, None, None

    client = genai.Client(api_key=api_key)

    src_enum  = ' | '.join(_SOURCE_TYPES)
    reg_enum  = ' | '.join(_INDUSTRY_REGIONS)
    lang_enum = ' | '.join(_LANGUAGES)

    prompt = (
        'You are a music metadata assistant. Given a soundtrack album title and artist name, '
        'classify the release using exactly these three fields:\n\n'
        f'  source_type: one of {src_enum}\n'
        f'  industry_region: ISO 3166-1 alpha-2 country code of the primary production industry. '
        f'Prefer one of these common values: {reg_enum} — but any valid ISO 3166-1 alpha-2 code is allowed.\n'
        f'  original_language: ISO 639-1 language code of the original content. '
        f'Prefer one of these common values: {lang_enum} — but any valid ISO 639-1 code is allowed.\n\n'
        f'Album title: {title}\n'
        f'Artist: {artist or "Unknown"}\n\n'
        'Respond with ONLY a JSON object, no explanation. '
        'Example: {"source_type": "film", "industry_region": "IN", "original_language": "hi"}'
    )

    import time, re as _re
    _MAX_RETRIES = 4
    for attempt in range(_MAX_RETRIES):
        try:
            response = client.models.generate_content(
                model=_GEMINI_MODEL,
                contents=prompt,
                config=genai_types.GenerateContentConfig(response_mime_type='application/json'),
            )
            data = json.loads(response.text)
            src = data.get('source_type')
            reg = data.get('industry_region')
            lng = data.get('original_language')
            # Validate against known enums; pass through unknown ISO codes (they may be valid)
            if src not in _SOURCE_TYPES:
                src = None
            return src, reg, lng, response.text
        except Exception as exc:
            msg = str(exc)
            # Parse retryDelay from the error message (e.g. "retryDelay: '10s'")
            m = _re.search(r"'retryDelay':\s*'(\d+(?:\.\d+)?)s'", msg)
            wait = float(m.group(1)) + 1 if m else 2 ** (attempt + 1)
            if attempt < _MAX_RETRIES - 1 and ('429' in msg or '503' in msg):
                console.print(f'  [dim]Gemini rate-limited, retrying in {wait:.0f}s… (attempt {attempt+1}/{_MAX_RETRIES})[/dim]')
                time.sleep(wait)
            else:
                console.print(f'  [yellow]Gemini error: {exc}[/yellow]')
                return None, None, None, None


def cmd_enrich_soundtracks(conn, skip: int = 0, limit: 'int|None' = None,
                            release_id: 'str|None' = None, overwrite: bool = False) -> None:
    """Interactive prompt to fill release_soundtrack_meta for soundtrack releases."""
    where = "WHERE r.type_secondary = 'soundtrack' AND r.hidden = 0"
    params: list = []
    if release_id:
        where += ' AND r.id = ?'
        params.append(release_id)
    if not overwrite:
        where += ' AND (sm.source_type IS NULL OR sm.industry_region IS NULL OR sm.original_language IS NULL)'

    rows = conn.execute(f'''
        SELECT r.id, r.title,
               GROUP_CONCAT(a.name, ', ') AS artist,
               sm.source_type, sm.industry_region, sm.original_language
        FROM   releases r
        LEFT JOIN release_artists ra ON r.id = ra.release_id AND ra.role = 'main'
        LEFT JOIN artists a ON ra.artist_id = a.id
        LEFT JOIN release_soundtrack_meta sm ON sm.release_id = r.id
        {where}
        GROUP BY r.id
        ORDER BY r.release_year DESC NULLS LAST, r.title
    ''', params).fetchall()

    queue = rows[skip:]
    if limit:
        queue = queue[:limit]

    console.print(f'[dim]{len(rows)} soundtrack release(s), processing {len(queue)}[/dim]')
    import os
    if os.environ.get('GEMINI_API_KEY'):
        console.print('[dim]Gemini pre-fill: enabled[/dim]')
    else:
        console.print('[dim]Gemini pre-fill: disabled (set GEMINI_API_KEY to enable)[/dim]')
    console.print('[dim]Press Ctrl+C or q to stop.[/dim]\n')

    saved = 0

    try:
        for i, row in enumerate(queue):
            rid     = row['id']
            title   = row['title']
            artist  = row['artist'] or ''
            cur_src = row['source_type']
            cur_reg = row['industry_region']
            cur_lng = row['original_language']

            console.print(f'[dim][{i+1}/{len(queue)}][/dim]  [bold]{title}[/bold]  [dim]{artist}[/dim]')

            # Gemini-assisted defaults (falls back to None/None/None if key absent or call fails)
            g_src, g_reg, g_lng, g_raw = _gemini_soundtrack_meta(title, artist)
            def_src = cur_src or g_src
            def_reg = cur_reg or g_reg
            def_lng = cur_lng or g_lng
            if g_raw:
                console.print(f'  [dim]Gemini raw: {g_raw.strip()}[/dim]')
            if g_src or g_reg or g_lng:
                console.print(f'  [dim]Gemini: {g_src or "?"} · {g_reg or "?"} · {g_lng or "?"}[/dim]')

            # ── source_type ──
            src, quit_, _, _ = _prompt_choice(
                'Source type', _SOURCE_TYPES, current=def_src,
            )
            if quit_:
                break

            # ── industry_region ──
            region_opts = _INDUSTRY_REGIONS + ['other']
            region_display = [
                f'{r} ({_REGION_LABELS[r]})' if r in _REGION_LABELS else r
                for r in region_opts
            ]
            # Map display→code for lookup
            display_to_code = dict(zip(region_display, region_opts))
            reg_display, quit_, _, _ = _prompt_choice(
                'Industry region (ISO 3166-1 alpha-2)', region_display,
                current=next((d for d, c in display_to_code.items() if c == def_reg), None),
                allow_back=True,
            )
            if quit_:
                break
            reg = display_to_code.get(reg_display) if reg_display else None
            if reg == 'other':
                console.print('  [dim]Enter ISO 3166-1 alpha-2 code (e.g. NG, PK, BD):[/dim] ', end='')
                raw = input().strip().upper() or None
                reg = raw

            # ── original_language ──
            lang_opts = _LANGUAGES + ['other']
            lang_display = [
                f'{l} ({_LANG_LABELS[l]})' if l in _LANG_LABELS else l
                for l in lang_opts
            ]
            display_to_lang = dict(zip(lang_display, lang_opts))
            lng_display, quit_, _, _ = _prompt_choice(
                'Original language (ISO 639-1)', lang_display,
                current=next((d for d, c in display_to_lang.items() if c == def_lng), None),
                allow_back=True,
            )
            if quit_:
                break
            lng = display_to_lang.get(lng_display) if lng_display else None
            if lng == 'other':
                console.print('  [dim]Enter ISO 639-1 code (e.g. ur, bn, ml):[/dim] ', end='')
                raw = input().strip().lower() or None
                lng = raw

            conn.execute('''
                INSERT INTO release_soundtrack_meta (release_id, source_type, industry_region, original_language)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(release_id) DO UPDATE SET
                    source_type       = excluded.source_type,
                    industry_region   = excluded.industry_region,
                    original_language = excluded.original_language
            ''', (rid, src, reg, lng))
            conn.commit()

            parts = [src or '?', reg or '?', lng or '?']
            console.print(f'  [green]✓[/green]  {" · ".join(parts)}')
            console.print()
            saved += 1

    except KeyboardInterrupt:
        console.print('\n  [yellow]Interrupted.[/yellow]')

    console.rule(style='dim')
    console.print(f'  [dim]Saved: {saved}[/dim]')

