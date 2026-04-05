"""mdb_cli — Rich UI helpers for the mdb interactive CLI."""

__all__ = [
    'render_diff',
    '_fmt_dur', '_trunc',
    '_format_mb_type', '_print_member',
    '_aoty_prompt', '_dates_prompt', '_prompt_choice',
]

import logging

from rich import box
from rich.console import Console
from rich.table   import Table

from mdb_apis     import compare_releases
from mdb_strings  import detect_variant_types, _parse_user_date
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

    console.print('[bold]Recommendation[/bold]')
    console.print('─' * 78)
    console.print(
        f'  [green]Canonical:[/green]  [bold]{canon.name}[/bold]  '
        f'[dim]({canon.id[:12]}…)  {canon.date}  {canon.track_count} tracks[/dim]'
    )
    for alt in ranked[1:]:
        reason_str = '; '.join(reasons.get(alt.id) or []) or 'lower canonical score'
        console.print(
            f'  [red]Hide:[/red]  [dim]{alt.name}  ({alt.id[:12]}…)  '
            f'{alt.date or "?"}  {alt.track_count} tracks[/dim]  — {reason_str}'
        )
    console.print()
