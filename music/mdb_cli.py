"""
mdb_cli — Rich UI helpers for the mdb interactive CLI.

Provides: formatting helpers, interactive prompts.

Dependency order: mdb_strings → mdb_apis → mdb_websources → mdb_cli
"""

import logging

from rich.console import Console

from mdb_strings import detect_variant_types, _parse_user_date
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
                   allow_hide: bool = False, multi: bool = False):
    """
    Display a numbered choice list and return the chosen value.

    Single-select (multi=False):
      Enter with no input keeps `current` (shown as default).
      Returns (value, quit, hide).

    Multi-select (multi=True):
      `current` is a list of currently-selected values.
      Input is space/comma-separated numbers.
      Enter keeps current selection.
      Returns (list_of_values, quit, hide).
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
        hide_hint   = '  \\[h]ide' if allow_hide else ''
        console.print(f'  [dim]Enter=keep ({current_str}){hide_hint}  q=quit:[/dim] ', end='')
        raw = input().strip().lower()
        if raw == 'q':
            return None, True, False
        if allow_hide and raw == 'h':
            return None, False, True
        if not raw:
            return current_list, False, False
        tokens = raw.replace(',', ' ').split()
        chosen = []
        for tok in tokens:
            if tok.isdigit():
                idx = int(tok)
                if 0 <= idx < len(options):
                    chosen.append(options[idx])
        if not chosen:
            return current_list, False, False
        # If 'none' is explicitly chosen alongside others, keep only 'none'
        if 'none' in chosen and len(chosen) > 1:
            chosen = ['none']
        return chosen, False, False

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
    console.print(f'  [dim]Enter=keep ({default}){hide_hint}  q=quit:[/dim] ', end='')
    raw = input().strip().lower()
    if raw == 'q':
        return None, True, False
    if allow_hide and raw == 'h':
        return None, False, True
    if raw == '' or not raw.isdigit():
        return default, False, False
    idx = int(raw)
    if 0 <= idx < len(options):
        return options[idx], False, False
    return default, False, False
