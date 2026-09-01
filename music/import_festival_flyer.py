#!/usr/bin/env python3
"""import_festival_flyer.py — turn a festival lineup (a flyer image, a
multi-page/multi-day PDF, or a Wikipedia lineup article) into
concert_events/concert_performances rows.

Pipeline:
  1. Get a structured lineup, from ANY ONE of three sources:
     a. Vision-parse a flyer/PDF via headless `claude -p` calls (Read tool
        only, no API key needed) — one call per page, run SEQUENTIALLY (not
        concurrently — see parse_flyer's docstring for why). Produces
        {page, stage, act, start, end}. Real clock times, but OCR can misread
        dense/small text on a busy grid.
     b. Parse a Wikipedia lineup article's raw wikitext (--wikitext) — no
        vision model at all, just markup stripping. Produces
        {page, stage, act, start: None, end: None, _list_pos}. No exact
        times, but the article text itself is usually already ordered
        "earliest to latest" per stage, and there's no OCR risk since it's
        human-transcribed. Prefer this over a flyer when a Wikipedia article
        exists — the whole reason to parse the flyer is to recover
        chronological order, and the article already gives you that order,
        more reliably.
     c. Reload a previously --dump-json'd lineup (--load-json) — lets you
        hand-fix OCR misreads from (a) in a text editor and resume without
        re-parsing.
  2. Compute the headliner per (page, stage) — last act on that stage that
     day, by real time (a) or list position (b/c) -- pure Python.
  3. Search setlist.fm (via mdb_apis.SetlistFmClient) for each act on the
     date mapped to its page. Acts with no setlist.fm listing are flagged,
     not silently dropped — this includes non-musical programming (comedy,
     Q&As, film screenings) that a lineup often lists alongside real sets;
     those just naturally return zero search results rather than needing
     special-case filtering during parsing.
  4. Interactive checkbox prompt (questionary) — pick exactly which sets you
     actually attended. Needs a real terminal (not a piped/backgrounded
     process) — this step can't run non-interactively.
  5. Import the selected sets via mdb_ops.upsert_concert_performance, then
     apply billing/set_order per (page, stage) group (headliner = latest
     starter; everyone else = support, set_order = chronological position
     among just the selected acts). Stage name is stashed in the
     performance's `notes` column.

Usage:
    python3 import_festival_flyer.py flyer.png --dates 31-10-2015 \\
        --venue "Fairplex" --festival "HARD Day of the Dead 2015" \\
        [--db master.sqlite] [--dry-run]

    python3 import_festival_flyer.py "Bonnaroo 2014.pdf" \\
        --dates 12-06-2014,13-06-2014,14-06-2014,15-06-2014 \\
        --venue "Great Stage Park" --festival "Bonnaroo Music & Arts Festival 2014"

    # Prefer this over the PDF above when a Wikipedia lineup article exists —
    # fetch its raw wikitext (e.g. via ?action=raw) into a file first:
    python3 import_festival_flyer.py --wikitext bonnaroo_2014.wiki \\
        --dates 12-06-2014,13-06-2014,14-06-2014,15-06-2014 \\
        --venue "Great Stage Park" --festival "Bonnaroo Music & Arts Festival 2014"

    # Fix OCR misreads from a flyer parse without re-running claude -p:
    python3 import_festival_flyer.py flyer.png --dump-json lineup.json
    #  ...hand-edit lineup.json...
    python3 import_festival_flyer.py --load-json lineup.json --dates ...

`--dates` is a comma-separated list, one per page, in setlist.fm's own
DD-MM-YYYY format (matches search_setlists()) — page 1 gets the first date,
page 2 the second, etc. Its length must match the number of distinct pages
the parse actually finds; the tool errors out (rather than guessing) if not.
`--festival` is optional — omit it to import as regular (non-festival)
multi-act nights instead of tagging concert_events.is_festival.
"""
import argparse
import json
import re
import subprocess
import sys

sys.path.insert(0, '.')

import questionary
from rich.console import Console
from rich.table import Table

from mdb_apis import _get_setlistfm_client, parse_setlistfm_setlist, setlistfm_id_from_url
from mdb_ops import managed_db, upsert_concert_performance

console = Console(width=100, highlight=False)

_FENCE_RE = re.compile(r'^```(?:json)?\s*|\s*```$', re.MULTILINE)


def _page_count(path: str) -> int:
    """1 for any image; the real page count for a PDF (via macOS Spotlight
    metadata — this whole environment is macOS, no extra dependency needed)."""
    if not path.lower().endswith('.pdf'):
        return 1
    proc = subprocess.run(['mdls', '-name', 'kMDItemNumberOfPages', '-raw', path],
                           capture_output=True, text=True, timeout=10)
    try:
        return max(1, int(proc.stdout.strip()))
    except ValueError:
        raise RuntimeError(f'could not determine page count for {path!r} '
                            f'(mdls returned {proc.stdout!r}) — is this a valid PDF?')


def _parse_page(path: str, page_num: int, total_pages: int) -> list:
    """Vision-parses ONE page into a list of {stage, act, start, end} dicts
    via a headless `claude -p` call. Splitting per-page (rather than one call
    across a whole multi-page PDF) is both faster and more reliable — each
    call only has to reason about one page's grid, not the whole festival."""
    page_clause = (f'ONLY page {page_num} of the {total_pages}-page PDF at {path} '
                    f'(use the Read tool\'s pages parameter set to "{page_num}") — '
                    'ignore every other page.') if total_pages > 1 else f'the flyer image at {path}'
    prompt = (
        f'Read {page_clause} and extract every named performance slot on it. '
        'Output ONLY a raw JSON array (no markdown fences, no prose, no explanation) '
        'of objects: {"stage": str, "act": str, "start": "HH:MM" in 24-hour time, '
        '"end": "HH:MM" in 24-hour time}. One entry per act per stage, in the order '
        'printed. Include EVERY named slot on the schedule (music acts, comedy sets, '
        'panels, screenings, superjams, DJ sets) even if you are not sure it is a live '
        'musical performance — do not filter anything out, a later step handles that. '
        'Skip only genuinely blank grid cells with no name printed. If an act\'s end '
        'time is not printed, infer it as the next act\'s start time on that same '
        'stage. For the last act on a stage with no printed end time, estimate a '
        'reasonable set length from the other sets on that same stage.'
    )
    proc = subprocess.run(
        ['claude', '-p', prompt, '--output-format', 'json',
         '--allowedTools', 'Read', '--dangerously-skip-permissions'],
        capture_output=True, text=True, timeout=300,
    )
    if proc.returncode != 0:
        raise RuntimeError(f'page {page_num}: claude -p exited {proc.returncode}: {proc.stderr[:2000]}')

    envelope = json.loads(proc.stdout)
    result_text = envelope.get('result', '') if isinstance(envelope, dict) else ''
    if not result_text:
        raise RuntimeError(f'page {page_num}: no result text in claude -p output: {proc.stdout[:2000]}')

    cleaned = _FENCE_RE.sub('', result_text.strip())
    try:
        entries = json.loads(cleaned)
    except json.JSONDecodeError as e:
        raise RuntimeError(f'page {page_num}: could not parse flyer JSON: {e}\nraw: {cleaned[:2000]}') from e

    if not isinstance(entries, list):
        raise RuntimeError(f'page {page_num}: expected a JSON array, got: {cleaned[:500]}')
    for entry in entries:
        entry['page'] = page_num
    return entries


def parse_flyer(path: str) -> list:
    """Vision-parses a flyer (single image, or a multi-page PDF — one call
    per page) into a list of {page, stage, act, start, end} dicts. `page` is
    the 1-indexed source page (always 1 for an image). Raises RuntimeError on
    any failure — the caller should not proceed with a guessed/partial lineup.

    Pages are parsed SEQUENTIALLY, not concurrently — running several
    `claude -p` subprocesses at once contends for the same resources and
    each one gets slower, to the point that a call that takes ~165s alone
    can blow well past a 300s timeout when 3 siblings are running alongside
    it. One page at a time is slower in wall-clock total but far more
    reliable, and each individual call still only reasons about one page's
    grid rather than the whole festival."""
    total_pages = _page_count(path)
    lineup = []
    for p in range(1, total_pages + 1):
        console.print(f'[dim]  parsing page {p}/{total_pages}...[/dim]')
        lineup.extend(_parse_page(path, p, total_pages))
    if not lineup:
        raise RuntimeError(f'parsed zero slots from {path!r} across {total_pages} page(s)')
    return lineup


def _to_minutes(hhmm: str) -> int:
    h, m = hhmm.split(':')
    return int(h) * 60 + int(m)


def _sort_key(entry: dict) -> int:
    """Sorts by real clock time when we have one (flyer/PDF path), or by
    document list position when we don't (Wikipedia path — the article text
    itself says "artists listed from earliest to latest set times" per
    stage, so position IS the chronological order, just without exact
    minutes)."""
    return entry['_list_pos'] if entry.get('_list_pos') is not None else _to_minutes(entry['start'])


_WIKI_REF_RE = re.compile(r'<ref[^>]*?/>|<ref[^>]*?>.*?</ref>', re.DOTALL)
_WIKI_LINK_PIPE_RE = re.compile(r'\[\[[^\]|]+\|([^\]]+)\]\]')
_WIKI_LINK_RE = re.compile(r'\[\[([^\]#|]+)(?:#[^\]|]+)?\]\]')
_WIKI_BOLD_ITALIC_RE = re.compile(r"'''?")
_DAY_HEADER_RE = re.compile(r'\n===\s*(.+?)\s*===\n')
_STAGE_BLOCK_RE = re.compile(r'^\*(?!\*)(.+?):?\s*$\n((?:^\*\*.*$\n?)*)', re.MULTILINE)


def _clean_wiki_text(s: str) -> str:
    s = _WIKI_REF_RE.sub('', s)
    s = _WIKI_LINK_PIPE_RE.sub(r'\1', s)
    s = _WIKI_LINK_RE.sub(r'\1', s)
    s = _WIKI_BOLD_ITALIC_RE.sub('', s)
    return s.strip()


def parse_wikitext(text: str) -> list:
    """Parses a Wikipedia festival-lineup article's wikitext (the raw markup,
    e.g. from https://en.wikipedia.org/wiki/<Article>?action=raw) into a list
    of {page, stage, act, start, end, _list_pos} dicts — one `page` per
    `===Day===` section, in document order. `start`/`end` are always None
    (Wikipedia gives ORDER, not exact clock times); `_list_pos` carries the
    1-indexed position within its stage, which is what billing/headliner
    computation actually needs.

    Skips any slot whose text contains "(canceled)"/"(cancelled)" — those
    never happened, so there's nothing to import even if the user thinks
    they saw it. Raises RuntimeError if no day sections are found at all."""
    text = re.split(r'\n==\s*References\s*==\n', text)[0]
    parts = _DAY_HEADER_RE.split('\n' + text)
    if len(parts) < 3:
        raise RuntimeError('found no "===Day===" section headers in this wikitext — '
                            'is this really a festival lineup article?')

    lineup = []
    canceled = []
    # parts[0] is whatever precedes the first day header (discarded); after
    # that it alternates [day_title, day_body, day_title, day_body, ...].
    for page, (day_title, day_body) in enumerate(zip(parts[1::2], parts[2::2]), start=1):
        for stage_raw, acts_block in _STAGE_BLOCK_RE.findall(day_body):
            stage = _clean_wiki_text(stage_raw)
            acts = [line[2:] for line in acts_block.splitlines() if line.startswith('**')]
            list_pos = 0
            for raw_act in acts:
                act = _clean_wiki_text(raw_act)
                if not act:
                    continue
                if re.search(r'cancel(l)?ed', act, re.IGNORECASE):
                    canceled.append(f'{day_title} · {stage} — {act}')
                    continue
                list_pos += 1
                lineup.append({'page': page, 'stage': stage, 'act': act,
                                'start': None, 'end': None, '_list_pos': list_pos})

    if canceled:
        console.print(f'[dim]Skipped {len(canceled)} canceled slot(s): {", ".join(canceled)}[/dim]')
    if not lineup:
        raise RuntimeError('parsed zero slots from this wikitext — day sections were found but '
                            'no "*Stage:" / "**Act" bullet lists matched inside them')
    return lineup


def compute_stage_order(lineup: list) -> dict:
    """Groups lineup entries by (page, stage), sorted by start time ascending.
    Returns {(page, stage): [entry, ...]} with each entry annotated with
    `_order` (1-indexed chronological position within that page+stage) and
    `_headliner` (True for the last act on that stage that day)."""
    by_group = {}
    for entry in lineup:
        by_group.setdefault((entry['page'], entry['stage']), []).append(entry)
    for entries in by_group.values():
        entries.sort(key=_sort_key)
        for i, e in enumerate(entries):
            e['_order'] = i + 1
            e['_headliner'] = (i == len(entries) - 1)
    return by_group


def resolve_dates(lineup: list, dates_arg: str) -> dict:
    """Maps each page number found in the lineup to a date string, in page
    order. Raises ValueError if --dates' length doesn't match the number of
    distinct pages — better to fail loudly than silently misdate half the
    festival."""
    pages = sorted({e['page'] for e in lineup})
    dates = [d.strip() for d in dates_arg.split(',') if d.strip()]
    if len(dates) != len(pages):
        raise ValueError(
            f'--dates has {len(dates)} date(s) but the flyer has {len(pages)} '
            f'distinct page(s) ({pages}) — pass exactly one date per page, in order.'
        )
    return dict(zip(pages, dates))


def search_acts(lineup: list, venue: 'str | None', page_dates: dict,
                 cache_path: 'str | None' = None) -> None:
    """Searches setlist.fm for every lineup entry that hasn't been searched
    yet (`setlistfm_status` missing/None) and mutates each entry IN PLACE
    with `setlistfm_status` ('found' | 'ambiguous' | 'not_found') and
    `setlistfm_url` (the resolved URL — the sole match, or the first
    candidate when ambiguous; `setlistfm_candidates` holds all of them in
    the ambiguous case for manual correction).

    If `cache_path` is given, the WHOLE lineup is re-dumped to that file
    after every single search — cheap at this size (a few hundred KB, a few
    hundred entries) and it means an interrupted run (Ctrl-C, crash, or just
    the ~1 req/sec pace taking minutes) loses at most the one search in
    flight, not everything already found. Rerunning with the same
    --load-json path resumes exactly where it left off, since already-
    searched entries are skipped."""
    client = _get_setlistfm_client()
    todo = [e for e in lineup if not e.get('setlistfm_status')]
    if not todo:
        console.print('[dim]All slots already have a cached setlist.fm result — skipping search.[/dim]')
        return
    console.print(f'[dim]Searching setlist.fm for {len(todo)}/{len(lineup)} '
                   f'not-yet-searched slot(s)...[/dim]')
    with console.status('[dim]Searching setlist.fm...[/dim]') as status:
        for e in todo:
            status.update(f'[dim]Searching setlist.fm... {e["act"]}[/dim]')
            matches = client.search_setlists(artist_name=e['act'], venue_name=venue,
                                              date=page_dates[e['page']])
            if len(matches) == 1:
                e['setlistfm_status'] = 'found'
                e['setlistfm_url'] = matches[0]['url']
            elif len(matches) > 1:
                e['setlistfm_status'] = 'ambiguous'
                e['setlistfm_url'] = matches[0]['url']
                e['setlistfm_candidates'] = [m['url'] for m in matches]
            else:
                e['setlistfm_status'] = 'not_found'
                e['setlistfm_url'] = None
            if cache_path:
                with open(cache_path, 'w') as f:
                    json.dump(lineup, f, indent=2)


def print_lineup_table(by_group: dict, page_dates: dict) -> None:
    table = Table(title='Parsed lineup', box=None, show_lines=False)
    table.add_column('Date')
    table.add_column('Stage')
    table.add_column('Time')
    table.add_column('Act')
    table.add_column('setlist.fm')
    status_str = {
        'found': '[green]✓[/green]', 'ambiguous': '[yellow]ambiguous[/yellow]',
        'not_found': '[red]not found[/red]', None: '[dim]unsearched[/dim]',
    }
    for page, stage in sorted(by_group):
        for e in by_group[(page, stage)]:
            name = e['act'] + (' [bold]★[/bold]' if e['_headliner'] else '')
            time_str = f"{e['start']}–{e['end']}" if e['start'] else '—'
            table.add_row(page_dates[page], stage, time_str, name,
                           status_str[e.get('setlistfm_status')])
    console.print(table)

    missing = sorted({e['act'] for entries in by_group.values() for e in entries
                       if e.get('setlistfm_status') == 'not_found'})
    if missing:
        console.print(f'\n[yellow]⚠ {len(missing)} act(s)/slot(s) have no setlist.fm listing '
                       f'(non-musical programming and unlisted acts alike — can\'t be imported '
                       f'automatically):[/yellow] {", ".join(missing)}')


def prompt_selection(by_group: dict, page_dates: dict) -> list:
    """Returns a list of (page, stage, entry) tuples for whatever the user
    checked. Slots with no cached setlistfm_url are shown but disabled;
    'ambiguous' slots ARE selectable (using the first candidate) but labeled
    so you know to double-check/hand-correct setlistfm_url in the JSON if
    it picked the wrong one."""
    entries = []  # (label, page, stage, entry)
    for page, stage in sorted(by_group):
        for e in by_group[(page, stage)]:
            time_str = f"{e['start']} " if e['start'] else ''
            flag = ' [ambiguous]' if e.get('setlistfm_status') == 'ambiguous' else ''
            label = (f"{page_dates[page]} · {stage} — {time_str}{e['act']}{flag}"
                     + (' ★' if e['_headliner'] else ''))
            entries.append((label, page, stage, e))

    if not any(e.get('setlistfm_url') for *_, e in entries):
        console.print('[red]No slot on this flyer has a setlist.fm match — nothing to select.[/red]')
        return []

    choices = [
        questionary.Choice(label, value=i,
                            disabled=None if e.get('setlistfm_url') else (e.get('setlistfm_status') or 'not searched'))
        for i, (label, _, _, e) in enumerate(entries)
    ]
    picked = questionary.checkbox('Which sets did you actually attend?', choices=choices).ask()
    if not picked:
        return []
    return [(entries[i][1], entries[i][2], entries[i][3]) for i in picked]


def import_selected(db_path: str, selected: list, festival_name: 'str | None') -> None:
    client = _get_setlistfm_client()
    with managed_db(db_path) as conn:
        # Track (page, stage) -> [(performance_id, entry)] so billing/set_order
        # can be computed per stage-per-day across just the acts actually
        # selected -- dropping an act from the selection shouldn't leave a
        # gap in set_order.
        by_group_selected = {}
        event_ids = set()
        for page, stage, entry in selected:
            url = entry['setlistfm_url']
            data = client.get_setlist(setlistfm_id_from_url(url))
            record = parse_setlistfm_setlist(data)
            perf_id = upsert_concert_performance(conn, url, record)
            conn.execute('UPDATE concert_performances SET notes = ? WHERE id = ?',
                         [f'Stage: {stage}', perf_id])
            event_id = conn.execute('SELECT event_id FROM concert_performances WHERE id = ?',
                                     [perf_id]).fetchone()[0]
            event_ids.add(event_id)
            by_group_selected.setdefault((page, stage), []).append((perf_id, entry))

        for (page, stage), perfs in by_group_selected.items():
            perfs.sort(key=lambda pe: _sort_key(pe[1]))
            for i, (perf_id, entry) in enumerate(perfs):
                billing = 'headliner' if i == len(perfs) - 1 else 'support'
                conn.execute('UPDATE concert_performances SET billing = ?, set_order = ? WHERE id = ?',
                             [billing, i + 1, perf_id])

        if festival_name:
            for event_id in event_ids:
                conn.execute('UPDATE concert_events SET is_festival = 1, festival_name = ? WHERE id = ?',
                             [festival_name, event_id])

        integrity = conn.execute('PRAGMA integrity_check').fetchone()[0]
        fk_violations = conn.execute('PRAGMA foreign_key_check').fetchall()
    console.print(f'\n[green]Imported {len(selected)} set(s).[/green] '
                  f'integrity_check={integrity}, fk_violations={len(fk_violations)}')
    if fk_violations:
        console.print(f'[red]{fk_violations}[/red]')


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('flyer', nargs='?', help='Path to the flyer image or PDF (vision-parsed)')
    ap.add_argument('--wikitext', help='Path to a Wikipedia lineup article\'s raw wikitext '
                                        '(alternative to `flyer` — no OCR, no claude -p calls; '
                                        'names/order come straight from the article text)')
    ap.add_argument('--load-json', help='Skip parsing entirely; load a lineup list from a JSON '
                                         'file previously written by --dump-json or a prior run '
                                         '(hand-correct OCR misreads or a wrong setlistfm_url '
                                         'there, then reload). Search results get written back '
                                         'into THIS file incrementally as they\'re found, unless '
                                         '--dump-json is also given — so a second run against the '
                                         'same path resumes instead of re-searching from scratch.')
    ap.add_argument('--dump-json', help='Write the lineup to this JSON file. Without --dates: '
                                         'dump the raw parse and exit (no search/prompt) — use '
                                         'this to hand-fix OCR misreads before ever hitting '
                                         'setlist.fm. With --dates: this becomes the incremental '
                                         'write-through cache for search results instead of '
                                         '--load-json\'s own path.')
    ap.add_argument('--dates', help='Comma-separated DD-MM-YYYY, one per page (setlist.fm format). '
                                     'Required unless --dump-json is also given.')
    ap.add_argument('--venue', help='Venue name, passed to setlist.fm search to disambiguate')
    ap.add_argument('--festival', help='Festival name — tags concert_events.is_festival if given')
    ap.add_argument('--db', default='master.sqlite')
    ap.add_argument('--dry-run', action='store_true', help='Parse + search + select, but do not write to the DB')
    args = ap.parse_args()

    sources = [s for s in (args.flyer, args.wikitext, args.load_json) if s]
    if len(sources) != 1:
        ap.error('pass exactly one of: flyer (positional), --wikitext, or --load-json')

    if args.load_json:
        console.print(f'[dim]Loading lineup from {args.load_json}...[/dim]')
        with open(args.load_json) as f:
            lineup = json.load(f)
    elif args.wikitext:
        console.print(f'[dim]Parsing wikitext {args.wikitext}...[/dim]')
        with open(args.wikitext) as f:
            lineup = parse_wikitext(f.read())
    else:
        console.print(f'[dim]Parsing {args.flyer} via claude -p...[/dim]')
        lineup = parse_flyer(args.flyer)

    n_pages = len({e['page'] for e in lineup})
    console.print(f'[green]Parsed {len(lineup)} slot(s) across '
                   f'{len({e["stage"] for e in lineup})} stage(s) and {n_pages} page(s).[/green]')

    # Write-through target for cached setlist.fm results: an explicit
    # --dump-json takes priority (keeps the original --load-json pristine,
    # writes enriched results elsewhere); otherwise default to updating the
    # file we loaded from in place, so a second run against the same
    # --load-json path picks up exactly where the last one left off.
    cache_path = args.dump_json or args.load_json

    if args.dump_json and not args.dates:
        # Dump-only mode (no --dates yet) — just save the raw parse for
        # hand-fixing OCR misreads, no search, no interactive prompt.
        with open(args.dump_json, 'w') as f:
            json.dump(lineup, f, indent=2)
        console.print(f'[green]Wrote raw lineup to {args.dump_json}.[/green] Hand-fix any OCR '
                       f'misreads (act names, start/end), then rerun with --load-json.')
        return

    if not args.dates:
        ap.error('--dates is required unless --dump-json is given without --dates')

    page_dates = resolve_dates(lineup, args.dates)

    if cache_path:
        # Save the parsed/loaded lineup immediately, before searching —
        # if the search phase gets interrupted before even one lookup
        # completes, the parse itself (the slow/fragile part for a flyer)
        # isn't lost.
        with open(cache_path, 'w') as f:
            json.dump(lineup, f, indent=2)

    by_group = compute_stage_order(lineup)
    search_acts(lineup, args.venue, page_dates, cache_path=cache_path)
    print_lineup_table(by_group, page_dates)

    selected = prompt_selection(by_group, page_dates)
    if not selected:
        console.print('[yellow]Nothing selected — exiting without writing to the DB.[/yellow]')
        return

    console.print(f'\n{len(selected)} set(s) selected.')
    if args.dry_run:
        console.print('[dim]--dry-run: skipping DB write.[/dim]')
        for page, stage, entry in selected:
            console.print(f'  {page_dates[page]} · {stage} — {entry["act"]} → {entry["setlistfm_url"]}')
        return

    import_selected(args.db, selected, args.festival)


if __name__ == '__main__':
    main()
