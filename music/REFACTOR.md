# Codebase Refactor Workplan

**Scope:** `mdb.py`, `sync.py`, `mdb_apis.py`, `mdb_ops.py`, `mdb_cli.py`,
`mdb_merge.py`, `mdb_strings.py`, `mdb_websources.py`

**Total lines:** ~11,856 across 8 files.
**Estimated reduction:** 700–900 lines after all improvements.

---

## Diagnosis

### mdb.py (4,402 lines) — over-grown

- **`import_album_from_beatport()` — ~537 lines of dead code.**
  `import_album_unified()` now handles the BP route via `_parse_import_url()`.
  The old function is never called. It has its own artist upsert loop, BPM update
  pass, image selection, and date priority logic — all of which `upsert_release_mdb`
  and `upsert_tracks_mdb` now handle correctly.

- **`import_album()` and `import_album_from_mb()` — parallel implementations.**
  Both predate `import_album_unified()`. `import_album()` is ~200 lines (Spotify);
  `import_album_from_mb()` is ~102 lines. They remain in the file as fallbacks
  but are no longer called by `cmd_import`. `import_album_from_mb` is still used
  by `_select_variants_unified()` so it cannot be deleted, but it can become a
  thin wrapper around `import_album_unified(url, no_gtin=True)`.

- **8 enrich commands with no shared framework.** `aoty`, `dates`, `art`, `tracks`,
  `artists`, `audio`, `soundtracks`, `popularity` each re-implement the same outer
  loop: build filter query → paginate → rate-limited API call → optional prompt →
  DB write → counter. No shared code. Conservative estimate: ~400 lines of
  duplication.

- **`cmd_release_variants` (~280 lines) duplicates `cmd_track_variants` structure.**
  Both are multi-stage interactive editors with the same prompt loop, `[a]dd`/
  `[s]kip`/`[q]uit` handling, and batch commit. Zero shared code.

### sync.py (1,814 lines) — justified complexity, some redundancy

The matching loop is legitimately complex. But sync.py duplicates from the main
codebase because it's explicitly isolated from circular imports:
- Own `open_db()` — adds WAL mode that `mdb_ops.open_db` silently omits
- Own `_prompt_choice()` — single-select variant of the one in `mdb_cli.py`
- Own `_write_variant_links()`
- Own `DB_PATH`, `PYTHON`, `MDB` constants
- `_mb_key()` / `_clean_key()` inside `bulk_rematch_by_name` duplicate
  `mdb_strings.parse_track_title()` logic

### API Classes — informal duck-typing, no shared contract

Five classes (`SpotifyRelease`, `MusicBrainzRelease`, `BeatportRelease`,
`ItunesRelease`, `BandcampRelease`) implement the same interface with no
formal declaration:

```
.name  .artist  .year  .date  .tracks  .track_count
.total_ms  .label  .album_type  .canonical_score()
._data: dict | None  ._ensure_full() -> None
```

`compare_releases()` type-hints `*SpotifyRelease` but works with any duck-typed
object. A `MetadataRelease` `Protocol` makes the contract explicit and enables
type checking without inheritance.

**HTTP fetch boilerplate** is copy-pasted 6+ times across the file. A single
`_http_get(url, *, headers, lim, timeout) -> dict` utility eliminates ~40 lines
and centralises error handling.

**`mdb_websources.py`** mixes scraping with DB-write logic (`_save_date`,
`save_aoty_data`). The DB writes belong in `mdb_ops.py`; the scrapers stay.

### CLI — 16 top-level commands, ~56 leaf commands, inconsistent flags

**Dead commands:**
- `mdb migrate` — one-shot migrations that run automatically in `init_schema()`
- `mdb diff` — developer debugging tool, not user-facing
- `mdb commits` — genre commit graph, highly specialized
- `mdb audit` — specialized introspection

**Flag inconsistency across enrich commands:**

| Command | Overwrite flag | `--skip`? | `--limit`? |
|---|---|---|---|
| `enrich aoty` | `--overwrite-date`, `--overwrite-type` (3 flags!) | ✅ | ✅ |
| `enrich art` | `--overwrite` | ✅ | ✅ |
| `enrich artists` | `--overwrite` | ❌ | ✅ |
| `enrich dates` | `--all` (inverted semantics) | ✅ | ✅ |
| `enrich tracks` | `--force` (different name) | ❌ | ✅ |

Target: uniform `--force` across all enrich commands.

### Performance

| Issue | Impact |
|---|---|
| `_discover_sources()` fetches sources sequentially | +3–5s per import |
| `mdb_ops.open_db()` doesn't set WAL mode (sync.py does) | Silent write slowdown |
| No Spotify token persistence across invocations | Extra round-trip per mdb call |
| AOTY prefetch only in interactive mode, not batch | Batch enrichment 5× slower than it needs to be |

---

## Task List (least → most destructive)

### Tier 1 — Additive / Zero Risk

- [ ] **T1** WAL mode + `synchronous=NORMAL` in `mdb_ops.open_db()` (2 lines)
- [ ] **T2** `_http_get()` utility in `mdb_apis.py`; update all callers
- [ ] **T3** `MetadataRelease` Protocol in `mdb_apis.py`; update `compare_releases()` sig
- [ ] **T4** Spotify token cache (`~/.cache/mdb/spotify_token.json`)

### Tier 2 — Refactor Existing Code

- [ ] **T5** Parallel source fetching in `_discover_sources()` via `ThreadPoolExecutor`
- [ ] **T6** Standardise enrich flags: `--force` everywhere, drop `--overwrite-date`/`--overwrite-type`/`--all` variants; add `--skip`/`--limit` to `enrich artists` and `enrich tracks`
- [ ] **T7** Extract enrich pipeline helper; refactor `cmd_enrich_aoty`, `cmd_enrich_art`, `cmd_enrich_audio`, `cmd_enrich_popularity` to use it (the 4 simplest)
- [ ] **T8** Move `_save_date` and `save_aoty_data` DB writes from `mdb_websources.py` into `mdb_ops.py`

### Tier 3 — Dead Code Deletion

- [ ] **T9** Delete `import_album_from_beatport()` (~537 lines)
- [ ] **T10** Reduce `import_album()` (Spotify, ~200 lines) to a thin wrapper around `import_album_unified()`
- [ ] **T11** Remove `mdb migrate`, `mdb diff`, `mdb commits`, `mdb audit` from CLI + argparse

### Tier 4 — Structural / Higher Risk

- [ ] **T12** `sync_utils.py`: extract shared `open_db`, `new_ulid`, `slugify`, artist primitives; update both `sync.py` and `mdb_ops.py` to import from it (fixes silent WAL inconsistency and sync isolation)
- [ ] **T13** Reduce `cmd_release_variants` to share skeleton with `cmd_track_variants` (already in `mdb_cli.py`)

---

## Expected Outcomes

| Metric | Before | After |
|---|---|---|
| `mdb.py` lines | 4,402 | ~3,300 |
| Total codebase | 11,856 | ~10,900 |
| Import wall time (3 sources) | ~5–7s | ~2–3s |
| `mdb enrich X` flag consistency | ❌ inconsistent | ✅ uniform |
| WAL mode on all connections | ❌ only sync.py | ✅ everywhere |
| Dead code | ~750 lines | 0 |
