# CLAUDE.md

## Project

Personal music listening tracker. Frontend: client-side SPA (GitHub Pages) loading SQLite via sql.js. Backend: local Python CLI for importing metadata, syncing listen history, and enriching the DB.

**Local dev:** `cd music && python3 -m http.server 8000` (`master.sqlite` must be present)\
**WAL note:** After any Python DB write, run `PRAGMA wal_checkpoint(TRUNCATE)` — sql.js reads only the main file, not the WAL.

---

## Frontend (`music/`)

**Entry:** `app.js` bootstraps sql.js, routes `?view=` params. **Views:** `home`, `artist`, `release`, `top-albums`, `top-artists`, `top-tracks`, `year`, `genre` — each exports `{ mount, unmount }`. **Shared utils:** `utils.js`. **Config:** `config.js`.

Key patterns: all nav links use `href="?view=X&id=Y"`; call `lucide.createIcons()` after every async DOM insertion; all queries add `AND NOT EXISTS (SELECT 1 FROM release_variants rv WHERE rv.variant_id = r.id)` to hide variants from browse/search. BPM column shown only when ≥1 track has `tempo_bpm`. `tracks.mix_name` drives `.tracklist-eti` dimmed styling on ETI. External service icons live in `images/links/*.svg` with CSS mask technique.

---

## Python Toolchain (`music/`)

| File | Role |
|------|------|
| `mdb.py` | Primary CLI — import, enrich, variants, delete, certs |
| `sync.py` | Listening history sync + interactive matching |
| `mdb_merge.py` | `ReleaseMerge`, `MDBRelease`, `MDBTrack`; `upsert_release_mdb`; `resolve_by_gtin` |
| `mdb_ops.py` | DB schema, migrations, upserts, `open_db`, `save_aoty_data`, `save_release_date` |
| `mdb_apis.py` | All provider clients: `SpotifyClient`, `MusicBrainzRelease`, `BeatportRelease`, `ItunesRelease`, `BandcampRelease`, `DeezerRelease`; `MetadataRelease` Protocol; `_http_get` |
| `mdb_strings.py` | Normalisation, title parsing, variant detection, `normalize_upc` |
| `mdb_websources.py` | AOTY scraper, Wikipedia fetcher, `_gemini_find_wiki_article` |
| `mdb_cli.py` | Rich UI helpers, soundtrack/track-variant prompts |

**Dep graph:** `mdb_strings` ← `mdb_ops`, `mdb_apis`, `mdb_merge` ← `mdb.py`; `mdb_websources` ← `mdb_cli` ← `mdb.py`

---

## `mdb.py` Commands

```
mdb import <url…|file> [--no-mb] [--no-aoty] [--no-wiki] [--no-gtin] [--no-variants] [--auto]
```
Accepts Spotify, MusicBrainz, Beatport, Apple Music, Bandcamp, Deezer URLs or batch files. GTIN broadcast auto-discovers cross-platform sources in parallel. Shows enrichment diff if release already exists. Offers MB release-group variant linking after import. `--no-gtin` skips discovery; `--auto` applies enrichment silently.

```
mdb enrich <aoty|dates|art|tracks|artists|audio|soundtracks|popularity> [--force] [--limit N] [--release-id ID]
```
All enrich commands use `--force` (overwrite) + `--skip`/`--limit`. AOTY requires off-network. `enrich soundtracks` uses Gemini (`GEMINI_API_KEY`). Art priority: Apple Music/Bandcamp (3000px) > Beatport (1400px) > CAA > Spotify.

```
mdb delete <releases|artists> <ID…> [-y] [--purge]
```
Default: soft-unlinks listens (returns them to match queue), shows per-track breakdown, `[Y/n]` prompt. `-y` skips prompt. `--purge` hard-deletes listen rows.

```
mdb release variants | mdb tracks variants [--all] | mdb certs refresh
mdb hide <entity> <csv> | mdb artist images <csv>
```

---

## `sync.py` Commands

```
sync fetch [--parquet FILE] [--sqlite FILE] [--spotify [DIR]] [--since N] [--full]
```
Ingest from Last.fm API, Parquet, old SQLite, or Spotify Extended History JSON. Deduplicates ±120s cross-source.

```
sync match [--limit N] [--skipped] [--recent] [--artist NAME [NAME …] [--interactive]] [--wiki]
```
Interactive matching loop. `--artist` accepts multiple names for non-interactive artist sweep. Prompt: numbers, any source URL, `db:ULID`, `[d]iff`, `[s]kip`, `[h]ide`. Post-import supplementary rematch catches single-named scrobbles (e.g. `"BUZZCUT (feat. X)"` → ROADRUNNER track).

---

## Database Schema

| Table | Key columns |
|-------|-------------|
| `releases` | `type`, `type_secondary`, `release_date`, `date_source`, `release_group_mbid`, `apple_music_id`, `hidden` |
| `tracks` | `mix_name`, `musical_key`, `beatport_genre`, `beatport_sub_genre`, `tempo_bpm`, `canonical_track_id`, `hidden` |
| `track_artists` | `role`: `main` \| `featured` \| `remixer` |
| `release_variants` | `canonical_id` ← `variant_id`; variants hidden from all browse/search views |
| `listens` | `track_id` (NULL until matched), `ms_played` (Spotify), `source` |
| `release_genres` | AOTY only — the sole user-facing genre source |
| `external_links` | services: 0=Wikipedia 1=MusicBrainz 2=Spotify 3=Apple Music 4=Deezer 5=Tidal 6=Bandcamp 7=Beatport |

**Date priority:** `manual(5) > wikipedia(4) > aoty(3) > musicbrainz(2) > spotify(1) = beatport(1)`\
**Wikipedia guards:** skips singles/remixes; rejects dates >10y from stored date; uses Gemini (`GEMINI_API_KEY`) for article disambiguation before keyword search.\
**Single absorption:** ISRC guard skips importing 1-track singles whose ISRC exists on a non-variant album. `_auto_rematch` supplementary pass matches scrobbles where `raw_album` = track title or track title + feat suffix.\
**Conventions:** ULIDs everywhere; `hidden=1` soft-deletes; `mix_name` is advisory (title always complete); `canonical_track_id` no chaining; `\[x]` to escape Rich markup literals.
