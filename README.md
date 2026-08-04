# actuallyaswin.github.io

![Ruby](https://img.shields.io/badge/ruby-%3E%3D3.3-CC342D?logo=ruby&logoColor=white)
![Jekyll](https://img.shields.io/badge/jekyll-%7E%3E4.3-red?logo=jekyll&logoColor=white)
![Python](https://img.shields.io/badge/python-3-3776AB?logo=python&logoColor=white)
![just](https://img.shields.io/badge/just-%3E%3D1.0-black?logo=just&logoColor=white)
![SQLite](https://img.shields.io/badge/sqlite3-%3E%3D3.35-003B57?logo=sqlite&logoColor=white)

Personal site: Jekyll blog + a static music-listening-history SPA under `music/`.

The working database, `music/master.sqlite`, is local-only (gitignored) and
mirrored to Turso by `music/turso_push.py`. What the SPA actually fetches is
`music/master_prod.sqlite.gz`, a stripped copy built by `music/make_prod_db.py`
and committed to the repo.

## Prerequisites

| Tool | Minimum version | Why |
|---|---|---|
| Ruby | >= 3.3 | Matches the version pinned in `.github/workflows/deploy.yml`. Build locally with anything older and CI may still diverge. |
| Bundler | >= 4.0 (see `Gemfile.lock`) | `bundle install` / `bundle exec jekyll ...` |
| Jekyll | ~> 4.3 (see `Gemfile`) | Installed via Bundler, not standalone. |
| [`just`](https://github.com/casey/just) | any recent 1.x (`brew install just`) | Runs the recipes below. |
| `sqlite3` CLI | >= 3.35 (for `PRAGMA wal_checkpoint`; preinstalled on macOS is fine) | `just db-checkpoint` / `just db-shell`. |
| Python | any actively-supported 3.x | Runs `music/`'s CLI toolchain (`mdb.py`, `sync.py`). Install deps with `python3 -m pip install -r music/requirements.txt`. |

CI additionally runs `ruff` and `pytest` over `music/`, and `node --check` over
every JS file. To run those locally, install `music/requirements-dev.txt`.

## Commands

Run `just` with no arguments to list all recipes. Full list:

| Command | What it does |
|---|---|
| `just serve` | Serves the Jekyll site locally at `http://localhost:4000` (blog + music SPA). Offers to kill whatever already holds port 4000. |
| `just watch` | Same as `serve`, but rebuilds automatically on file changes (`--livereload`). |
| `just build` | Builds the static site into `_site/`, exactly as CI does. |
| `just verify-build` | Builds from a throwaway `git worktree` checked out at `HEAD` rather than your working tree, so you can confirm a push will build *before* pushing. Output goes to `/tmp/_site_verify`. |
| `just db-checkpoint` | Refreshes certs and stats, checkpoints the WAL, pushes to Turso, rebuilds `music/master_prod.sqlite.gz`, then rebuilds `_site/`. Run this after any write to `music/master.sqlite`: the frontend only ever fetches the `.gz`, so changes are invisible until it runs. |
| `just db-shell` | Opens an interactive `sqlite3` shell on `music/master.sqlite`. |
| `just mdb <args>` | Runs the `music/mdb.py` CLI (import releases, enrich metadata, manage artists/releases/tracks, etc.), e.g. `just mdb import <url>`. |
| `just music-fetch <args>` | Runs `music/sync.py fetch` to pull new Last.fm scrobbles, then checkpoints the DB. |
| `just music-match <args>` | Runs `music/sync.py match` to interactively match unresolved scrobbles to tracks, then checkpoints the DB. |
| `just clean` | Removes Jekyll build artifacts (`bundle exec jekyll clean`). |
