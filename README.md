# actuallyaswin.github.io

![Ruby](https://img.shields.io/badge/ruby-%3E%3D3.3-CC342D?logo=ruby&logoColor=white)
![Jekyll](https://img.shields.io/badge/jekyll-%7E%3E4.3-red?logo=jekyll&logoColor=white)
![Python](https://img.shields.io/badge/python-3-3776AB?logo=python&logoColor=white)
![just](https://img.shields.io/badge/just-%3E%3D1.0-black?logo=just&logoColor=white)
![Git LFS](https://img.shields.io/badge/git--lfs-required-orange?logo=git&logoColor=white)
![SQLite](https://img.shields.io/badge/sqlite3-%3E%3D3.35-003B57?logo=sqlite&logoColor=white)

Personal site: Jekyll blog + a static music-listening-history SPA under `music/`,
backed by a SQLite database (`music/master.sqlite`, tracked via Git LFS) that's
fetched client-side as `music/master.sqlite.gz`.

## Prerequisites

| Tool | Minimum version | Why |
|---|---|---|
| Ruby | >= 3.3 | Matches the version pinned in `.github/workflows/deploy.yml` — build locally with anything older and CI may still diverge. |
| Bundler | >= 4.0 (see `Gemfile.lock`) | `bundle install` / `bundle exec jekyll ...` |
| Jekyll | ~> 4.3 (see `Gemfile`) | Installed via Bundler, not standalone. |
| [`just`](https://github.com/casey/just) | any recent 1.x (`brew install just`) | Runs the recipes below. |
| Git LFS | any recent version (`brew install git-lfs`) | `music/master.sqlite` is tracked via LFS — clone/pull without it and you get a pointer file, not the real DB. |
| `sqlite3` CLI | >= 3.35 (for `PRAGMA wal_checkpoint`; preinstalled on macOS is fine) | `just db-checkpoint` / `just db-shell`. |
| Python | any actively-supported 3.x | Runs `music/`'s CLI toolchain (`mdb.py`, `sync.py`). Install deps with `python3 -m pip install -r music/requirements.txt`. |

## Commands

Run `just` with no arguments to list all recipes. Full list:

| Command | What it does |
|---|---|
| `just serve` | Serves the Jekyll site locally at `http://localhost:4000` (blog + music SPA). |
| `just watch` | Same as `serve`, but rebuilds automatically on file changes (`--livereload`). |
| `just build` | Builds the static site into `_site/`, exactly as CI does. |
| `just verify-build` | Builds a throwaway `git worktree` checked out at `HEAD` (not your working tree) and runs `git lfs pull` first — mirrors exactly what the GitHub Actions deploy workflow builds, so you can confirm a push will succeed *before* pushing. |
| `just db-checkpoint` | Runs `PRAGMA wal_checkpoint(TRUNCATE)` on `music/master.sqlite` and regenerates `music/master.sqlite.gz`. Run this after any write to the DB — the frontend only ever fetches the `.gz`, so changes are invisible until this runs. |
| `just db-shell` | Opens an interactive `sqlite3` shell on `music/master.sqlite`. |
| `just mdb <args>` | Runs the `music/mdb.py` CLI (import releases, enrich metadata, manage artists/releases/tracks, etc.), e.g. `just mdb import <url>`. |
| `just music-fetch <args>` | Runs `music/sync.py fetch` to pull new Last.fm scrobbles. |
| `just music-match <args>` | Runs `music/sync.py match` to interactively match unresolved scrobbles to tracks. |
| `just clean` | Removes Jekyll build artifacts (`bundle exec jekyll clean`). |
