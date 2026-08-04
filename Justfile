# Run `just` with no arguments to list all recipes.
default:
    @just --list

# Serve the Jekyll site locally at http://localhost:4000 (blog + music SPA).
serve:
    #!/usr/bin/env bash
    set -euo pipefail
    pid=$(lsof -nP -tiTCP:4000 -sTCP:LISTEN || true)
    if [ -n "$pid" ]; then
        cmd=$(ps -p "$pid" -o comm= 2>/dev/null || echo "unknown")
        echo "Port 4000 is already in use by PID $pid ($cmd)."
        read -r -p "Kill it and continue? [y/N] " answer
        if [[ "$answer" =~ ^[Yy]$ ]]; then
            kill "$pid"
            sleep 1
        else
            echo "Leaving it running — aborting."
            exit 1
        fi
    fi
    bundle exec jekyll serve

# Same as `serve`, but rebuilds on file changes.
watch:
    #!/usr/bin/env bash
    set -euo pipefail
    pid=$(lsof -nP -tiTCP:4000 -sTCP:LISTEN || true)
    if [ -n "$pid" ]; then
        cmd=$(ps -p "$pid" -o comm= 2>/dev/null || echo "unknown")
        echo "Port 4000 is already in use by PID $pid ($cmd)."
        read -r -p "Kill it and continue? [y/N] " answer
        if [[ "$answer" =~ ^[Yy]$ ]]; then
            kill "$pid"
            sleep 1
        else
            echo "Leaving it running — aborting."
            exit 1
        fi
    fi
    bundle exec jekyll serve --livereload

# Build the static site into _site/, exactly as CI does.
build:
    bundle exec jekyll build

# Build into a scratch dir and verify against the *committed* state (HEAD),
# not the working tree — mirrors what GitHub Actions actually deploys.
# Requires a clean working tree state to be meaningful; uses a throwaway worktree.
verify-build:
    #!/usr/bin/env bash
    set -euo pipefail
    tmp_worktree=$(mktemp -d)
    trap 'git worktree remove "$tmp_worktree" --force 2>/dev/null; rm -rf "$tmp_worktree"' EXIT
    git worktree add "$tmp_worktree" HEAD
    (cd "$tmp_worktree" && bundle exec jekyll build --destination /tmp/_site_verify)
    echo "Build OK — output at /tmp/_site_verify"

# Checkpoint the music DB: push master.sqlite to Turso (the durable off-repo
# copy), then regenerate master_prod.sqlite.gz (the stripped copy the SPA
# fetches) and rebuild _site/ so the served copy never goes
# stale relative to it. A stale or truncated _site/ copy produces a cryptic
# sql.js "Extra bytes past the end" error in the browser with no other
# symptom. Run this after any music/master.sqlite write before the change is
# visible on the frontend (dev server or deployed site).
db-checkpoint:
    cd music && {{mdb_python}} mdb.py certs refresh
    cd music && {{mdb_python}} mdb.py stats refresh
    cd music && sqlite3 master.sqlite "PRAGMA wal_checkpoint(TRUNCATE);"
    cd music && {{mdb_python}} turso_push.py
    cd music && {{mdb_python}} make_prod_db.py
    cd music && gzip -k -f -9 master_prod.sqlite
    bundle exec jekyll build --destination _site
    @echo "Checkpointed to Turso, regenerated music/master_prod.sqlite.gz, and rebuilt _site/"

# Open an interactive sqlite3 shell on the music DB.
db-shell:
    sqlite3 music/master.sqlite

mdb_python := `which python3`

# Fetch new Last.fm scrobbles. Always checkpoints the DB afterward (even on
# quit/Ctrl-C) so master_prod.sqlite.gz never goes stale relative to the SPA.
music-fetch *ARGS:
    -cd music && {{mdb_python}} sync.py fetch {{ARGS}}
    @just db-checkpoint

# Interactively match unresolved scrobbles to tracks. Always checkpoints the
# DB afterward (even on quit/Ctrl-C) so master_prod.sqlite.gz never goes stale
# relative to the SPA.
music-match *ARGS:
    -cd music && {{mdb_python}} sync.py match {{ARGS}}
    @just db-checkpoint

# Run the mdb CLI, e.g. `just mdb import <url>`.
mdb *ARGS:
    cd music && {{mdb_python}} mdb.py {{ARGS}}

# Clean Jekyll build artifacts.
clean:
    bundle exec jekyll clean
