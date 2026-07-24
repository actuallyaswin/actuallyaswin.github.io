#!/opt/homebrew/opt/python@3.11/libexec/bin/python
"""Generate music/genre-tree.js from the genres/genre_relations tables.

AOTY's genre taxonomy is fixed editorial data — it almost never changes,
unlike listens/tracks which update on every sync. Baking it into a static
JS file avoids a WITH RECURSIVE SQL query on every genre page load, at the
cost of needing a manual re-run whenever the genre taxonomy itself changes
(new genre added, a parent/child relation edited).

Usage: ./generate_genre_tree.py
"""

import json
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "master.sqlite"
OUT_PATH = Path(__file__).parent / "genre-tree.js"


def main():
    conn = sqlite3.connect(DB_PATH)
    genres = {
        str(aoty_id): {"name": name, "parents": [], "children": []}
        for aoty_id, name in conn.execute(
            "SELECT aoty_id, name FROM genres ORDER BY aoty_id"
        )
    }

    for parent_id, child_id in conn.execute(
        "SELECT parent_aoty_id, child_aoty_id FROM genre_relations"
    ):
        genres[str(child_id)]["parents"].append(parent_id)
        genres[str(parent_id)]["children"].append(child_id)

    conn.close()

    for node in genres.values():
        node["parents"].sort()
        node["children"].sort()

    js = (
        "// Static genre hierarchy — generated from genres/genre_relations by\n"
        "// generate_genre_tree.py. AOTY's genre taxonomy rarely changes; re-run\n"
        "// that script if a genre or parent/child relation is added or edited.\n"
        "const GENRE_TREE = "
        + json.dumps(genres, separators=(",", ":"))
        + ";\n"
    )
    OUT_PATH.write_text(js)
    print(f"Wrote {OUT_PATH} ({len(genres)} genres)")


if __name__ == "__main__":
    main()
