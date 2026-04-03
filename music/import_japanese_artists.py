#!/usr/bin/env python3
"""
Definitive import of Japanese game/city-pop artists with hardcoded Spotify IDs.
Upserts each artist, applies canonical name overrides, adds native_script aliases.

Run from music/:  python import_japanese_artists.py
"""

import os, sys, time
sys.path.insert(0, os.path.dirname(__file__))
os.chdir(os.path.dirname(__file__))
from mdb import DB_PATH, SpotifyClient, init_schema, open_db, upsert_artist

from rich.console import Console
console = Console(width=80, highlight=False)

# ── Artist manifest ────────────────────────────────────────────────────────────
# (japanese_name, canonical_name, spotify_id, extra_aliases)
# extra_aliases: list of (alias, alias_type) for anything beyond the native_script
ARTISTS = [
    # ── Already in DB — alias already added in previous session ──────────────
    # ("山岡晃", "Akira Yamaoka", ...) — skipped

    # ── Game / anime composers ────────────────────────────────────────────────
    ("植松伸夫",       "Nobuo Uematsu",       "3V79CTgRnsDdJSTqKitROv", []),
    ("高田雅史",       "Masafumi Takada",     "6u08wRMmY0Myz8HaNbIOQb", []),
    ("増田順一",       "Junichi Masuda",      "4VIuhf1E0e1gwkftD5VSXr", []),
    ("久石譲",         "Joe Hisaishi",        "7nzSoJISlVJsn7O0yTeMOB", []),
    ("下村陽子",       "Yoko Shimomura",      "2uDsUIyCIqk9wKj17I8WAH", []),
    ("宇多田ヒカル",   "Hikaru Utada",        "7lbSsjYACZHn1MSDXPxNF2", []),
    ("近藤浩治",       "Koji Kondo",          "1CRvJnCbPjgx0xmBdoex0c", []),
    ("古代祐三",       "Yuzo Koshiro",        "6Tvw2YNOLrcxATPGnFVTzz", []),
    ("山根ミチル",     "Michiru Yamane",      "6yHzqH70UbRHmoTOcEBmRz", []),
    # Hip Tanaka: Spotify name is "Hirokazu Tanaka"; canonical is "Hip Tanaka"
    ("田中宏和",       "Hip Tanaka",          "6vmDNVkmtWoBw6TnzF9hIw",
                                              [("Hirokazu Tanaka", "common")]),
    ("目黒将司",       "Shoji Meguro",        "6ssIZE2zSXsIOTs1mh8ePq", []),
    ("山下絹代",       "Kinuyo Yamashita",    "1aQZ53vh1tmTROhWsBrJIx", []),
    ("中野順也",       "Junya Nakano",        "6MmPy3kU5brlgFNlQP5ACa", []),
    ("坂本龍一",       "Ryuichi Sakamoto",    "1tcgfoMTT1szjUeaikxRjA", []),
    ("細野晴臣",       "Haruomi Hosono",      "370nbSkMB9kDWyTypwWYak", []),
    ("高橋幸宏",       "Yukihiro Takahashi",  "5Rv28BOArteQRhL8YUYgD5", []),
    # TOWA TEI → normalize casing
    ("テイ・トウワ",   "Towa Tei",            "5FLbE1s9bnHwJhmngtVXpD", []),
    # 光田康典 account has the bulk of followers; rename to romanized form
    ("光田康典",       "Yasunori Mitsuda",    "7cGkvEcOOYVtNdfkf3s1tK", []),
    ("菅野よう子",     "Yoko Kanno",          "0lbYsAt8JNKNjttbncKg8i", []),
    ("岡部啓一",       "Keiichi Okabe",       "0fj8MsM0gSugGUVd9RGV06", []),
    # Sawano Hiroyuki → flip to Western name order
    ("澤野弘之",       "Hiroyuki Sawano",     "0Riv2KnFcLZA3JSVryRg4y", []),
    # Shiro SAGISU → normalize casing
    ("鷺巣詩郎",       "Shiro Sagisu",        "5k3NfhEeZHpouIGDpjKOPo", []),
    ("瀬場潤",         "Nujabes",             "3Rq3YOF9YG9YfCWD4D56RZ", []),
    ("山下達郎",       "Tatsuro Yamashita",   "41hQ0PoEyj9xEBhwt73aWC", []),
    ("竹内まりや",     "Mariya Takeuchi",     "3WwGRA2o4Ux1RRMYaYDh7N", []),
    ("すぎやまこういち","Koichi Sugiyama",    "0JuWnarwRTjiTfY5zOuOfH", []),
    ("桜庭統",         "Motoi Sakuraba",      "1DdhScDGl9AceKnbvgkFgz", []),
    ("阿部隆大",       "Ryudai Abe",          "1jrJkD39owXnVYkDpkKjeu", []),
    # 椎名豪 account → rename to Go Shiina
    ("椎名豪",         "Go Shiina",           "5SwtzFnHzuWGai6tPOHBeH", []),
    ("戸高一生",       "Kazumi Totaka",       "2TlipSeFNg53zqUnBwOcAE", []),
    # 並木学 account → rename to Manabu Namiki
    ("並木学",         "Manabu Namiki",       "1Bd6yTkaogNP1B14vW4Ayv", []),
    ("崎元仁",         "Hitoshi Sakimoto",    "5ogVrEHxkGUuyavOqRapnm", []),
    ("岩垂徳行",       "Noriyuki Iwadare",    "04rLUcC1lxVptIx0qjcVQJ", []),
]

def load_env():
    try:
        with open('.env') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line: continue
                k, _, v = line.partition('=')
                os.environ.setdefault(k.strip(), v.strip().strip('"\''))
    except FileNotFoundError: pass


def main():
    load_env()
    cid = os.environ.get('SPOTIFY_CLIENT_ID')
    csc = os.environ.get('SPOTIFY_CLIENT_SECRET')
    if not cid or not csc:
        console.print('[red]Missing SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET[/red]')
        sys.exit(1)

    client = SpotifyClient(cid, csc)
    conn   = open_db(DB_PATH)
    init_schema(conn)
    now    = int(time.time())

    # Batch-fetch all Spotify artist records (50 per call)
    spotify_ids = [row[2] for row in ARTISTS]
    sp_by_id    = {}
    for i in range(0, len(spotify_ids), 50):
        batch = client.get('/artists', {'ids': ','.join(spotify_ids[i:i+50])})
        for a in batch.get('artists') or []:
            if a:
                sp_by_id[a['id']] = a

    console.print(f'  Fetched {len(sp_by_id)}/{len(spotify_ids)} artists from Spotify\n')

    imported = aliased = name_fixed = skipped = 0

    for japanese, canonical, spotify_id, extra_aliases in ARTISTS:
        console.rule(style='dim')
        sp = sp_by_id.get(spotify_id)
        if not sp:
            console.print(f'  [red]Not found on Spotify:[/red] {canonical} ({spotify_id})')
            skipped += 1
            continue

        cur = conn.cursor()
        artist_id, created = upsert_artist(cur, sp)
        conn.commit()

        sp_name = sp['name']
        status = '[green]created[/green]' if created else '[dim]exists[/dim]'
        console.print(f'  {status}  {sp_name}  [dim]{spotify_id}[/dim]')

        # Apply canonical name override if Spotify name differs
        if sp_name != canonical:
            conn.execute(
                'UPDATE artists SET name = ?, slug = ?, updated_at = ? WHERE id = ?',
                [canonical,
                 # re-slug: slugify the canonical name, unique among existing
                 _unique_slug(conn, canonical, artist_id),
                 now, artist_id]
            )
            conn.commit()
            console.print(f'  [cyan]renamed[/cyan]  "{sp_name}" → "{canonical}"')
            name_fixed += 1

        # Add native_script alias
        if _ensure_alias(conn, artist_id, japanese, 'native_script', 'ja'):
            console.print(f'  [green]alias[/green]  {japanese}  (native_script, ja)')
            aliased += 1
        else:
            console.print(f'  [dim]alias already present: {japanese}[/dim]')

        # Extra aliases (e.g. "Hirokazu Tanaka" common alias for Hip Tanaka)
        for alias_text, alias_type in extra_aliases:
            if _ensure_alias(conn, artist_id, alias_text, alias_type, None):
                console.print(f'  [green]alias[/green]  {alias_text}  ({alias_type})')
                aliased += 1

        if created:
            imported += 1

    conn.close()
    console.rule(style='dim')
    console.print(
        f'  Done — [green]{imported}[/green] created · '
        f'[cyan]{name_fixed}[/cyan] renamed · '
        f'[green]{aliased}[/green] aliases added · '
        f'[yellow]{skipped}[/yellow] skipped'
    )


def _ensure_alias(conn, artist_id, alias, alias_type, language):
    existing = conn.execute(
        'SELECT 1 FROM artist_aliases WHERE artist_id = ? AND alias = ?',
        [artist_id, alias]
    ).fetchone()
    if existing:
        return False
    conn.execute(
        '''INSERT INTO artist_aliases (artist_id, alias, alias_type, language, source, sort_order)
           VALUES (?, ?, ?, ?, 'manual', 0)''',
        [artist_id, alias, alias_type, language]
    )
    conn.commit()
    return True


def _unique_slug(conn, name, artist_id):
    """Slugify name, avoiding collisions with other artists."""
    import unicodedata, re
    def slugify(s):
        s = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode()
        s = re.sub(r'[^\w\s-]', '', s).strip().lower()
        return re.sub(r'[-\s]+', '-', s)

    base     = slugify(name)
    existing = {r[0] for r in conn.execute(
        'SELECT slug FROM artists WHERE slug IS NOT NULL AND id != ?', [artist_id]
    ).fetchall()}
    slug = base
    n    = 2
    while slug in existing:
        slug = f'{base}-{n}'
        n += 1
    return slug


if __name__ == '__main__':
    main()
