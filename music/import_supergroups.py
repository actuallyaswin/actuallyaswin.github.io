#!/usr/bin/env python3
"""
One-time import: populate supergroup artists + artist_members relationships.

Each entry has a Spotify ID for the group and for each member.
Members already in the DB are matched via spotify_id; others are upserted.

Run from music/:  python import_supergroups.py
"""

import os, sys, time, sqlite3
sys.path.insert(0, os.path.dirname(__file__))
os.chdir(os.path.dirname(__file__))
from mdb import DB_PATH, SpotifyClient, init_schema, open_db, upsert_artist, load_dotenv

from rich.console import Console
console = Console(width=80, highlight=False)

# ── Supergroup manifest ────────────────────────────────────────────────────────
# Each entry: (group_spotify_id, canonical_group_name, [(member_spotify_id, member_display_name), ...])
# Entries in SUPERGROUPS get is_supergroup=1; entries in BANDS get is_supergroup=0.
# canonical_group_name: only used if it differs from Spotify's stored name
# member_display_name: informational only (actual name comes from Spotify)

SUPERGROUPS = [  # is_supergroup = 1
    # ── Hip-hop ───────────────────────────────────────────────────────────────
    (
        '6PvvGcCY2XtUcSRld1Wilr', 'Silk Sonic',
        [
            ('0du5cEVh5yTK9QJze8zA0C', 'Bruno Mars'),
            ('3jK9MiCrA42lLAdMGUZpwa', 'Anderson .Paak'),
        ]
    ),
    (
        '4fpTMHe34LC5t3h5ztK8qu', 'The Carters',
        [
            ('6vWDO969PvNqNYHIOW5v0m', 'Beyoncé'),
            ('3nFkdlSjzX9mRTtwJOzDYB', 'Jay-Z'),
        ]
    ),
    (
        '4xPQFgDA5M2xa0ZGo5iIsv', '¥$',
        [
            ('3NlsBPwqJuDgtXZ2rv5Dmq', 'Ye'),
            ('7c0XG5cIJTrrAgEC3ULPiq', 'Ty Dolla $ign'),
        ]
    ),
    (
        '3DELNHPLdJgXkDHOTt3ok8', 'Mount Westmore',
        [
            ('7hJcb9fa4alzcOq3EaNPoG', 'Snoop Dogg'),
            ('3Mcii5XWf6E0lrY3Uky4cA', 'Ice Cube'),
            ('3crnzLy8R4lVwaigKEOz7V', 'E-40'),
            ('4sb7rZNN93BSS6Gqgepo4v', 'Too $hort'),
        ]
    ),
    (
        '2aoFQUeHD1U7pL098lRsDU', 'Madvillain',
        [
            ('2pAWfrd7WFF3XhVt9GooDL', 'MF DOOM'),
            ('5LhTec3c7dcqBvpLRWbMcf', 'Madlib'),
        ]
    ),
    (
        '7a9KRWdaSZktpGGnWndzbC', 'Black Hippy',
        [
            ('2YZyLoL8N0Wb9xBt1NhZWg', 'Kendrick Lamar'),
            ('28ExwzUQsvgJooOI0X1mr3', 'Jay Rock'),
            ('5IcR3N7QB1j6KBL8eImZ8m', 'ScHoolboy Q'),
            ('0g9vAlRPK9Gt3FKCekk4TW', 'Ab-Soul'),
        ]
    ),
    # ── Electronic ────────────────────────────────────────────────────────────
    (
        '1HxJeLhIuegM3KgvPn8sTa', 'Jack Ü',
        [
            ('5he5w2lnU9x7JFhnwcekXX', 'Skrillex'),
            ('5fMUXHkw8R8eOP2RNVYEZX', 'Diplo'),
        ]
    ),
    (
        '2X97ZAqRKRMYFIDqtvGgGc', 'Silk City',
        [
            ('3hv9jJF3adDNsBSIQDqcjp', 'Mark Ronson'),
            ('5fMUXHkw8R8eOP2RNVYEZX', 'Diplo'),
        ]
    ),
    (
        '6IZ4ctovY9dl7bgHClAvKJ', 'LSD',
        [
            ('2feDdbD5araYcm6JhFHHw7', 'Labrinth'),
            ('5WUlDfRSoLAfcVSX1WnrxN', 'Sia'),
            ('5fMUXHkw8R8eOP2RNVYEZX', 'Diplo'),
        ]
    ),
    (
        '1h6Cn3P4NGzXbaXidqURXs', 'Swedish House Mafia',
        [
            ('1xNmvlEiICkRlRGqlNFZ43', 'Axwell'),
            ('4FqPRilb0Ja0TKG3RS3y4s', 'Steve Angello'),
            ('6hyMWrxGBsOx6sWcVj1DqP', 'Sebastian Ingrosso'),
        ]
    ),
    (
        '5yV1qdnmxyIYiSFB02wpDj', 'The Postal Service',
        [
            ('4CvZd3qzC2HbLxAoAEBRIL', 'Ben Gibbard'),
            ('2qsH5gO7JnshW9X4hUCfvU', 'Jimmy Tamborello'),
        ]
    ),
    # ── Indie / rock ──────────────────────────────────────────────────────────
    (
        '1hLiboQ98IQWhpKeP9vRFw', 'boygenius',
        [
            ('12zbUHbPHL5DGuJtiUfsip', 'Julien Baker'),
            ('1r1uxoy19fzMxunt3ONAkG', 'Phoebe Bridgers'),
            ('07D1Bjaof0NFlU32KXiqUP', 'Lucy Dacus'),
        ]
    ),
    (
        '2ziB7fzrXBoh1HUPS6sVFn', 'Audioslave',
        [
            ('0XHiH53dHrvbwfjYM7en7I', 'Chris Cornell'),
            ('74NBPbyyftqJ4SpDZ4c1Ed', 'Tom Morello'),
            ('1SIJJWKJcU7Eg2sW5QuQOf', 'Tim Commerford'),
            ('1kQiJ0OIEzwr1oVCSZ1Y6o', 'Brad Wilk'),
        ]
    ),
    (
        '4zYQWYmtimAEmI6WWEzGfO', 'Them Crooked Vultures',
        [
            ('03xb2BUdIFzuRQ6o88yfCB', 'Josh Homme'),
            ('7mRVAzlt1fAAR9Cut6Rq8c', 'Dave Grohl'),
            ('6RhcZuUOb20IZvR8BbdnJX', 'John Paul Jones'),
        ]
    ),
    (
        '7CHilrn81OdYjkh4uSVnYM', 'Velvet Revolver',
        [
            ('0RMOWaq3zw0fdgvaGRMcdA', 'Scott Weiland'),
            ('4Cqia9vrAbm7ANXbJGXsTE', 'Slash'),
            ('3KEe5d2p5jKihMMvuXVhr1', 'Duff McKagan'),
            ('1icjlI6iYtR1JjXTJLf4gG', 'Matt Sorum'),
            ('5gAm0wZrGQJOpQuq5b8ul6', 'Dave Kushner'),
        ]
    ),
    # ── Classic rock ──────────────────────────────────────────────────────────
    (
        '74oJ4qxwOZvX6oSsu1DGnw', 'Cream',
        [
            ('6PAt558ZEZl0DmdXlnjMgD', 'Eric Clapton'),
            ('73ndLgs6jSrpZzjyzU9TJV', 'Jack Bruce'),
            ('5xTbqEbkihxdjj2jyYSthw', 'Ginger Baker'),
        ]
    ),
    (
        '2hO4YtXUFJiUYS2uYFvHNK', 'Traveling Wilburys',
        [
            ('7FIoB5PHdrMZVC3q2HE5MS', 'George Harrison'),
            ('74ASZWbe4lXaubB36ztrGX', 'Bob Dylan'),
            ('2UZMlIwnkgAEDBsw1Rejkn', 'Tom Petty'),
            ('3bTAaMx9nf237AkBnGw3vL', 'Jeff Lynne'),
            ('0JDkhL4rjiPNEp92jAgJnS', 'Roy Orbison'),
        ]
    ),
    (
        '1CYsQCypByMVgnv17qsSbQ', 'Crosby, Stills, Nash & Young',
        [
            ('59zdhVoWxSoHMc74n098Re', 'David Crosby'),
            ('4WlSvDKaq1PA2Nr7cCIPxX', 'Stephen Stills'),
            ('2E6Roj0oQnJIm2BeXwDica', 'Graham Nash'),
            ('6v8FB84lnmJs434UJf2Mrm', 'Neil Young'),
        ]
    ),
    (
        '3D2hT0opR7rrYIngiAdwgC', 'SuperHeavy',
        [
            ('3d2pb1dHTm8b61zAGVUVvO', 'Mick Jagger'),
            ('7bvcQXJHkFiN1ppIN3q4fi', 'Joss Stone'),
            ('7gcCQIlkkfbul5Mt0jBQkg', 'Dave Stewart'),
            ('3QJzdZJYIAcoET1GcfpNGi', 'Damian Marley'),
            ('1mYsTxnqsietFxj1OgoGbG', 'A.R. Rahman'),
        ]
    ),
    # ── Country ───────────────────────────────────────────────────────────────
    (
        '6e7QpHYqEiyJGiM98IysLa', 'The Highwaymen',
        [
            ('6kACVPfCOnqzgfEF5ryl0x', 'Johnny Cash'),
            ('5W5bDNCqJ1jbCgTxDD0Cb3', 'Willie Nelson'),
            ('7wCjDgV6nqBsHguQXPAaIM', 'Waylon Jennings'),
            ('0vYQRW5LIDeYQOccTviQNX', 'Kris Kristofferson'),
        ]
    ),
    (
        '3FjOdEflyH6wWrmTfj4xUo', 'Trio',
        [
            ('32vWCbZh0xZ4o9gkz4PsEU', 'Dolly Parton'),
            ('1sXbwvCQLGZnaH0Jp2HTVc', 'Linda Ronstadt'),
            ('5s6TJEuHTr9GR894wc6VfP', 'Emmylou Harris'),
        ]
    ),
    # ── More supergroups ──────────────────────────────────────────────────────
    (
        '0iHb0mCbqZTYeb4y9Pirrd', 'Temple of the Dog',
        [
            ('0XHiH53dHrvbwfjYM7en7I', 'Chris Cornell'),
            ('3WQx0LWkYh95zn8McSjbJh', 'Jeff Ament'),
            ('4NfvOU2TMtQhyBOW0erSDf', 'Matt Cameron'),
            ('6AaWik9LKRViQFnIK2PSI9', 'Stone Gossard'),
            ('7njqqUBXHc5fpyXmUlfOUL', 'Mike McCready'),
            ('0mXTJETA4XUa12MmmXxZJh', 'Eddie Vedder'),
        ]
    ),
    (
        '2avRYQUWQpIkzJOEkf0MdY', 'Kx5',
        [
            ('2CIMQHirSU0MQqyYHq0eOx', 'deadmau5'),
            ('6TQj5BFPooTa08A7pk8AQ1', 'Kaskade'),
        ]
    ),
    (
        '3iyG1duuxWpcuWa57VSeZ0', 'The Highwomen',
        [
            ('2sG4zTOLvjKG1PSoOyf5Ej', 'Brandi Carlile'),
            ('5yN0nwLpUCaZ2gr67bndCN', 'Amanda Shires'),
            ('6WY7D3jk8zTrHtmkqqo5GI', 'Maren Morris'),
            ('32opPqLCT3sF24Aso7wTXw', 'Natalie Hemby'),
        ]
    ),
    (
        '5SbkVQYYzlw1kte75QIabH', 'Gnarls Barkley',
        [
            ('5nLYd9ST4Cnwy6NHaCxbj8', 'CeeLo Green'),
            ('2dBj3prW7gP9bCCOIQeDUf', 'Danger Mouse'),
        ]
    ),
    (
        '6iy8nrBbtL57i4eUttHTww', 'The Good, the Bad & the Queen',
        [
            ('0O98jlCaPzvsoei6U5jfEL', 'Damon Albarn'),
            ('6JpZEemWmunccsrHXFUOgi', 'Tony Allen'),
            ('62bYKAZ5EdmG5Aca9dtVan', 'Paul Simonon'),
            ('2dBj3prW7gP9bCCOIQeDUf', 'Danger Mouse'),
            # Simon Tong — no Spotify artist page
        ]
    ),
    (
        '6styCzc1Ej4NxISL0LiigM', 'The Smile',
        [
            ('4CvTDPKA6W06DRfBnZKrau', 'Thom Yorke'),
            ('0z9s3P5vCzKcUBSxgBDyLU', 'Jonny Greenwood'),
            ('6WUuwGEgtKowXDyQtfB8S7', 'Tom Skinner'),
        ]
    ),
    (
        '5bIZrkukT53Jqrc4Vl4dvI', 'Metafive',
        [
            ('5Rv28BOArteQRhL8YUYgD5', 'Yukihiro Takahashi'),
            ('5FLbE1s9bnHwJhmngtVXpD', 'Towa Tei'),
            ('2vJObElaIZWYDLpiXiJMo9', 'Cornelius'),
            ('1WimZWJ79dwC7jNGCFyJs2', 'Yoshinori Sunahara'),
            ('03fLIoIkJhDNoM38INcXUO', 'Tomohiko Gondo'),
            ('052wDw54ZAwrWQraLblXZb', 'Leo Imai'),
        ]
    ),
    (
        '7jy3rLJdDQY21OgRLCZ9sD', 'Foo Fighters',
        [
            ('7mRVAzlt1fAAR9Cut6Rq8c', 'Dave Grohl'),
            ('388Eu6HFpzWQ0XYvfl2RnM', 'Nate Mendel'),
            ('36AOO7vOYRSjm2nVgvu63E', 'Pat Smear'),
            ('5tv5SsSRqR7uLtpKZgcRrg', 'Chris Shiflett'),
            ('4dqtxWwMcibGF4JHEqSIgI', 'Rami Jaffee'),
            ('11jeKM5q8nnYr1tssNmGVO', 'Ilan Rubin'),
        ]
    ),
    # ── Band memberships ──────────────────────────────────────────────────────
    # These are regular bands, listed here to record member relationships that
    # came up in the supergroup context above.
]

BANDS = [  # is_supergroup = 0
    (
        '5xUf6j4upBrXZPg6AI4MRK', 'Soundgarden',
        [
            ('0XHiH53dHrvbwfjYM7en7I', 'Chris Cornell'),
            ('4NfvOU2TMtQhyBOW0erSDf', 'Matt Cameron'),
        ]
    ),
    (
        '1w5Kfo2jwwIPruYS2UWh56', 'Pearl Jam',
        [
            ('0mXTJETA4XUa12MmmXxZJh', 'Eddie Vedder'),
            ('3WQx0LWkYh95zn8McSjbJh', 'Jeff Ament'),
            ('6AaWik9LKRViQFnIK2PSI9', 'Stone Gossard'),
            ('7njqqUBXHc5fpyXmUlfOUL', 'Mike McCready'),
            ('4NfvOU2TMtQhyBOW0erSDf', 'Matt Cameron'),
        ]
    ),
    (
        '3XR64HmFo4OvexUUNW7TP0', 'Mother Love Bone',
        [
            ('3WQx0LWkYh95zn8McSjbJh', 'Jeff Ament'),
            ('6AaWik9LKRViQFnIK2PSI9', 'Stone Gossard'),
        ]
    ),
    (
        '1XIIxzmo6BNRR4QkImSdsX', 'Green River',
        [
            ('3WQx0LWkYh95zn8McSjbJh', 'Jeff Ament'),
            ('6AaWik9LKRViQFnIK2PSI9', 'Stone Gossard'),
        ]
    ),
    (
        '738wLrAtLtCtFOLvQBXOXp', 'Major Lazer',
        [
            ('5fMUXHkw8R8eOP2RNVYEZX', 'Diplo'),
        ]
    ),
    (
        '3AA28KZvwAUcZuOKwyblJQ', 'Gorillaz',
        [
            ('0O98jlCaPzvsoei6U5jfEL', 'Damon Albarn'),
        ]
    ),
    (
        '7MhMgCo0Bl0Kukl93PZbYS', 'Blur',
        [
            ('0O98jlCaPzvsoei6U5jfEL', 'Damon Albarn'),
        ]
    ),
    (
        '4Z8W4fKeB5YxbusRsdQVPb', 'Radiohead',
        [
            ('4CvTDPKA6W06DRfBnZKrau', 'Thom Yorke'),
            ('0z9s3P5vCzKcUBSxgBDyLU', 'Jonny Greenwood'),
        ]
    ),
    (
        '7tA9Eeeb68kkiG9Nrvuzmi', 'Atoms For Peace',
        [
            ('4CvTDPKA6W06DRfBnZKrau', 'Thom Yorke'),
        ]
    ),
    (
        '3pvRbmrqOyFxB2Eext4Dki', 'Sons Of Kemet',
        [
            ('6WUuwGEgtKowXDyQtfB8S7', 'Tom Skinner'),
        ]
    ),
    (
        '0LWlgth3CFLC6eD8mtWCOA', 'Melt Yourself Down',
        [
            ('6WUuwGEgtKowXDyQtfB8S7', 'Tom Skinner'),
        ]
    ),
    (
        '2JIf5JxI3ypOSfrfNIIMQE', 'Yellow Magic Orchestra',
        [
            ('5Rv28BOArteQRhL8YUYgD5', 'Yukihiro Takahashi'),
        ]
    ),
    (
        '4eQJIXFEujzhTVVS1gIfu5', 'Deee-Lite',
        [
            ('5FLbE1s9bnHwJhmngtVXpD', 'Towa Tei'),
        ]
    ),
    (
        '5fkHPXDNaqz3GrYYhO8APB', "Flipper's Guitar",
        [
            ('2vJObElaIZWYDLpiXiJMo9', 'Cornelius'),
        ]
    ),
    (
        '3JByu9VCNA1Rs6puGfRupj', 'Denki Groove',
        [
            ('1WimZWJ79dwC7jNGCFyJs2', 'Yoshinori Sunahara'),
        ]
    ),
    (
        '0wIhCBrT02x0GG5bKqcSAh', 'Scream',
        [
            ('7mRVAzlt1fAAR9Cut6Rq8c', 'Dave Grohl'),
        ]
    ),
    (
        '6olE6TJLqED3rqDCT0FyPh', 'Nirvana',
        [
            ('7mRVAzlt1fAAR9Cut6Rq8c', 'Dave Grohl'),
            ('36AOO7vOYRSjm2nVgvu63E', 'Pat Smear'),
        ]
    ),
    (
        '2lZkXWxkZsZzBocxMjN1or', 'Sunny Day Real Estate',
        [
            ('388Eu6HFpzWQ0XYvfl2RnM', 'Nate Mendel'),
        ]
    ),
    (
        '7CVHpcuVERzqAJcP2ddoFy', 'The Fire Theft',
        [
            ('388Eu6HFpzWQ0XYvfl2RnM', 'Nate Mendel'),
        ]
    ),
    (
        '39zgKjGWsiZzJ9h6gbrPFY', 'The Germs',
        [
            ('36AOO7vOYRSjm2nVgvu63E', 'Pat Smear'),
        ]
    ),
    (
        '5p3WimI9yquAF6Lqhlm4Ol', 'No Use for a Name',
        [
            ('5tv5SsSRqR7uLtpKZgcRrg', 'Chris Shiflett'),
        ]
    ),
    (
        '0cOVRC8EOwDwXrs3JTrRN5', 'Me First and the Gimme Gimmes',
        [
            ('5tv5SsSRqR7uLtpKZgcRrg', 'Chris Shiflett'),
        ]
    ),
    (
        '0jJNGWrpjGIHUdTTJiIYeB', 'The Wallflowers',
        [
            ('4dqtxWwMcibGF4JHEqSIgI', 'Rami Jaffee'),
        ]
    ),
    (
        '5l2EAkfckNPYZbEDbQtEkO', 'Pete Yorn',
        [
            ('4dqtxWwMcibGF4JHEqSIgI', 'Rami Jaffee'),
        ]
    ),
    (
        '7xklw3WodFZiNNmQt3DIgp', 'Angels & Airwaves',
        [
            ('11jeKM5q8nnYr1tssNmGVO', 'Ilan Rubin'),
        ]
    ),
]


def main():
    load_dotenv()
    cid = os.environ.get('SPOTIFY_CLIENT_ID')
    csc = os.environ.get('SPOTIFY_CLIENT_SECRET')
    if not cid or not csc:
        console.print('[red]Missing SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET[/red]')
        sys.exit(1)

    client = SpotifyClient(cid, csc)
    conn   = open_db(DB_PATH)
    init_schema(conn)

    # Collect all unique Spotify IDs to fetch
    all_ids = set()
    for group_sid, _, members in SUPERGROUPS + BANDS:
        all_ids.add(group_sid)
        for member_sid, _ in members:
            all_ids.add(member_sid)

    all_ids = list(all_ids)
    sp_by_id = {}
    for i in range(0, len(all_ids), 50):
        batch = client.get('/artists', {'ids': ','.join(all_ids[i:i+50])})
        for a in batch.get('artists') or []:
            if a:
                sp_by_id[a['id']] = a

    console.print(f'  Fetched [green]{len(sp_by_id)}[/green] / {len(all_ids)} artists from Spotify\n')

    groups_created = groups_exists = members_created = members_exists = links_added = links_exists = groups_skipped = 0

    for is_sg, manifest in [(True, SUPERGROUPS), (False, BANDS)]:
        for group_sid, canonical_name, members in manifest:
            console.rule(style='dim')

            sp_group = sp_by_id.get(group_sid)
            if not sp_group:
                console.print(f'  [red]Group not found on Spotify:[/red] {canonical_name} ({group_sid})')
                groups_skipped += 1
                continue

            cur = conn.cursor()
            group_db_id, created = upsert_artist(cur, sp_group)
            conn.commit()

            sp_name = sp_group['name']
            status = '[green]created[/green]' if created else '[dim]exists[/dim]'
            console.print(f'  GROUP  {status}  {sp_name}  [dim]{group_sid}[/dim]')

            if created:
                groups_created += 1
            else:
                groups_exists += 1

            # Stamp is_supergroup
            conn.execute(
                'UPDATE artists SET is_supergroup = ? WHERE id = ?',
                [int(is_sg), group_db_id]
            )
            conn.commit()

            # Apply canonical name override if Spotify name differs
            if sp_name != canonical_name:
                now = int(time.time())
                conn.execute(
                    'UPDATE artists SET name = ?, updated_at = ? WHERE id = ?',
                    [canonical_name, now, group_db_id]
                )
                conn.commit()
                console.print(f'  [cyan]renamed[/cyan]  "{sp_name}" → "{canonical_name}"')

            # Process each member
            for sort_order, (member_sid, member_display) in enumerate(members):
                sp_member = sp_by_id.get(member_sid)
                if not sp_member:
                    console.print(f'    [yellow]member not found on Spotify:[/yellow] {member_display} ({member_sid})')
                    continue

                cur2 = conn.cursor()
                member_db_id, m_created = upsert_artist(cur2, sp_member)
                conn.commit()

                m_status = '[green]created[/green]' if m_created else '[dim]exists[/dim]'
                console.print(f'    member  {m_status}  {sp_member["name"]}')

                if m_created:
                    members_created += 1
                else:
                    members_exists += 1

                # Insert artist_members row
                try:
                    conn.execute(
                        'INSERT INTO artist_members (group_artist_id, member_artist_id, sort_order)'
                        ' VALUES (?, ?, ?)',
                        [group_db_id, member_db_id, sort_order]
                    )
                    conn.commit()
                    console.print(f'    [green]linked[/green]  {sp_member["name"]} → {canonical_name}')
                    links_added += 1
                except sqlite3.IntegrityError:
                    console.print(f'    [dim]link already exists: {sp_member["name"]} → {canonical_name}[/dim]')
                    links_exists += 1

    conn.close()
    console.rule(style='dim')
    console.print(
        f'  Done — '
        f'groups: [green]{groups_created}[/green] created · [dim]{groups_exists}[/dim] exist · [yellow]{groups_skipped}[/yellow] skipped\n'
        f'         members: [green]{members_created}[/green] created · [dim]{members_exists}[/dim] exist\n'
        f'         links: [green]{links_added}[/green] added · [dim]{links_exists}[/dim] already exist'
    )


if __name__ == '__main__':
    main()
