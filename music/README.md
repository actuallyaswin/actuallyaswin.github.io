# aswin.db/music

A visual diary for Aswin's music listening history (logged from 2011 onward). It is an interactive, client-side web app for exploring 14+ years of music listening data with detailed statistics. Backed by a SQLite database, everything runs entirely in-browser without a backend. The webapp combines data from multiple sources: <b><i>Last.fm</i></b> for listening history (scrobbles with timestamps), <b><i>Spotify</i></b> for artist photos + album art, and <b><i>MusicBrainz</i></b> for album metadata.

## Other Ideas / TODOs

- Real-time now playing — poll Last.fm's recent tracks API every 30s, show a subtle "currently playing" bar at the top when active. Data is already there, just needs a live fetch.
- On This Day — "3 years ago you were obsessed with Kid A. 7 years ago you discovered Burial." Pull top artists/albums for this exact week in past years.
- Catalogue completion per artist — a progress bar showing how many of an artist's tracks/albums you've actually heard. "You've heard 47/112 tracks across 9 albums."
- Trending arrows on top lists — small ↑↓ indicators comparing this week/month to the previous period. Low effort, high signal.
- Discovery timeline — a scrollable visual showing when you first heard each artist, in chronological order. Shows how taste expanded over 14 years.
- Random deep dive ("Surprise me") — button that surfaces a random release you haven't played in over a year. Good for rediscovery.
- Language diversity breakdown — now that `tracks.language` is populated, a stat showing what % of listening is in each language. Probably surprising how much Japanese is in there.
- Listening companions — surface artists that almost always appear in the same sessions: "You often listen to Burial alongside Grouper."
- Era clustering — auto-group listening history into "eras" based on dominant genres/artists: "The Post-Rock Era, 2013–2015". Interesting retrospective lens.
- Release context note — on the release page, "You were 17 when this came out" or "Released during your heaviest listening year." Personal and grounding.
- First listen dates — show on every artist and release page exactly when you first heard them.
- Listening anniversaries — callout when you hit the 1-year, 5-year, 10-year mark with an artist.
- Listening persona — a short description derived from genre/tempo/mood distribution: "Night-time melancholic. Drawn to slow builds, minor keys, post-rock textures." Changes over time.
- Time-of-day patterns — heatmap of what genres/artists you play at different hours of the day.
- Seasonal patterns — do you listen to heavier music in winter? Brighter albums in summer?
- Loyalty vs. exploration score per artist — how much of their catalogue you've heard vs. how concentrated your plays are on just a few albums.
- Deep cuts ratio — for each artist, the split between popular tracks and album deep cuts. A high ratio means you really know them.
- Listening velocity — how intensely you listened in the first 30 days after discovering an artist vs. now. Captures the "honeymoon phase."
- Obscurity score — cross-referenced against Spotify popularity scores to show how mainstream vs. niche your taste is overall and per genre.
- Milestones feed — a live ticker of personal records: "You just hit 1,000 plays of Radiohead." "New personal best: 47-day streak."
- Come up with an innovative way for me to embed DJ mixes here in a neat way
- Import concert and festival attendance data from [setlist.fm](https://www.setlist.fm)
- Import my CD and vinyl collection data from [Discogs](https://www.discogs.com)
- Add music videos or provide references to external streaming sources