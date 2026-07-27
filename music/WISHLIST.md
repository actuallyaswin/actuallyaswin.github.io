# Feature Wishlist


## Small (a stat_cache section or a stat_* column, same shape as recent work)

- [ ] **Streak tracking** — consecutive-day listening streaks ("your longest streak:
      47 days, Jan 3 – Feb 15, 2023"), plus the current active streak. Also worth
      tracking: longest streak of *not* listening at all, and consecutive-play
      streaks at the track/album level ("you played this 7 times in a row").
- [ ] **Language diversity as a home-page card** — `views/stats.js` already has the
      "Language Breakdown" section; consider whether it deserves a headline stat
      card on the home page too, given the note in the original README about how
      surprising the split can be.

## Medium (a new view, or a meaningfully new chart type)

- [ ] **Hour-of-day / day-of-week / month-of-year distribution charts** — simple bar
      charts over listen timestamps. Support toggling total vs. average-per-year.
- [ ] **Punchcard heatmap** — week × month 2D grid, color intensity = scrobbles.
      Shows seasonal listening patterns at a glance.
- [ ] **Monthly ranking dynamics** — biggest climbers/fallers month over month per
      artist/album/track; "best month for new artist discoveries." Overlaps with
      Maloja's rank-change (↑/↓/NEW badges) and "performance chart" (rank-over-time
      line graph) ideas — same underlying data, different presentations.
- [ ] **Weekly #1 counter** — "this artist was #1 for N weeks," shown on the artist
      page alongside the existing yearly medals.
- [ ] **Scrobbles feed with filtering** — a chronological, paginated feed of every
      play, filterable by artist/track/album/period. Raw history browsing, distinct
      from the existing chart/aggregate views.
- [ ] **Featured entity rotation** — home page rotates a random pick from your top
      N artists/albums/tracks. Low-effort discovery prompt.
- [ ] **Period navigation (prev/current/next)** — walk forward/backward through
      consecutive time periods on stats pages (e.g. "← March | April | May →")
      instead of only a date-range picker.
- [ ] **Dataset / data-table tab** — sortable, filterable table of all artists/albums/
      tracks (count, rank, trend). A "power user" way to find something specific
      without navigating through detail pages. Exportable as CSV/JSON.
- [ ] **Listening companions** — artists that almost always show up in the same
      listening sessions ("you often listen to Burial alongside Grouper").
- [ ] **Time-of-day / seasonal genre patterns** — do certain genres cluster at
      certain hours or times of year? Overlaps with the hour/month distribution
      charts above but sliced by genre instead of raw count.
- [ ] **Release context note** — on the release page, "You were 17 when this came
      out" or "Released during your heaviest listening year." Personal and
      grounding; needs a reference birth/age point to compute against.
- [ ] **Discovery timeline** — a scrollable chronological visual of when you first
      heard each artist. Shows how taste expanded over the years. Data (first
      listen date) already exists per-artist via `stat_first_listen_ts`; this is
      mostly a new visual, not a new query.
- [ ] **Milestones feed** — a live ticker of personal records: "You just hit 1,000
      plays of Radiohead," "New personal best: 47-day streak" (depends on Streak
      Tracking above). Needs a defined set of milestone thresholds to check.

## Deep-dive metrics (each is a new per-artist stat, similar shape to Drift)

- [ ] **Loyalty vs. exploration score** — per artist, how much of their catalogue
      you've heard vs. how concentrated your plays are on just a few albums/tracks.
- [ ] **Deep cuts ratio** — per artist, the split between plays on their popular
      tracks vs. album deep cuts. A high ratio means you really know them beyond
      the singles.
- [ ] **Listening velocity** — how intensely you listened in the first 30 days
      after discovering an artist vs. now. Captures the "honeymoon phase" and its
      falloff.
- [ ] **Obscurity score** — cross-referenced against Spotify popularity scores,
      how mainstream vs. niche your taste is overall and per genre. Related to
      the already-built "Mainstream Meter" stats section, but as a personal
      score/trend rather than a listen-share breakdown.

## Larger (real design + build effort)

- [ ] **World map (choropleth) for Artist Country** — the standout visual on
      Letterboxd's stats page. Needs a world GeoJSON/SVG and fill-by-country
      rendering; would likely replace or sit alongside the existing Artist Country
      bar list in `views/stats.js`.
- [ ] **Race chart (animated ranking over time)** — animated bar-chart race for top
      artists/albums/tracks, with play/pause, speed, and rolling-vs-cumulative
      window controls.
- [ ] **Z-score anomaly chart** — per month, compute how statistically unusual that
      month's top artist was vs. your baseline. Surfaces "what were you weirdly
      into in [month]" moments.
- [ ] **Real-time now-playing bar** — poll Last.fm's recent-tracks API every ~30s,
      show a subtle "currently playing" indicator when active. Different from
      everything else on this list in that it needs a live polling loop rather
      than a precomputed/static query.
- [ ] **Listening persona / era clustering** — a short generated description of your
      current taste ("night-time melancholic, drawn to slow builds and minor
      keys"), or auto-grouping your history into named eras ("The Post-Rock Era,
      2013–2015"). Needs a defined clustering/summarization approach, not just a
      new query — the fuzziest item on this list.
- [ ] **Composers / producers section** — Letterboxd breaks out Director and
      Composer as separate ranked lists distinct from Cast. Possible if
      `artist_members`/production-credit data is populated well enough to be
      interesting; needs a data-availability check first.

## Python toolchain (mdb.py, not frontend)

- [ ] **Rules-based metadata fixing** — Maloja lets you write `.tsv` rule files to
      merge duplicate artists, fix misspellings, and split "Artist A & Artist B"
      into separately-credited artists, re-applied on every rebuild. `mdb.py`
      currently handles this via one-off manual upserts (`mdb.py artist merge`,
      alias commands); a rule-file approach would scale better for ongoing
      corrections but is a genuinely different workflow, not a small tweak.
- [ ] **Associated-artists system** — group project aliases (e.g. a solo project
      vs. its parent band) so charts can show them "separately" or "combined"
      without actually merging scrobble history. Conceptually different from the
      existing `artist alias`/`artist merge` commands, which fold one artist
      permanently into another.

## Out of scope for the current data model (need new external data, not new queries)

- **Concert/festival attendance** — import from [setlist.fm](https://www.setlist.fm).
- **DJ mix embedding** — a neat way to embed DJ mixes on the site; needs a hosting/
  embed mechanism, not a DB query.
- **Music videos / external streaming links** — add video refs or external
  streaming links per track.

## Explicitly not worth chasing

- **List-completion donut rings** (Letterboxd's Top 500/AFI 100 % complete). No
  equivalent canonical "top N albums of all time" list exists for music the way it
  does for film critics' lists; building one would be curation work disconnected
  from listening-history data.
- **Deep crew granularity** (hairstyling, camera operators, etc., from Letterboxd).
  No music equivalent at that granularity — composers/producers/labels is the
  right stopping depth for this schema.
- **Most Watched vs. Highest Rated toggle** — no personal star-rating field exists
  for tracks/albums, so this has no direct analog. Reconsider only if such a field
  gets added later.
- **"Longest gap between consecutive plays"** — considered and rejected in favor of
  Drift (avg. days between plays per artist/release/track), which is a more
  interesting and now-built alternative. See "Already built" below.

## Already built (kept here so this doesn't get re-proposed)

- Eddington number, artist cutover/Pareto point, one-hit wonders, every-year
  artists, peak month, total listening time — all in `views/stats.js`'s "Stats for
  Nerds" section, computed in `mdb.py cmd_stats_refresh`.
- Album completion %, Most Relistened Tracks, Top Labels, drill-down accordions —
  `views/stats.js`.
- Golden Oldies / Latest Discoveries (oldest/newest average listen date) and
  release-year filtering on top albums/tracks — `views/top.js`.
- Yearly medals (gold/silver/bronze for #1/#2/#3-ranked years) on artist pages —
  `views/artist.js`, backed by `artist_year_medals`, computed in
  `mdb.py cmd_stats_refresh`.
- First listen date, last played (relative time), and **Drift** (average days
  between consecutive plays) on artist and release pages — `views/artist.js`,
  `views/release.js`, backed by `stat_first_listen_ts`/`stat_last_listen_ts`/
  `stat_drift_days` columns on `artists`/`releases`/`tracks`, computed in
  `mdb.py cmd_stats_refresh`. (2026-07-27)
- Language diversity breakdown — `views/stats.js`'s "Language Breakdown" section.
- A large recommendations/"surprise me" shelf system already covers several ideas
  from this list under different names — `views/recommendations.js`:
  - "This Month, Past Years" / "Anniversary" ≈ On This Day / listening anniversaries
  - "Fading Favorites" ≈ rediscovery / surprise-me deep dive
  - "Rising" ≈ trending-up signal (as a shelf, not inline ↑↓ arrows)
  - "One Track Away" / "Deep Cut Needed" / "Only Heard Once" ≈ catalogue
    completion and loyalty-vs-exploration ideas, adapted to albums
