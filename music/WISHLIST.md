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

## Trends view (views/trends.js — new shelf pattern, same shape as Recommendations)

`views/trends.js` + `mdb.py`'s `_stats_trends()` established a "behavioral archetype"
shelf pattern computed offline in Python and read from `stats_cache`, distinct from
Recommendations' live-SQL shelves. Natural next shelves in the same shape:

- [ ] **Comeback Artist** — the inverse of Burnout Trajectory: a long silence
      followed by a renewed burst of plays. Reuses the same per-release timestamp
      pass `_trend_hyperfixation_and_burnout` already does.
- [ ] **Post-Concert Spike decay curve** — how long the spike from
      `trendsPostConcertSpike` actually lasts before returning to baseline, shown
      per-artist rather than as a single week-long window.
- [ ] **Genre Pivot / Genre Whiplash** — a period where the dominant genre shifted
      sharply, or consecutive months with maximally dissimilar top genres. Would
      reuse `release_genres` joins already written in `views/top.js`'s
      `_genreFilterSql`.
- [ ] **One-hit-wonder disambiguation** — `views/stats.js`'s existing "one-hit
      wonders" metric (artists heard exactly once, ever) and `trends.js`'s "One-Hit
      Wonders" shelf (a single track holds ≥90% of an artist's plays) are two
      different, correctly-computed things that happen to share a label. Worth a
      short tooltip/subtitle clarifying which is which wherever both could be seen.

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
      Composer as separate ranked lists distinct from Cast. **Checked this session:
      not buildable yet** — `artist_members` only has
      `(group_artist_id, member_artist_id, sort_order)`, no role/credit-type
      column, so there's no way to distinguish "band member" from "producer" from
      "composer" in the current data. Needs a new `role` column (or a separate
      credits table) sourced from MusicBrainz relationship data during future
      imports before this is buildable at all.
- [ ] **Concert venue map** — now that `venues` has city/state/country and
      `views/concert-stats.js` already has a venues section, a simple pin-map of
      attended venues reuses the exact data the Artist Country choropleth idea
      above needs, at much smaller scope (dozens of points, not a full GeoJSON
      country fill). Good staging step before attempting the bigger map.
- [ ] **Setlist ↔ personal plays overlay** — `concert_performances.setlistfm_url`
      links to the actual setlist played; cross-referencing those songs against
      personal listen history per track would show "how well you knew the songs
      going in" for each show.
- [ ] **Festival billing visualization** — `concert_events.is_festival`/
      `festival_name` and `concert_performances.billing` (headliner/support/
      co-headliner/festival) already exist; a "festivals attended, by year, with
      who you saw headline vs. support" view is a natural fit once enough billing
      data is populated.

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

## Frontend design & code debt (from the 2026-08-31 SPA audit — not new features)

Two rounds of subagent review (design/UX across all 20 views; code simplicity across
the same) turned up real, fixable issues distinct from the feature ideas elsewhere on
this list. Most of the list has since been fixed (see "Already built" below for the
full rundown: empty states, keyboard accessibility, the pulse-render extraction, the
shelf-helpers boilerplate absorption, the SQL bind-params sweep, and the collection-
view drift). Genuinely remaining:

- [ ] **Five near-identical "album/artist tile" components** (`disc-card`,
      `release-card` via `createWideCard()`, `image-card` via `createImageCard()`,
      `mainstream-spotlight-card`, `pub-list-card`) each hand-rolled separately
      across views — the single biggest visual-consolidation opportunity, worth
      tackling before any broader redesign. Deliberately not attempted piecemeal
      alongside the smaller fixes above — this one needs its own design pass.
- [ ] **`soundtracks.js` only covers video-game soundtracks**, despite the nav
      calling it "Soundtracks" — film/TV soundtracks exist but are only reachable
      via `browse.js`'s separate soundtrack filter, with a different UI and no
      cross-link between the two. Needs a product decision (expand scope vs. leave
      as a deliberate video-game-only subsection) before touching code.
- [ ] **Minor navigation gaps**: Compare is only reachable from an artist page
      breadcrumb, not from Top Artists or Year Artists where "how do these two
      stack up" is an equally natural thought; concerts.js and concert-stats.js
      have no cross-link between the raw event list and its aggregate stats; icon
      drift between search.js's shortcuts (`mic-vocal`/`disc-album`) and the
      in-page sort toggles (`mic-2`/`disc-3`) for the same Artists/Albums concept.
- [ ] **`year.js` duplicates `top.js`'s rendering logic instead of reusing its
      `ENTITY_CONFIG` pattern** — `renderReleases`/`renderArtists` in `year.js`
      are two ~60-line near-clones with the same List/Tiles/Collage branches
      `top.js` already solved once via a config object; `year.js` also lacks the
      genre filter, Discoveries/Oldies sort, and collage themes/export that
      `top.js` has, so the two "ranked list" UIs quietly diverge in capability
      despite looking identical.
- [ ] **Genre tag has two unrelated implementations** — `utils.js`'s
      `renderGenreTags()` (`.genre-tag`/`.genre-tag-secondary`) and `stats.js`'s
      breakdown-row tags (`.stat-genre-tag`) style the same concept differently
      only because one context has a chevron icon and the other doesn't. Worth
      merging into one tag component with an optional icon slot.
- [ ] **Genre-DAG ancestor traversal implemented three separate times** —
      `genre.js`'s `_ancestorLevels` (BFS up via `parents`), `genres.js`'s
      `_genreDepth`/`_breadcrumbPath` (depth-memoized single-chain walk), and
      `genres.js`'s own `_buildForest` ancestor-marking recursion all solve
      overlapping "find this genre's ancestors in `GENRE_TREE`" problems with
      different code. A shared `genreTree.js` module with one canonical
      ancestor-walk primitive would remove the triplication.
- [ ] **`browse.js`'s filter state is duplicated across two UI surfaces** — every
      filter (sort/decade/genre/type/soundtrack/status/owned) has its
      apply-logic written once in the desktop dropdown config and again in the
      mobile filter sheet's handlers, with no shared source of truth for "what
      happens when this filter changes." A real refactor (extract the
      state-mutation per filter into one function both surfaces call), not a
      quick fix.

## Out of scope for the current data model (need new external data, not new queries)

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

- Eddington number, artist cutover/Pareto point, one-hit wonders (artists heard
  exactly once, ever — distinct from `views/trends.js`'s same-named shelf, see
  Trends section above), every-year artists, peak month, total listening time —
  all in `views/stats.js`'s "Stats for Nerds" section, computed in
  `mdb.py cmd_stats_refresh`.
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
- **Concert/festival attendance** — full setlist.fm import pipeline
  (`SetlistFmClient`/`parse_setlistfm_setlist` in `mdb_apis.py`), `venues`/
  `concert_events`/`concert_performances` tables, and two frontend views:
  `views/concerts.js` (raw event list/search) and `views/concert-stats.js`
  (headline stats, venues, top artists, per-year chart). Billing (headliner/
  support/co-headliner/festival) is tracked and rendered.
- **Trends view** — a second shelf-of-cards page alongside Recommendations, but
  computed offline in Python instead of live SQL (`mdb.py`'s `_stats_trends()`,
  cached to `stats_cache` keys `trendsHyperFixation`/`trendsBurnout`/
  `trendsSequentialLoop`/`trendsPostConcertSpike`/`trendsOneHitWonder`, rendered by
  `views/trends.js` via shared `views/shelf-helpers.js`). Covers Hyper-Fixation
  Phases, Burnout Trajectory, Sequential Album Loop, Post-Concert Spike, and
  One-Hit Wonders (track-dominance version). (2026-08-31)
- A large recommendations/"surprise me" shelf system already covers several ideas
  from this list under different names — `views/recommendations.js`:
  - "This Month, Past Years" / "Anniversary" ≈ On This Day / listening anniversaries
  - "Fading Favorites" ≈ rediscovery / surprise-me deep dive
  - "Rising" ≈ trending-up signal (as a shelf, not inline ↑↓ arrows)
  - "One Track Away" / "Deep Cut Needed" / "Only Heard Once" ≈ catalogue
    completion and loyalty-vs-exploration ideas, adapted to albums
- **EP/Single type filter on Top Albums** — `views/top.js`'s albums entity config
  now has a `FORMAT_OPTIONS` dropdown (album/ep/single via `rtype`, mirroring
  `browse.js`'s existing pattern). (2026-09-01)
- **2026-08-31 SPA-audit fixes** (see git history for exact commits):
  - Empty states standardized on the `.empty-state` icon+title+hint pattern —
    `views/year.js`/`views/top.js`'s zero-result cases no longer reuse the
    loading-spinner style; `views/collection-digital.js` now has a real empty
    state instead of a blank page (and `views/collection-physical.js`'s ad-hoc
    inline message was upgraded to match). New shared `renderEmptyState()` in
    `utils.js`.
  - Keyboard accessibility added to `.pulse-row` (shared, see below),
    `.commit-cell` heatmap cells in `views/home.js` (tabindex + aria-label +
    focus/blur tooltip), and collection-table rows in both collection views
    (tabindex + Enter/Space keydown).
  - `renderPulse` extracted into shared `views/pulse-helpers.js`, used by both
    `views/artist.js` and `views/release.js` (their `buildMonthlyChartData`/
    `buildYearlyChartData` copies stayed put — `release.js`'s is a real, working
    flagged-off chart feature; `artist.js`'s was dead code, deleted instead).
  - `views/shelf-helpers.js` gained `shelfPageMount`/`renderShelfPage`, absorbing
    the header/`AbortController`/listener-wiring/subtitle-count boilerplate
    `views/recommendations.js` and `views/trends.js` were each still duplicating.
  - Manual SQL quote-escaping (`.replace(/'/g, "''")`) swept to bind params
    across `views/artist.js`, `views/release.js`, `views/compare.js`, and
    `views/browse.js`'s soundtrack filter (which got input validation instead,
    since it's spliced into a hand-built multi-fragment WHERE clause).
  - `views/collection-digital.js`/`views/collection-physical.js`'s accidental
    drift reconciled: `_spineColor`'s HSL values and `GENRE_ORDER` now match
    exactly between the two files. Their `_buildSection`/`_renderShelf`/`mount()`
    grouping logic was investigated and left separate — confirmed to be a real
    design difference (different sort vocabularies, different click behavior:
    `window.open` vs. a rich detail drawer), not copy-paste drift.
  - Dead code removed from `views/artist.js`: `CHART_ENABLED`,
    `TIMELINE_RELEASE_MARKERS`, unused `_chartState`/`_currentChart`, and the
    wasted `buildMonthlyChartData`/`buildYearlyChartData` calls (results were
    computed but never read — only `.monthlyRaw` was).
  - Two bugs fixed immediately during the audit itself: `.release-cert-dot-*`
    hardcoded hex instead of `var(--cert-*)` (broke dark mode);
    `recommendations.js`'s `_moreFromThisArtist` manually quote-escaped a value
    into SQL text instead of using a bind param.
  - (2026-09-01 follow-up round) `groupConsecutivePlays`/`renderRecentPlayRow`
    extracted into shared `views/recent-plays-helpers.js` — `views/home.js` and
    `views/artist.js` had near-identical "collapse consecutive same-release
    plays" grouping and row templates. Also removed two more dead-code findings:
    an unreachable `'artist'` sort branch in `views/collection-digital.js`'s
    `_sortItems` (never exposed in `sortLabels`), and a vestigial unused `_db`
    module variable in `views/list.js` (all its queries already take `db` as an
    explicit argument via `_cache(db, key)`).
