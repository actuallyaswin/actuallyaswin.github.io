// Unified catalog browser — supersedes Top Artists/Albums and Soundtracks.
// Letterboxd's "Films" page is the model: Sort/Decade/Genre/Platform/Status
// pill row, a decade-then-year carousel instead of a flat year dropdown,
// and a plain-language summary line above the grid instead of a filter-count
// badge. Tracks stay on views/top.js — this is album/artist browsing only.
const ViewBrowse = (() => {
    let _db = null;
    let _container = null;
    // Guards the document-level dropdown-close listener (added once in
    // mount(), torn down in unmount()) -- _setupControls re-runs on every
    // entityType toggle via _remount, so binding it there instead would
    // stack a new listener per toggle.
    let _ac = null;
    // 'albums' | 'artists'
    let entityType = 'albums';
    // 'discoveries' | 'recent' | 'plays' | 'az' | 'release-date' | 'random'
    let sortBy = 'release-date';
    // 'all' or e.g. '2020s'
    let decade = 'all';
    // set only when a specific year within `decade` is picked via the carousel
    let year = null;
    // genre id as string, or 'all'
    let genreFilter = 'all';
    // 'all' | 'album' | 'ep' | 'single' — releases.type; only meaningful for
    // entityType 'albums'
    let typeFilter = 'all';
    // 'all' | 'film' | 'tv_series' | 'video_game' | 'video_game:<platform>' —
    // only meaningful while entityType is 'albums'. The video_game:<platform>
    // form narrows to one console; the bare 'video_game' value matches every
    // video-game soundtrack regardless of platform.
    let soundtrackFilter = 'all';
    // 'all' | 'heard' | 'unheard'
    let status = 'all';
    // 'all' | 'owned' | 'unowned' — only meaningful for entityType 'albums'
    // (artists aren't physical media, nothing to own)
    let ownedFilter = 'all';
    // 'list' | 'poster-sm' | 'poster-lg'
    let viewMode = 'poster-lg';
    let _rows = [];
    let _genreSelect = null;
    // Cached once per mount and reused by both the desktop dropdown and the
    // mobile filter sheet, so there's one source of truth for the options
    // list instead of two separate queries drifting apart.
    let _genresIndex = [];
    // Raw video-game platform slugs (unprefixed) -- kept separately from
    // _soundtrackOptions since _loadAlbums() only needs the plain slug list,
    // not the display-ready {value, label, icon, indent} rows.
    let _platformsList = [];
    let _soundtrackOptions = [{ value: 'all', label: 'All soundtracks' }];
    // Which row's option list the mobile filter sheet is drilled into, or
    // null when showing the root Filters list. Sheet DOM is torn down and
    // rebuilt on every change rather than patched in place -- this whole
    // interaction is opened rarely enough that simplicity wins over the
    // extra plumbing a diff-based update would need.
    let _sheetSubviewKey = null;
    let _shuffleSeed = 0;
    // How many of the sorted/filtered rows are actually in the DOM — grows by
    // PAGE_SIZE on "Load more" / scroll-near-bottom. Without this, a cleared
    // filter set (5,900+ releases) renders every card in one shot.
    let _visibleCount = 0;
    const PAGE_SIZE = 60;

    const DECADES = ['1960s', '1970s', '1980s', '1990s', '2000s', '2010s', '2020s'];
    const DECADE_OPTIONS = [{ value: 'all', label: 'All time' }, ...DECADES.map(d => ({ value: d, label: d }))];

    // Labels/order for Sort By's single-select rows -- shared by the mobile
    // filter sheet's drill-in list and the desktop dropdown popover, so they
    // can never drift out of sync with each other.
    const SORT_OPTIONS = [
        { value: 'discoveries',  label: 'Discoveries', icon: 'sparkles' },
        { value: 'recent',       label: 'Recent',      icon: 'clock' },
        { value: 'plays',        label: 'Plays',       icon: 'headphones' },
        { value: 'az',           label: 'A–Z' },
        { value: 'release-date', label: 'Release' },
        { value: 'random',       label: 'Random',      icon: 'shuffle' },
    ];
    const STATUS_OPTIONS = [
        { value: 'all',     label: 'All' },
        { value: 'heard',   label: 'Heard' },
        { value: 'unheard', label: 'Unheard' },
    ];
    const OWNED_OPTIONS = [
        { value: 'all',     label: 'All' },
        { value: 'owned',   label: 'Owned' },
        { value: 'unowned', label: 'Unowned' },
    ];
    const TYPE_OPTIONS = [
        { value: 'all',    label: 'All types' },
        { value: 'album',  label: 'Album' },
        { value: 'ep',     label: 'EP' },
        { value: 'single', label: 'Single' },
    ];
    // Top-level Soundtrack categories -- platform rows (one per distinct
    // release_soundtrack_meta.platform under source_type='video_game') are
    // appended after these by _populateSoundtrackOptions().
    const SOUNDTRACK_BASE_OPTIONS = [
        { value: 'all',        label: 'All soundtracks' },
        { value: 'film',       label: 'Movie' },
        { value: 'tv_series',  label: 'TV Series' },
        { value: 'video_game', label: 'Video Game' },
    ];

    function _decadeStart(d) { return parseInt(d, 10); }

    // Tracks under 30s already excluded site-wide via SCROBBLABLE_TRACK_FILTER
    // (utils.js) — donuts here reuse that same denominator so an album's
    // Browse-card ring always agrees with its own release page.
    function _albumDonut(row) {
        return donutHtml(row.tracksHeard, row.totalTracks);
    }

    function _load() {
        if (entityType === 'albums') {
            _loadAlbums();
        } else {
            _loadArtists();
        }
    }

    function _yearRange() {
        if (year) return [year, year];
        if (decade !== 'all') { const s = _decadeStart(decade); return [s, s + 9]; }
        return null;
    }

    // 'all' | 'film' | 'tv_series' | 'video_game' | 'video_game:<platform>' —
    // see soundtrackFilter's declaration for the encoding.
    function _soundtrackClause() {
        if (soundtrackFilter === 'all') return '';
        const [sourceType, platform] = soundtrackFilter.split(':');
        const safeType = sourceType.replace(/'/g, "''");
        const platformPart = platform ? ` AND sm.platform = '${platform.replace(/'/g, "''")}'` : '';
        return `AND EXISTS (SELECT 1 FROM release_soundtrack_meta sm WHERE sm.release_id = r.id AND sm.source_type = '${safeType}'${platformPart})`;
    }

    function _loadAlbums() {
        const yr = _yearRange();
        const yearClause = yr ? `AND r.release_year BETWEEN ${yr[0]} AND ${yr[1]}` : '';
        const gid = parseInt(genreFilter, 10);
        const genreClause = (genreFilter !== 'all' && !isNaN(gid))
            ? `AND EXISTS (SELECT 1 FROM release_genres rg WHERE rg.release_id = r.id AND rg.aoty_genre_id = ${gid})`
            : '';
        const typeClause = typeFilter !== 'all' ? `AND r.type = '${typeFilter}'` : '';
        const soundtrackClause = _soundtrackClause();

        const result = _db.exec(`
            SELECT r.id, r.title, r.slug, r.release_year,
                   COALESCE(r.album_art_thumb_url, r.album_art_url) as art,
                   (SELECT a2.name FROM artists a2 WHERE a2.id = r.primary_artist_id) as artist_name,
                   r.primary_artist_id,
                   (SELECT a2.slug FROM artists a2 WHERE a2.id = r.primary_artist_id) as artist_slug,
                   (SELECT COUNT(*) FROM tracks t WHERE t.release_id = r.id AND ${SCROBBLABLE_TRACK_FILTER}) as total_tracks,
                   (SELECT COUNT(*) FROM tracks t
                        WHERE t.release_id = r.id AND ${SCROBBLABLE_TRACK_FILTER}
                          AND EXISTS (SELECT 1 FROM listens l WHERE l.track_id = t.id)) as tracks_heard,
                   (SELECT COUNT(*) FROM tracks t JOIN listens l ON l.track_id = t.id
                        WHERE t.release_id = r.id AND t.hidden = 0) as total_listens,
                   (SELECT MIN(l.timestamp) FROM tracks t JOIN listens l ON l.track_id = t.id
                        WHERE t.release_id = r.id AND t.hidden = 0) as first_listen_ts,
                   (SELECT MAX(l.timestamp) FROM tracks t JOIN listens l ON l.track_id = t.id
                        WHERE t.release_id = r.id AND t.hidden = 0) as last_listen_ts,
                   ${OWNED_MEDIUM_SQL} as owned_medium
            FROM releases r
            WHERE r.hidden = 0 ${yearClause} ${genreClause} ${typeClause} ${soundtrackClause}
        `)[0];

        _rows = result ? result.values.map(([id, title, slug, releaseYear, art, artistName, artistId, artistSlug,
                                              totalTracks, tracksHeard, totalListens, firstListenTs, lastListenTs,
                                              ownedMedium]) => ({
            id, title, slug, releaseYear, art, artistName, artistId, artistSlug,
            totalTracks: totalTracks || 0, tracksHeard: tracksHeard || 0,
            totalListens: totalListens || 0, firstListenTs, lastListenTs, ownedMedium,
        })) : [];
    }

    function _loadArtists() {
        const yr = _yearRange();
        // Artists don't have a release_year of their own — filtering to a
        // decade means "has at least one release from that decade", not
        // "formed in that decade" (formed_year is a different, rarer field).
        const yearClause = yr
            ? `AND EXISTS (
                 SELECT 1 FROM track_artists ta2
                 JOIN tracks t2 ON t2.id = ta2.track_id
                 JOIN releases r2 ON r2.id = t2.release_id
                 WHERE ta2.artist_id = a.id AND r2.release_year BETWEEN ${yr[0]} AND ${yr[1]}
               )`
            : '';
        const gid = parseInt(genreFilter, 10);
        const genreClause = (genreFilter !== 'all' && !isNaN(gid))
            ? `AND EXISTS (SELECT 1 FROM release_genres rg JOIN tracks t3 ON t3.release_id = rg.release_id
                 JOIN track_artists ta3 ON ta3.track_id = t3.id
                 WHERE ta3.artist_id = a.id AND rg.aoty_genre_id = ${gid})`
            : '';

        const result = _db.exec(`
            SELECT a.id, a.name, a.slug,
                   COALESCE(a.image_thumb_url, a.image_url) as art,
                   COUNT(DISTINCT CASE WHEN t.hidden = 0 AND l.id IS NOT NULL THEN t.id END) as tracks_heard_raw,
                   COUNT(DISTINCT CASE WHEN ${SCROBBLABLE_TRACK_FILTER} THEN t.id END) as total_tracks,
                   COUNT(CASE WHEN t.hidden = 0 THEN l.id END) as total_listens,
                   MIN(CASE WHEN t.hidden = 0 THEN l.timestamp END) as first_listen_ts,
                   MAX(CASE WHEN t.hidden = 0 THEN l.timestamp END) as last_listen_ts
            FROM artists a
            LEFT JOIN track_artists ta ON a.id = ta.artist_id AND ta.role IN ('main','primary')
            LEFT JOIN tracks t ON ta.track_id = t.id
            LEFT JOIN listens l ON l.track_id = t.id
            WHERE a.hidden = 0 ${yearClause} ${genreClause}
            GROUP BY a.id
            HAVING total_tracks > 0
        `)[0];

        _rows = result ? result.values.map(([id, name, slug, art, tracksHeardRaw, totalTracks, totalListens,
                                              firstListenTs, lastListenTs]) => ({
            id, title: name, slug, art,
            totalTracks: totalTracks || 0, tracksHeard: tracksHeardRaw || 0,
            totalListens: totalListens || 0, firstListenTs, lastListenTs,
        })) : [];
    }

    function _matchesStatus(row) {
        if (status === 'heard') return row.totalListens > 0;
        if (status === 'unheard') return row.totalListens === 0;
        return true;
    }

    function _matchesOwned(row) {
        if (entityType !== 'albums' || ownedFilter === 'all') return true;
        if (ownedFilter === 'owned') return !!row.ownedMedium;
        return !row.ownedMedium;
    }

    // Deterministic-per-load shuffle rather than Math.random() directly —
    // re-sorting on every _render() call (e.g. after a status-filter toggle
    // that doesn't touch sort) would otherwise reshuffle the grid out from
    // under the user for no reason.
    function _shuffleKey(id) {
        let h = _shuffleSeed;
        for (let i = 0; i < id.length; i++) h = (h * 31 + id.charCodeAt(i)) | 0;
        return h;
    }

    function _sortedRows() {
        const rows = _rows.filter(r => _matchesStatus(r) && _matchesOwned(r));
        switch (sortBy) {
            case 'recent':
                return rows.sort((a, b) => (b.lastListenTs || 0) - (a.lastListenTs || 0));
            case 'plays':
                return rows.sort((a, b) => b.totalListens - a.totalListens);
            case 'az':
                return rows.sort((a, b) => a.title.localeCompare(b.title));
            case 'release-date':
                return rows.sort((a, b) => (b.releaseYear || 0) - (a.releaseYear || 0));
            case 'random':
                return rows.sort((a, b) => _shuffleKey(a.id) - _shuffleKey(b.id));
            case 'discoveries':
            default:
                // Never-heard entries have no first_listen_ts — they sort last,
                // not first, since "discoveries" means newest real discovery.
                return rows.sort((a, b) => (b.firstListenTs || 0) - (a.firstListenTs || 0));
        }
    }

    function _cardHtml(row) {
        const href = entityType === 'albums' ? releaseHref(row.id, row.slug) : artistHref(row.id, row.slug);
        const sub = entityType === 'albums'
            ? [row.artistName, row.releaseYear].filter(Boolean).join(' · ')
            : formatNumber(row.totalListens) + ' plays';
        // Native title tooltip -- a literal \n renders as a real line break
        // here (unlike an HTML <br/>, which title attributes show as literal
        // text), so this needs no CSS tooltip machinery of its own.
        const tooltip = entityType === 'albums'
            ? `"${row.title}" (${row.releaseYear})\n${row.artistName} · ${row.tracksHeard}/${row.totalTracks}`
            : `${row.title}\n${formatNumber(row.totalListens)} plays`;
        const donut = _albumDonut(row);
        const posterOnly = viewMode !== 'list';
        if (posterOnly) {
            // Donut lives inside the art itself (bottom-right), not the caption
            // below it -- that caption is hidden entirely on narrow viewports
            // (see the max-width:768px rule for .browse-poster-caption) to fit
            // more columns per row without losing the play-progress indicator.
            return `<a href="${href}" class="browse-poster${row.totalListens === 0 ? ' unplayed' : ''}" title="${escapeHtml(tooltip)}">
                <div class="browse-poster-img" style="background-image:url('${cssUrl(row.art || getFallbackImageUrl())}')">
                    ${entityType === 'albums' ? ownedBadgeHtml(row.ownedMedium) : ''}
                    <div class="browse-poster-donut">${donut}</div>
                </div>
                <div class="browse-poster-caption">
                    <div class="browse-poster-info">
                        <div class="browse-poster-title">${escapeHtml(row.title)}</div>
                        <div class="browse-poster-sub">${escapeHtml(sub)}</div>
                    </div>
                    ${donut}
                </div>
            </a>`;
        }
        return `<a href="${href}" class="disc-card${row.totalListens === 0 ? ' unplayed' : ''}" title="${escapeHtml(tooltip)}">
            <div class="disc-card-img" style="background-image:url('${cssUrl(row.art || getFallbackImageUrl())}')">
                ${entityType === 'albums' ? ownedBadgeHtml(row.ownedMedium) : ''}
            </div>
            <div class="disc-card-meta">
                <div class="disc-card-info">
                    <div class="disc-card-title">${escapeHtml(row.title)}</div>
                    <div class="disc-card-sub">${escapeHtml(sub)}</div>
                </div>
                ${donut}
            </div>
        </a>`;
    }

    function _summarySentence(rows) {
        const n = rows.length;
        const noun = entityType === 'albums' ? (n === 1 ? 'album' : 'albums') : (n === 1 ? 'artist' : 'artists');
        const parts = [`${formatNumber(n)} ${noun}`];
        if (year) parts.push(String(year));
        else if (decade !== 'all') parts.push(decade);
        if (genreFilter !== 'all' && _genreSelect) {
            const opt = _genreSelect.options[genreFilter];
            if (opt) parts.push(opt.text);
        }
        if (typeFilter !== 'all') parts.push(TYPE_OPTIONS.find(o => o.value === typeFilter)?.label || typeFilter);
        if (soundtrackFilter !== 'all') parts.push(_soundtrackFilterLabel());
        if (status !== 'all') parts.push(status === 'heard' ? 'heard' : 'not yet heard');
        if (entityType === 'albums' && ownedFilter !== 'all') parts.push(ownedFilter === 'owned' ? 'owned' : 'not owned');
        return parts.join(' · ');
    }

    function _renderSidebar(rows) {
        const el = document.getElementById('browseSidebar');
        if (!el) return;
        const heardCount = rows.filter(r => r.totalListens > 0).length;
        const pct = rows.length ? Math.round((heardCount / rows.length) * 100) : 0;
        const totalTracksHeard = rows.reduce((s, r) => s + r.tracksHeard, 0);
        const totalTracksAll = rows.reduce((s, r) => s + r.totalTracks, 0);
        const trackPct = totalTracksAll ? Math.round((totalTracksHeard / totalTracksAll) * 100) : 0;

        el.innerHTML = `
            <div class="sidebar-section">
                <div class="sidebar-progress-card">
                    <div class="sidebar-progress-top">
                        <span class="sidebar-progress-label">You've heard<br>${heardCount} of ${rows.length}</span>
                        <span class="sidebar-progress-pct">${pct}<sup>%</sup></span>
                    </div>
                    <div class="sidebar-progress-track">
                        <div class="sidebar-progress-fill" style="width:${pct}%"></div>
                    </div>
                </div>
            </div>
            <div class="sidebar-section">
                <dl class="nerds-list" style="border:none;border-radius:0">
                    <div class="nerds-row"><dt>Track completion</dt><dd>${trackPct}%</dd></div>
                    <div class="nerds-row"><dt>Total plays</dt><dd>${formatNumber(rows.reduce((s, r) => s + r.totalListens, 0))}</dd></div>
                </dl>
            </div>`;
    }

    let _sentinelObserver = null;

    function _render() {
        const gridEl = document.getElementById('browseGrid');
        const subtitleEl = document.getElementById('browseSubtitle');
        if (!gridEl) return;

        const rows = _sortedRows();
        subtitleEl.textContent = _summarySentence(rows);
        gridEl.className = viewMode === 'list' ? 'disc-grid browse-grid-list'
            : viewMode === 'poster-sm' ? 'browse-grid-posters browse-grid-posters-sm'
            : 'browse-grid-posters';

        const visible = rows.slice(0, _visibleCount);
        gridEl.innerHTML = rows.length
            ? visible.map(row => `<li>${_cardHtml(row)}</li>`).join('')
            : '<li class="browse-empty">Nothing matches these filters.</li>';

        const sentinel = document.getElementById('browseSentinel');
        if (sentinel) sentinel.hidden = _visibleCount >= rows.length;

        if (_sentinelObserver) _sentinelObserver.disconnect();
        if (sentinel && _visibleCount < rows.length) {
            _sentinelObserver = new IntersectionObserver(entries => {
                if (entries[0].isIntersecting) {
                    _visibleCount += PAGE_SIZE;
                    _render();
                }
            }, { rootMargin: '600px' });
            _sentinelObserver.observe(sentinel);
        }

        // Sidebar summarizes the whole filtered set, not just what's paged in.
        _renderSidebar(rows);
    }

    // Any control that narrows/reorders the result set (filters, sort, type,
    // status) must reset paging back to page 1 — otherwise switching from
    // "5,900 albums" to "12 albums from the 1960s" would leave _visibleCount
    // at whatever large number it grew to under the old, bigger filter.
    function _applyFiltersAndRender() {
        _visibleCount = PAGE_SIZE;
        _render();
    }

    // ── Mobile filter sheet ──────────────────────────────────────────────
    // Letterboxd's "Filters" sheet is the model: a full-screen overlay with
    // Cancel/Filters/Done up top, grouped rows below. Ours applies instantly
    // (everything here is a client-side re-filter of already-loaded rows, or
    // a fast indexed query), so Cancel and Done both just dismiss — there's
    // no staged state to revert or commit, unlike a query that only runs on
    // a server round-trip.
    function _sortLabel() { return (SORT_OPTIONS.find(o => o.value === sortBy) || SORT_OPTIONS[0]).label; }
    function _decadeLabel() { return decade === 'all' ? 'All time' : decade; }
    function _genreLabel() {
        if (genreFilter === 'all') return 'All genres';
        const g = _genresIndex.find(g => String(g.id) === genreFilter);
        return g ? g.name : 'All genres';
    }
    function _typeLabel() { return (TYPE_OPTIONS.find(o => o.value === typeFilter) || TYPE_OPTIONS[0]).label; }
    // soundtrackFilter is 'all' | 'film' | 'tv_series' | 'video_game' |
    // 'video_game:<platform>' -- _soundtrackOptions already has a matching
    // {value, label} row for every one of those forms, platform rows included.
    function _soundtrackFilterLabel() {
        return (_soundtrackOptions.find(o => o.value === soundtrackFilter) || _soundtrackOptions[0]).label;
    }
    function _statusLabel() { return status === 'all' ? 'All' : status === 'heard' ? 'Heard' : 'Unheard'; }
    function _ownedLabel() { return ownedFilter === 'all' ? 'All' : ownedFilter === 'owned' ? 'Owned' : 'Unowned'; }

    // A tap advances all -> first -> second -> all, matching Letterboxd's
    // Watched/Liked/etc. rows under ACCOUNT (image referenced: tap once for
    // "Watched", again for "Not Watched", again back to "Any").
    function _cycle3(current, first, second) {
        return current === 'all' ? first : current === first ? second : 'all';
    }

    // ── Desktop toolbar dropdowns ─────────────────────────────────────────
    // Shared component (utils.js's dropdownHtml/setupDropdowns) -- rendered
    // as a small anchored popover under its own trigger instead of a
    // full-screen sheet, since a bottom sheet reads as a mobile pattern
    // even on a mouse-driven wide screen; desktop gets Letterboxd's other
    // treatment (its own #content-nav row of click-to-open dropdown
    // labels). Decade used to be a plain <select> -- a native <select>'s
    // open list is the browser's own OS-level popup, which no CSS can
    // restyle, so it kept looking different from Sort/Status even after the
    // closed box was restyled to match. Converting it to this shared
    // component fixed that at the root, and now every view with a small
    // "pick one of these" filter reuses the exact same component instead of
    // each hand-rolling its own.
    function _filterSheetRootHtml() {
        return `
            <div class="filter-sheet-group">
                <div class="filter-sheet-group-label">Browse</div>
                <button type="button" class="filter-sheet-row" data-sheet-open="sort">
                    <span>Sort By</span>
                    <span class="filter-sheet-row-value">${escapeHtml(_sortLabel())} <i data-lucide="chevron-right"></i></span>
                </button>
            </div>
            <div class="filter-sheet-group">
                <div class="filter-sheet-group-label">Content</div>
                <button type="button" class="filter-sheet-row" data-sheet-open="decade">
                    <span>Decade</span>
                    <span class="filter-sheet-row-value">${escapeHtml(_decadeLabel())} <i data-lucide="chevron-right"></i></span>
                </button>
                <button type="button" class="filter-sheet-row" data-sheet-open="genre">
                    <span>Genre</span>
                    <span class="filter-sheet-row-value">${escapeHtml(_genreLabel())} <i data-lucide="chevron-right"></i></span>
                </button>
                ${entityType === 'albums' ? `
                <button type="button" class="filter-sheet-row" data-sheet-open="type">
                    <span>Type</span>
                    <span class="filter-sheet-row-value">${escapeHtml(_typeLabel())} <i data-lucide="chevron-right"></i></span>
                </button>` : ''}
                ${entityType === 'albums' ? `
                <button type="button" class="filter-sheet-row" data-sheet-open="soundtrack">
                    <span>Soundtrack</span>
                    <span class="filter-sheet-row-value">${escapeHtml(_soundtrackFilterLabel())} <i data-lucide="chevron-right"></i></span>
                </button>` : ''}
            </div>
            <div class="filter-sheet-group">
                <div class="filter-sheet-group-label">Collection</div>
                <button type="button" class="filter-sheet-row" data-sheet-cycle="status">
                    <span>Status</span>
                    <span class="filter-sheet-row-value">${_statusLabel()}</span>
                </button>
                ${entityType === 'albums' ? `
                <button type="button" class="filter-sheet-row" data-sheet-cycle="owned">
                    <span>Collection</span>
                    <span class="filter-sheet-row-value">${_ownedLabel()}</span>
                </button>` : ''}
            </div>`;
    }

    function _filterSheetSubviewHtml(key) {
        const rowHtml = (value, label, active, indent) =>
            `<button type="button" class="filter-sheet-row${indent ? ' filter-sheet-row--indent' : ''}" data-sheet-pick="${escapeHtml(String(value))}">
                <span>${escapeHtml(label)}</span>
                ${active ? '<i data-lucide="check"></i>' : ''}
            </button>`;
        if (key === 'sort') {
            return `<div class="filter-sheet-group">${
                SORT_OPTIONS.map(o => rowHtml(o.value, o.label, sortBy === o.value)).join('')
            }</div>`;
        }
        if (key === 'decade') {
            return `<div class="filter-sheet-group">${
                [rowHtml('all', 'All time', decade === 'all')]
                    .concat(DECADES.map(d => rowHtml(d, d, decade === d)))
                    .join('')
            }</div>`;
        }
        if (key === 'genre') {
            return `<div class="filter-sheet-group">${
                [rowHtml('all', 'All genres', genreFilter === 'all')]
                    .concat(_genresIndex.map(g => rowHtml(g.id, g.name, genreFilter === String(g.id))))
                    .join('')
            }</div>`;
        }
        if (key === 'type') {
            return `<div class="filter-sheet-group">${
                TYPE_OPTIONS.map(o => rowHtml(o.value, o.label, typeFilter === o.value)).join('')
            }</div>`;
        }
        if (key === 'soundtrack') {
            return `<div class="filter-sheet-group">${
                _soundtrackOptions.map(o => rowHtml(o.value, o.label, soundtrackFilter === o.value, o.indent)).join('')
            }</div>`;
        }
        return '';
    }

    const _SHEET_SUBVIEW_TITLES = { sort: 'Sort By', decade: 'Decade', genre: 'Genre', type: 'Type', soundtrack: 'Soundtrack' };

    function _renderFilterSheet() {
        const header = document.getElementById('browseFilterSheetHeader');
        const body = document.getElementById('browseFilterSheetBody');
        if (!header || !body) return;

        header.innerHTML = _sheetSubviewKey
            ? `<button type="button" class="filter-sheet-back" id="filterSheetBack"><i data-lucide="chevron-left"></i> Filters</button>
               <span class="filter-sheet-title">${_SHEET_SUBVIEW_TITLES[_sheetSubviewKey] || ''}</span>
               <button type="button" class="filter-sheet-done" id="filterSheetDone">Done</button>`
            : `<span class="filter-sheet-title">Filters</span>
               <button type="button" class="filter-sheet-done" id="filterSheetDone">Done</button>`;

        body.innerHTML = _sheetSubviewKey ? _filterSheetSubviewHtml(_sheetSubviewKey) : _filterSheetRootHtml();

        document.getElementById('filterSheetDone')?.addEventListener('click', _closeFilterSheet);
        document.getElementById('filterSheetBack')?.addEventListener('click', () => {
            _sheetSubviewKey = null;
            _renderFilterSheet();
        });

        body.querySelectorAll('[data-sheet-open]').forEach(btn => btn.addEventListener('click', () => {
            _sheetSubviewKey = btn.dataset.sheetOpen;
            _renderFilterSheet();
        }));
        body.querySelectorAll('[data-sheet-pick]').forEach(btn => btn.addEventListener('click', () => {
            const value = btn.dataset.sheetPick;
            if (_sheetSubviewKey === 'sort') {
                sortBy = value;
                if (sortBy === 'random') _shuffleSeed = Date.now() % 100000;
                refreshDropdownTrigger(_container, 'sort', SORT_OPTIONS, () => sortBy);
                _syncUrl();
                _applyFiltersAndRender();
            } else if (_sheetSubviewKey === 'decade') {
                decade = value;
                year = null;
                document.getElementById('browseDecadeCarousel').innerHTML = _decadeCarouselHtml();
                _wireCarousel(_container);
                refreshDropdownTrigger(_container, 'decade', DECADE_OPTIONS, () => decade);
                _syncUrl();
                _reloadAndRender();
            } else if (_sheetSubviewKey === 'genre') {
                genreFilter = value;
                if (_genreSelect) _genreSelect.setValue(value, true);
                _syncUrl();
                _reloadAndRender();
            } else if (_sheetSubviewKey === 'type') {
                typeFilter = value;
                refreshDropdownTrigger(_container, 'type', TYPE_OPTIONS, () => typeFilter);
                _syncUrl();
                _reloadAndRender();
            } else if (_sheetSubviewKey === 'soundtrack') {
                soundtrackFilter = value;
                refreshDropdownTrigger(_container, 'soundtrack', () => _soundtrackOptions, () => soundtrackFilter);
                _syncUrl();
                _reloadAndRender();
            }
            _sheetSubviewKey = null;
            _renderFilterSheet();
        }));
        body.querySelectorAll('[data-sheet-cycle]').forEach(btn => btn.addEventListener('click', () => {
            const key = btn.dataset.sheetCycle;
            if (key === 'status') {
                status = _cycle3(status, 'heard', 'unheard');
                refreshDropdownTrigger(_container, 'status', STATUS_OPTIONS, () => status);
            } else if (key === 'owned') {
                ownedFilter = _cycle3(ownedFilter, 'owned', 'unowned');
                refreshDropdownTrigger(_container, 'owned', OWNED_OPTIONS, () => ownedFilter);
            }
            _syncUrl();
            _applyFiltersAndRender();
            _renderFilterSheet();
        }));

        lucide.createIcons({ el: header });
        lucide.createIcons({ el: body });
    }

    function _openFilterSheet() {
        _sheetSubviewKey = null;
        // Appended straight to <body>, not left in the shell markup: the view
        // container gets `transform: translateY(...)` from app.js's mount
        // fade-in animation (left behind at rest by the `forwards` fill mode),
        // and any transform on an ancestor turns `position: fixed` descendants
        // into positioned-relative-to-that-ancestor instead of the viewport --
        // on an 11k-row page that put the sheet thousands of pixels below the
        // fold. views/release.js's art modal sidesteps the same trap the same
        // way.
        let backdrop = document.getElementById('browseFilterBackdrop');
        let sheet = document.getElementById('browseFilterSheet');
        if (!backdrop || !sheet) {
            backdrop = document.createElement('div');
            backdrop.className = 'filter-sheet-backdrop';
            backdrop.id = 'browseFilterBackdrop';
            backdrop.addEventListener('click', _closeFilterSheet);

            sheet = document.createElement('div');
            sheet.className = 'filter-sheet';
            sheet.id = 'browseFilterSheet';
            sheet.innerHTML = `
                <div class="filter-sheet-header" id="browseFilterSheetHeader"></div>
                <div class="filter-sheet-body" id="browseFilterSheetBody"></div>`;

            document.body.appendChild(backdrop);
            document.body.appendChild(sheet);
        }
        backdrop.hidden = false;
        sheet.hidden = false;
        _renderFilterSheet();
    }

    function _closeFilterSheet() {
        const backdrop = document.getElementById('browseFilterBackdrop');
        const sheet = document.getElementById('browseFilterSheet');
        if (backdrop) backdrop.hidden = true;
        if (sheet) sheet.hidden = true;
    }

    // Decade/year/genre/platform are baked into the SQL WHERE clause (see
    // _loadAlbums/_loadArtists), unlike sort/status which just re-slice the
    // already-fetched _rows — those three need a fresh _load() before
    // re-rendering, not just a re-sort of stale rows.
    function _reloadAndRender() {
        _load();
        _applyFiltersAndRender();
    }

    function _decadeCarouselHtml() {
        if (decade === 'all') return '';
        const start = _decadeStart(decade);
        const idx = DECADES.indexOf(decade);
        const prevDecade = idx > 0 ? DECADES[idx - 1] : null;
        const nextDecade = idx < DECADES.length - 1 ? DECADES[idx + 1] : null;
        const years = Array.from({ length: 10 }, (_, i) => start + i);
        return `
            <div class="decade-carousel">
                <button type="button" class="decade-carousel-arrow" id="decadePrev" ${prevDecade ? '' : 'disabled'} aria-label="Previous decade">‹</button>
                <button type="button" class="decade-carousel-year${!year ? ' active' : ''}" data-year="">${decade}</button>
                ${years.map(y => `<button type="button" class="decade-carousel-year${year === y ? ' active' : ''}" data-year="${y}">${y}</button>`).join('')}
                <button type="button" class="decade-carousel-arrow" id="decadeNext" ${nextDecade ? '' : 'disabled'} aria-label="Next decade">›</button>
            </div>`;
    }

    function _renderShell() {
        return `
            <header class="browse-header">
                <h1>Browse</h1>
                <p class="subtitle" id="browseSubtitle"></p>
            </header>

            <div class="browse-mobile-bar">
                <div class="browse-mobile-bar-scroll">
                    <div class="sort-controls">
                        <button class="sort-btn${entityType === 'albums' ? ' active' : ''}" data-type="albums"><i data-lucide="disc-3"></i>Albums</button>
                        <button class="sort-btn${entityType === 'artists' ? ' active' : ''}" data-type="artists"><i data-lucide="mic-2"></i>Artists</button>
                    </div>
                    <div class="sort-controls">
                        <button class="sort-btn${viewMode === 'list' ? ' active' : ''}" data-view="list" title="List"><i data-lucide="layout-list"></i></button>
                        <button class="sort-btn${viewMode === 'poster-sm' ? ' active' : ''}" data-view="poster-sm" title="Small posters"><i data-lucide="grid-3x3"></i></button>
                        <button class="sort-btn${viewMode === 'poster-lg' ? ' active' : ''}" data-view="poster-lg" title="Large posters"><i data-lucide="layout-grid"></i></button>
                    </div>
                </div>
                <button type="button" class="browse-filters-btn" id="browseFiltersBtn">
                    <i data-lucide="sliders-horizontal"></i> Filters
                </button>
            </div>

            <div class="browse-toolbar-desktop">
                <div class="sort-controls">
                    <button class="sort-btn${entityType === 'albums' ? ' active' : ''}" data-type="albums"><i data-lucide="disc-3"></i>Albums</button>
                    <button class="sort-btn${entityType === 'artists' ? ' active' : ''}" data-type="artists"><i data-lucide="mic-2"></i>Artists</button>
                </div>
                ${dropdownHtml('sort', 'Sort by', SORT_OPTIONS, () => sortBy)}
                ${dropdownHtml('decade', 'Decade', DECADE_OPTIONS, () => decade)}
                <select id="browseGenreFilter"><option value="all">All genres</option></select>
                ${entityType === 'albums' ? dropdownHtml('type', 'Type', TYPE_OPTIONS, () => typeFilter) : ''}
                ${entityType === 'albums' ? dropdownHtml('soundtrack', 'Soundtrack', () => _soundtrackOptions, () => soundtrackFilter) : ''}
                ${dropdownHtml('status', 'Status', STATUS_OPTIONS, () => status)}
                ${entityType === 'albums' ? dropdownHtml('owned', 'Collection', OWNED_OPTIONS, () => ownedFilter) : ''}
                <div class="sort-controls">
                    <button class="sort-btn${viewMode === 'list' ? ' active' : ''}" data-view="list" title="List"><i data-lucide="layout-list"></i></button>
                    <button class="sort-btn${viewMode === 'poster-sm' ? ' active' : ''}" data-view="poster-sm" title="Small posters"><i data-lucide="grid-3x3"></i></button>
                    <button class="sort-btn${viewMode === 'poster-lg' ? ' active' : ''}" data-view="poster-lg" title="Large posters"><i data-lucide="layout-grid"></i></button>
                </div>
            </div>

            <div id="browseDecadeCarousel">${_decadeCarouselHtml()}</div>

            <div class="list-with-sidebar browse-list-with-sidebar">
                <section>
                    <ul id="browseGrid"></ul>
                    <div id="browseSentinel" style="height:1px" hidden></div>
                </section>
                <aside class="view-sidebar" id="browseSidebar"></aside>
            </div>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>`;
    }

    function _populateGenreSelect() {
        const cacheRes = _db.exec("SELECT value_json FROM stats_cache WHERE key = 'genresIndex'")[0];
        const genres = cacheRes ? JSON.parse(cacheRes.values[0][0]) : [];
        genres.sort((a, b) => a.name.localeCompare(b.name));
        _genresIndex = genres;

        const el = document.getElementById('browseGenreFilter');
        if (!el || typeof TomSelect === 'undefined') return;
        for (const g of genres) {
            const opt = document.createElement('option');
            opt.value = String(g.id);
            opt.textContent = g.name;
            el.appendChild(opt);
        }
        if (_genreSelect) _genreSelect.destroy();
        _genreSelect = new TomSelect(el, {
            create: false,
            maxOptions: null,
            placeholder: 'All genres',
        });
        _genreSelect.setValue(genreFilter, true);
        _genreSelect.on('change', v => {
            genreFilter = v || 'all';
            _syncUrl();
            _reloadAndRender();
        });
    }

    // ~25 platforms -- small enough (like Sort/Status/Decade) that TomSelect's
    // search box was never load-bearing here the way it is for Genre's ~600
    // entries; using the shared dropdown instead keeps the trigger/panel
    // chrome (sticky header, current-row, scroll-fade) consistent with the
    // rest of the toolbar rather than a fourth visual species. Controllercons
    // icon carried through as pre-built HTML in each option (see
    // _dropdownPanelContentHtml's optional `icon` field) -- same markup the
    // TomSelect render.option/item functions used to build.
    //
    // Platforms are always video-game-specific (release_soundtrack_meta.platform
    // is only ever populated for source_type='video_game' rows), so they're
    // appended as indented children under the "Video Game" row rather than as
    // siblings of Movie/TV Series/Video Game -- picking "Video Game" itself
    // stays platform-agnostic (matches every video-game soundtrack), while a
    // platform row narrows further to that one console.
    function _populateSoundtrackOptions() {
        const res = _db.exec(`
            SELECT DISTINCT platform FROM release_soundtrack_meta
            WHERE source_type = 'video_game' AND platform IS NOT NULL AND platform != 'None'
            ORDER BY platform
        `)[0];
        _platformsList = res ? res.values.map(r => r[0]) : [];
        _soundtrackOptions = SOUNDTRACK_BASE_OPTIONS.concat(
            _platformsList.map(p => {
                const icon = platformIconMarkup(p);
                return {
                    value: `video_game:${p}`,
                    label: platformLabel(p),
                    icon: icon ? `<span class="ts-platform-icon">${icon}</span>` : null,
                    indent: true,
                };
            })
        );
    }

    function _syncUrl() {
        const p = new URLSearchParams();
        p.set('view', 'browse');
        if (entityType !== 'albums') p.set('type', entityType);
        if (sortBy !== 'release-date') p.set('sort', sortBy);
        if (decade !== 'all') p.set('decade', decade);
        if (year) p.set('year', String(year));
        if (genreFilter !== 'all') p.set('genre', genreFilter);
        // 'rtype', not 'type' -- that param name is already the entityType
        // (albums/artists) toggle above.
        if (typeFilter !== 'all') p.set('rtype', typeFilter);
        if (soundtrackFilter !== 'all') p.set('soundtrack', soundtrackFilter);
        if (status !== 'all') p.set('status', status);
        if (entityType === 'albums' && ownedFilter !== 'all') p.set('owned', ownedFilter);
        if (viewMode !== 'poster-lg') p.set('display', viewMode);
        history.replaceState(Object.fromEntries(p), '', `?${p.toString()}`);
    }

    function _setupControls(container) {
        container.querySelectorAll('[data-type]').forEach(btn => btn.addEventListener('click', () => {
            entityType = btn.dataset.type;
            _syncUrl();
            _remount(container);
        }));
        // Display exists in both the mobile bar and the desktop toolbar --
        // match by value across every button sharing the attribute, not by
        // identity with the clicked one, or the other bar's copy goes stale.
        container.querySelectorAll('[data-view]').forEach(btn => btn.addEventListener('click', () => {
            viewMode = btn.dataset.view;
            container.querySelectorAll('[data-view]').forEach(b => b.classList.toggle('active', b.dataset.view === viewMode));
            _syncUrl();
            _render();
        }));
        _wireCarousel(container);

        document.getElementById('browseFiltersBtn')?.addEventListener('click', _openFilterSheet);
        document.getElementById('browseFilterBackdrop')?.addEventListener('click', _closeFilterSheet);
    }

    function _wireCarousel(container) {
        const carousel = document.getElementById('browseDecadeCarousel');
        if (!carousel) return;
        carousel.querySelectorAll('.decade-carousel-year').forEach(btn => btn.addEventListener('click', () => {
            const y = btn.dataset.year;
            year = y ? parseInt(y, 10) : null;
            carousel.querySelectorAll('.decade-carousel-year').forEach(b => b.classList.toggle('active', b === btn));
            _syncUrl();
            _reloadAndRender();
        }));
        const prev = document.getElementById('decadePrev');
        const next = document.getElementById('decadeNext');
        const idx = DECADES.indexOf(decade);
        if (prev) prev.addEventListener('click', () => {
            if (idx > 0) { decade = DECADES[idx - 1]; year = null; carousel.innerHTML = _decadeCarouselHtml(); _wireCarousel(container); _syncUrl(); _reloadAndRender(); }
        });
        if (next) next.addEventListener('click', () => {
            if (idx < DECADES.length - 1) { decade = DECADES[idx + 1]; year = null; carousel.innerHTML = _decadeCarouselHtml(); _wireCarousel(container); _syncUrl(); _reloadAndRender(); }
        });
    }

    function _remount(container) {
        _container = container;
        // Soundtrack options must be populated before _renderShell(): dropdownHtml()
        // bakes the trigger's initial label from _soundtrackOptions at render
        // time, so a stale ("All soundtracks") cache would show through on
        // first paint. Genre doesn't have this problem -- it's still
        // TomSelect, which needs its <select> DOM to exist first, so
        // _populateGenreSelect() stays after the render below.
        if (entityType === 'albums') _populateSoundtrackOptions();
        container.innerHTML = _renderShell();
        _setupControls(container);
        _populateGenreSelect();
        _load();
        _applyFiltersAndRender();
    }

    function mount(container, db, params) {
        _db = db;
        entityType = params.type === 'artists' ? 'artists' : 'albums';
        sortBy = ['discoveries', 'recent', 'plays', 'az', 'release-date', 'random'].includes(params.sort) ? params.sort : 'release-date';
        decade = DECADES.includes(params.decade) ? params.decade : 'all';
        year = params.year && /^\d{4}$/.test(params.year) ? parseInt(params.year, 10) : null;
        genreFilter = params.genre || 'all';
        typeFilter = ['all', 'album', 'ep', 'single'].includes(params.rtype) ? params.rtype : 'all';
        soundtrackFilter = params.soundtrack || 'all';
        status = ['all', 'heard', 'unheard'].includes(params.status) ? params.status : 'all';
        ownedFilter = ['all', 'owned', 'unowned'].includes(params.owned) ? params.owned : 'all';
        viewMode = ['list', 'poster-sm', 'poster-lg'].includes(params.display) ? params.display : 'poster-lg';
        setPageTitle('Browse');
        _ac = new AbortController();
        // Bound once here rather than in _setupControls (which reruns on
        // every entityType toggle via _remount, against the same container
        // node) -- binding there would stack a duplicate listener per toggle.
        setupDropdowns(container, {
            sort: {
                label: 'Sort by', options: SORT_OPTIONS, getValue: () => sortBy,
                onPick: value => {
                    sortBy = value;
                    if (sortBy === 'random') _shuffleSeed = Date.now() % 100000;
                    _syncUrl();
                    _applyFiltersAndRender();
                },
            },
            status: {
                label: 'Status', options: STATUS_OPTIONS, getValue: () => status,
                onPick: value => { status = value; _syncUrl(); _applyFiltersAndRender(); },
            },
            owned: {
                label: 'Collection', options: OWNED_OPTIONS, getValue: () => ownedFilter,
                onPick: value => { ownedFilter = value; _syncUrl(); _applyFiltersAndRender(); },
            },
            decade: {
                label: 'Decade', options: DECADE_OPTIONS, getValue: () => decade,
                onPick: value => {
                    decade = value;
                    year = null;
                    document.getElementById('browseDecadeCarousel').innerHTML = _decadeCarouselHtml();
                    _wireCarousel(container);
                    _syncUrl();
                    _reloadAndRender();
                },
            },
            type: {
                label: 'Type', options: TYPE_OPTIONS, getValue: () => typeFilter,
                onPick: value => { typeFilter = value; _syncUrl(); _reloadAndRender(); },
            },
            soundtrack: {
                label: 'Soundtrack', options: () => _soundtrackOptions, getValue: () => soundtrackFilter,
                onPick: value => { soundtrackFilter = value; _syncUrl(); _reloadAndRender(); },
            },
        }, _ac.signal);
        _remount(container);
    }

    function unmount() {
        _db = null;
        _container = null;
        _rows = [];
        if (_genreSelect) { _genreSelect.destroy(); _genreSelect = null; }
        _ac?.abort();
        _ac = null;
        // Body-appended, not part of the shell markup -- navigating away
        // wouldn't otherwise remove these (see _openFilterSheet).
        document.getElementById('browseFilterBackdrop')?.remove();
        document.getElementById('browseFilterSheet')?.remove();
    }

    return { mount, unmount };
})();
