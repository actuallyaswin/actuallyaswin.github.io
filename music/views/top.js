const ViewTop = (() => {
    let _db = null;
    // 'artists' | 'albums' | 'tracks'
    let entityType = 'artists';
    let sortBy = 'listens';
    // artists only (existing Week/Month/Year/All)
    let range = 'all';
    // List/Tiles item count (existing 10/20/50/100)
    let countLimit = 10;
    // 'list' | 'tiles'
    let viewMode = 'list';
    // albums/tracks only (existing Released filter)
    let releaseYear = 'all';
    // all entity types -- 'all' or a specific calendar year, filtering by
    // when a listen happened (listens.year) rather than release_year. Ported
    // from the standalone Year view this replaced -- unlike releaseYear,
    // this applies to Artists too.
    let listenYear = 'all';
    // aoty_id as string, or 'all'
    let genreFilter = 'all';
    // albums only -- 'all' | 'album' | 'ep' | 'single' (releases.type)
    let formatFilter = 'all';
    let cachedResults = [];

    // Tracks-only virtualized-list state (List mode keeps its existing
    // dedicated UI rather than createWideCard()).
    let _scrollEl = null;
    let _raf = null;
    const ROW_H = 44;
    const BUFFER = 8;
    // setupDropdowns() binds a document-level click listener to close open
    // panels on an outside click -- document persists across navigation, so
    // this needs explicit cleanup in unmount() or it leaks one per visit.
    let _ac = null;
    // Populated once per mount by _populateYearFilter()/_populateGenreFilter()
    // (DB queries), then read by their dropdownHtml() calls in _renderShell()
    // -- same cache-then-render split browse.js uses for Genre/Platform.
    let _yearOptions = [{ value: 'all', label: 'All years' }];
    let _genreOptions = [{ value: 'all', label: 'All genres' }];
    let _listenYearOptions = [{ value: 'all', label: 'All years' }];
    let _minListenYear = null;
    let _maxListenYear = null;

    const COUNT_OPTIONS = [10, 20, 50, 100].map(n => ({ value: n, label: String(n) }));
    // Same value set as browse.js's TYPE_OPTIONS / 'rtype' param, so a URL
    // built by either view means the same thing.
    const FORMAT_OPTIONS = [
        { value: 'all',    label: 'All formats' },
        { value: 'album',  label: 'Album' },
        { value: 'ep',     label: 'EP' },
        { value: 'single', label: 'Single' },
    ];
    const RANGE_OPTIONS = [
        { value: 'this-week', label: 'This Week' }, { value: 'this-month', label: 'This Month' },
        { value: 'this-year', label: 'This Year' }, { value: 'week', label: 'Last Week' },
        { value: 'month', label: 'Last Month' }, { value: 'year', label: 'Last Year' },
        { value: 'all', label: 'All-Time' },
    ];

    const CERT_LABELS = {
        gold:     'Gold — 250+ plays',
        platinum: 'Platinum — 500+ plays',
        diamond:  'Diamond — 1,000+ plays',
    };

    function _rangeStartTs() {
        if (range === 'all') return null;
        const now = Math.floor(Date.now() / 1000);
        if (range === 'week')  return now - 7   * 86400;
        if (range === 'month') return now - 30  * 86400;
        if (range === 'year')  return now - 365 * 86400;
        // Calendar-boundary ranges — start of the current week/month/year in
        // local time, not a rolling N-day window like their 'last-' siblings.
        const d = new Date();
        if (range === 'this-week') {
            const start = new Date(d.getFullYear(), d.getMonth(), d.getDate() - d.getDay());
            return Math.floor(start.getTime() / 1000);
        }
        if (range === 'this-month') {
            const start = new Date(d.getFullYear(), d.getMonth(), 1);
            return Math.floor(start.getTime() / 1000);
        }
        if (range === 'this-year') {
            const start = new Date(d.getFullYear(), 0, 1);
            return Math.floor(start.getTime() / 1000);
        }
        return null;
    }

    // Genre filter — shared by all three entity types. `releaseIdExpr` is the
    // SQL expression for "this row's release_id" (differs per entity).
    function _genreFilterSql(releaseIdExpr) {
        const gid = parseInt(genreFilter);
        if (genreFilter === 'all' || isNaN(gid)) return '';
        return `AND ${releaseIdExpr} IN (SELECT release_id FROM release_genres WHERE aoty_genre_id = ${gid})`;
    }

    // "Listened In" year — filters by when a listen happened (l.year), not
    // by any entity's own release/formed year. Available to every entity
    // type, unlike releaseYear (albums/tracks only) — an artist has no
    // single release year of its own to filter by.
    function _listenYearSql() {
        const ly = parseInt(listenYear);
        if (listenYear === 'all' || isNaN(ly)) return '';
        return `AND l.year = ${ly}`;
    }

    // Each entry: { title, sortOptions, hasRange, hasYearFilter, query(),
    // cardHref(id), buildCardFields(row) }
    const ENTITY_CONFIG = {
        artists: {
            title: 'Top Artists',
            sortOptions: [
                { key: 'listens',     icon: 'headphones', label: 'Listens',     title: 'Sort by listens' },
                { key: 'minutes',     icon: 'clock',       label: 'Minutes',     title: 'Sort by minutes' },
                { key: 'discoveries', icon: 'sparkles',    label: 'Discoveries', title: 'Latest discoveries — artists with newest average listen date' },
                { key: 'oldies',      icon: 'history',     label: 'Oldies',      title: 'Golden oldies — artists with oldest average listen date' },
            ],
            hasRange: true,
            hasYearFilter: false,
            hasGenreFilter: true,
            query() {
                const isTemporalSort = sortBy === 'discoveries' || sortBy === 'oldies';
                const startTs = isTemporalSort ? null : _rangeStartTs();
                const tsFilter = startTs ? `AND l.timestamp >= ${startTs}` : '';
                document.getElementById('rangeControlBlock')?.classList.toggle('controls-dimmed', isTemporalSort);

                let orderClause;
                if (sortBy === 'minutes')     orderClause = 'total_minutes DESC';
                else if (sortBy === 'discoveries') orderClause = 'avg_ts DESC';
                else if (sortBy === 'oldies')      orderClause = 'avg_ts ASC';
                else                               orderClause = 'total_listens DESC';
                const genreClause = _genreFilterSql('t.release_id');

                return _db.exec(`
                    SELECT
                        a.id, a.name,
                        COALESCE(a.image_thumb_url, a.image_url) as image_url,
                        a.cert,
                        COUNT(DISTINCT CASE WHEN t.hidden = 0 AND l.id IS NOT NULL THEN t.id END) as unique_tracks,
                        COUNT(CASE WHEN t.hidden = 0 THEN l.id END) as total_listens,
                        CAST(SUM(CASE WHEN t.hidden = 0 AND l.id IS NOT NULL THEN COALESCE(t.duration_ms, 0) ELSE 0 END) / 60000.0 AS INTEGER) as total_minutes,
                        CAST(AVG(CASE WHEN t.hidden = 0 THEN l.timestamp END) AS INTEGER) as avg_ts,
                        a.slug
                    FROM artists a
                    LEFT JOIN track_artists ta ON a.id = ta.artist_id AND ta.role IN (${PRIMARY_ROLES_SQL})
                    LEFT JOIN tracks t ON ta.track_id = t.id ${genreClause}
                    LEFT JOIN listens l ON t.id = l.track_id ${tsFilter}
                    WHERE a.hidden = 0 ${_listenYearSql()}
                    GROUP BY a.id
                    HAVING total_listens > 0
                    ORDER BY ${orderClause}
                    LIMIT 100
                `)[0];
            },
            cardHref: f => artistHref(f.id, f.slug),
            buildCardFields(row) {
                const [id, name, imageUrl, cert, uniqueTracks, totalListens, totalMinutes, avgTs, slug] = row;
                return { id, name, imageUrl, cert, meta2: uniqueTracks, totalListens, totalMinutes, avgTs, label: name, slug };
            },
        },
        albums: {
            title: 'Top Albums',
            sortOptions: [
                { key: 'listens',     icon: 'headphones', label: 'Listens',     title: 'Sort by listens' },
                { key: 'minutes',     icon: 'clock',       label: 'Minutes',     title: 'Sort by minutes' },
                { key: 'discoveries', icon: 'sparkles',    label: 'Discoveries', title: 'Latest discoveries — albums with newest average listen date' },
                { key: 'oldies',      icon: 'history',     label: 'Oldies',      title: 'Golden oldies — albums with oldest average listen date' },
            ],
            hasRange: false,
            hasYearFilter: true,
            hasGenreFilter: true,
            hasFormatFilter: true,
            query() {
                let orderClause;
                if (sortBy === 'minutes')          orderClause = 'total_minutes DESC';
                else if (sortBy === 'discoveries') orderClause = 'avg_ts DESC';
                else if (sortBy === 'oldies')      orderClause = 'avg_ts ASC';
                else                                orderClause = 'total_listens DESC';
                const yearInt = parseInt(releaseYear);
                const yearFilter = releaseYear !== 'all' && !isNaN(yearInt) ? `AND r.release_year = ${yearInt}` : '';
                const genreClause = _genreFilterSql('r.id');
                const formatClause = ['album', 'ep', 'single'].includes(formatFilter) ? `AND r.type = '${formatFilter}'` : '';

                return _db.exec(`
                    SELECT
                        r.id, r.title, r.release_year, r.type,
                        COALESCE(r.album_art_thumb_url, r.album_art_url) as album_art_url,
                        a.name, a.id as artist_id,
                        COUNT(DISTINCT CASE WHEN t.hidden = 0 AND l.id IS NOT NULL THEN t.id END) as tracks_listened,
                        COUNT(CASE WHEN t.hidden = 0 THEN l.id END) as total_listens,
                        CAST(SUM(CASE WHEN t.hidden = 0 AND l.id IS NOT NULL THEN COALESCE(t.duration_ms, 0) ELSE 0 END) / 60000.0 AS INTEGER) as total_minutes,
                        r.stat_avg_listen_ts as avg_ts,
                        r.slug
                    FROM releases r
                    LEFT JOIN artists a ON a.id = r.primary_artist_id
                    LEFT JOIN tracks t ON t.release_id = r.id
                    LEFT JOIN listens l ON l.track_id = t.id
                    WHERE r.hidden = 0 AND (a.id IS NULL OR a.hidden = 0) ${yearFilter} ${genreClause} ${formatClause} ${_listenYearSql()}
                    GROUP BY r.id
                    HAVING total_listens > 0
                    ORDER BY ${orderClause}
                    LIMIT 100
                `)[0];
            },
            cardHref: f => releaseHref(f.id, f.slug),
            buildCardFields(row) {
                const [id, title, year, type, albumArtUrl, artistName, artistId, tracksListened, totalListens, totalMinutes, avgTs, slug] = row;
                return {
                    id, title, name: title, imageUrl: albumArtUrl, artistName: artistName || 'Various Artists',
                    meta: `${escapeHtml(artistName || 'Various Artists')} · ${year || 'Unknown'}`,
                    totalListens, totalMinutes, avgTs, label: title, slug,
                };
            },
        },
        tracks: {
            title: 'Top Tracks',
            sortOptions: [
                { key: 'listens',     icon: 'headphones', label: 'Listens',     title: 'Sort by listens' },
                { key: 'minutes',     icon: 'clock',       label: 'Minutes',     title: 'Sort by minutes' },
                { key: 'discoveries', icon: 'sparkles',    label: 'Discoveries', title: 'Latest discoveries — tracks with newest average listen date' },
                { key: 'oldies',      icon: 'history',     label: 'Oldies',      title: 'Golden oldies — tracks with oldest average listen date' },
            ],
            hasRange: false,
            hasYearFilter: true,
            hasGenreFilter: true,
            query() {
                let orderClause;
                if (sortBy === 'minutes')          orderClause = 'total_minutes DESC';
                else if (sortBy === 'discoveries') orderClause = 'avg_ts DESC';
                else if (sortBy === 'oldies')      orderClause = 'avg_ts ASC';
                else                                orderClause = 'total_listens DESC';
                const yearInt = parseInt(releaseYear);
                const yf = releaseYear !== 'all' && !isNaN(yearInt) ? `AND r.release_year = ${yearInt}` : '';
                const genreClause = _genreFilterSql('t.release_id');

                return _db.exec(`
                    SELECT t.id, t.title,
                           (SELECT a2.name FROM track_artists ta2 JOIN artists a2 ON a2.id = ta2.artist_id
                            WHERE ta2.track_id = t.id AND ta2.role IN (${PRIMARY_ROLES_SQL})
                            ORDER BY CASE ta2.role WHEN 'main' THEN 0 ELSE 1 END, a2.name LIMIT 1) as artist_name,
                           (SELECT a2.id FROM track_artists ta2 JOIN artists a2 ON a2.id = ta2.artist_id
                            WHERE ta2.track_id = t.id AND ta2.role IN (${PRIMARY_ROLES_SQL})
                            ORDER BY CASE ta2.role WHEN 'main' THEN 0 ELSE 1 END, a2.name LIMIT 1) as artist_id,
                           COALESCE(r.album_art_thumb_url, r.album_art_url),
                           r.id,
                           COUNT(l.id) total_listens,
                           CAST(SUM(COALESCE(t.duration_ms,0))/60000.0 AS INTEGER) total_minutes,
                           t.stat_avg_listen_ts as avg_ts,
                           r.slug
                    FROM tracks t
                    LEFT JOIN releases r ON t.release_id = r.id
                    LEFT JOIN listens l ON t.id = l.track_id
                    WHERE t.hidden = 0 ${yf} ${genreClause} ${_listenYearSql()}
                    GROUP BY t.id
                    HAVING total_listens > 0
                    ORDER BY ${orderClause}
                    LIMIT 5000
                `)[0];
            },
            cardHref: f => f.releaseId ? releaseHref(f.releaseId, f.releaseSlug) : '#',
            buildCardFields(row) {
                const [id, title, artistName, artistId, art, releaseId, totalListens, totalMinutes, avgTs, releaseSlug] = row;
                return { id, title, name: title, artistName, imageUrl: art, releaseId, totalListens, totalMinutes, avgTs, label: title, releaseSlug };
            },
        },
    };

    // setupDropdowns() is called once per mount(), against the persistent
    // #view-container node -- _rerenderForModeChange()/entityType toggles
    // only replace its innerHTML, so binding this inside those functions
    // instead would stack a duplicate delegated listener per toggle (same
    // class of bug browse.js hit with its own dropdowns).
    function _dropdownSpecs() {
        return {
            range: {
                label: 'Range', options: RANGE_OPTIONS, getValue: () => range,
                onPick: value => { range = value; _syncUrl(); _load(); },
            },
            count: {
                label: 'Count', options: COUNT_OPTIONS, getValue: () => countLimit,
                onPick: value => { countLimit = parseInt(value, 10); _syncUrl(); _applyCount(); },
            },
            year: {
                label: 'Released', options: () => _yearOptions, getValue: () => releaseYear,
                onPick: value => { releaseYear = value; _syncUrl(); _load(); },
            },
            genre: {
                label: 'Genre', options: () => _genreOptions, getValue: () => genreFilter,
                onPick: value => { genreFilter = value; _syncUrl(); _load(); },
            },
            listenYear: {
                label: 'Listened In', options: () => _listenYearOptions, getValue: () => listenYear,
                onPick: value => { listenYear = value; _syncUrl(); _refreshListenYearNav(); _load(); },
            },
            format: {
                label: 'Format', options: FORMAT_OPTIONS, getValue: () => formatFilter,
                onPick: value => { formatFilter = value; _syncUrl(); _load(); },
            },
        };
    }

    function mount(container, db, params) {
        _db = db;
        entityType = ['artists', 'albums', 'tracks'].includes(params.type) ? params.type : 'artists';
        setPageTitle(ENTITY_CONFIG[entityType]?.title || 'Top');

        // Restore remaining state from URL params
        const cfg = ENTITY_CONFIG[entityType];
        if (cfg && params.sort && cfg.sortOptions.some(o => o.key === params.sort)) sortBy = params.sort;
        else sortBy = 'listens';
        if (params.range && ['this-week','this-month','this-year','week','month','year','all'].includes(params.range)) range = params.range;
        if (params.count && [10,20,50,100].includes(+params.count)) countLimit = +params.count;
        if (params.display && ['list','tiles'].includes(params.display)) viewMode = params.display;
        if (params.year) releaseYear = params.year;
        if (params.ly) listenYear = params.ly;
        if (params.genre) genreFilter = params.genre;
        if (['all', 'album', 'ep', 'single'].includes(params.rtype)) formatFilter = params.rtype;

        // Must run before _renderShell(): dropdownHtml() bakes the trigger's
        // initial label from _yearOptions/_genreOptions at render time, so a
        // stale (default "All ...") cache would show through until some
        // other action forced a re-render.
        if (ENTITY_CONFIG[entityType].hasYearFilter) _populateYearFilter();
        _populateListenYearFilter();
        if (ENTITY_CONFIG[entityType].hasGenreFilter) _populateGenreFilter();
        container.innerHTML = _renderShell();
        _ac = new AbortController();
        setupDropdowns(container, _dropdownSpecs(), _ac.signal);
        _setupControls();
        _load();
    }

    function unmount() {
        if (_raf) { cancelAnimationFrame(_raf); _raf = null; }
        _scrollEl = null;
        _ac?.abort();
        _ac = null;
    }

    function _renderShell() {
        const primaryControls = `
            ${_entityToggleHtml()}
            ${_sortControlsHtml()}
            ${ENTITY_CONFIG[entityType]?.hasRange ? _rangeControlsHtml() : ''}
            ${_countControlsHtml()}
            ${ENTITY_CONFIG[entityType]?.hasYearFilter ? _yearFilterHtml() : ''}
            ${_listenYearFilterHtml()}
            ${ENTITY_CONFIG[entityType]?.hasGenreFilter ? _genreFilterHtml() : ''}
            ${ENTITY_CONFIG[entityType]?.hasFormatFilter ? _formatFilterHtml() : ''}
            ${_displayControlsHtml()}
        `;
        return `
            <header>
                <h1>${ENTITY_CONFIG[entityType]?.title || 'Top'}</h1>
                <p class="subtitle" id="topSubtitle"></p>
            </header>
            <div class="page-controls">${primaryControls}</div>
            <div id="topContainer" class="image-grid">
                ${renderLoading()}
            </div>
            <section class="year-section" id="listenYearGenresSection" style="display:none">
                <h2 id="listenYearGenresTitle">Top Genres</h2>
                <div id="listenYearGenres" class="genre-list"></div>
            </section>
            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;
    }

    function _entityToggleHtml() {
        return `
            <div class="control-block">
                <span class="control-block-label">Type</span>
                <div class="sort-controls">
                    <button class="sort-btn${entityType === 'artists' ? ' active' : ''}" data-type="artists" title="Artists"><i data-lucide="mic-2"></i>Artists</button>
                    <button class="sort-btn${entityType === 'albums'  ? ' active' : ''}" data-type="albums"  title="Albums"><i data-lucide="disc-3"></i>Albums</button>
                    <button class="sort-btn${entityType === 'tracks'  ? ' active' : ''}" data-type="tracks"  title="Tracks"><i data-lucide="music"></i>Tracks</button>
                </div>
            </div>`;
    }

    function _sortControlsHtml() {
        const cfg = ENTITY_CONFIG[entityType];
        if (!cfg) return '';
        return `
            <div class="control-block">
                <span class="control-block-label">Sort By</span>
                <div class="sort-controls">
                    ${cfg.sortOptions.map(o => `
                        <button class="sort-btn${sortBy === o.key ? ' active' : ''}" data-sort="${o.key}" title="${o.title}"><i data-lucide="${o.icon}"></i>${o.label}</button>
                    `).join('')}
                </div>
            </div>`;
    }

    function _rangeControlsHtml() {
        return `
            <div class="control-block" id="rangeControlBlock">
                <span class="control-block-label">Range</span>
                <div class="sort-controls">
                    ${dropdownHtml('range', 'Range', RANGE_OPTIONS, () => range)}
                </div>
            </div>`;
    }

    function _countControlsHtml() {
        return `
            <div class="control-block">
                <span class="control-block-label">#</span>
                <div class="sort-controls">
                    ${dropdownHtml('count', 'Count', COUNT_OPTIONS, () => countLimit)}
                </div>
            </div>`;
    }

    function _yearFilterHtml() {
        return `
            <div class="control-block">
                <span class="control-block-label">Released</span>
                <div class="sort-controls">
                    ${dropdownHtml('year', 'Released', _yearOptions, () => releaseYear)}
                </div>
            </div>`;
    }

    // Applies to every entity type (unlike Released, which is albums/tracks
    // only) — ported from views/year.js's prev/next arrows + <select>, now a
    // dropdown + arrow pair to match this page's own filter styling.
    function _listenYearFilterHtml() {
        const ly = parseInt(listenYear);
        const atMin = listenYear !== 'all' && !isNaN(ly) && ly <= _minListenYear;
        const atMax = listenYear !== 'all' && !isNaN(ly) && ly >= _maxListenYear;
        return `
            <div class="control-block">
                <span class="control-block-label">Listened In</span>
                <div class="sort-controls">
                    <button class="year-nav-arrow" id="listenYearPrev" aria-label="Previous year" title="Previous year"${listenYear === 'all' || atMin ? ' disabled' : ''}>←</button>
                    ${dropdownHtml('listenYear', 'Listened In', _listenYearOptions, () => listenYear)}
                    <button class="year-nav-arrow" id="listenYearNext" aria-label="Next year" title="Next year"${listenYear === 'all' || atMax ? ' disabled' : ''}>→</button>
                </div>
            </div>`;
    }

    function _genreFilterHtml() {
        return `
            <div class="control-block">
                <span class="control-block-label">Genre</span>
                <div class="sort-controls">
                    ${dropdownHtml('genre', 'Genre', _genreOptions, () => genreFilter)}
                </div>
            </div>`;
    }

    function _formatFilterHtml() {
        return `
            <div class="control-block">
                <span class="control-block-label">Format</span>
                <div class="sort-controls">
                    ${dropdownHtml('format', 'Format', FORMAT_OPTIONS, () => formatFilter)}
                </div>
            </div>`;
    }

    function _displayControlsHtml() {
        return `
            <div class="control-block">
                <span class="control-block-label">Display</span>
                <div class="sort-controls">
                    <button class="sort-btn${viewMode === 'list'    ? ' active' : ''}" data-view="list"    title="List"><i data-lucide="layout-list"></i>List</button>
                    <button class="sort-btn${viewMode === 'tiles'   ? ' active' : ''}" data-view="tiles"   title="Tiles"><i data-lucide="layout-grid"></i>Tiles</button>
                    <button class="sort-btn" id="makeCollageBtn" title="Make a collage"><i data-lucide="grid-3x3"></i>Collage</button>
                </div>
            </div>`;
    }

    function _syncUrl() {
        const p = new URLSearchParams({ view: 'top', type: entityType, sort: sortBy, count: countLimit, display: viewMode });
        if (ENTITY_CONFIG[entityType]?.hasRange) p.set('range', range);
        if (ENTITY_CONFIG[entityType]?.hasYearFilter) p.set('year', releaseYear);
        if (listenYear !== 'all') p.set('ly', listenYear);
        if (ENTITY_CONFIG[entityType]?.hasGenreFilter) p.set('genre', genreFilter);
        if (ENTITY_CONFIG[entityType]?.hasFormatFilter) p.set('rtype', formatFilter);
        history.replaceState(Object.fromEntries(p), '', '?' + p.toString());
    }

    function _setupControls() {
        setupToggleGroup('[data-type]', btn => {
            entityType = btn.dataset.type;
            sortBy = 'listens';
            releaseYear = 'all';
            genreFilter = 'all';
            formatFilter = 'all';
            _syncUrl();
            // tear down tracks' virtualized-scroll listener/RAF before switching away
            unmount();
            mount(document.getElementById('view-container'), _db, Object.fromEntries(new URLSearchParams(location.search)));
        });
        setupToggleGroup('[data-sort]', btn => { sortBy = btn.dataset.sort; _syncUrl(); _load(); });
        setupToggleGroup('[data-view]', btn => { viewMode = btn.dataset.view; _syncUrl(); _rerenderForModeChange(); });

        document.getElementById('makeCollageBtn')?.addEventListener('click', () => {
            const cfg = ENTITY_CONFIG[entityType];
            setCollagePayload({
                title: cfg.title,
                filenamePrefix: `top-${entityType}`,
                backHref: `?${new URLSearchParams(location.search).toString()}`,
                cards: cachedResults.map(f => ({
                    href: cfg.cardHref(f),
                    imageUrl: f.imageUrl,
                    label: f.label || f.name || f.title || '',
                    artistName: f.artistName || null,
                })),
            });
            navigate({ view: 'collage' });
        });

        document.getElementById('listenYearPrev')?.addEventListener('click', () => {
            const ly = parseInt(listenYear);
            if (isNaN(ly) || ly <= _minListenYear) return;
            listenYear = String(ly - 1);
            _syncUrl();
            _refreshListenYearNav();
            _load();
        });
        document.getElementById('listenYearNext')?.addEventListener('click', () => {
            const ly = parseInt(listenYear);
            if (isNaN(ly) || ly >= _maxListenYear) return;
            listenYear = String(ly + 1);
            _syncUrl();
            _refreshListenYearNav();
            _load();
        });
    }

    function _rerenderForModeChange() {
        // Re-render the whole shell on a List/Tiles Display switch.
        // Tear down tracks' virtualized-scroll listener/RAF first — the shell rebuild below replaces
        // #ttScroll, orphaning the old listener/RAF if left running.
        if (_raf) { cancelAnimationFrame(_raf); _raf = null; }
        _scrollEl = null;
        const container = document.getElementById('view-container');
        if (container) {
            // Populate before rendering, same reason as mount() -- dropdownHtml()
            // bakes the trigger's label from the cache at render time.
            if (ENTITY_CONFIG[entityType].hasYearFilter) _populateYearFilter();
            _populateListenYearFilter();
            if (ENTITY_CONFIG[entityType].hasGenreFilter) _populateGenreFilter();
            container.innerHTML = _renderShell();
            _setupControls();
            _load();
        }
    }

    function _populateYearFilter() {
        const res = _db.exec(`
            SELECT DISTINCT release_year FROM releases
            WHERE release_year IS NOT NULL AND hidden = 0
            ORDER BY release_year DESC
        `)[0];
        _yearOptions = [{ value: 'all', label: 'All years' }]
            .concat(res ? res.values.map(([yr]) => ({ value: String(yr), label: String(yr) })) : []);
    }

    // Genres index is precomputed by `mdb.py stats refresh` (see views/genres.js) —
    // reuse it here rather than re-running the release_genres/tracks/listens join.
    function _populateGenreFilter() {
        const res = _db.exec("SELECT value_json FROM stats_cache WHERE key = 'genresIndex'")[0];
        const rows = res ? JSON.parse(res.values[0][0]) : [];
        rows.sort((a, b) => b.plays - a.plays);
        _genreOptions = [{ value: 'all', label: 'All genres' }]
            .concat(rows.map(({ id, name }) => ({ value: String(id), label: name })));
    }

    // Ported from views/year.js's MIN_YEAR/MAX_YEAR — min/max only need
    // computing once per page load, not once per mount.
    function _populateListenYearFilter() {
        if (_minListenYear === null) {
            const res = _db.exec(`SELECT MIN(year), MAX(year) FROM listens WHERE year IS NOT NULL`)[0];
            if (res && res.values[0][0] !== null) {
                _minListenYear = res.values[0][0];
                _maxListenYear = res.values[0][1];
            } else {
                _minListenYear = 1960;
                _maxListenYear = new Date().getFullYear();
            }
        }
        _listenYearOptions = [{ value: 'all', label: 'All years' }];
        for (let y = _maxListenYear; y >= _minListenYear; y--) {
            _listenYearOptions.push({ value: String(y), label: String(y) });
        }
    }

    function _refreshListenYearNav() {
        const container = document.getElementById('view-container');
        if (container) refreshDropdownTrigger(container, 'listenYear', _listenYearOptions, () => listenYear);
        const ly = parseInt(listenYear);
        const prev = document.getElementById('listenYearPrev');
        const next = document.getElementById('listenYearNext');
        if (prev) prev.disabled = listenYear === 'all' || isNaN(ly) || ly <= _minListenYear;
        if (next) next.disabled = listenYear === 'all' || isNaN(ly) || ly >= _maxListenYear;
    }

    // Genre breakdown for the selected listen year — ported from
    // views/year.js's loadYearGenres(). Only meaningful once a specific
    // year is picked (an "All years" breakdown is just the Genres view).
    function _loadListenYearGenres() {
        const section = document.getElementById('listenYearGenresSection');
        const el = document.getElementById('listenYearGenres');
        if (!section || !el) return;
        const ly = parseInt(listenYear);
        if (listenYear === 'all' || isNaN(ly)) { section.style.display = 'none'; return; }

        const result = _db.exec(`
            SELECT g.aoty_id, g.name, COUNT(*) as listen_count
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN release_genres rg ON t.release_id = rg.release_id AND rg.is_primary = 1
            JOIN genres g ON rg.aoty_genre_id = g.aoty_id
            WHERE l.year = ${ly} AND t.hidden = 0
            GROUP BY g.aoty_id
            ORDER BY listen_count DESC
            LIMIT 10
        `)[0];

        if (!result || result.values.length === 0) { section.style.display = 'none'; return; }
        const title = document.getElementById('listenYearGenresTitle');
        if (title) title.textContent = `Top Genres Listened In ${ly}`;
        el.innerHTML = result.values.map(([id, name, count]) =>
            `<a href="?view=genre&id=${id}" class="genre-tag">${escapeHtml(name)}</a> <span class="genre-year-count">(${formatNumber(count)})</span>`
        ).join(', ');
        section.style.display = '';
    }

    function _load() {
        const result = ENTITY_CONFIG[entityType].query();
        cachedResults = result ? result.values.map(ENTITY_CONFIG[entityType].buildCardFields) : [];
        // only set for the tracks virtualized list
        if (_scrollEl) _scrollEl.scrollTop = 0;
        const subtitleEl = document.getElementById('topSubtitle');
        if (subtitleEl) subtitleEl.textContent = `${formatNumber(cachedResults.length)} ${entityType}`;
        _render();
        _loadListenYearGenres();
    }

    function _render() {
        if (entityType === 'tracks' && viewMode === 'list') return _renderTrackList();
        return _renderListOrTiles();
    }

    function _renderListOrTiles() {
        const container = document.getElementById('topContainer');
        if (!container) return;
        container.innerHTML = '';
        container.style.gridTemplateColumns = '';

        if (cachedResults.length === 0) {
            container.className = 'image-grid';
            const icon = entityType === 'artists' ? 'mic-2' : entityType === 'tracks' ? 'music' : 'disc-3';
            container.innerHTML = `<li>${renderEmptyState(`No ${entityType} found`, 'Try a different filter, range, or year.', icon)}</li>`;
            return;
        }

        const cfg = ENTITY_CONFIG[entityType];
        const isTemporalSort = sortBy === 'discoveries' || sortBy === 'oldies';

        if (viewMode === 'list') {
            // Same "ranked list + summary sidebar" shell as Top Tracks/History,
            // rather than a bare full-width grid — every ranked list view
            // gets the same right-rail summary.
            container.className = '';
            container.innerHTML = `
                <div class="list-with-sidebar">
                    <ul class="wide-grid" id="topListGrid"></ul>
                    <aside class="view-sidebar" id="topSidebar"></aside>
                </div>`;
            const listEl = document.getElementById('topListGrid');
            cachedResults.forEach((f, i) => {
                let meta;
                if (isTemporalSort && f.avgTs) {
                    const d = new Date(f.avgTs * 1000);
                    meta = `avg. ${d.toLocaleString('en-US', { month: 'short', year: 'numeric' })}`;
                } else {
                    meta = f.meta || (f.meta2 != null ? `${formatNumber(f.meta2)} tracks` : '');
                }
                const card = createWideCard({
                    href: cfg.cardHref(f),
                    imageUrl: f.imageUrl,
                    name: f.name || f.title,
                    meta,
                    totalListens: f.totalListens,
                    totalMinutes: f.totalMinutes,
                    rounded: entityType === 'artists',
                    cert: f.cert || null,
                });
                const li = document.createElement('li');
                if (i >= countLimit) li.style.display = 'none';
                li.appendChild(card);
                if (entityType === 'artists') {
                    const compareLink = document.createElement('a');
                    compareLink.className = 'row-compare-link';
                    compareLink.href = `?view=compare&a=${encodeURIComponent(f.id)}`;
                    compareLink.innerHTML = `<i data-lucide="git-compare"></i> Compare`;
                    li.appendChild(compareLink);
                }
                listEl.appendChild(li);
            });
            _renderListSidebar();
        } else {
            container.className = '';
            container.innerHTML = '<ul class="image-grid" id="topImageGrid"></ul>';
            const gridEl = document.getElementById('topImageGrid');
            cachedResults.forEach((f, i) => {
                const card = createImageCard({
                    href: cfg.cardHref(f),
                    imageUrl: f.imageUrl,
                    title: f.name || f.title || '',
                    subtitle: f.artistName || null,
                    totalListens: f.totalListens,
                    totalMinutes: f.totalMinutes
                });
                const li = document.createElement('li');
                if (i >= countLimit) li.style.display = 'none';
                li.appendChild(card);
                gridEl.appendChild(li);
            });
        }
        lucide.createIcons();
    }

    // Summary sidebar for Top Artists/Top Albums list mode — same shape as
    // Top Tracks' sidebar (_renderTrackSidebar) and History's, just fed from
    // cachedResults instead of a virtualized row set. Scoped to the visible
    // (countLimit-limited) slice, since Count actually hides cards here.
    function _renderListSidebar() {
        const el = document.getElementById('topSidebar');
        if (!el) return;
        const visible = cachedResults.slice(0, countLimit);

        const totalPlays = visible.reduce((s, f) => s + (f.totalListens || 0), 0);
        const totalMins  = visible.reduce((s, f) => s + (f.totalMinutes || 0), 0);
        const avgPlays   = visible.length ? Math.round(totalPlays / visible.length) : 0;

        const summaryRows = [
            [ENTITY_CONFIG[entityType].title.replace('Top ', ''), visible.length.toLocaleString()],
            ['Total plays',    formatNumber(totalPlays)],
            ['Listening time', `${Math.round(totalMins / 60).toLocaleString()} hr`],
            ['Avg plays',      formatNumber(avgPlays)],
        ];

        // Each album card carries an artist name — worth a "Top Artists"
        // breakdown, same as Tracks. Artists list has no comparable
        // secondary dimension, so it stays Summary-only.
        let secondary = '';
        if (entityType === 'albums') {
            const artistCount = {};
            visible.forEach(f => { if (f.artistName) artistCount[f.artistName] = (artistCount[f.artistName] || 0) + 1; });
            const topArtists = Object.entries(artistCount).sort(([,a],[,b]) => b-a).slice(0, 7);
            if (topArtists.length) {
                secondary = `
                    <div class="sidebar-section">
                        <p class="sidebar-heading">Top Artists</p>
                        ${topArtists.map(([name, count], i) => `
                            <div class="sidebar-row">
                                <span class="track-rank">${i + 1}</span>
                                <span class="sidebar-row-name">${escapeHtml(name)}</span>
                                <span class="sidebar-row-count">${count} album${count === 1 ? '' : 's'}</span>
                            </div>`).join('')}
                    </div>`;
            }
        }

        el.innerHTML = `
            <div class="sidebar-section">
                <p class="sidebar-heading">Summary</p>
                <dl class="nerds-list" style="border:none;border-radius:0">
                    ${summaryRows.map(([k,v]) => `<div class="nerds-row"><dt>${k}</dt><dd>${v}</dd></div>`).join('')}
                </dl>
            </div>
            ${secondary}`;
    }

    function _applyCount() {
        const listGrid = document.getElementById('topListGrid');
        // Tile mode nests its cards inside #topImageGrid (see _renderListOrTiles)
        // so its <li> children, not #topContainer's single <ul> child, are what
        // Count needs to hide/show.
        const container = listGrid || document.getElementById('topImageGrid') || document.getElementById('topContainer');
        if (!container) return;
        Array.from(container.children).forEach((el, i) => {
            el.style.display = i < countLimit ? '' : 'none';
        });
        if (listGrid) _renderListSidebar();
    }

    function _renderTrackList() {
        const container = document.getElementById('topContainer');
        if (!container) return;
        container.className = '';
        container.innerHTML = `
            <div class="list-with-sidebar">
                <div class="list-scroll" id="ttScroll">
                    <div id="ttSpacerTop" style="height:0"></div>
                    <div id="ttList"></div>
                    <div id="ttSpacerBot" style="height:0"></div>
                </div>
                <aside class="view-sidebar" id="ttSidebar"></aside>
            </div>`;

        _scrollEl = document.getElementById('ttScroll');
        requestAnimationFrame(() => {
            const top = _scrollEl.getBoundingClientRect().top;
            _scrollEl.style.height = `${window.innerHeight - top - 16}px`;
        });
        _scrollEl.addEventListener('scroll', _scheduleTrackRender, { passive: true });
        _renderTrackRows();
        _renderTrackSidebar();
    }

    function _scheduleTrackRender() {
        if (_raf) cancelAnimationFrame(_raf);
        _raf = requestAnimationFrame(() => { _raf = null; _renderTrackRows(); });
    }

    function _renderTrackRows() {
        if (!_scrollEl) return;
        const scrollTop = _scrollEl.scrollTop;
        const start = Math.max(0, Math.floor(scrollTop / ROW_H) - BUFFER);
        const end   = Math.min(cachedResults.length, start + Math.ceil(_scrollEl.clientHeight / ROW_H) + BUFFER * 2);

        const list = document.getElementById('ttList');
        list.innerHTML = '';
        for (let i = start; i < end; i++) list.appendChild(_buildTrackRow(cachedResults[i], i));

        document.getElementById('ttSpacerTop').style.height = `${start * ROW_H}px`;
        document.getElementById('ttSpacerBot').style.height = `${(cachedResults.length - end) * ROW_H}px`;
    }

    function _buildTrackRow(f, rank) {
        const el = document.createElement('a');
        el.className = 'recent-play-row';
        el.href = ENTITY_CONFIG.tracks.cardHref(f);
        el.style.height = ROW_H + 'px';
        el.style.boxSizing = 'border-box';

        const isTemporalSort = sortBy === 'discoveries' || sortBy === 'oldies';
        const stat = isTemporalSort && f.avgTs
            ? new Date(f.avgTs * 1000).toLocaleString('en-US', { month: 'short', year: 'numeric' })
            : (sortBy === 'minutes' ? `${formatNumber(f.totalMinutes)} min` : `${formatNumber(f.totalListens)} plays`);

        el.innerHTML = `
            <span class="track-rank">${rank + 1}</span>
            <div class="recent-play-thumb" style="background-image:url('${cssUrl(f.imageUrl || getFallbackImageUrl())}')"></div>
            <div class="recent-play-info">
                <div class="recent-play-name">${escapeHtml(f.title || '')}</div>
                ${f.artistName ? `<div class="recent-play-album">${escapeHtml(f.artistName)}</div>` : ''}
            </div>
            <span class="recent-play-date">${stat}</span>`;
        return el;
    }

    function _renderTrackSidebar() {
        const el = document.getElementById('ttSidebar');
        if (!el) return;

        const totalPlays = cachedResults.reduce((s, f) => s + (f.totalListens || 0), 0);
        const totalMins  = cachedResults.reduce((s, f) => s + (f.totalMinutes || 0), 0);
        const avgPlays   = cachedResults.length ? Math.round(totalPlays / cachedResults.length) : 0;

        const artistCount = {};
        cachedResults.forEach(f => { if (f.artistName) artistCount[f.artistName] = (artistCount[f.artistName] || 0) + 1; });
        const topArtists = Object.entries(artistCount).sort(([,a],[,b]) => b-a).slice(0, 7);

        const summaryRows = [
            ['Tracks',         cachedResults.length.toLocaleString()],
            ['Total plays',    formatNumber(totalPlays)],
            ['Listening time', `${Math.round(totalMins / 60).toLocaleString()} hr`],
            ['Avg plays',      formatNumber(avgPlays)],
        ];

        el.innerHTML = `
            <div class="sidebar-section">
                <p class="sidebar-heading">Summary</p>
                <dl class="nerds-list" style="border:none;border-radius:0">
                    ${summaryRows.map(([k,v]) => `<div class="nerds-row"><dt>${k}</dt><dd>${v}</dd></div>`).join('')}
                </dl>
            </div>
            <div class="sidebar-section">
                <p class="sidebar-heading">Top Artists</p>
                ${topArtists.map(([name, count], i) => `
                    <div class="sidebar-row">
                        <span class="track-rank">${i + 1}</span>
                        <span class="sidebar-row-name">${escapeHtml(name)}</span>
                        <span class="sidebar-row-count">${count} tracks</span>
                    </div>`).join('')}
            </div>`;
    }

    return { mount, unmount };
})();
