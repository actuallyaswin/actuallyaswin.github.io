const ViewTop = (() => {
    let _db = null;
    let entityType = 'artists';   // 'artists' | 'albums' | 'tracks'
    let sortBy = 'listens';
    let range = 'all';             // artists only (existing Week/Month/Year/All)
    let countLimit = 10;           // List/Tiles item count (existing 10/20/50/100)
    let viewMode = 'list';         // 'list' | 'tiles' | 'collage' — canonicalizes today's
                                    // inconsistent display-param values (top-artists.js
                                    // validated `['list','wide','collage']`, top-albums.js
                                    // validated `['list','tiles','collage']` for the same
                                    // visual mode). The new URL param is always `tiles`,
                                    // never `wide`. Since old `?view=top-artists` routes
                                    // aren't preserved (see Overview), this is a clean
                                    // break, not a compatibility concern.
    let gridShape = { rows: 3, cols: 3 };  // Collage mode only, see later task
    let releaseYear = 'all';       // albums/tracks only (existing Released filter)
    let cachedResults = [];

    const CERT_LABELS = {
        gold:     'Gold — 250+ plays',
        platinum: 'Platinum — 500+ plays',
        diamond:  'Diamond — 1,000+ plays',
    };

    // Filled in by later tasks. Each entry: { title, sortOptions, hasRange,
    // hasYearFilter, query(), cardHref(id), buildCardFields(row) }
    const ENTITY_CONFIG = {
        artists: null,
        albums:  null,
        tracks:  null,
    };

    function mount(container, db, params) {
        _db = db;
        entityType = ['artists', 'albums', 'tracks'].includes(params.type) ? params.type : 'artists';
        document.title = `${ENTITY_CONFIG[entityType]?.title || 'Top'} | Aswin Sivaraman`;

        // Restore remaining state from URL params
        const cfg = ENTITY_CONFIG[entityType];
        if (cfg && params.sort && cfg.sortOptions.some(o => o.key === params.sort)) sortBy = params.sort;
        else sortBy = 'listens';
        if (params.range && ['week','month','year','all'].includes(params.range)) range = params.range;
        if (params.count && [10,20,50,100].includes(+params.count)) countLimit = +params.count;
        // Note: the old top-artists.js accepted `display=wide` for this same
        // mode; the unified param is always `tiles`. Per spec, old
        // ?view=top-artists URLs (including &display=wide) aren't preserved
        // or redirected — this is an accepted, intentional gap, not a bug.
        if (params.display && ['list','tiles','collage'].includes(params.display)) viewMode = params.display;
        if (params.year) releaseYear = params.year;

        container.innerHTML = _renderShell();
        _setupControls();
        _load();
    }

    function unmount() {
        // A later task adds track-specific virtualized-scroll teardown here.
    }

    function _renderShell() {
        return `
            <header><h1>${ENTITY_CONFIG[entityType]?.title || 'Top'}</h1></header>
            <div class="page-controls">
                ${_entityToggleHtml()}
                ${_sortControlsHtml()}
                ${ENTITY_CONFIG[entityType]?.hasRange ? _rangeControlsHtml() : ''}
                ${_countControlsHtml()}
                ${ENTITY_CONFIG[entityType]?.hasYearFilter ? _yearFilterHtml() : ''}
                ${_displayControlsHtml()}
            </div>
            <div id="topContainer" class="image-grid">
                <div class="loading">Loading...</div>
            </div>
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
                    <button class="sort-btn${entityType === 'artists' ? ' active' : ''}" data-type="artists" title="Artists"><i data-lucide="mic-2"></i></button>
                    <button class="sort-btn${entityType === 'albums'  ? ' active' : ''}" data-type="albums"  title="Albums"><i data-lucide="disc-3"></i></button>
                    <button class="sort-btn${entityType === 'tracks'  ? ' active' : ''}" data-type="tracks"  title="Tracks"><i data-lucide="music"></i></button>
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
                        <button class="sort-btn${sortBy === o.key ? ' active' : ''}" data-sort="${o.key}" title="${o.title}"><i data-lucide="${o.icon}"></i></button>
                    `).join('')}
                </div>
            </div>`;
    }

    function _rangeControlsHtml() {
        return `
            <div class="control-block" id="rangeControlBlock">
                <span class="control-block-label">Range</span>
                <div class="sort-controls">
                    <button class="sort-btn${range === 'week'  ? ' active' : ''}" data-range="week">Week</button>
                    <button class="sort-btn${range === 'month' ? ' active' : ''}" data-range="month">Month</button>
                    <button class="sort-btn${range === 'year'  ? ' active' : ''}" data-range="year">Year</button>
                    <button class="sort-btn${range === 'all'   ? ' active' : ''}" data-range="all">All</button>
                </div>
            </div>`;
    }

    function _countControlsHtml() {
        if (viewMode === 'collage') return '';  // A later task replaces this in collage mode
        const countBtns = [10, 20, 50, 100].map(n =>
            `<button class="sort-btn${countLimit === n ? ' active' : ''}" data-count="${n}">${n}</button>`
        ).join('');
        return `
            <div class="control-block">
                <span class="control-block-label">#</span>
                <div class="sort-controls">${countBtns}</div>
            </div>`;
    }

    function _yearFilterHtml() {
        return `
            <div class="control-block">
                <span class="control-block-label">Released</span>
                <div class="sort-controls">
                    <select id="yearFilter" class="year-filter-select">
                        <option value="all">All years</option>
                    </select>
                </div>
            </div>`;
    }

    function _displayControlsHtml() {
        return `
            <div class="control-block">
                <span class="control-block-label">Display</span>
                <div class="sort-controls">
                    <button class="sort-btn${viewMode === 'list'    ? ' active' : ''}" data-view="list"    title="List"><i data-lucide="layout-list"></i></button>
                    <button class="sort-btn${viewMode === 'tiles'   ? ' active' : ''}" data-view="tiles"   title="Tiles"><i data-lucide="layout-grid"></i></button>
                    <button class="sort-btn${viewMode === 'collage' ? ' active' : ''}" data-view="collage" title="Collage"><i data-lucide="grid-3x3"></i></button>
                </div>
            </div>`;
    }

    function _syncUrl() {
        const p = new URLSearchParams({ view: 'top', type: entityType, sort: sortBy, count: countLimit, display: viewMode });
        if (ENTITY_CONFIG[entityType]?.hasRange) p.set('range', range);
        if (ENTITY_CONFIG[entityType]?.hasYearFilter) p.set('year', releaseYear);
        history.replaceState(Object.fromEntries(p), '', '?' + p.toString());
    }

    function _setupControls() {
        setupToggleGroup('[data-type]', btn => {
            entityType = btn.dataset.type;
            sortBy = 'listens';
            releaseYear = 'all';
            _syncUrl();
            mount(document.getElementById('view-container'), _db, Object.fromEntries(new URLSearchParams(location.search)));
        });
        setupToggleGroup('[data-sort]', btn => { sortBy = btn.dataset.sort; _syncUrl(); _load(); });
        setupToggleGroup('[data-range]', btn => { range = btn.dataset.range; _syncUrl(); _load(); });
        setupToggleGroup('[data-count]', btn => { countLimit = parseInt(btn.dataset.count); _syncUrl(); _applyCount(); });
        setupToggleGroup('[data-view]', btn => { viewMode = btn.dataset.view; _syncUrl(); _rerenderForModeChange(); });

        const yearSel = document.getElementById('yearFilter');
        if (yearSel) yearSel.addEventListener('change', () => { releaseYear = yearSel.value; _syncUrl(); _load(); });
    }

    function _rerenderForModeChange() {
        // Re-render the whole shell since Count vs. grid-shape controls differ by mode (a later task).
        const container = document.getElementById('view-container');
        if (container) { container.innerHTML = _renderShell(); _setupControls(); _load(); }
    }

    function _load() {
        // Later tasks fill in per-entity query execution here.
        cachedResults = [];
        _render();
    }

    function _render() {
        // Later tasks fill in real rendering; another later task adds collage.
        const el = document.getElementById('topContainer');
        if (el) el.innerHTML = '<div class="loading">Not yet implemented</div>';
    }

    function _applyCount() {
        // Mirrors existing top-artists.js/top-albums.js applyCount() — filled in fully once _render() is real (a later task).
    }

    return { mount, unmount };
})();
