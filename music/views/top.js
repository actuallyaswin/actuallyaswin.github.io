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

    function _rangeStartTs() {
        if (range === 'all') return null;
        const now = Math.floor(Date.now() / 1000);
        if (range === 'week')  return now - 7   * 86400;
        if (range === 'month') return now - 30  * 86400;
        if (range === 'year')  return now - 365 * 86400;
        return null;
    }

    // Filled in by later tasks. Each entry: { title, sortOptions, hasRange,
    // hasYearFilter, query(), cardHref(id), buildCardFields(row) }
    const ENTITY_CONFIG = {
        artists: {
            title: 'Top Artists',
            sortOptions: [
                { key: 'listens',     icon: 'headphones', title: 'Sort by listens' },
                { key: 'minutes',     icon: 'clock',       title: 'Sort by minutes' },
                { key: 'discoveries', icon: 'sparkles',    title: 'Latest discoveries — artists with newest average listen date' },
                { key: 'oldies',      icon: 'history',     title: 'Golden oldies — artists with oldest average listen date' },
            ],
            hasRange: true,
            hasYearFilter: false,
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

                return _db.exec(`
                    SELECT
                        a.id, a.name,
                        COALESCE(a.image_thumb_url, a.image_url) as image_url,
                        a.cert,
                        COUNT(DISTINCT CASE WHEN t.hidden = 0 AND l.id IS NOT NULL THEN t.id END) as unique_tracks,
                        COUNT(CASE WHEN t.hidden = 0 THEN l.id END) as total_listens,
                        CAST(SUM(CASE WHEN t.hidden = 0 AND l.id IS NOT NULL THEN COALESCE(t.duration_ms, 0) ELSE 0 END) / 60000.0 AS INTEGER) as total_minutes,
                        CAST(AVG(CASE WHEN t.hidden = 0 THEN l.timestamp END) AS INTEGER) as avg_ts
                    FROM artists a
                    LEFT JOIN track_artists ta ON a.id = ta.artist_id AND ta.role = 'main'
                    LEFT JOIN tracks t ON ta.track_id = t.id
                    LEFT JOIN listens l ON t.id = l.track_id ${tsFilter}
                    WHERE a.hidden = 0
                    GROUP BY a.id
                    HAVING total_listens > 0
                    ORDER BY ${orderClause}
                    LIMIT 100
                `)[0];
            },
            cardHref: id => `?view=artist&id=${encodeURIComponent(id)}`,
            buildCardFields(row) {
                const [id, name, imageUrl, cert, uniqueTracks, totalListens, totalMinutes, avgTs] = row;
                return { id, name, imageUrl, cert, meta2: uniqueTracks, totalListens, totalMinutes, avgTs, label: name };
            },
        },
        albums: {
            title: 'Top Albums',
            sortOptions: [
                { key: 'listens',     icon: 'headphones', title: 'Sort by listens' },
                { key: 'minutes',     icon: 'clock',       title: 'Sort by minutes' },
                { key: 'discoveries', icon: 'sparkles',    title: 'Latest discoveries — albums with newest average listen date' },
                { key: 'oldies',      icon: 'history',     title: 'Golden oldies — albums with oldest average listen date' },
            ],
            hasRange: false,
            hasYearFilter: true,
            query() {
                let orderClause;
                if (sortBy === 'minutes')          orderClause = 'total_minutes DESC';
                else if (sortBy === 'discoveries') orderClause = 'avg_ts DESC';
                else if (sortBy === 'oldies')      orderClause = 'avg_ts ASC';
                else                                orderClause = 'total_listens DESC';
                const yearInt = parseInt(releaseYear);
                const yearFilter = releaseYear !== 'all' && !isNaN(yearInt) ? `AND r.release_year = ${yearInt}` : '';

                return _db.exec(`
                    SELECT
                        r.id, r.title, r.release_year, r.type,
                        COALESCE(r.album_art_thumb_url, r.album_art_url) as album_art_url,
                        a.name, a.id as artist_id,
                        COUNT(DISTINCT CASE WHEN t.hidden = 0 AND l.id IS NOT NULL THEN t.id END) as tracks_listened,
                        COUNT(CASE WHEN t.hidden = 0 THEN l.id END) as total_listens,
                        CAST(SUM(CASE WHEN t.hidden = 0 AND l.id IS NOT NULL THEN COALESCE(t.duration_ms, 0) ELSE 0 END) / 60000.0 AS INTEGER) as total_minutes,
                        r.avg_listen_ts as avg_ts
                    FROM releases r
                    LEFT JOIN artists a ON a.id = r.primary_artist_id
                    LEFT JOIN tracks t ON t.release_id = r.id
                    LEFT JOIN listens l ON l.track_id = t.id
                    WHERE r.hidden = 0 AND (a.id IS NULL OR a.hidden = 0) ${yearFilter}
                    GROUP BY r.id
                    HAVING total_listens > 0
                    ORDER BY ${orderClause}
                    LIMIT 100
                `)[0];
            },
            cardHref: id => `?view=release&id=${encodeURIComponent(id)}`,
            buildCardFields(row) {
                const [id, title, year, type, albumArtUrl, artistName, artistId, tracksListened, totalListens, totalMinutes, avgTs] = row;
                return {
                    id, title, name: title, imageUrl: albumArtUrl, artistName: artistName || 'Various Artists',
                    meta: `${escapeHtml(artistName || 'Various Artists')} · ${year || 'Unknown'}`,
                    totalListens, totalMinutes, avgTs, label: title,
                };
            },
        },
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
        if (ENTITY_CONFIG[entityType].hasYearFilter) _populateYearFilter();
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
        if (container) {
            container.innerHTML = _renderShell();
            _setupControls();
            if (ENTITY_CONFIG[entityType].hasYearFilter) _populateYearFilter();
            _load();
        }
    }

    function _populateYearFilter() {
        const sel = document.getElementById('yearFilter');
        if (!sel) return;
        const res = _db.exec(`
            SELECT DISTINCT release_year FROM releases
            WHERE release_year IS NOT NULL AND hidden = 0
            ORDER BY release_year DESC
        `)[0];
        if (res) res.values.forEach(([yr]) => {
            const opt = document.createElement('option');
            opt.value = yr; opt.textContent = yr;
            if (String(yr) === String(releaseYear)) opt.selected = true;
            sel.appendChild(opt);
        });
    }

    function _load() {
        const result = ENTITY_CONFIG[entityType].query();
        cachedResults = result ? result.values.map(ENTITY_CONFIG[entityType].buildCardFields) : [];
        if (_scrollEl) _scrollEl.scrollTop = 0;  // no-op until a later task defines _scrollEl for tracks
        _render();
    }

    function _render() {
        if (viewMode === 'collage') return _renderCollage();   // a later task
        if (entityType === 'tracks' && viewMode === 'list') return _renderTrackList();  // a later task
        return _renderListOrTiles();
    }

    function _renderListOrTiles() {
        const container = document.getElementById('topContainer');
        if (!container) return;
        container.innerHTML = '';
        container.style.gridTemplateColumns = '';

        if (cachedResults.length === 0) {
            container.className = 'image-grid';
            container.innerHTML = `<div class="loading">No ${entityType} found</div>`;
            return;
        }

        const cfg = ENTITY_CONFIG[entityType];
        const isTemporalSort = sortBy === 'discoveries' || sortBy === 'oldies';

        if (viewMode === 'list') {
            container.className = 'wide-grid';
            cachedResults.forEach((f, i) => {
                let meta;
                if (isTemporalSort && f.avgTs) {
                    const d = new Date(f.avgTs * 1000);
                    meta = `avg. ${d.toLocaleString('en-US', { month: 'short', year: 'numeric' })}`;
                } else {
                    meta = f.meta || (f.meta2 != null ? `${formatNumber(f.meta2)} tracks` : '');
                }
                const card = createWideCard({
                    href: cfg.cardHref(f.id),
                    imageUrl: f.imageUrl,
                    name: f.name || f.title,
                    meta,
                    totalListens: f.totalListens,
                    totalMinutes: f.totalMinutes,
                    rounded: entityType === 'artists',
                    cert: f.cert || null,
                });
                if (i >= countLimit) card.style.display = 'none';
                container.appendChild(card);
            });
        } else {
            container.className = 'image-grid';
            cachedResults.forEach((f, i) => {
                const card = document.createElement('a');
                card.className = 'image-card';
                card.href = cfg.cardHref(f.id);
                const imgSrc = f.imageUrl || getFallbackImageUrl();
                card.innerHTML = `
                    <div class="image-card-img" style="background-image: url('${imgSrc}')"></div>
                    <div class="image-card-overlay">
                        <div class="image-card-name">${escapeHtml(f.name || f.title || '')}</div>
                        ${f.artistName ? `<div class="image-card-artist">${escapeHtml(f.artistName)}</div>` : ''}
                        <div class="image-card-stats">
                            <span class="stat-item"><i data-lucide="headphones" style="width:14px;height:14px;"></i>${formatNumber(f.totalListens)}</span>
                            <span class="stat-item"><i data-lucide="clock" style="width:14px;height:14px;"></i>${formatNumber(f.totalMinutes)} min</span>
                        </div>
                    </div>`;
                if (i >= countLimit) card.style.display = 'none';
                container.appendChild(card);
            });
        }
        lucide.createIcons();
    }

    function _applyCount() {
        const container = document.getElementById('topContainer');
        if (!container) return;
        Array.from(container.children).forEach((el, i) => {
            el.style.display = i < countLimit ? '' : 'none';
        });
    }

    return { mount, unmount };
})();
