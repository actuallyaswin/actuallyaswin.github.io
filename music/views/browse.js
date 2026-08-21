// Unified catalog browser — supersedes Top Artists/Albums and Soundtracks.
// Letterboxd's "Films" page is the model: Sort/Decade/Genre/Platform/Status
// pill row, a decade-then-year carousel instead of a flat year dropdown,
// and a plain-language summary line above the grid instead of a filter-count
// badge. Tracks stay on views/top.js — this is album/artist browsing only.
const ViewBrowse = (() => {
    let _db = null;
    // 'albums' | 'artists'
    let entityType = 'albums';
    // 'discoveries' | 'recent' | 'plays' | 'az' | 'release-date' | 'random'
    let sortBy = 'discoveries';
    // 'all' or e.g. '2020s'
    let decade = 'all';
    // set only when a specific year within `decade` is picked via the carousel
    let year = null;
    // genre id as string, or 'all'
    let genreFilter = 'all';
    // platform slug, or 'all' — only meaningful while browsing video game OSTs
    let platformFilter = 'all';
    // 'all' | 'heard' | 'unheard'
    let status = 'all';
    // 'list' | 'poster-sm' | 'poster-lg'
    let viewMode = 'poster-lg';
    let _rows = [];
    let _genreSelect = null;
    let _platformSelect = null;
    let _shuffleSeed = 0;
    // How many of the sorted/filtered rows are actually in the DOM — grows by
    // PAGE_SIZE on "Load more" / scroll-near-bottom. Without this, a cleared
    // filter set (5,900+ releases) renders every card in one shot.
    let _visibleCount = 0;
    const PAGE_SIZE = 60;

    const DECADES = ['1960s', '1970s', '1980s', '1990s', '2000s', '2010s', '2020s'];

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

    function _loadAlbums() {
        const yr = _yearRange();
        const yearClause = yr ? `AND r.release_year BETWEEN ${yr[0]} AND ${yr[1]}` : '';
        const gid = parseInt(genreFilter, 10);
        const genreClause = (genreFilter !== 'all' && !isNaN(gid))
            ? `AND EXISTS (SELECT 1 FROM release_genres rg WHERE rg.release_id = r.id AND rg.aoty_genre_id = ${gid})`
            : '';
        const platformClause = platformFilter !== 'all'
            ? `AND EXISTS (SELECT 1 FROM release_soundtrack_meta sm WHERE sm.release_id = r.id AND sm.platform = '${platformFilter.replace(/'/g, "''")}')`
            : '';

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
                        WHERE t.release_id = r.id AND t.hidden = 0) as last_listen_ts
            FROM releases r
            WHERE r.hidden = 0 ${yearClause} ${genreClause} ${platformClause}
        `)[0];

        _rows = result ? result.values.map(([id, title, slug, releaseYear, art, artistName, artistId, artistSlug,
                                              totalTracks, tracksHeard, totalListens, firstListenTs, lastListenTs]) => ({
            id, title, slug, releaseYear, art, artistName, artistId, artistSlug,
            totalTracks: totalTracks || 0, tracksHeard: tracksHeard || 0,
            totalListens: totalListens || 0, firstListenTs, lastListenTs,
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
        const rows = _rows.filter(_matchesStatus);
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
        const donut = _albumDonut(row);
        const posterOnly = viewMode !== 'list';
        if (posterOnly) {
            return `<a href="${href}" class="browse-poster${row.totalListens === 0 ? ' unplayed' : ''}" title="${escapeHtml(row.title)}">
                <div class="browse-poster-img" style="background-image:url('${cssUrl(row.art || getFallbackImageUrl())}')"></div>
                <div class="browse-poster-donut">${donut}</div>
                <div class="browse-poster-caption">
                    <div class="browse-poster-title">${escapeHtml(row.title)}</div>
                    <div class="browse-poster-sub">${escapeHtml(sub)}</div>
                </div>
            </a>`;
        }
        return `<a href="${href}" class="disc-card${row.totalListens === 0 ? ' unplayed' : ''}" title="${escapeHtml(row.title)}">
            <div class="disc-card-img" style="background-image:url('${cssUrl(row.art || getFallbackImageUrl())}')"></div>
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
        if (platformFilter !== 'all') parts.push(platformLabel(platformFilter));
        if (status !== 'all') parts.push(status === 'heard' ? 'heard' : 'not yet heard');
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
            ? visible.map(_cardHtml).join('')
            : '<p class="browse-empty">Nothing matches these filters.</p>';

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
            <header>
                <h1>Browse</h1>
                <p class="subtitle" id="browseSubtitle"></p>
            </header>

            <div class="page-controls">
                <div class="control-block">
                    <span class="control-block-label">Type</span>
                    <div class="sort-controls">
                        <button class="sort-btn${entityType === 'albums' ? ' active' : ''}" data-type="albums"><i data-lucide="disc-3"></i>Albums</button>
                        <button class="sort-btn${entityType === 'artists' ? ' active' : ''}" data-type="artists"><i data-lucide="mic-2"></i>Artists</button>
                    </div>
                </div>
                <div class="control-block">
                    <span class="control-block-label">Sort By</span>
                    <div class="sort-controls">
                        <button class="sort-btn${sortBy === 'discoveries' ? ' active' : ''}" data-sort="discoveries" title="Newest first-listen date"><i data-lucide="sparkles"></i>Discoveries</button>
                        <button class="sort-btn${sortBy === 'recent' ? ' active' : ''}" data-sort="recent" title="Most recently played"><i data-lucide="clock"></i>Recent</button>
                        <button class="sort-btn${sortBy === 'plays' ? ' active' : ''}" data-sort="plays" title="Most played"><i data-lucide="headphones"></i>Plays</button>
                        <button class="sort-btn${sortBy === 'az' ? ' active' : ''}" data-sort="az" title="A to Z">A–Z</button>
                        <button class="sort-btn${sortBy === 'release-date' ? ' active' : ''}" data-sort="release-date" title="Newest release date">Release</button>
                        <button class="sort-btn${sortBy === 'random' ? ' active' : ''}" data-sort="random" title="Shuffle"><i data-lucide="shuffle"></i>Random</button>
                    </div>
                </div>
                <div class="control-block">
                    <span class="control-block-label">Decade</span>
                    <select id="decadeFilter" class="year-filter-select">
                        <option value="all"${decade === 'all' ? ' selected' : ''}>All time</option>
                        ${DECADES.map(d => `<option value="${d}"${decade === d ? ' selected' : ''}>${d}</option>`).join('')}
                    </select>
                </div>
                <div class="control-block">
                    <span class="control-block-label">Genre</span>
                    <select id="browseGenreFilter"><option value="all">All genres</option></select>
                </div>
                ${entityType === 'albums' ? `
                <div class="control-block">
                    <span class="control-block-label">Platform</span>
                    <select id="browsePlatformFilter"><option value="all">All platforms</option></select>
                </div>` : ''}
                <div class="control-block">
                    <span class="control-block-label">Status</span>
                    <div class="sort-controls">
                        <button class="sort-btn${status === 'all' ? ' active' : ''}" data-status="all">All</button>
                        <button class="sort-btn${status === 'heard' ? ' active' : ''}" data-status="heard">Heard</button>
                        <button class="sort-btn${status === 'unheard' ? ' active' : ''}" data-status="unheard">Unheard</button>
                    </div>
                </div>
                <div class="control-block">
                    <span class="control-block-label">Display</span>
                    <div class="sort-controls">
                        <button class="sort-btn${viewMode === 'list' ? ' active' : ''}" data-view="list" title="List"><i data-lucide="layout-list"></i></button>
                        <button class="sort-btn${viewMode === 'poster-sm' ? ' active' : ''}" data-view="poster-sm" title="Small posters"><i data-lucide="grid-3x3"></i></button>
                        <button class="sort-btn${viewMode === 'poster-lg' ? ' active' : ''}" data-view="poster-lg" title="Large posters"><i data-lucide="layout-grid"></i></button>
                    </div>
                </div>
            </div>

            <div id="browseDecadeCarousel">${_decadeCarouselHtml()}</div>

            <div class="list-with-sidebar">
                <section>
                    <div id="browseGrid"></div>
                    <div id="browseSentinel" style="height:1px" hidden></div>
                </section>
                <aside class="view-sidebar" id="browseSidebar"></aside>
            </div>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>`;
    }

    function _populateGenreSelect() {
        const el = document.getElementById('browseGenreFilter');
        if (!el || typeof TomSelect === 'undefined') return;
        const cacheRes = _db.exec("SELECT value_json FROM stats_cache WHERE key = 'genresIndex'")[0];
        const genres = cacheRes ? JSON.parse(cacheRes.values[0][0]) : [];
        genres.sort((a, b) => a.name.localeCompare(b.name));
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

    function _populatePlatformSelect() {
        const el = document.getElementById('browsePlatformFilter');
        if (!el || typeof TomSelect === 'undefined') return;
        const res = _db.exec(`
            SELECT DISTINCT platform FROM release_soundtrack_meta
            WHERE platform IS NOT NULL AND platform != 'None'
            ORDER BY platform
        `)[0];
        const platforms = res ? res.values.map(r => r[0]) : [];
        for (const p of platforms) {
            const opt = document.createElement('option');
            opt.value = p;
            opt.textContent = platformLabel(p);
            el.appendChild(opt);
        }
        if (_platformSelect) _platformSelect.destroy();
        _platformSelect = new TomSelect(el, {
            create: false,
            maxOptions: null,
            placeholder: 'All platforms',
        });
        _platformSelect.setValue(platformFilter, true);
        _platformSelect.on('change', v => {
            platformFilter = v || 'all';
            _syncUrl();
            _reloadAndRender();
        });
    }

    function _syncUrl() {
        const p = new URLSearchParams();
        p.set('view', 'browse');
        if (entityType !== 'albums') p.set('type', entityType);
        if (sortBy !== 'discoveries') p.set('sort', sortBy);
        if (decade !== 'all') p.set('decade', decade);
        if (year) p.set('year', String(year));
        if (genreFilter !== 'all') p.set('genre', genreFilter);
        if (platformFilter !== 'all') p.set('platform', platformFilter);
        if (status !== 'all') p.set('status', status);
        if (viewMode !== 'poster-lg') p.set('display', viewMode);
        history.replaceState(Object.fromEntries(p), '', `?${p.toString()}`);
    }

    function _setupControls(container) {
        container.querySelectorAll('[data-type]').forEach(btn => btn.addEventListener('click', () => {
            entityType = btn.dataset.type;
            _syncUrl();
            _remount(container);
        }));
        container.querySelectorAll('[data-sort]').forEach(btn => btn.addEventListener('click', () => {
            sortBy = btn.dataset.sort;
            container.querySelectorAll('[data-sort]').forEach(b => b.classList.toggle('active', b === btn));
            if (sortBy === 'random') _shuffleSeed = Date.now() % 100000;
            _syncUrl();
            _applyFiltersAndRender();
        }));
        container.querySelectorAll('[data-status]').forEach(btn => btn.addEventListener('click', () => {
            status = btn.dataset.status;
            container.querySelectorAll('[data-status]').forEach(b => b.classList.toggle('active', b === btn));
            _syncUrl();
            _applyFiltersAndRender();
        }));
        container.querySelectorAll('[data-view]').forEach(btn => btn.addEventListener('click', () => {
            viewMode = btn.dataset.view;
            container.querySelectorAll('[data-view]').forEach(b => b.classList.toggle('active', b === btn));
            _syncUrl();
            _render();
        }));
        const decadeSel = document.getElementById('decadeFilter');
        if (decadeSel) decadeSel.addEventListener('change', () => {
            decade = decadeSel.value;
            year = null;
            document.getElementById('browseDecadeCarousel').innerHTML = _decadeCarouselHtml();
            _wireCarousel(container);
            _syncUrl();
            _reloadAndRender();
        });
        _wireCarousel(container);
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
        container.innerHTML = _renderShell();
        _setupControls(container);
        _populateGenreSelect();
        if (entityType === 'albums') _populatePlatformSelect();
        _load();
        _applyFiltersAndRender();
    }

    function mount(container, db, params) {
        _db = db;
        entityType = params.type === 'artists' ? 'artists' : 'albums';
        sortBy = ['discoveries', 'recent', 'plays', 'az', 'release-date', 'random'].includes(params.sort) ? params.sort : 'discoveries';
        decade = DECADES.includes(params.decade) ? params.decade : 'all';
        year = params.year && /^\d{4}$/.test(params.year) ? parseInt(params.year, 10) : null;
        genreFilter = params.genre || 'all';
        platformFilter = params.platform || 'all';
        status = ['all', 'heard', 'unheard'].includes(params.status) ? params.status : 'all';
        viewMode = ['list', 'poster-sm', 'poster-lg'].includes(params.display) ? params.display : 'poster-lg';
        setPageTitle('Browse');
        _remount(container);
    }

    function unmount() {
        _db = null;
        _rows = [];
        if (_genreSelect) { _genreSelect.destroy(); _genreSelect = null; }
        if (_platformSelect) { _platformSelect.destroy(); _platformSelect = null; }
    }

    return { mount, unmount };
})();
