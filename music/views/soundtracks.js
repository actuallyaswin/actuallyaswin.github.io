// Video game soundtracks, grouped by platform or series. A dedicated page
// rather than folding into Genres/Stats — this is a browsing view over
// release_soundtrack_meta (source_type='video_game'), not an aggregate.
const ViewSoundtracks = (() => {
    let _db = null;
    // [{id, title, slug, art, artist, platform, series, year}]
    let _rows = [];
    // 'platform' | 'series'
    let _groupBy = 'platform';
    // 'title' | 'year' — order of cards within each group
    let _cardSort = 'title';
    let _query = '';
    // exact series match, set via ?series= deep link
    let _seriesFilter = null;
    // exact platform match, set via ?platform= deep link
    let _platformFilter = null;

    // Which groups are expanded — collapsed by default (16-20+ groups would
    // otherwise dump 170+ cards on the page at once). Persisted per tab via
    // sessionStorage, same pattern as genres.js's tree-expand state. Keyed by
    // `${_groupBy}:${key}` rather than bare key so switching Platform/Series
    // grouping doesn't cross-contaminate open state between the two modes.
    let _openGroups = new Set();
    const OPEN_GROUPS_KEY = 'vgstOpenGroups';

    function _loadOpenGroups() {
        try {
            const raw = sessionStorage.getItem(OPEN_GROUPS_KEY);
            _openGroups = raw ? new Set(JSON.parse(raw)) : new Set();
        } catch (_) {
            _openGroups = new Set();
        }
    }

    function _saveOpenGroups() {
        try {
            sessionStorage.setItem(OPEN_GROUPS_KEY, JSON.stringify([..._openGroups]));
        } catch (_) {}
    }

    // Platform labels + icon markup now live in platform-icons.js
    // (shared with views/release.js) — see platformLabel()/platformIconMarkup().

    function _donutColor(pct) {
        if (pct <= 0)   return 'var(--border)';
        if (pct < 0.5)  return '#3b82f6';
        if (pct < 0.75) return '#f59e0b';
        if (pct < 1.0)  return '#f97316';
        return '#22c55e';
    }

    function _cache(key) {
        const res = _db.exec('SELECT value_json FROM stats_cache WHERE key = ?', [key])[0];
        return res ? JSON.parse(res.values[0][0]) : null;
    }

    function _load() {
        const result = _db.exec(`
            SELECT r.id, r.title, r.slug,
                   COALESCE(r.album_art_thumb_url, r.album_art_url) as art,
                   (SELECT GROUP_CONCAT(a.name, ', ') FROM artists a WHERE a.id IN (
                        SELECT artist_id FROM release_artists WHERE release_id = r.id AND role = 'main'
                        UNION
                        SELECT r.primary_artist_id WHERE r.primary_artist_id IS NOT NULL
                   )) as artist,
                   sm.platform, sm.series, r.release_year,
                   (SELECT COUNT(*) FROM tracks t WHERE t.release_id = r.id AND t.hidden = 0) as total_tracks,
                   (SELECT COUNT(*) FROM tracks t
                        WHERE t.release_id = r.id AND t.hidden = 0
                          AND EXISTS (SELECT 1 FROM listens l WHERE l.track_id = t.id)) as tracks_heard
            FROM releases r
            JOIN release_soundtrack_meta sm ON sm.release_id = r.id
            WHERE sm.source_type = 'video_game' AND r.hidden = 0
            ORDER BY r.title
        `)[0];

        _rows = result ? result.values.map(([id, title, slug, art, artist, platform, series, year, totalTracks, tracksHeard]) => ({
            id, title, slug, art, artist, platform, series, year,
            totalTracks: totalTracks || 0, tracksHeard: tracksHeard || 0,
        })) : [];
    }

    function _matchesQuery(row) {
        if (_seriesFilter && row.series !== _seriesFilter) return false;
        if (_platformFilter && row.platform !== _platformFilter) return false;
        if (!_query) return true;
        const q = _query.toLowerCase();
        return row.title.toLowerCase().includes(q)
            || (row.artist || '').toLowerCase().includes(q)
            || (row.series || '').toLowerCase().includes(q);
    }

    function _groupKey(row) {
        if (_groupBy === 'series') return row.series || null;
        return row.platform || null;
    }

    function _groupLabel(key) {
        if (key == null) return _groupBy === 'series' ? 'Unknown series' : 'Unknown platform';
        return _groupBy === 'series' ? key : platformLabel(key);
    }

    function _cardHtml(row) {
        const sub = [row.artist, row.year].filter(Boolean).join(' · ');
        const tag = _groupBy === 'series'
            ? (row.platform ? platformLabel(row.platform) : '')
            : (row.series || '');
        const hasTracks = row.totalTracks > 0;
        const pct = hasTracks ? row.tracksHeard / row.totalTracks : 0;
        const donut = hasTracks
            ? `<div class="donut-wrap" style="--p:${Math.round(pct * 100)};--c:${_donutColor(pct)}"
                   data-tooltip="${row.tracksHeard} / ${row.totalTracks} tracks"><div class="donut"></div></div>`
            : '';
        return `<a href="${releaseHref(row.id, row.slug)}" class="disc-card${row.tracksHeard === 0 ? ' unplayed' : ''}" title="${escapeHtml(row.title)}">
            <div class="disc-card-img" style="background-image:url('${cssUrl(row.art || getFallbackImageUrl())}')"></div>
            <div class="disc-card-meta">
                <div class="disc-card-info">
                    <div class="disc-card-title">${escapeHtml(row.title)}</div>
                    <div class="disc-card-sub">${escapeHtml(sub)}${tag ? ' · ' + escapeHtml(tag) : ''}</div>
                </div>
                ${donut}
            </div>
        </a>`;
    }

    // A one-item "N64 (1)" row wastes as much vertical space as a 6-item row
    // while only filling a sliver of the width — with 176 releases spread
    // across dozens of platforms/series, most groups are this small. Folding
    // anything under the threshold into a single "Other" bucket keeps the
    // grid dense; each card still shows its own tag in the sub-line, so
    // nothing is actually hidden, just not given its own mostly-empty row.
    const MIN_GROUP_SIZE = 3;

    function _render() {
        const gridEl = document.getElementById('vgstGrid');
        const subtitleEl = document.getElementById('vgstSubtitle');
        if (!gridEl) return;

        const filtered = _rows.filter(_matchesQuery);
        subtitleEl.textContent = `${formatNumber(filtered.length)} soundtrack${filtered.length === 1 ? '' : 's'}`;

        const buckets = new Map();
        for (const row of filtered) {
            const key = _groupKey(row);
            if (!buckets.has(key)) buckets.set(key, []);
            buckets.get(key).push(row);
        }
        // Order of cards WITHIN each group — independent of how groups
        // themselves are ordered (biggest-first, just below).
        const cardCompare = _cardSort === 'year'
            ? (a, b) => (a.year || 0) - (b.year || 0) || a.title.localeCompare(b.title)
            : (a, b) => a.title.localeCompare(b.title);
        for (const group of buckets.values()) group.sort(cardCompare);

        const OTHER = Symbol('other');
        const named = [];
        let other = [];
        for (const [key, group] of buckets) {
            if (key != null && group.length >= MIN_GROUP_SIZE) named.push([key, group]);
            else other = other.concat(group);
        }
        if (other.length) other.sort(cardCompare);
        // Biggest groups first — the point of grouping is to spotlight the
        // series/platforms with real depth, not to alphabetize dead space.
        named.sort((a, b) => b[1].length - a[1].length || _groupLabel(a[0]).localeCompare(_groupLabel(b[0])));
        if (other.length) named.push([OTHER, other]);

        let html = '';
        for (const [key, group] of named) {
            const storageKey = `${_groupBy}:${key === OTHER ? '__other__' : key}`;
            // Arriving via a series/platform deep link (e.g. the release-page
            // pill) narrows to exactly one group — force it open, since
            // collapsed-by-default would otherwise hide the very thing the
            // link was for behind an extra click.
            const isOpen = (_seriesFilter || _platformFilter) ? true : _openGroups.has(storageKey);
            const iconMarkup = key !== OTHER && _groupBy === 'platform' ? platformIconMarkup(key) : null;
            const countHtml = `<span class="vgst-group-count">(${group.length})</span>`;
            let headerInner;
            if (iconMarkup) {
                const label = _groupLabel(key);
                headerInner = `<span class="vgst-group-icon">${iconMarkup}</span>`
                    + `${escapeHtml(label)} ${countHtml}`;
            } else {
                const label = key === OTHER
                    ? `Other ${_groupBy === 'series' ? 'series' : 'platforms'}`
                    : _groupLabel(key);
                headerInner = `${escapeHtml(label)} ${countHtml}`;
            }
            const chevron = `<span class="lang-chevron${isOpen ? ' expanded' : ''}">▶</span>`;
            html += `<h2 class="list-year-header vgst-group-header vgst-group-header-clickable" data-vgst-key="${escapeHtml(storageKey)}">${headerInner}${chevron}</h2>`;
            html += `<div class="disc-grid"${isOpen ? '' : ' hidden'}>${group.map(_cardHtml).join('')}</div>`;
        }
        gridEl.innerHTML = html || '<p class="vgst-empty">No soundtracks match.</p>';
    }

    function _wireControls(container) {
        container.querySelectorAll('.vgst-group-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                _groupBy = btn.dataset.group;
                container.querySelectorAll('.vgst-group-btn').forEach(b => b.classList.toggle('active', b === btn));
                _render();
            });
        });
        container.querySelectorAll('.vgst-sort-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                _cardSort = btn.dataset.sort;
                container.querySelectorAll('.vgst-sort-btn').forEach(b => b.classList.toggle('active', b === btn));
                _render();
            });
        });
        const search = document.getElementById('vgstSearch');
        if (search) {
            search.addEventListener('input', () => {
                _query = search.value;
                _render();
            });
        }
        // Event delegation on the stable container — gridEl's own innerHTML
        // is fully replaced on every _render(), so a listener attached
        // directly to a header/panel wouldn't survive a re-render.
        container.addEventListener('click', e => {
            const header = e.target.closest('.vgst-group-header-clickable');
            if (!header || !container.contains(header)) return;
            const panel = header.nextElementSibling;
            const chevron = header.querySelector('.lang-chevron');
            if (!panel || !chevron) return;
            const key = header.dataset.vgstKey;
            // opening if it was hidden
            const nowOpen = panel.hidden;
            panel.hidden = !nowOpen;
            chevron.classList.toggle('expanded', nowOpen);
            if (nowOpen) _openGroups.add(key); else _openGroups.delete(key);
            _saveOpenGroups();
        });
    }

    function mount(container, db, params) {
        _db = db;
        _seriesFilter = params.series || null;
        _platformFilter = params.platform || null;
        _groupBy = params.group === 'series' ? 'series' : (params.group === 'platform' ? 'platform' : (_seriesFilter ? 'series' : 'platform'));
        _cardSort = params.sort === 'year' ? 'year' : 'title';
        _query = params.q || '';
        _loadOpenGroups();
        const pageTitle = _seriesFilter ? `${_seriesFilter} — Video Game Soundtracks` : 'Video Game Soundtracks';
        setPageTitle(pageTitle);
        _load();

        const filterNote = _seriesFilter
            ? `<p class="subtitle">Showing only <strong>${escapeHtml(_seriesFilter)}</strong> — <a href="?view=soundtracks">clear filter</a></p>`
            : _platformFilter
            ? `<p class="subtitle">Showing only <strong>${escapeHtml(platformLabel(_platformFilter))}</strong> — <a href="?view=soundtracks">clear filter</a></p>`
            : '';

        container.innerHTML = `
            <header>
                <h1>${_seriesFilter ? escapeHtml(_seriesFilter) : 'Video Game Soundtracks'}</h1>
                ${filterNote}
                <p class="subtitle" id="vgstSubtitle"></p>
            </header>

            <div class="page-controls">
                <div class="control-block control-block-grow">
                    <span class="control-block-label">Filter</span>
                    <div class="filter-search" style="width:100%">
                        <i data-lucide="search" class="filter-search-icon"></i>
                        <input id="vgstSearch" class="filter-search-input" placeholder="Filter by title, composer, series…"
                               autocomplete="off" value="${escapeHtml(_query)}">
                    </div>
                </div>
                <div class="control-block">
                    <span class="control-block-label">Group By</span>
                    <div class="sort-controls">
                        <button type="button" class="sort-btn vgst-group-btn${_groupBy === 'platform' ? ' active' : ''}" data-group="platform">Platform</button>
                        <button type="button" class="sort-btn vgst-group-btn${_groupBy === 'series' ? ' active' : ''}" data-group="series">Series</button>
                    </div>
                </div>
                <div class="control-block">
                    <span class="control-block-label">Sort By</span>
                    <div class="sort-controls">
                        <button type="button" class="sort-btn vgst-sort-btn${_cardSort === 'title' ? ' active' : ''}" data-sort="title">A–Z</button>
                        <button type="button" class="sort-btn vgst-sort-btn${_cardSort === 'year' ? ' active' : ''}" data-sort="year">Year</button>
                    </div>
                </div>
            </div>

            <section>
                <div id="vgstGrid"></div>
            </section>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        _wireControls(container);
        _render();
        if (window.lucide) window.lucide.createIcons();
    }

    function unmount() { _db = null; _rows = []; }

    return { mount, unmount };
})();
