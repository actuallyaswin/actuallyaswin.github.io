// Full-page ranked-album list (RS500, Apple Music 100, etc.) — Letterboxd's
// numbered-poster-grid pattern. A dedicated page rather than a modal: a
// 260-entry list doesn't fit a modal well, and (the bug that motivated this
// view existing at all) a body-level modal has no way to close itself when
// the SPA router navigates away from under it via a normal <a> click.
const ViewList = (() => {
    let _db = null;
    let _lst = null;
    // 'rank' (list's own order) | 'completion-asc' | 'completion-desc'
    let _sortMode = 'rank';
    let _groupByYear = false;
    let _lastRandomPick = null;
    // 'year' or 'decade' — decided per list at mount time (see _pickGroupUnit).
    let _groupUnit = 'year';

    function _cache(db, key) {
        const res = db.exec('SELECT value_json FROM stats_cache WHERE key = ?', [key])[0];
        return res ? JSON.parse(res.values[0][0]) : null;
    }

    function _donutColor(pct) {
        if (pct <= 0)   return 'var(--border)';
        if (pct < 0.5)  return '#3b82f6';
        if (pct < 0.75) return '#f59e0b';
        if (pct < 1.0)  return '#f97316';
        return '#22c55e';
    }

    // Unmatched entries (not in the library at all) sort as least-complete —
    // distinct from a matched-but-unheard album, which is exactly 0.
    function _completionOf(e) {
        if (!e.release_id) return -1;
        if (!e.total_tracks) return e.heard ? 1 : 0;
        return e.listened_tracks / e.total_tracks;
    }

    function _cardHtml(e) {
        const heardClass = e.release_id ? (e.heard ? '' : ' unplayed') : ' canon-unmatched';
        const rankLabel = e.position_label || `#${e.rank}`;
        const hasTracks = e.release_id && e.total_tracks > 0;
        const trackPct = hasTracks ? e.listened_tracks / e.total_tracks : 0;
        const donut = hasTracks
            ? `<div class="donut-wrap" style="--p:${Math.round(trackPct * 100)};--c:${_donutColor(trackPct)}"
                   data-tooltip="${e.listened_tracks} / ${e.total_tracks} tracks"><div class="donut"></div></div>`
            : '';
        const inner = `
            <div class="disc-card-img" style="background-image:url('${cssUrl(e.art || getFallbackImageUrl())}')"></div>
            <span class="canon-list-rank">${escapeHtml(rankLabel)}</span>
            <div class="disc-card-meta">
                <div class="disc-card-info">
                    <div class="disc-card-title">${escapeHtml(e.title)}</div>
                    <div class="disc-card-sub">${escapeHtml(e.artist)}${e.year ? ' · ' + e.year : ''}</div>
                </div>
                ${donut}
            </div>`;
        if (e.release_id) {
            return `<a href="${releaseHref(e.release_id, e.release_slug)}"
                       class="disc-card${heardClass}" title="${e.heard ? 'Heard' : 'Not heard yet'}">${inner}</a>`;
        }
        return `<div class="disc-card${heardClass}" title="Not yet in your library">${inner}</div>`;
    }

    function _sortedEntries() {
        const entries = _lst.entries.slice();
        if (_sortMode === 'completion-asc') entries.sort((a, b) => _completionOf(a) - _completionOf(b));
        else if (_sortMode === 'completion-desc') entries.sort((a, b) => _completionOf(b) - _completionOf(a));
        return entries;
    }

    // Some lists are one-album-per-year rankings (Apple Music 100, AOTY User
    // Top 100) rather than a dense chronological survey (NME AOTY, RS 500) —
    // grouping those by exact year barely groups anything and just adds a
    // header to nearly every card. Decide once per list, from its own data,
    // rather than hardcoding which lists get which treatment.
    function _pickGroupUnit(lst) {
        const years = lst.entries.map(e => e.year).filter(y => y != null);
        const distinct = new Set(years).size;
        const avgPerYear = distinct ? years.length / distinct : 0;
        return avgPerYear < 2.5 ? 'decade' : 'year';
    }

    function _groupKey(e) {
        if (e.year == null) return null;
        return _groupUnit === 'decade' ? Math.floor(e.year / 10) * 10 : e.year;
    }

    function _groupLabel(key) {
        if (key == null) return 'Unknown year';
        return _groupUnit === 'decade' ? `${key}s` : String(key);
    }

    function _renderGrid() {
        const gridEl = document.getElementById('listGrid');
        if (!gridEl) return;

        // Year/decade grouping only makes sense in the list's own order — a
        // completion sort scatters years across the page, so a "1974"
        // header re-appearing three times would just be confusing.
        if (_groupByYear && _sortMode === 'rank') {
            const buckets = new Map();
            for (const e of _lst.entries) {
                const key = _groupKey(e);
                if (!buckets.has(key)) buckets.set(key, []);
                buckets.get(key).push(e);
            }
            // Chronological, earliest first — not list/rank order, which for
            // a quality-ranked list (not a chronological one) would produce
            // headers in an arbitrary jumble. Unknown-year bucket goes last.
            const keys = [...buckets.keys()].sort((a, b) => {
                if (a == null) return 1;
                if (b == null) return -1;
                return a - b;
            });
            let html = '';
            for (const key of keys) {
                html += `<h2 class="list-year-header">${escapeHtml(_groupLabel(key))}</h2>`;
                html += `<div class="disc-grid">${buckets.get(key).map(_cardHtml).join('')}</div>`;
            }
            gridEl.innerHTML = html;
        } else {
            gridEl.innerHTML = `<div class="disc-grid">${_sortedEntries().map(_cardHtml).join('')}</div>`;
        }
    }

    function _updateControls() {
        document.querySelectorAll('.list-sort-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.sort === _sortMode);
        });
        const groupBtn = document.getElementById('listGroupBtn');
        if (groupBtn) {
            groupBtn.classList.toggle('active', _groupByYear);
            groupBtn.disabled = _sortMode !== 'rank';
            groupBtn.title = _sortMode !== 'rank' ? 'Only available in list order' : '';
        }
        const randomBtn = document.getElementById('listRandomBtn');
        if (randomBtn) {
            const candidates = _lst.entries.filter(e => e.release_id && !e.heard);
            randomBtn.disabled = candidates.length === 0;
        }
    }

    function _wireControls() {
        document.querySelectorAll('.list-sort-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                _sortMode = btn.dataset.sort;
                if (_sortMode !== 'rank') _groupByYear = false;
                _updateControls();
                _renderGrid();
            });
        });
        const groupBtn = document.getElementById('listGroupBtn');
        if (groupBtn) {
            groupBtn.addEventListener('click', () => {
                if (_sortMode !== 'rank') return;
                _groupByYear = !_groupByYear;
                _updateControls();
                _renderGrid();
            });
        }
        const randomBtn = document.getElementById('listRandomBtn');
        if (randomBtn) {
            randomBtn.addEventListener('click', () => {
                let candidates = _lst.entries.filter(e => e.release_id && !e.heard);
                if (!candidates.length) return;
                // Avoid immediately repeating the same pick — with a small
                // unheard pool (a near-complete list often has just a
                // handful left), plain uniform random repeats often enough
                // to feel broken even though Math.random() itself is fine.
                if (candidates.length > 1 && _lastRandomPick) {
                    candidates = candidates.filter(e => e.release_id !== _lastRandomPick);
                }
                const pick = candidates[Math.floor(Math.random() * candidates.length)];
                _lastRandomPick = pick.release_id;
                navigate(pick.release_slug ? { view: 'release', slug: pick.release_slug } : { view: 'release', id: pick.release_id });
            });
        }
    }

    function mount(container, db, params) {
        _db = db;
        _sortMode = 'rank';
        _groupByYear = false;
        _lastRandomPick = null;
        const listId = params.id;

        const lists = _cache(db, 'canonicalLists');
        _lst = lists && lists.find(l => l.id === listId);

        if (!_lst) {
            navigate({ view: 'stats' });
            return;
        }
        const lst = _lst;
        _groupUnit = _pickGroupUnit(lst);

        setPageTitle(lst.short_name || lst.name);

        const pct = lst.total ? Math.round((lst.heard / lst.total) * 100) : 0;
        const gapNote = lst.matched < lst.total
            ? `<span class="list-progress-gap">${lst.total - lst.matched} not yet in your library</span>`
            : '';
        const sourceLink = lst.source_url
            ? `<a href="${escapeHtml(lst.source_url)}" target="_blank" rel="noopener" class="list-progress-source">Source ↗</a>`
            : '';

        container.innerHTML = `
            <h1>${escapeHtml(lst.name)}</h1>
            <p class="list-progress-summary">
                <span class="list-progress-pct">${pct}%</span>
                <span class="list-progress-count">${lst.heard} of ${lst.total} heard</span>
                ${gapNote}
                ${sourceLink}
            </p>
            <p class="list-progress-summary list-progress-summary-secondary">
                <span class="list-progress-count">${lst.avg_completion}% average completion across matched albums</span>
            </p>

            <div class="list-toolbar">
                <div class="sort-controls">
                    <span class="disc-sort-label">Sort by</span>
                    <button type="button" class="sort-btn list-sort-btn" data-sort="rank">List order</button>
                    <button type="button" class="sort-btn list-sort-btn" data-sort="completion-desc">Most complete</button>
                    <button type="button" class="sort-btn list-sort-btn" data-sort="completion-asc">Least complete</button>
                </div>
                <div class="sort-controls">
                    <button type="button" id="listGroupBtn" class="sort-btn">Group by ${_groupUnit}</button>
                    <button type="button" id="listRandomBtn" class="sort-btn">
                        <i data-lucide="shuffle"></i> Random unheard
                    </button>
                </div>
            </div>

            <section>
                <div id="listGrid"></div>
            </section>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        _wireControls();
        _updateControls();
        _renderGrid();
    }

    function unmount() {}

    return { mount, unmount };
})();
