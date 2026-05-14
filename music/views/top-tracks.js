const ViewTopTracks = (() => {
    let _db       = null;
    let _rows     = [];
    let _scrollEl = null;
    let _raf      = null;
    let _sortBy   = 'listens';
    let _year     = 'all';

    const ROW_H  = 44;
    const BUFFER = 8;

    function mount(container, db, params) {
        _db = db;
        document.title = 'aswin.db/music – Top Tracks';

        container.innerHTML = `
            <header><h1>Top Tracks</h1></header>
            <div class="page-controls">
                <div class="control-block">
                    <span class="control-block-label">Sort by</span>
                    <div class="sort-controls">
                        <button class="sort-btn${_sortBy==='listens'?' active':''}" data-sort="listens" title="By plays"><i data-lucide="headphones"></i></button>
                        <button class="sort-btn${_sortBy==='minutes'?' active':''}" data-sort="minutes" title="By minutes"><i data-lucide="clock"></i></button>
                    </div>
                </div>
                <div class="control-block">
                    <span class="control-block-label">Released</span>
                    <div class="sort-controls">
                        <select id="ttYear" class="year-filter-select">
                            <option value="all">All years</option>
                        </select>
                    </div>
                </div>
            </div>
            <div class="list-with-sidebar">
                <div class="list-scroll" id="ttScroll">
                    <div id="ttSpacerTop" style="height:0"></div>
                    <div id="ttList"></div>
                    <div id="ttSpacerBot" style="height:0"></div>
                </div>
                <aside class="view-sidebar" id="ttSidebar"></aside>
            </div>
        `;

        _scrollEl = document.getElementById('ttScroll');
        requestAnimationFrame(() => {
            const top = _scrollEl.getBoundingClientRect().top;
            _scrollEl.style.height = `${window.innerHeight - top - 16}px`;
        });
        _scrollEl.addEventListener('scroll', _schedule, { passive: true });

        _populateYears();
        _setupControls();
        _load();
    }

    function unmount() {
        if (_raf) { cancelAnimationFrame(_raf); _raf = null; }
        _scrollEl = null;
        _rows = [];
    }

    function _schedule() {
        if (_raf) cancelAnimationFrame(_raf);
        _raf = requestAnimationFrame(() => { _raf = null; _render(); });
    }

    function _render() {
        if (!_scrollEl) return;
        const scrollTop = _scrollEl.scrollTop;
        const start = Math.max(0, Math.floor(scrollTop / ROW_H) - BUFFER);
        const end   = Math.min(_rows.length, start + Math.ceil(_scrollEl.clientHeight / ROW_H) + BUFFER * 2);

        const list = document.getElementById('ttList');
        list.innerHTML = '';
        for (let i = start; i < end; i++) list.appendChild(_buildRow(_rows[i], i));

        document.getElementById('ttSpacerTop').style.height = `${start * ROW_H}px`;
        document.getElementById('ttSpacerBot').style.height = `${(_rows.length - end) * ROW_H}px`;
    }

    function _buildRow([, title, artist, , art, releaseId, listens, minutes], rank) {
        const el     = document.createElement('a');
        el.className = 'recent-play-row';
        el.href      = releaseId ? `?view=release&id=${encodeURIComponent(releaseId)}` : '#';
        el.style.height    = ROW_H + 'px';
        el.style.boxSizing = 'border-box';

        const stat = _sortBy === 'minutes'
            ? `${formatNumber(minutes)} min`
            : `${formatNumber(listens)} plays`;

        el.innerHTML = `
            <span class="track-rank">${rank + 1}</span>
            <div class="recent-play-thumb" style="background-image:url('${art || getFallbackImageUrl()}')"></div>
            <div class="recent-play-info">
                <div class="recent-play-name">${escapeHtml(title || '')}</div>
                ${artist ? `<div class="recent-play-album">${escapeHtml(artist)}</div>` : ''}
            </div>
            <span class="recent-play-date">${stat}</span>`;
        return el;
    }

    function _load() {
        const order = _sortBy === 'minutes' ? 'total_minutes DESC' : 'total_listens DESC';
        const yf    = _year !== 'all' ? `AND r.release_year = ${parseInt(_year)}` : '';
        const res   = _db.exec(`
            SELECT t.id, t.title, a.name, a.id,
                   COALESCE(r.album_art_thumb_url, r.album_art_url),
                   r.id,
                   COUNT(l.id)                                            total_listens,
                   CAST(SUM(COALESCE(t.duration_ms,0))/60000.0 AS INTEGER) total_minutes
            FROM tracks t
            LEFT JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
            LEFT JOIN artists a ON ta.artist_id = a.id
            LEFT JOIN releases r ON t.release_id = r.id
            LEFT JOIN listens l ON t.id = l.track_id
            WHERE t.hidden = 0 ${yf}
            GROUP BY t.id
            HAVING total_listens > 0
            ORDER BY ${order}
            LIMIT 5000
        `)[0];

        _rows = res ? res.values : [];
        if (_scrollEl) _scrollEl.scrollTop = 0;
        _render();
        _renderSidebar();
    }

    function _renderSidebar() {
        const el = document.getElementById('ttSidebar');
        if (!el) return;

        const totalPlays = _rows.reduce((s, r) => s + (r[6] || 0), 0);
        const totalMins  = _rows.reduce((s, r) => s + (r[7] || 0), 0);
        const avgPlays   = _rows.length ? Math.round(totalPlays / _rows.length) : 0;

        // Top artists by track count
        const artistCount = {};
        _rows.forEach(([,, name]) => { if (name) artistCount[name] = (artistCount[name] || 0) + 1; });
        const topArtists = Object.entries(artistCount).sort(([,a],[,b]) => b-a).slice(0, 7);

        const summaryRows = [
            ['Tracks',         _rows.length.toLocaleString()],
            ['Total plays',    formatNumber(totalPlays)],
            ['Listening time', `${Math.round(totalMins / 60).toLocaleString()} hr`],
            ['Avg plays',      formatNumber(avgPlays)],
        ];

        el.innerHTML = `
            <div class="sidebar-section">
                <p class="sidebar-heading">Summary</p>
                <dl class="nerds-list" style="border:none;border-radius:0">
                    ${summaryRows.map(([k,v]) =>
                        `<div class="nerds-row"><dt>${k}</dt><dd>${v}</dd></div>`
                    ).join('')}
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
            </div>
        `;
    }

    function _populateYears() {
        const sel = document.getElementById('ttYear');
        const res = _db.exec(`
            SELECT DISTINCT r.release_year FROM tracks t
            LEFT JOIN releases r ON t.release_id = r.id
            WHERE r.release_year IS NOT NULL AND t.hidden = 0
            ORDER BY r.release_year DESC
        `)[0];
        if (res) res.values.forEach(([yr]) => {
            const o = document.createElement('option');
            o.value = yr; o.textContent = yr;
            sel.appendChild(o);
        });
    }

    function _setupControls() {
        setupToggleGroup('[data-sort]', btn => {
            _sortBy = btn.dataset.sort;
            _load();
        });
        document.getElementById('ttYear')?.addEventListener('change', e => {
            _year = e.target.value;
            _load();
        });
    }

    return { mount, unmount };
})();
