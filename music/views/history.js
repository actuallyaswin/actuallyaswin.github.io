const ViewHistory = (() => {
    let _db       = null;
    let _allRows  = [];
    let _rows     = [];
    let _scrollEl = null;
    let _raf      = null;
    let _window   = 'month';
    let _source   = 'all';
    let _query    = '';
    let _debounce = null;

    const ROW_H  = 48;
    const BUFFER = 10;

    // ── Time window options ────────────────────────────────────────────────────
    const WINDOWS = {
        week:  7  * 86400,
        month: 30 * 86400,
        '3mo': 90 * 86400,
        year:  365 * 86400,
        all:   null,
    };

    function mount(container, db, params) {
        _db = db;
        document.title = 'aswin.db/music – History';

        container.innerHTML = `
            <header><h1>History</h1></header>
            <div class="page-controls">
                <div class="control-block">
                    <span class="control-block-label">Period</span>
                    <div class="sort-controls">
                        ${Object.keys(WINDOWS).map(k =>
                            `<button class="sort-btn${k===_window?' active':''}" data-win="${k}">${
                                {week:'Week',month:'Month','3mo':'3 Mo',year:'Year',all:'All'}[k]
                            }</button>`
                        ).join('')}
                    </div>
                </div>
                <div class="control-block">
                    <span class="control-block-label">Source</span>
                    <div class="sort-controls">
                        <button class="sort-btn${_source==='all'?' active':''}" data-src="all">All</button>
                        <button class="sort-btn${_source==='lastfm'?' active':''}" data-src="lastfm">Last.fm</button>
                        <button class="sort-btn${_source==='spotify'?' active':''}" data-src="spotify">Spotify</button>
                    </div>
                </div>
                <div class="control-block" style="flex:1;max-width:260px">
                    <input id="historySearch" class="admin-filter-input" placeholder="Search track, artist…"
                           style="width:100%" autocomplete="off">
                </div>
                <span id="historyCount" style="font-size:0.75rem;color:var(--text-tertiary);margin-left:auto;white-space:nowrap"></span>
            </div>
            <div class="list-with-sidebar">
                <div class="list-scroll" id="historyScroll">
                    <div id="historySpacerTop" style="height:0"></div>
                    <div id="historyList"></div>
                    <div id="historySpacerBot" style="height:0"></div>
                </div>
                <aside class="view-sidebar" id="historySidebar"></aside>
            </div>
        `;

        _scrollEl = document.getElementById('historyScroll');
        requestAnimationFrame(() => {
            const top = _scrollEl.getBoundingClientRect().top;
            _scrollEl.style.height = `${window.innerHeight - top - 16}px`;
        });
        _scrollEl.addEventListener('scroll', _schedule, { passive: true });

        _setupControls();
        _load();
    }

    function unmount() {
        if (_raf)      { cancelAnimationFrame(_raf); _raf = null; }
        if (_debounce) { clearTimeout(_debounce); _debounce = null; }
        _scrollEl = null;
        _allRows = [];
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

        const list = document.getElementById('historyList');
        list.innerHTML = '';
        for (let i = start; i < end; i++) list.appendChild(_buildRow(_rows[i]));

        document.getElementById('historySpacerTop').style.height = `${start * ROW_H}px`;
        document.getElementById('historySpacerBot').style.height = `${(_rows.length - end) * ROW_H}px`;

        const countEl = document.getElementById('historyCount');
        if (countEl) countEl.textContent = `${_rows.length.toLocaleString()} listens`;
    }

    function _buildRow(r) {
        // r: [id, timestamp, source, ms_played, raw_track, raw_artist, raw_album,
        //     track_id, track_title, release_id, release_title, art, artist_id, artist_name]
        const [id, ts, source, ms, rawTrack, rawArtist, rawAlbum,
               trackId, trackTitle, releaseId, releaseTitle, art, artistId, artistName] = r;

        const matched = !!trackId;
        const el = document.createElement(matched && releaseId ? 'a' : 'div');
        el.className = 'recent-play-row history-row' + (matched ? '' : ' history-unmatched');
        if (matched && releaseId) el.href = `?view=release&id=${encodeURIComponent(releaseId)}`;
        el.style.height = ROW_H + 'px';
        el.style.boxSizing = 'border-box';

        const title  = matched ? (trackTitle || rawTrack) : rawTrack;
        const sub    = matched
            ? [artistName, releaseTitle].filter(Boolean).join(' · ')
            : [rawArtist, rawAlbum].filter(Boolean).join(' · ');
        const thumb  = matched && art
            ? `<div class="recent-play-thumb" style="background-image:url('${art}')"></div>`
            : `<div class="recent-play-thumb history-thumb-empty"></div>`;
        const srcBadge = `<span class="history-source history-source-${source}">${source === 'spotify' ? 'SP' : 'LFM'}</span>`;
        const timeStr  = _relTime(ts);
        const durStr   = ms ? ` · ${Math.round(ms/1000/60)}m${Math.round((ms/1000)%60).toString().padStart(2,'0')}s` : '';

        el.innerHTML = `
            ${thumb}
            <div class="recent-play-info">
                <div class="recent-play-name">${escapeHtml(title || '')}</div>
                ${sub ? `<div class="recent-play-album">${escapeHtml(sub)}${durStr}</div>` : ''}
            </div>
            ${srcBadge}
            <span class="recent-play-date">${timeStr}</span>`;
        return el;
    }

    function _relTime(ts) {
        const diff = Math.floor(Date.now()/1000) - ts;
        if (diff < 60)     return `${diff}s ago`;
        if (diff < 3600)   return `${Math.floor(diff/60)}m ago`;
        if (diff < 86400)  return `${Math.floor(diff/3600)}h ago`;
        if (diff < 604800) return `${Math.floor(diff/86400)}d ago`;
        const d = new Date(ts * 1000);
        return d.toLocaleDateString('en-US', { month:'short', day:'numeric', year:'numeric' });
    }

    function _load() {
        const now   = Math.floor(Date.now() / 1000);
        const secs  = WINDOWS[_window];
        const since = secs ? now - secs : null;

        const timeClause   = since ? `AND l.timestamp >= ${since}` : '';
        const srcClause    = _source !== 'all' ? `AND l.source = '${_source}'` : '';
        const matchClause  = (typeof HISTORY_SHOW_UNMATCHED !== 'undefined' && !HISTORY_SHOW_UNMATCHED)
            ? 'AND l.track_id IS NOT NULL'
            : '';

        const res = _db.exec(`
            SELECT l.id, l.timestamp, l.source, l.ms_played,
                   l.raw_track_name, l.raw_artist_name, l.raw_album_name,
                   l.track_id,
                   t.title,
                   r.id, r.title,
                   COALESCE(r.album_art_thumb_url, r.album_art_url),
                   a.id, a.name
            FROM listens l
            LEFT JOIN tracks t  ON l.track_id = t.id
            LEFT JOIN releases r ON r.id = t.release_id
            LEFT JOIN artists a  ON a.id = r.primary_artist_id
            WHERE 1=1 ${timeClause} ${srcClause} ${matchClause}
            ORDER BY l.timestamp DESC
        `)[0];

        _allRows = res ? res.values : [];
        _applySearch();
    }

    function _applySearch() {
        const q = _query.toLowerCase().trim();
        _rows = q
            ? _allRows.filter(r =>
                (r[4] || '').toLowerCase().includes(q) ||
                (r[5] || '').toLowerCase().includes(q) ||
                (r[8] || '').toLowerCase().includes(q) ||
                (r[13]|| '').toLowerCase().includes(q))
            : _allRows;

        if (_scrollEl) _scrollEl.scrollTop = 0;
        _render();
        _renderSidebar();
    }

    function _renderSidebar() {
        const el = document.getElementById('historySidebar');
        if (!el) return;

        const total   = _rows.length;
        const matched = _rows.filter(r => r[7]).length;
        const lfm     = _rows.filter(r => r[2] === 'lastfm').length;
        const sp      = _rows.filter(r => r[2] === 'spotify').length;
        const matchPct = total ? Math.round((matched / total) * 100) : 0;

        // Peak hour from timestamps
        const hourBucket = new Array(24).fill(0);
        _rows.forEach(r => { hourBucket[new Date(r[1] * 1000).getHours()]++; });
        const peakHour = hourBucket.indexOf(Math.max(...hourBucket));

        // Top releases (by listens count in current view)
        const relCount = {}, relName = {};
        _rows.forEach(r => { if (r[9]) { relCount[r[9]] = (relCount[r[9]]||0)+1; relName[r[9]] = r[10]; }});
        const topReleases = Object.entries(relCount).sort(([,a],[,b])=>b-a).slice(0,5);

        // Top artists
        const artCount = {}, artName = {};
        _rows.forEach(r => { if (r[12]) { artCount[r[12]] = (artCount[r[12]]||0)+1; artName[r[12]] = r[13]; }});
        const topArtists = Object.entries(artCount).sort(([,a],[,b])=>b-a).slice(0,5);

        const fmt = n => n.toLocaleString();
        const summaryRows = [
            ['Listens',    fmt(total)],
            ['Matched',    `${matchPct}%`],
            ['Last.fm',    fmt(lfm)],
            ['Spotify',    fmt(sp)],
            ['Peak hour',  `${peakHour}:00`],
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
            ${topReleases.length ? `<div class="sidebar-section">
                <p class="sidebar-heading">Top Releases</p>
                ${topReleases.map(([id, n], i) => `
                    <div class="sidebar-row">
                        <span class="track-rank">${i+1}</span>
                        <a class="sidebar-row-name" href="?view=release&id=${encodeURIComponent(id)}">${escapeHtml(relName[id]||id)}</a>
                        <span class="sidebar-row-count">${n}</span>
                    </div>`).join('')}
            </div>` : ''}
            ${topArtists.length ? `<div class="sidebar-section">
                <p class="sidebar-heading">Top Artists</p>
                ${topArtists.map(([id, n], i) => `
                    <div class="sidebar-row">
                        <span class="track-rank">${i+1}</span>
                        <a class="sidebar-row-name" href="?view=artist&id=${encodeURIComponent(id)}">${escapeHtml(artName[id]||id)}</a>
                        <span class="sidebar-row-count">${n}</span>
                    </div>`).join('')}
            </div>` : ''}
        `;
    }

    function _setupControls() {
        setupToggleGroup('[data-win]', btn => {
            _window = btn.dataset.win;
            _load();
        });
        setupToggleGroup('[data-src]', btn => {
            _source = btn.dataset.src;
            _load();
        });
        document.getElementById('historySearch')?.addEventListener('input', e => {
            clearTimeout(_debounce);
            _debounce = setTimeout(() => { _query = e.target.value; _applySearch(); }, 150);
        });
    }

    return { mount, unmount };
})();
