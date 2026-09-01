const ViewConcerts = (() => {
    let _db        = null;
    let _allEvents = [];   // chronological (oldest first), unfiltered
    let _events    = [];   // filtered + sorted for display
    let _scrollEl  = null;
    let _raf       = null;
    let _sortDir   = 'desc';   // 'desc' = newest first
    let _query     = '';
    let _debounce  = null;
    let _ac        = null;

    const ROW_H  = 56;
    const BUFFER = 10;

    function mount(container, db, params) {
        _db = db;
        setPageTitle('Concerts');

        container.innerHTML = `
            <header>
                <h1>Concerts</h1>
                <p class="subtitle" id="concertsCount"></p>
                <a href="?view=stats" class="back-button" style="margin-top:1rem">View concert stats</a>
            </header>
            <div class="page-controls">
                <div class="control-block">
                    <span class="control-block-label">Sort</span>
                    <div class="sort-controls">
                        <button class="sort-btn${_sortDir === 'desc' ? ' active' : ''}" data-sort="desc">Newest</button>
                        <button class="sort-btn${_sortDir === 'asc'  ? ' active' : ''}" data-sort="asc">Oldest</button>
                    </div>
                </div>
                <div class="control-block control-block-grow">
                    <span class="control-block-label">Search</span>
                    <div class="filter-search" style="width:100%">
                        <i data-lucide="search" class="filter-search-icon"></i>
                        <input id="concertsSearch" class="filter-search-input" placeholder="Search artist, venue, festival…"
                               autocomplete="off" value="${escapeHtml(_query)}">
                    </div>
                </div>
            </div>
            <div class="list-with-sidebar">
                <div class="list-scroll" id="concertsScroll">
                    <div id="concertsSpacerTop" style="height:0"></div>
                    <div id="concertsList"></div>
                    <div id="concertsSpacerBot" style="height:0"></div>
                </div>
                <aside class="view-sidebar" id="concertsSidebar"></aside>
            </div>
        `;

        _scrollEl = document.getElementById('concertsScroll');
        requestAnimationFrame(() => {
            const top = _scrollEl.getBoundingClientRect().top;
            _scrollEl.style.height = `${window.innerHeight - top - 16}px`;
        });
        _scrollEl.addEventListener('scroll', _schedule, { passive: true });

        _ac = new AbortController();
        _setupControls();
        _load();
    }

    function unmount() {
        if (_raf)      { cancelAnimationFrame(_raf); _raf = null; }
        if (_debounce) { clearTimeout(_debounce); _debounce = null; }
        _scrollEl  = null;
        _allEvents = [];
        _events    = [];
        _ac?.abort();
        _ac = null;
    }

    function _schedule() {
        if (_raf) cancelAnimationFrame(_raf);
        _raf = requestAnimationFrame(() => { _raf = null; _render(); });
    }

    function _render() {
        if (!_scrollEl) return;
        const scrollTop = _scrollEl.scrollTop;
        const start = Math.max(0, Math.floor(scrollTop / ROW_H) - BUFFER);
        const end   = Math.min(_events.length, start + Math.ceil(_scrollEl.clientHeight / ROW_H) + BUFFER * 2);

        const list = document.getElementById('concertsList');
        list.innerHTML = '';
        for (let i = start; i < end; i++) list.appendChild(_buildRow(_events[i]));

        document.getElementById('concertsSpacerTop').style.height = `${start * ROW_H}px`;
        document.getElementById('concertsSpacerBot').style.height = `${(_events.length - end) * ROW_H}px`;

        const countEl = document.getElementById('concertsCount');
        if (countEl) countEl.textContent = `${_events.length.toLocaleString()} concerts`;
    }

    function _buildRow(ev) {
        const el = document.createElement('div');
        el.className = 'concert-row';
        el.style.height = ROW_H + 'px';
        el.style.boxSizing = 'border-box';

        const d     = new Date(ev.date + 'T00:00:00Z');
        const month = d.toLocaleDateString('en-US', { month: 'short', timeZone: 'UTC' });
        const day   = d.toLocaleDateString('en-US', { day: 'numeric', timeZone: 'UTC' });
        const year  = d.getUTCFullYear();
        const dateBoxHtml = `<div class="concert-date">
            <span class="concert-date-month">${escapeHtml(month)}</span>
            <span class="concert-date-day">${escapeHtml(day)}</span>
            <span class="concert-date-year">${year}</span>
        </div>`;

        const headliner = ev.headliners[0] || ev.festivalActs[0] || null;
        const thumbSrc   = (headliner && (headliner.imageThumb || headliner.imageUrl)) || getFallbackImageUrl();
        const thumbHtml  = `<div class="concert-thumb" style="background-image:url('${cssUrl(thumbSrc)}')"></div>`;

        let venueStr = [ev.venue, [ev.city, ev.state].filter(Boolean).join(', ')]
            .filter(Boolean).join(' · ');
        if (headliner && headliner.tourName) venueStr += ` — ${headliner.tourName}`;

        const festivalBadge = ev.isFestival
            ? `<span class="concert-festival-badge" title="${escapeHtml(ev.festivalName || 'Festival')}">${escapeHtml(ev.festivalName || 'Festival')}</span>`
            : '';
        const linkIcon = ev.primaryUrl
            ? `<a href="${escapeHtml(ev.primaryUrl)}" target="_blank" rel="noopener" class="concert-external-link" title="View on setlist.fm">
                   <i data-lucide="external-link"></i></a>`
            : '';

        el.innerHTML = `
            ${dateBoxHtml}
            ${thumbHtml}
            <div class="concert-info">
                <div class="concert-byline">${_bylineHtml(ev)}</div>
                <div class="concert-venue">${escapeHtml(venueStr)}</div>
            </div>
            ${festivalBadge}
            ${linkIcon}
        `;
        return el;
    }

    // "<b>Headliner</b> ∙ Supporting Act 1, Supporting Act 2" — co-headliners
    // both bold and joined with "&"; a festival day with no defined headliner
    // (billing='festival') just lists who you saw, no bolding.
    function _bylineHtml(ev) {
        const link = p => `<a href="${artistHref(p.id, p.slug)}">${escapeHtml(p.name)}</a>${_guestNoteHtml(p)}`;

        if (ev.headliners.length) {
            const headStr    = ev.headliners.map(p => `<b>${link(p)}</b>`).join(' &amp; ');
            const supportStr = ev.supports.map(link).join(', ');
            return supportStr ? `${headStr} ∙ ${supportStr}` : headStr;
        }
        return ev.festivalActs.map(link).join(', ');
    }

    function _guestNoteHtml(p) {
        if (!p.guests.length) return '';
        const names = p.guests.map(g => `<a href="${artistHref(g.id, g.slug)}">${escapeHtml(g.name)}</a>`);
        return ` <span class="concert-guest-note">(with ${names.join(', ')})</span>`;
    }

    function _load() {
        const res = _db.exec(`
            SELECT ce.id, ce.event_date, v.id, v.name, v.city, v.state,
                   ce.is_festival, ce.festival_name,
                   cp.id, cp.billing, cp.set_order, cp.setlistfm_url, cp.tour_name,
                   a.id, a.name, a.slug, a.image_thumb_url, a.image_url
            FROM concert_events ce
            JOIN venues v ON v.id = ce.venue_id
            JOIN concert_performances cp ON cp.event_id = ce.id
            JOIN artists a ON a.id = cp.artist_id
            ORDER BY ce.event_date ASC, ce.id,
                     CASE cp.billing WHEN 'headliner' THEN 0 WHEN 'co-headliner' THEN 0
                                      WHEN 'festival' THEN 1 WHEN 'support' THEN 2 ELSE 3 END,
                     cp.set_order IS NULL, cp.set_order ASC, a.name ASC
        `)[0];
        const rows = res ? res.values : [];

        const guestRes = _db.exec(`
            SELECT g.performance_id, a.id, a.name, a.slug
            FROM concert_performance_guests g
            JOIN artists a ON a.id = g.artist_id
        `)[0];
        const guestsByPerf = {};
        (guestRes ? guestRes.values : []).forEach(([perfId, aid, aname, aslug]) => {
            (guestsByPerf[perfId] = guestsByPerf[perfId] || []).push({ id: aid, name: aname, slug: aslug });
        });

        const byId = {};
        const order = [];
        rows.forEach(r => {
            const [eventId, date, venueId, venue, city, state, isFestival, festivalName,
                   perfId, billing, setOrder, url, tourName, artistId, artistName, artistSlug,
                   imageThumb, imageUrl] = r;
            if (!byId[eventId]) {
                byId[eventId] = {
                    id: eventId, date, venueId, venue, city, state,
                    isFestival: !!isFestival, festivalName,
                    headliners: [], supports: [], festivalActs: [], primaryUrl: null,
                };
                order.push(eventId);
            }
            const ev = byId[eventId];
            const perf = {
                id: artistId, name: artistName, slug: artistSlug, guests: guestsByPerf[perfId] || [],
                imageThumb, imageUrl, tourName,
            };
            if (billing === 'headliner' || billing === 'co-headliner') {
                ev.headliners.push(perf);
                if (!ev.primaryUrl) ev.primaryUrl = url;
            } else if (billing === 'festival') {
                ev.festivalActs.push(perf);
                if (!ev.primaryUrl) ev.primaryUrl = url;
            } else {
                ev.supports.push(perf);
            }
        });

        _allEvents = order.map(id => byId[id]);
        _applyFilters();
    }

    function _applyFilters() {
        const q = _query.toLowerCase().trim();
        let evs = _allEvents;
        if (q) {
            evs = evs.filter(ev => {
                const hay = [
                    ev.venue, ev.city, ev.state, ev.festivalName,
                    ...ev.headliners.map(p => p.name),
                    ...ev.supports.map(p => p.name),
                    ...ev.festivalActs.map(p => p.name),
                ].filter(Boolean).join(' ').toLowerCase();
                return hay.includes(q);
            });
        }
        _events = _sortDir === 'desc' ? [...evs].reverse() : evs;

        if (_scrollEl) _scrollEl.scrollTop = 0;
        _render();
        _renderSidebar();
    }

    function _renderSidebar() {
        const el = document.getElementById('concertsSidebar');
        if (!el) return;

        const total = _events.length;
        const venueIds  = new Set();
        const artistIds = new Map(); // id -> { name, slug, count }
        const festivals = new Set();
        let firstDate = null, lastDate = null;

        const trackArtist = p => {
            if (!artistIds.has(p.id)) artistIds.set(p.id, { name: p.name, slug: p.slug, count: 0 });
            artistIds.get(p.id).count++;
        };

        _events.forEach(ev => {
            venueIds.add(ev.venueId);
            if (ev.isFestival && ev.festivalName) festivals.add(ev.festivalName);
            if (!firstDate || ev.date < firstDate) firstDate = ev.date;
            if (!lastDate  || ev.date > lastDate)  lastDate  = ev.date;
            [...ev.headliners, ...ev.supports, ...ev.festivalActs].forEach(p => {
                trackArtist(p);
                p.guests.forEach(trackArtist);
            });
        });

        const venueCounts = new Map(); // venueId -> { name, sub, count }
        _events.forEach(ev => {
            if (!venueCounts.has(ev.venueId)) {
                venueCounts.set(ev.venueId, { name: ev.venue, sub: [ev.city, ev.state].filter(Boolean).join(', '), count: 0 });
            }
            venueCounts.get(ev.venueId).count++;
        });
        const topVenues  = [...venueCounts.values()].sort((a, b) => b.count - a.count).slice(0, 5);
        const topArtists = [...artistIds.values()].sort((a, b) => b.count - a.count).slice(0, 5);

        const fmtDate = s => s ? new Date(s + 'T00:00:00Z').toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' }) : '—';
        const summaryRows = [
            ['Concerts',     total.toLocaleString()],
            ['Venues',       venueIds.size.toLocaleString()],
            ['Artists seen', artistIds.size.toLocaleString()],
            ['Festivals',    festivals.size.toLocaleString()],
            ['First show',   fmtDate(firstDate)],
            ['Latest show',  fmtDate(lastDate)],
        ];

        el.innerHTML = `
            <div class="sidebar-section">
                <p class="sidebar-heading">Summary</p>
                <dl class="nerds-list" style="border:none;border-radius:0">
                    ${summaryRows.map(([k, v]) => `<div class="nerds-row"><dt>${k}</dt><dd>${v}</dd></div>`).join('')}
                </dl>
            </div>
            ${topVenues.length ? `<div class="sidebar-section">
                <p class="sidebar-heading">Top Venues</p>
                ${topVenues.map((v, i) => `
                    <div class="sidebar-row">
                        <span class="track-rank">${i + 1}</span>
                        <span class="sidebar-row-name" title="${escapeHtml(v.name)}">${escapeHtml(v.name)}</span>
                        ${v.sub ? `<span class="concert-venue-sub">(${escapeHtml(v.sub)})</span>` : ''}
                        <span class="sidebar-row-count">${v.count}</span>
                    </div>`).join('')}
            </div>` : ''}
            ${topArtists.length ? `<div class="sidebar-section">
                <p class="sidebar-heading">Most Seen Artists</p>
                ${topArtists.map((a, i) => `
                    <div class="sidebar-row">
                        <span class="track-rank">${i + 1}</span>
                        <a class="sidebar-row-name" href="${artistHref(a.id, a.slug)}">${escapeHtml(a.name)}</a>
                        <span class="sidebar-row-count">${a.count}</span>
                    </div>`).join('')}
            </div>` : ''}
        `;
    }

    function _setupControls() {
        setupToggleGroup('[data-sort]', btn => {
            _sortDir = btn.dataset.sort;
            _applyFilters();
        });
        document.getElementById('concertsSearch')?.addEventListener('input', e => {
            clearTimeout(_debounce);
            _debounce = setTimeout(() => { _query = e.target.value; _applyFilters(); }, 150);
        });
    }

    return { mount, unmount };
})();
