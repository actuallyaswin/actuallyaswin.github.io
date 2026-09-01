// ConcertStats — renders every concerts* stats_cache section computed by
// mdb_concert_stats.py. Deliberately its own file/global, called from
// views/stats.js's _render()/unmount() rather than folded in directly — see
// the top of mdb_concert_stats.py for why the Python side is split the same
// way. Reuses the generic bar-row/drill-down/stat-card renderers that used
// to live only in stats.js (moved to utils.js so both files can call them).
const ConcertStats = (() => {
    let _perYearChart = null;

    function _empty(note) {
        return `<section class="stat-section">
            <h3>Concerts</h3>
            <div class="stats-empty">
                <i data-lucide="construction" style="width:20px;height:20px;color:var(--text-tertiary)"></i>
                <span>${escapeHtml(note)}</span>
            </div>
        </section>`;
    }

    // ── Headline stat cards ──────────────────────────────────────────────────
    function _headlineSection(cache) {
        const h = cache('concertsHeadline');
        if (!h || !h.concerts) return _empty('No concert data yet — run `mdb stats refresh` after importing some shows.');

        const fmtDate = s => s
            ? new Date(s + 'T00:00:00Z').toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' })
            : '—';
        const items = [
            ['Concerts', formatNumber(h.concerts)],
            ['Venues', formatNumber(h.venues)],
            ['Artists seen', formatNumber(h.artists)],
            ['Festivals', formatNumber(h.festivals)],
            ['Cities', formatNumber(h.cities)],
            ['Countries', formatNumber(h.countries)],
            ['First show', fmtDate(h.firstShow)],
            ['Latest show', fmtDate(h.latestShow)],
        ];
        return `<section class="stat-section">
            <h3>Concerts</h3>
            <p class="rec-desc">Live shows you've actually attended, tracked via setlist.fm.
                <a href="?view=concerts" style="color:var(--primary);text-decoration:underline">View all concerts</a></p>
            ${_statCards(items, 'stats--quarters')}
        </section>`;
    }

    // ── Top venues ───────────────────────────────────────────────────────────
    function _venuesSection(cache) {
        const items = cache('concertsVenues');
        if (!items || !items.length) return '';
        return `<section class="stat-section">
            <h3>Top Venues</h3>
            <p class="rec-desc">Most-visited venues, by distinct nights (not sets — a 4-act festival day counts once).</p>
            <div class="bar-list has-drilldown">${_breakdownRows(items, null, () => 'concert-artist')}</div>
        </section>`;
    }

    // ── Top artists seen live ────────────────────────────────────────────────
    function _topArtistsSection(cache) {
        const items = cache('concertsTopArtists');
        if (!items || !items.length) return '';
        const cards = items.map(a => `
            <a href="${artistHref(a.id, a.slug)}" class="bar-expand-card">
                <div class="bar-expand-thumb rounded" style="background-image:url('${cssUrl(a.img || getFallbackImageUrl())}')"></div>
                <div class="bar-expand-name">${escapeHtml(a.name)}</div>
                <div class="bar-expand-count">${a.n} show${a.n === 1 ? '' : 's'}</div>
            </a>`).join('');
        return `<section class="stat-section">
            <h3>Most Seen Live</h3>
            <p class="rec-desc">Artists you've caught the most times in person.</p>
            <div class="bar-expand bar-expand-static">${cards}</div>
        </section>`;
    }

    // ── Concerts per year (Chart.js bar chart, same pattern as stats.js's
    // mainstream-popularity histogram) ──────────────────────────────────────
    function _perYearSection(cache) {
        const items = cache('concertsPerYear');
        if (!items || !items.length) return '';
        return `<section class="stat-section">
            <h3>Concerts Per Year</h3>
            <div class="mainstream-hist-wrap"><canvas id="concertsPerYearChart"></canvas></div>
        </section>`;
    }

    function _renderPerYearChart(cache) {
        const items = cache('concertsPerYear');
        const canvas = document.getElementById('concertsPerYearChart');
        if (!items || !items.length || !canvas) return;

        if (_perYearChart) { _perYearChart.destroy(); _perYearChart = null; }

        const primaryColor  = getCSSColor('--primary');
        const textSecondary = getCSSColor('--text-secondary');
        const borderColor   = getCSSColor('--border');
        const bgSecondary   = getCSSColor('--bg-secondary');
        const textColor     = getCSSColor('--text');

        _perYearChart = new Chart(canvas.getContext('2d'), {
            type: 'bar',
            data: {
                labels: items.map(it => it.year),
                datasets: [{
                    data: items.map(it => it.n),
                    backgroundColor: primaryColor,
                    borderRadius: 4,
                    maxBarThickness: 40,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        backgroundColor: bgSecondary,
                        titleColor: textColor,
                        bodyColor: textColor,
                        borderColor,
                        borderWidth: 1,
                        callbacks: { label: item => `${formatNumber(item.raw)} concert${item.raw === 1 ? '' : 's'}` },
                    },
                },
                scales: {
                    y: { beginAtZero: true, ticks: { color: textSecondary, precision: 0 }, grid: { color: borderColor } },
                    x: { ticks: { color: textSecondary }, grid: { display: false } },
                },
            },
        });
    }

    // ── Spotlights: never-seen-live vs. seen-but-barely-played ──────────────
    function _spotlightCardHtml(label, icon, artist, playsLabel) {
        if (!artist) return '';
        return `
            <a href="${artistHref(artist.id, artist.slug)}" class="mainstream-spotlight-card">
                <div class="mainstream-spotlight-icon"><i data-lucide="${icon}"></i></div>
                <div class="mainstream-spotlight-text">
                    <div class="mainstream-spotlight-label">${escapeHtml(label)}</div>
                    <div class="mainstream-spotlight-name">${escapeHtml(artist.name)}</div>
                    <div class="mainstream-spotlight-sub">${playsLabel}</div>
                </div>
            </a>`;
    }

    function _spotlightsSection(cache) {
        const data = cache('concertsSpotlights');
        if (!data) return '';
        const cards = [
            _spotlightCardHtml('Most-played, never seen live', 'headphones', data.neverSeen,
                data.neverSeen ? `${formatNumber(data.neverSeen.plays)} plays` : ''),
            _spotlightCardHtml('Seen live, barely played', 'ticket', data.obscureSeen,
                data.obscureSeen ? `${formatNumber(data.obscureSeen.plays)} plays` : ''),
        ].filter(Boolean).join('');
        if (!cards) return '';
        return `<section class="stat-section">
            <h3>Live vs. Listening</h3>
            <div class="mainstream-spotlight-grid">${cards}</div>
        </section>`;
    }

    // ── Coverage: top-N most-played artists, how many seen live ─────────────
    function _coverageSection(cache) {
        const c = cache('concertsCoverage');
        if (!c || !c.topN) return '';
        const pct = c.topN ? Math.round((c.seenCount / c.topN) * 100) : 0;
        const unseenCards = c.unseen.map(a => `
            <a href="${artistHref(a.id, a.slug)}" class="bar-expand-card">
                <div class="bar-expand-thumb rounded" style="background-image:url('${cssUrl(a.img || getFallbackImageUrl())}')"></div>
                <div class="bar-expand-name">${escapeHtml(a.name)}</div>
                <div class="bar-expand-count">${formatNumber(a.plays)} plays</div>
            </a>`).join('');
        return `<section class="stat-section">
            <h3>Live Coverage</h3>
            <p class="rec-desc">Of your top ${c.topN} most-played artists, how many have you seen live?</p>
            ${_statCards([['Seen live', `${c.seenCount} of ${c.topN} (${pct}%)`]])}
            ${unseenCards ? `<p class="rec-desc" style="margin-top:1rem">Highest-played artists you haven't caught yet:</p>
                <div class="bar-expand bar-expand-static">${unseenCards}</div>` : ''}
        </section>`;
    }

    // ── Public API ───────────────────────────────────────────────────────────
    // Each render* function returns one bare <section> — no group/grid
    // wrapper — so stats.js's LAYOUT array can slot any of them in at
    // whatever width/position it wants, same as its own native sections.
    // `cache` is stats.js's own _cache(key) function, passed through rather
    // than duplicated — this file has no _db of its own.
    return {
        renderHeadline: _headlineSection,
        renderVenues: _venuesSection,
        renderTopArtists: _topArtistsSection,
        renderPerYear: _perYearSection,
        renderSpotlights: _spotlightsSection,
        renderCoverage: _coverageSection,
        // Called after the section HTML above is actually in the DOM (canvas
        // elements don't exist until then) — mirrors stats.js's own
        // _renderMainstreamHistogram() timing.
        afterRender: _renderPerYearChart,
        destroy() {
            if (_perYearChart) { _perYearChart.destroy(); _perYearChart = null; }
        },
    };
})();
