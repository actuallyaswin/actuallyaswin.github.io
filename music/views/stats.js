const ViewStats = (() => {
    let _db = null;
    // Chart.js instances -- created after _render()'s innerHTML assignment
    // (canvases don't exist until then), destroyed on unmount/re-render so
    // this view doesn't leak a chart bound to a canvas that no longer exists.
    let _histChart = null;
    let _labelsChart = null;
    let _listensPerYearChart = null;

    function mount(container, db, params) {
        _db = db;
        setPageTitle('Stats');

        container.innerHTML = `
            <header>
                <h1>Stats</h1>
            </header>
            <div id="statsContent"></div>
        `;

        _render();
    }

    function unmount() {
        _db = null;
        if (_histChart) { _histChart.destroy(); _histChart = null; }
        if (_labelsChart) { _labelsChart.destroy(); _labelsChart = null; }
        if (_listensPerYearChart) { _listensPerYearChart.destroy(); _listensPerYearChart = null; }
        ConcertStats.destroy();
    }

    // ── Cache access ─────────────────────────────────────────────────────────
    // Every section below is precomputed into `stats_cache` by `mdb.py stats
    // refresh` (part of `just db-checkpoint`). Rendering only parses pre-baked
    // JSON — no live SQL here beyond the cache lookup.
    function _cache(key) {
        const res = _db.exec('SELECT value_json FROM stats_cache WHERE key = ?', [key])[0];
        return res ? JSON.parse(res.values[0][0]) : null;
    }

    // ── Section copy ───────────────────────────────────────────────────────────
    // Single source of truth for each section's title + description text, so
    // renaming a section only means editing one entry here instead of hunting
    // down the matching <h3> call sites separately. `noDrill: true` on a
    // drill-capable section means "don't render a chevron at all" — set on
    // sections where expanding a row would just echo the section itself
    // (Release Type/Recency/Explicit already show every category as a full
    // pie; drilling into one category doesn't surface anything new).
    const SECTIONS = {
        language: {
            title: 'Language Breakdown',
            desc: 'Share of plays by track language.',
            kind: 'release',
        },
        gender: {
            title: 'Artist Gender',
            desc: 'Share of plays by artist gender, excluding bands and groups.',
            kind: 'artist',
        },
        artistType: {
            title: 'Artist Type',
            desc: 'Share of plays by artist type (solo, group, etc.).',
            kind: 'artist',
        },
        era: {
            title: 'Release Era',
            desc: 'Share of plays by the decade the album came out.',
            kind: 'release',
        },
        country: {
            title: 'Artist Country',
            desc: 'Share of plays by artist country. Showing the top 12.',
            kind: 'artist',
        },
        releaseType: {
            title: 'Release Type',
            desc: 'Share of plays by release type (album, EP, single, etc.).',
            kind: 'release',
            noDrill: true,
        },
        recency: {
            title: 'Release Recency',
            desc: 'Share of plays by how old the music was when you played it.',
            kind: 'release',
            noDrill: true,
        },
        explicit: {
            title: 'Explicit Content',
            desc: 'Share of plays that are explicit vs. clean.',
            kind: 'artist',
            noDrill: true,
        },
        tasteMainstream: {
            title: 'Mainstream Score',
            desc: 'Play-weighted average Spotify popularity (0-100) across everything you\'ve heard.',
        },
        labels: {
            title: 'Top Labels',
            desc: 'Most-played record labels.',
        },
        completion: {
            title: 'Album Completion',
            desc: 'Albums you keep coming back to but have never finished.',
        },
        canonicalLists: {
            title: 'Canonical Lists',
            desc: 'Progress against curated all-time album rankings.',
        },
        relistened: {
            title: 'Most Relistened Tracks',
            desc: 'The individual songs you replay the most.',
        },
        vinyl: {
            title: 'Vinyl Ownership',
        },
        cert: {
            title: 'Certified Artists',
            desc: 'Gold at 250 plays, platinum at 500, diamond at 1,000.',
        },
        everyYear: {
            title: 'Every-Year Artists',
            desc: 'Artists you\'ve listened to in every single year of your library.',
        },
        nerd: {
            title: 'Stats for Nerds',
        },
    };

    // Every drill-down section has a single, fixed drill kind ('artist' or
    // 'release') for all of its rows — stored on SECTIONS rather than per-row,
    // since a row's own drill array doesn't carry which kind it links to.
    function _drillKindFor(items) {
        return items._kind || null;
    }

    function _sectionHeader(key) {
        const s = SECTIONS[key];
        return `<h3>${escapeHtml(s.title)}</h3>` +
            (s.desc ? `<p class="rec-desc">${escapeHtml(s.desc)}</p>` : '');
    }

    function _emptySection(key, note) {
        return `<section class="stat-section">
            <h3>${escapeHtml(SECTIONS[key].title)}</h3>
            <div class="stats-empty">
                <i data-lucide="construction" style="width:20px;height:20px;color:var(--text-tertiary)"></i>
                <span>${escapeHtml(note)}</span>
            </div>
        </section>`;
    }

    // Wraps rendered bar rows in a <section>. `noDrill` switches the wrapper
    // between the has-drilldown (5-column, chevron reserved) and no-drill
    // (4-column, no chevron at all) .bar-list variants.
    function _section(key, rowsHtml, noDrill) {
        return `<section class="stat-section">
            ${_sectionHeader(key)}
            <div class="bar-list ${noDrill ? 'no-drill' : 'has-drilldown'}">${rowsHtml}</div>
        </section>`;
    }

    // ── Sections ────────────────────────────────────────────────────────────────
    // Each is a thin cache lookup + render — all data (including drill-downs)
    // came from `mdb.py stats refresh`; see that command for the source query
    // this section's numbers are computed from.

    function _drillSection(key, style, formatLabel) {
        const items = _cache(key);
        if (!items || !items.length) return _emptySection(key, 'No data yet — run `mdb stats refresh`.');
        const noDrill = !!SECTIONS[key].noDrill;
        items._kind = SECTIONS[key].kind;
        const rowsHtml = style === 'colored'
            ? _coloredRows(items, _drillKindFor, noDrill)
            : _breakdownRows(items, formatLabel, _drillKindFor, noDrill);
        return _section(key, rowsHtml, noDrill);
    }

    function _languageSection() { return _drillSection('language', 'breakdown'); }
    function _genderSection()   { return _drillSection('gender', 'colored'); }
    function _artistTypeSection() { return _drillSection('artistType', 'colored'); }
    function _decadeSection()   { return _drillSection('era', 'breakdown'); }
    function _releaseTypeSection() { return _drillSection('releaseType', 'breakdown'); }
    function _recencySection()  { return _drillSection('recency', 'breakdown'); }
    function _explicitSection() { return _drillSection('explicit', 'colored'); }

    function _mainstreamSpotlightCardHtml(label, icon, artist) {
        if (!artist) return '';
        return `
            <a href="${artistHref(artist.id, artist.slug)}" class="mainstream-spotlight-card">
                <div class="mainstream-spotlight-icon"><i data-lucide="${icon}"></i></div>
                <div class="mainstream-spotlight-text">
                    <div class="mainstream-spotlight-label">${escapeHtml(label)}</div>
                    <div class="mainstream-spotlight-name">${escapeHtml(artist.name)}</div>
                    <div class="mainstream-spotlight-sub">Popularity ${artist.popularity} · ${formatNumber(artist.plays)} plays</div>
                </div>
            </a>`;
    }

    function _tasteMainstreamSection() {
        const data = _cache('tasteMainstream');
        if (!data || !data.n_listens) return _emptySection('tasteMainstream', 'No data yet — run `mdb stats refresh`.');

        const spotlights = [
            _mainstreamSpotlightCardHtml('Most mainstream', 'trending-up', data.most_mainstream),
            _mainstreamSpotlightCardHtml('Most obscure', 'compass', data.most_obscure),
        ].join('');

        return `<section class="stat-section">
            ${_sectionHeader('tasteMainstream')}
            ${_statCards([['Mean', data.mean], ['Median', data.median]])}
            <div class="mainstream-hist-wrap"><canvas id="mainstreamHistChart"></canvas></div>
            ${spotlights ? `<div class="mainstream-spotlight-grid">${spotlights}</div>` : ''}
        </section>`;
    }

    // ── Listens Per Year (split out of Mainstream Score -- that section's
    // by-year list was the single largest element on the page, and its bar
    // length encoded mean popularity, which barely moves year to year
    // (everything clusters ~60) and told you little. Total listens is the
    // actually-interesting number, and as a standalone chart it can sit
    // side by side with Concerts Per Year (same X axis: year) instead of
    // burying a 16-row list inside an already-large section. ─────────────
    function _listensPerYearSection() {
        const data = _cache('tasteMainstream');
        const years = data?.by_year?.filter(y => y.n_listens >= 20);
        if (!years || !years.length) return '';
        return `<section class="stat-section">
            <h3>Listens Per Year</h3>
            <div class="mainstream-hist-wrap"><canvas id="listensPerYearChart"></canvas></div>
        </section>`;
    }

    function _renderListensPerYearChart() {
        const data = _cache('tasteMainstream');
        const years = data?.by_year?.filter(y => y.n_listens >= 20);
        const canvas = document.getElementById('listensPerYearChart');
        if (!years || !years.length || !canvas) return;

        if (_listensPerYearChart) { _listensPerYearChart.destroy(); _listensPerYearChart = null; }

        const primaryColor  = getCSSColor('--primary');
        const textSecondary = getCSSColor('--text-secondary');
        const borderColor   = getCSSColor('--border');
        const bgSecondary   = getCSSColor('--bg-secondary');
        const textColor     = getCSSColor('--text');

        _listensPerYearChart = new Chart(canvas.getContext('2d'), {
            type: 'bar',
            data: {
                labels: years.map(y => y.year),
                datasets: [{
                    data: years.map(y => y.n_listens),
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
                        callbacks: {
                            label: item => `${formatNumber(item.raw)} listens`,
                            afterLabel: item => `avg. popularity ${years[item.dataIndex].mean}`,
                        },
                    },
                },
                scales: {
                    y: { beginAtZero: true, ticks: { color: textSecondary, precision: 0 }, grid: { color: borderColor } },
                    x: { ticks: { color: textSecondary }, grid: { display: false } },
                },
            },
        });
    }

    function _renderMainstreamHistogram() {
        const data = _cache('tasteMainstream');
        const canvas = document.getElementById('mainstreamHistChart');
        if (!data || !data.histogram || !canvas) return;

        if (_histChart) { _histChart.destroy(); _histChart = null; }

        const labels = data.histogram.map((_, i) => i === 9 ? '90-100' : `${i * 10}-${i * 10 + 9}`);
        const primaryColor  = getCSSColor('--primary');
        const textSecondary = getCSSColor('--text-secondary');
        const borderColor   = getCSSColor('--border');
        const bgSecondary   = getCSSColor('--bg-secondary');
        const textColor     = getCSSColor('--text');

        _histChart = new Chart(canvas.getContext('2d'), {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    data: data.histogram,
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
                        callbacks: {
                            title: items => `Popularity ${items[0].label}`,
                            label: item => `${formatNumber(item.raw)} listens`,
                        },
                    },
                },
                scales: {
                    y: { beginAtZero: true, ticks: { color: textSecondary }, grid: { color: borderColor } },
                    x: { ticks: { color: textSecondary }, grid: { display: false } },
                },
            },
        });
    }

    const _countryNames = (() => {
        try { return new Intl.DisplayNames(['en'], { type: 'region' }); }
        catch { return null; }
    })();

    function _countrySection() {
        return _drillSection('country', 'breakdown', code => {
            const iso  = String(code).toLowerCase();
            const name = _countryNames?.of(String(code).toUpperCase()) || code;
            return `<span class="fi fi-${iso}" style="margin-right:0.4rem;flex-shrink:0"></span>${escapeHtml(name)}`;
        });
    }

    // Recast as a horizontal bar chart rather than a .bar-list -- the section
    // is naturally chart-shaped (top-N categories with counts, no drill data
    // to preserve), and reusing this shape breaks up the run of consecutive
    // bar-list sections elsewhere on the page. Horizontal (indexAxis: 'y')
    // rather than vertical bars because label names (record labels) can run
    // long ("Sub Pop Records") and read better unrotated on a y-axis.
    function _labelsChartSection() {
        const items = _cache('labels');
        if (!items || !items.length) return _emptySection('labels', 'No data yet — run `mdb stats refresh`.');
        return `<section class="stat-section">
            ${_sectionHeader('labels')}
            <div class="mainstream-hist-wrap tall"><canvas id="labelsChart"></canvas></div>
        </section>`;
    }

    function _renderLabelsChart() {
        const items = _cache('labels');
        const canvas = document.getElementById('labelsChart');
        if (!items || !items.length || !canvas) return;

        if (_labelsChart) { _labelsChart.destroy(); _labelsChart = null; }

        const primaryColor  = getCSSColor('--primary');
        const textSecondary = getCSSColor('--text-secondary');
        const borderColor   = getCSSColor('--border');
        const bgSecondary   = getCSSColor('--bg-secondary');
        const textColor     = getCSSColor('--text');

        _labelsChart = new Chart(canvas.getContext('2d'), {
            type: 'bar',
            data: {
                labels: items.map(it => it.label),
                datasets: [{
                    data: items.map(it => it.n),
                    backgroundColor: primaryColor,
                    borderRadius: 4,
                    maxBarThickness: 24,
                }],
            },
            options: {
                indexAxis: 'y',
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
                        callbacks: { label: item => `${formatNumber(item.raw)} plays` },
                    },
                },
                scales: {
                    x: { beginAtZero: true, ticks: { color: textSecondary, precision: 0 }, grid: { color: borderColor } },
                    y: {
                        ticks: {
                            color: textSecondary,
                            // Truncate long label names (e.g. "Sub Pop
                            // Records") on the render axis only -- Chart.js
                            // has no built-in wrap/ellipsis for category
                            // ticks, and without this a long name reserves
                            // however much width it needs, squeezing the
                            // actual bar area to a sliver on a narrow
                            // (~350px) phone canvas. Tooltips still show the
                            // full name since they read from the untouched
                            // data label, not this tick render.
                            callback(value) {
                                const label = this.getLabelForValue(value);
                                return label.length > 18 ? label.slice(0, 17) + '…' : label;
                            },
                        },
                        grid: { display: false },
                    },
                },
            },
        });
    }

    function _completionSection() {
        const rows = _cache('completion');
        if (!rows || !rows.length) return _emptySection('completion', 'No data yet — run `mdb stats refresh`.');

        const max = Math.max(...rows.map(r => r.listens), 1);
        const rowsHtml = rows.map(({ id, title, total, heard, listens, slug }) => {
            const pct     = total ? ((heard / total) * 100).toFixed(0) : '0';
            const opacity = (0.35 + 0.65 * (listens / max)).toFixed(2);
            const href    = releaseHref(id, slug);
            return `<div class="bar-row bar-row-static">
                <a class="bar-code bar-code-link" href="${href}">${escapeHtml(title)}</a>
                <div class="bar-track">
                    <div class="bar-fill" style="width:${pct}%;background:var(--primary);opacity:${opacity}"></div>
                </div>
                <span class="bar-count">${heard}/${total}</span>
                <span class="bar-pct">${pct}%</span>
            </div>`;
        }).join('');
        return _section('completion', rowsHtml, true);
    }

    // ── Canonical Lists (RS500, etc.) ───────────────────────────────────────
    // Row-based (not a donut grid) so this scales past a handful of lists --
    // sorted alphabetically by name (not completion %) so lists from the same
    // publisher (multiple RS/Pitchfork/Complex lists) stay grouped together
    // rather than scattering across the section. Uses .bar-columns/.bar-row-flow
    // (CSS multi-column, not a JS array split into two .bar-list halves) so
    // the single alphabetized order flows continuously column-to-column and
    // collapses to one column on mobile with no leftover gap.
    function _canonicalListsSection() {
        const lists = _cache('canonicalLists');
        if (!lists || !lists.length) return '';

        const sorted = [...lists].sort((a, b) =>
            (a.short_name || a.name).localeCompare(b.short_name || b.name));

        const rows = sorted.map(lst => {
            const pct = lst.total ? Math.round((lst.heard / lst.total) * 100) : 0;
            const label = lst.short_name || lst.name;
            return `<a class="bar-row-flow" href="?view=list&id=${encodeURIComponent(lst.id)}" title="${escapeHtml(lst.name)}">
                <span class="bar-code">${escapeHtml(label)}</span>
                <div class="bar-track">
                    <div class="bar-fill" style="width:${pct}%;background:var(--primary)"></div>
                </div>
                <span class="bar-count">${lst.heard}/${lst.total}</span>
                <span class="bar-pct">${pct}%</span>
            </a>`;
        }).join('');

        return `<section class="stat-section">
            ${_sectionHeader('canonicalLists')}
            <div class="bar-columns">${rows}</div>
        </section>`;
    }

    function _relistenedSection() {
        const rows = _cache('relistened');
        if (!rows || !rows.length) return _emptySection('relistened', 'No data yet — run `mdb stats refresh`.');

        const cards = rows.map(({ id, title, artist, art_url, release_id, n, release_slug }) => {
            const href  = release_id ? releaseHref(release_id, release_slug) : '#';
            const thumb = art_url || getFallbackImageUrl();
            const sub   = artist ? `${escapeHtml(artist)} · ${formatNumber(n)} plays` : `${formatNumber(n)} plays`;
            return `<a href="${href}" class="bar-expand-card bar-expand-card-wide">
                <div class="bar-expand-thumb" style="background-image:url('${cssUrl(thumb)}')"></div>
                <div class="bar-expand-name">${escapeHtml(title)}</div>
                <div class="bar-expand-count">${sub}</div>
            </a>`;
        }).join('');
        return `<section class="stat-section">
            ${_sectionHeader('relistened')}
            <div class="bar-expand bar-expand-static">${cards}</div>
        </section>`;
    }

    function _vinylOverlapCard() {
        const v = _cache('vinyl');
        if (!v || !v.total) return '';
        const pct = ((v.owned / v.total) * 100).toFixed(1);
        return `<section class="stat-section">
            ${_sectionHeader('vinyl')}
            ${_statCards([['Owned on vinyl', `${pct}%`]])}
        </section>`;
    }

    const CERT_LABELS = { gold: 'Gold — 250+ plays', platinum: 'Platinum — 500+ plays', diamond: 'Diamond — 1,000+ plays' };
    // Capped by default so this section's uncapped, ever-growing artist
    // count (no LIMIT in _stats_certified -- see mdb.py) doesn't structurally
    // dominate the page's visual weight. Sorted diamond>platinum>gold, then
    // by play count within a tier, so the cap always surfaces the most
    // impressive/most-played certifications first, not an arbitrary DB order.
    const CERT_VISIBLE_DEFAULT = 30;

    function _certPillHtml({ id, name, cert, slug }) {
        return `<a href="${artistHref(id, slug)}" class="badge-cert badge-cert-${cert}"
               title="${escapeHtml(CERT_LABELS[cert] || cert)}" style="text-decoration:none;width:auto;margin:0.15rem;display:inline-flex;gap:0.35rem">
                    ${escapeHtml(name)}
                </a>`;
    }

    function _certSpotlightSection() {
        const rows = _cache('cert');
        if (!rows || !rows.length) return _emptySection('cert', 'No data yet — run `mdb certs refresh`.');

        const visible = rows.slice(0, CERT_VISIBLE_DEFAULT);
        const rest = rows.slice(CERT_VISIBLE_DEFAULT);
        const visiblePills = visible.map(_certPillHtml).join('');
        const restPills = rest.map(_certPillHtml).join('');

        return `<section class="stat-section">
            ${_sectionHeader('cert')}
            <div style="display:flex;flex-wrap:wrap;margin-top:0.75rem">${visiblePills}</div>
            ${rest.length ? `
                <div style="display:none;flex-wrap:wrap" data-cert-rest>${restPills}</div>
                <button type="button" class="home-see-all" style="background:none;border:none;cursor:pointer;padding:0;margin-top:0.5rem"
                        data-cert-toggle>Show all ${rows.length}</button>
            ` : ''}
        </section>`;
    }

    // One toggle per page (Certified Artists is the only user of
    // data-cert-toggle), but delegated like _wireDrillDowns rather than
    // bound inline -- consistent with how every other interactive element
    // on this page is wired after a fresh innerHTML render.
    function _wireCertToggle(container) {
        container.addEventListener('click', e => {
            const btn = e.target.closest('[data-cert-toggle]');
            if (!btn || !container.contains(btn)) return;
            const rest = btn.previousElementSibling;
            if (!rest || !rest.hasAttribute('data-cert-rest')) return;
            rest.style.display = 'flex';
            btn.remove();
        });
    }

    // ── Stats for Nerds (moved here from views/home.js so its dataset-wide
    // computations get the same precomputed-cache treatment as everything
    // else on this page — the "every year artists" query in particular
    // scans all of listens/track_artists on every home-page load otherwise) ──
    function _everyYearSection() {
        const n = _cache('nerd');
        if (!n || !n.every_year_artists.length) return '';

        const cards = n.every_year_artists.map(a => `
                <a href="${artistHref(a.id, a.slug)}" class="bar-expand-card">
                    <div class="bar-expand-thumb rounded" style="background-image:url('${cssUrl(a.img || getFallbackImageUrl())}')"></div>
                    <div class="bar-expand-name">${escapeHtml(a.name)}</div>
                </a>`).join('');

        return `<section class="stat-section">
            ${_sectionHeader('everyYear')}
            <div class="bar-expand bar-expand-static">${cards}</div>
        </section>`;
    }

    function _nerdSection() {
        const n = _cache('nerd');
        if (!n) return _emptySection('nerd', 'No data yet — run `mdb stats refresh`.');

        const days = days => days.toLocaleString();
        const cards = [
            ['Days scrobbling', `${days(n.active_days)} (${n.active_pct}%)`],
            ['Avg. listens/day', n.avg_per_day],
            ['Total listening time', `${days(n.total_hours)} hrs`],
            ['Peak month', n.peak_month ? `${n.peak_month} · ${days(n.peak_month_count)}` : '—'],
            ['One-hit wonders', `${days(n.one_hit_wonders)} (${n.one_hit_wonders_total ? ((n.one_hit_wonders / n.one_hit_wonders_total) * 100).toFixed(1) : '0'}%)`],
            ['Every-year artists', n.every_year_total_years ? `${n.every_year_artists.length} (of ${n.every_year_total_years}y)` : '0'],
            ['Eddington number', days(n.eddington)],
            ['Artist cutover point', days(n.artist_cutover)],
        ];

        return `<section class="stat-section">
            ${_sectionHeader('nerd')}
            ${_statCards(cards)}
        </section>`;
    }

    // ── Layout ───────────────────────────────────────────────────────────────
    // The page is a flat, ordered list of *groups* (clusters of related
    // sections with breathing room between clusters), each holding one or
    // more width-tagged *entries*. To reorder sections, move an entry (or a
    // whole group) up/down in this array. To resize one, change its `width`
    // ('full' | 'half' | 'third') — no grid-template-columns math to update
    // elsewhere, since .insights-flex/.w-* (styles.css) just flex-wrap
    // whatever's given. Widths within one group should sum to (or evenly
    // divide) 100% for a clean row — e.g. two halves, or three thirds; a
    // half next to a third leaves a gap and wraps the third down.
    const LAYOUT = [
        [{ render: _canonicalListsSection, width: 'full' }],
        [
            { render: _listensPerYearSection, width: 'half' },
            { render: () => ConcertStats.renderPerYear(_cache), width: 'half' },
        ],
        [
            { render: () => ConcertStats.renderHeadline(_cache), width: 'full' },
            { render: () => ConcertStats.renderVenues(_cache), width: 'half' },
            { render: () => ConcertStats.renderTopArtists(_cache), width: 'half' },
            { render: () => ConcertStats.renderSpotlights(_cache), width: 'half' },
            { render: () => ConcertStats.renderCoverage(_cache), width: 'half' },
        ],
        [
            { render: _genderSection, width: 'third' },
            { render: _artistTypeSection, width: 'third' },
            { render: _explicitSection, width: 'third' },
        ],
        [{ render: _tasteMainstreamSection, width: 'full' }],
        [
            { render: _decadeSection, width: 'half' },
            { render: _countrySection, width: 'half' },
        ],
        [
            { render: _languageSection, width: 'third' },
            { render: _releaseTypeSection, width: 'third' },
            { render: _recencySection, width: 'third' },
        ],
        [{ render: _labelsChartSection, width: 'full' }],
        [
            { render: _vinylOverlapCard, width: 'half' },
            { render: _completionSection, width: 'half' },
        ],
        [{ render: _relistenedSection, width: 'full' }],
        [{ render: _certSpotlightSection, width: 'full' }],
        [{ render: _everyYearSection, width: 'full' }],
        [{ render: _nerdSection, width: 'full' }],
    ];

    function _renderGroup(entries) {
        const items = entries
            .map(({ render, width }) => ({ html: render(), width }))
            .filter(e => e.html);
        if (!items.length) return '';
        const rowHtml = items.map(e => `<div class="w-${e.width}">${e.html}</div>`).join('');
        return `<div class="insights-group"><div class="insights-flex">${rowHtml}</div></div>`;
    }

    // ── Main render ────────────────────────────────────────────────────────────
    function _render() {
        const el = document.getElementById('statsContent');
        if (!el) return;

        el.innerHTML = LAYOUT.map(_renderGroup).join('');

        _wireDrillDowns(el);
        _wireCertToggle(el);
        _renderMainstreamHistogram();
        _renderLabelsChart();
        _renderListensPerYearChart();
        ConcertStats.afterRender(_cache);
        lucide.createIcons();
    }

    return { mount, unmount };
})();
