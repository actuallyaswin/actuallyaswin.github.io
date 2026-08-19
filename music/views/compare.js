const ViewCompare = (() => {
    let _db = null;
    let _idA = null;
    let _idB = null;
    let _nameA = null;
    let _nameB = null;
    let _chart = null;
    let _themeObserver = null;
    // 'monthly' | 'yearly'
    let _granularity = 'monthly';
    let _timelineRenderToken = 0;

    function mount(container, db, params) {
        _db = db;
        _idA = params.a || null;
        _idB = params.b || null;
        _nameA = null;
        _nameB = null;
        _chart = null;
        _granularity = 'monthly';

        setPageTitle('Compare Artists');

        container.innerHTML = `
            <nav class="genre-breadcrumb">
                <a href="?" class="bc-home"><i data-lucide="home"></i></a>
                <i data-lucide="chevron-right" class="bc-sep"></i>
                <span class="bc-current">Compare</span>
            </nav>

            <header class="compare-header">
                <div class="compare-slot" id="compareSlotA"></div>
                <div class="compare-vs">VS</div>
                <div class="compare-slot" id="compareSlotB"></div>
            </header>

            <div id="compareBody"></div>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        lucide.createIcons();
        renderSlot('A', _idA);
        renderSlot('B', _idB);
        renderBody();
    }

    function unmount() {
        if (_chart) { _chart.destroy(); _chart = null; }
        if (_themeObserver) { _themeObserver.disconnect(); _themeObserver = null; }
        if (_markerTooltipEl) { _markerTooltipEl.remove(); _markerTooltipEl = null; }
    }

    function _updateUrl() {
        const params = { view: 'compare' };
        if (_idA) params.a = _idA;
        if (_idB) params.b = _idB;
        navigate(params);
    }

    // ── Picker slot — either a filled artist card or a search-to-pick box ──
    function renderSlot(which, artistId) {
        const el = document.getElementById(`compareSlot${which}`);
        if (!el) return;

        if (!artistId) {
            el.innerHTML = `
                <div class="compare-picker">
                    <i data-lucide="search" class="compare-picker-icon"></i>
                    <input type="text" class="compare-picker-input" placeholder="Choose an artist…"
                           autocomplete="off" spellcheck="false" data-slot="${which}">
                    <div class="compare-picker-results" hidden></div>
                </div>`;
            lucide.createIcons({ el });
            wirePicker(which, el.querySelector('.compare-picker-input'), el.querySelector('.compare-picker-results'));
            return;
        }

        const safeId = artistId.replace(/'/g, "''");
        const row = _db.exec(`
            SELECT name, COALESCE(image_thumb_url, image_url), cert, slug
            FROM artists WHERE id = '${safeId}'
        `)[0];

        if (!row || row.values.length === 0) {
            el.innerHTML = `<div class="compare-picker-empty">Artist not found</div>`;
            return;
        }

        const [name, img, cert, slug] = row.values[0];
        if (which === 'A') _nameA = name; else _nameB = name;
        el.innerHTML = `
            <a href="${artistHref(artistId, slug)}" class="compare-card-photo">
                <img src="${cssUrl(img || getFallbackImageUrl())}" alt="">
            </a>
            <div class="compare-card-name">
                <a href="${artistHref(artistId, slug)}">${escapeHtml(name)}</a>
                ${cert ? `<span class="badge-cert badge-cert-${cert}" style="margin-left:0.4rem;width:auto;padding:0.1rem 0.4rem">${cert}</span>` : ''}
            </div>
            <button type="button" class="compare-card-swap" data-slot="${which}" title="Change artist">
                <i data-lucide="repeat-2"></i>
            </button>`;
        lucide.createIcons({ el });
        el.querySelector('.compare-card-swap').addEventListener('click', () => {
            if (which === 'A') { _idA = null; _nameA = null; } else { _idB = null; _nameB = null; }
            renderSlot(which, null);
            renderBody();
            _updateUrl();
        });
    }

    function wirePicker(which, input, resultsEl) {
        let debounce = null;
        input.addEventListener('input', () => {
            clearTimeout(debounce);
            const q = input.value.trim();
            if (q.length < 2) { resultsEl.innerHTML = ''; resultsEl.hidden = true; return; }
            debounce = setTimeout(() => {
                const safe = q.replace(/'/g, "''");
                const otherId = which === 'A' ? _idB : _idA;
                const excludeClause = otherId ? `AND a.id != '${otherId.replace(/'/g, "''")}'` : '';
                const rows = _db.exec(`
                    SELECT a.id, a.name, COALESCE(a.image_thumb_url, a.image_url)
                    FROM artists a
                    WHERE (a.hidden IS NULL OR a.hidden = 0) ${excludeClause}
                      AND (lower(a.name) LIKE lower('%${safe}%')
                        OR EXISTS (SELECT 1 FROM artist_aliases aa
                                   WHERE aa.artist_id = a.id AND lower(aa.alias) LIKE lower('%${safe}%')))
                    ORDER BY (lower(a.name) LIKE lower('${safe}%')) DESC, a.stat_total_plays DESC
                    LIMIT 8
                `)[0];
                if (!rows || rows.values.length === 0) {
                    resultsEl.innerHTML = `<div class="compare-picker-empty">No matches</div>`;
                    resultsEl.hidden = false;
                    return;
                }
                resultsEl.innerHTML = rows.values.map(([id, name, img]) => `
                    <div class="compare-picker-row" data-id="${escapeHtml(id)}">
                        <img src="${cssUrl(img || getFallbackImageUrl())}" alt="">
                        <span>${escapeHtml(name)}</span>
                    </div>`).join('');
                resultsEl.hidden = false;
                resultsEl.querySelectorAll('.compare-picker-row').forEach(row => {
                    row.addEventListener('click', () => {
                        if (which === 'A') _idA = row.dataset.id; else _idB = row.dataset.id;
                        renderSlot(which, row.dataset.id);
                        renderBody();
                        _updateUrl();
                    });
                });
            }, 150);
        });
    }

    // ── Comparison body ──────────────────────────────────────────────────────
    function renderBody() {
        const bodyEl = document.getElementById('compareBody');
        if (!bodyEl) return;

        if (_chart) { _chart.destroy(); _chart = null; }

        if (!_idA || !_idB) {
            bodyEl.innerHTML = `
                <div class="compare-empty-state">
                    <i data-lucide="git-compare"></i>
                    <p>Pick two artists to see how your listening stacks up.</p>
                </div>`;
            lucide.createIcons({ el: bodyEl });
            return;
        }

        const statsA = _artistStats(_idA);
        const statsB = _artistStats(_idB);

        bodyEl.innerHTML = `
            <section class="compare-metrics">
                ${_metricRow('Total listens', statsA.totalPlays, statsB.totalPlays, formatNumber)}
                ${_metricRow('Unique tracks', statsA.uniqueTracks, statsB.uniqueTracks, formatNumber)}
                ${_metricRow('Minutes played', statsA.totalMinutes, statsB.totalMinutes, formatNumber)}
                ${_metricRow('Releases heard', statsA.totalReleases, statsB.totalReleases, formatNumber)}
                ${_dateMetricRow('First heard', statsA.firstTs, statsB.firstTs)}
            </section>

            <section class="compare-timeline">
                <div class="chart-header">
                    <h2>Listening Timeline</h2>
                    <div class="control-group">
                        <button class="control-btn${_granularity === 'monthly' ? ' active' : ''}" data-granularity="monthly">Monthly</button>
                        <button class="control-btn${_granularity === 'yearly' ? ' active' : ''}" data-granularity="yearly">Yearly</button>
                    </div>
                </div>
                <div class="compare-line-legend">
                    <span class="compare-line-legend-dot" style="background:var(--primary)"></span>${escapeHtml(_nameA || 'Artist A')}
                    <span class="compare-line-legend-dot" style="background:var(--text-tertiary)"></span>${escapeHtml(_nameB || 'Artist B')}
                </div>
                <div class="compare-chart-wrap">
                    <canvas id="compareChart"></canvas>
                </div>
                <p class="compare-marker-legend" id="compareMarkerLegend" hidden>
                    <span class="compare-marker-legend-dot compare-marker-a"></span> ${escapeHtml(_nameA || 'Artist A')} release
                    <span class="compare-marker-legend-dot compare-marker-b"></span> ${escapeHtml(_nameB || 'Artist B')} release
                </p>
            </section>

            <section class="compare-tracks-grid">
                <div class="compare-tracks-col">
                    <h2>Top Tracks</h2>
                    <div id="compareTracksA">${renderLoading()}</div>
                </div>
                <div class="compare-tracks-col">
                    <h2>Top Tracks</h2>
                    <div id="compareTracksB">${renderLoading()}</div>
                </div>
            </section>

            <section class="compare-genres" id="compareGenresSection" hidden>
                <h2>Shared Genres</h2>
                <div class="compare-genres-tags" id="compareGenresTags"></div>
            </section>
        `;
        lucide.createIcons({ el: bodyEl });

        setupToggleGroup('[data-granularity]', btn => {
            _granularity = btn.dataset.granularity;
            renderTimeline();
        });

        renderTimeline(statsA, statsB);
        renderTopTracks(_idA, 'compareTracksA');
        renderTopTracks(_idB, 'compareTracksB');
        renderSharedGenres(_idA, _idB);
    }

    function _artistStats(artistId) {
        const safeId = artistId.replace(/'/g, "''");
        const row = _db.exec(`
            SELECT stat_total_plays, stat_unique_tracks, stat_total_releases, stat_first_listen_ts
            FROM artists WHERE id = '${safeId}'
        `)[0];
        const [totalPlays, uniqueTracks, totalReleases, firstTs] = row ? row.values[0] : [0, 0, 0, null];

        const minutesRow = _db.exec(`
            SELECT CAST(SUM(COALESCE(t.duration_ms, 0)) / 60000.0 AS INTEGER)
            FROM listens l
            JOIN tracks t ON t.id = l.track_id AND t.hidden = 0
            JOIN track_artists ta ON ta.track_id = t.id AND ta.role IN (${PRIMARY_ROLES_SQL})
            WHERE ta.artist_id = '${safeId}'
        `)[0];
        const totalMinutes = minutesRow ? (minutesRow.values[0][0] || 0) : 0;

        return {
            totalPlays: totalPlays || 0,
            uniqueTracks: uniqueTracks || 0,
            totalReleases: totalReleases || 0,
            firstTs: firstTs || null,
            totalMinutes,
        };
    }

    // A single mirrored bar: label centered, A's bar grows leftward from
    // center, B's bar grows rightward — direct visual "who's ahead."
    function _metricRow(label, valA, valB, fmt) {
        const max = Math.max(valA, valB, 1);
        const pctA = Math.round((valA / max) * 100);
        const pctB = Math.round((valB / max) * 100);
        const aWins = valA > valB;
        const bWins = valB > valA;
        return `
            <div class="compare-metric-row">
                <span class="compare-metric-val${aWins ? ' compare-metric-winner' : ''}">${fmt(valA)}</span>
                <div class="compare-metric-bars">
                    <div class="compare-metric-bar-track compare-metric-bar-left">
                        <div class="compare-metric-bar-fill" style="width:${pctA}%"></div>
                    </div>
                    <span class="compare-metric-label">${escapeHtml(label)}</span>
                    <div class="compare-metric-bar-track compare-metric-bar-right">
                        <div class="compare-metric-bar-fill" style="width:${pctB}%"></div>
                    </div>
                </div>
                <span class="compare-metric-val${bWins ? ' compare-metric-winner' : ''}">${fmt(valB)}</span>
            </div>`;
    }

    function _dateMetricRow(label, tsA, tsB) {
        const fmt = ts => ts ? new Date(ts * 1000).toLocaleDateString('en-US', { month: 'short', year: 'numeric' }) : '—';
        return `
            <div class="compare-metric-row compare-metric-row-date">
                <span class="compare-metric-val">${fmt(tsA)}</span>
                <span class="compare-metric-label compare-metric-label-standalone">${escapeHtml(label)}</span>
                <span class="compare-metric-val">${fmt(tsB)}</span>
            </div>`;
    }

    function _monthlyListens(artistId) {
        const safeId = artistId.replace(/'/g, "''");
        const result = _db.exec(`
            SELECT l.year, l.month, COUNT(*) as n
            FROM listens l
            JOIN tracks t ON t.id = l.track_id AND t.hidden = 0
            JOIN track_artists ta ON ta.track_id = t.id AND ta.role IN (${PRIMARY_ROLES_SQL})
            WHERE ta.artist_id = '${safeId}'
            GROUP BY l.year, l.month ORDER BY l.year, l.month
        `)[0];
        const map = new Map();
        (result ? result.values : []).forEach(([year, month, n]) => map.set(`${year}-${month}`, n));
        return map;
    }

    function _yearlyListens(artistId) {
        const safeId = artistId.replace(/'/g, "''");
        const result = _db.exec(`
            SELECT l.year, COUNT(*) as n
            FROM listens l
            JOIN tracks t ON t.id = l.track_id AND t.hidden = 0
            JOIN track_artists ta ON ta.track_id = t.id AND ta.role IN (${PRIMARY_ROLES_SQL})
            WHERE ta.artist_id = '${safeId}'
            GROUP BY l.year ORDER BY l.year
        `)[0];
        const map = new Map();
        (result ? result.values : []).forEach(([year, n]) => map.set(String(year), n));
        return map;
    }

    // Studio albums/EPs only — singles/compilations would clutter the axis
    // with releases that rarely explain a listening spike on their own.
    // Bucketed by month or by year depending on the active granularity so
    // multiple same-year releases can be grouped into one stack.
    function _releasesByBucket(artistId, granularity) {
        const safeId = artistId.replace(/'/g, "''");
        const result = _db.exec(`
            SELECT r.id, r.title, r.release_date, COALESCE(r.album_art_thumb_url, r.album_art_url)
            FROM releases r
            WHERE r.primary_artist_id = '${safeId}' AND r.hidden = 0
              AND r.type IN ('album', 'ep') AND r.release_date IS NOT NULL
            ORDER BY r.release_date
        `)[0];
        const map = new Map();
        (result ? result.values : []).forEach(([id, title, date, art]) => {
            const [y, m] = date.split('-');
            const key = granularity === 'yearly' ? String(parseInt(y, 10)) : `${parseInt(y, 10)}-${parseInt(m, 10)}`;
            if (!map.has(key)) map.set(key, []);
            map.get(key).push({ id, title, art });
        });
        return map;
    }

    function renderTimeline() {
        if (_chart) { _chart.destroy(); _chart = null; }

        const isYearly = _granularity === 'yearly';
        const mapA = isYearly ? _yearlyListens(_idA) : _monthlyListens(_idA);
        const mapB = isYearly ? _yearlyListens(_idB) : _monthlyListens(_idB);
        if (mapA.size === 0 && mapB.size === 0) {
            const wrap = document.querySelector('.compare-chart-wrap');
            if (wrap) wrap.innerHTML = `<p class="compare-empty-inline">No listening history for either artist yet.</p>`;
            return;
        }

        // array of { key, label }
        let buckets;
        if (isYearly) {
            const years = [...new Set([...mapA.keys(), ...mapB.keys()])].map(Number).sort((a, b) => a - b);
            const minYear = years[0], maxYear = years[years.length - 1];
            buckets = [];
            for (let y = minYear; y <= maxYear; y++) buckets.push({ key: String(y), label: String(y) });
        } else {
            const keys = [...new Set([...mapA.keys(), ...mapB.keys()])]
                .map(k => k.split('-').map(Number))
                .sort(([ya, ma], [yb, mb]) => ya - yb || ma - mb);
            const [minYear, minMonth] = keys[0];
            const [maxYear, maxMonth] = keys[keys.length - 1];
            const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
            buckets = [];
            for (let y = minYear; y <= maxYear; y++) {
                for (let m = 1; m <= 12; m++) {
                    if (y === minYear && m < minMonth) continue;
                    if (y === maxYear && m > maxMonth) break;
                    buckets.push({ key: `${y}-${m}`, label: `${monthNames[m - 1]} ${y}` });
                }
            }
        }

        const releasesA = _releasesByBucket(_idA, _granularity);
        const releasesB = _releasesByBucket(_idB, _granularity);
        const nameA = _nameA || 'Artist A';
        const nameB = _nameB || 'Artist B';

        const primaryColor   = getCSSColor('--primary');
        const secondaryColor = getCSSColor('--text-tertiary');
        const bgSecondary    = getCSSColor('--bg-secondary');
        const textColor      = getCSSColor('--text');
        const textSecondary  = getCSSColor('--text-secondary');
        const borderColor    = getCSSColor('--border');

        const skipFactor = isYearly ? 1 : Math.max(1, Math.ceil(buckets.length / 15));
        const labels = buckets.map(b => b.label);
        const labelCallback = (value, index) => index % skipFactor === 0 ? labels[index] : '';

        const ctx = document.getElementById('compareChart');
        if (!ctx) return;

        const hasReleases = buckets.some(b => releasesA.has(b.key) || releasesB.has(b.key));
        const legendEl = document.getElementById('compareMarkerLegend');
        if (legendEl) legendEl.hidden = !hasReleases;

        // Both sides' stacks occupy the same vertical column (B sits above
        // A) — sum their max depths for a safe upper bound on the headroom
        // the chart needs above its own plot area so art never clips. The
        // wrap's own height grows with this padding rather than staying
        // fixed, so a deep stack doesn't eat into (and shrink) the plot area.
        const maxStackA = Math.max(0, ...buckets.map(b => (releasesA.get(b.key) || []).length));
        const maxStackB = Math.max(0, ...buckets.map(b => (releasesB.get(b.key) || []).length));
        const topPadding = 30 + (maxStackA + maxStackB) * (MARKER_SIZE + MARKER_GAP);
        const wrapEl = document.querySelector('.compare-chart-wrap');
        if (wrapEl) wrapEl.style.height = `${300 + Math.max(0, topPadding - 30)}px`;

        // Album art loads async — build the chart once every needed image is
        // ready so annotations don't pop in a frame after the initial draw.
        // A render token guards against a stale response landing after a
        // second granularity toggle click fires a newer render.
        const renderToken = ++_timelineRenderToken;
        _loadReleaseArt(releasesA, releasesB).then(() => {
            if (renderToken !== _timelineRenderToken) return;
            const annotations = _buildReleaseAnnotations(buckets, releasesA, releasesB);

            _chart = new Chart(ctx.getContext('2d'), {
                type: 'line',
                data: {
                    labels,
                    datasets: [
                        {
                            label: nameA,
                            data: buckets.map(b => mapA.get(b.key) || 0),
                            borderColor: primaryColor,
                            backgroundColor: primaryColor,
                            tension: 0.3,
                            pointRadius: 0,
                            borderWidth: 1.5,
                        },
                        {
                            label: nameB,
                            data: buckets.map(b => mapB.get(b.key) || 0),
                            borderColor: secondaryColor,
                            backgroundColor: secondaryColor,
                            tension: 0.3,
                            pointRadius: 0,
                            borderWidth: 1.5,
                        },
                    ],
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    layout: { padding: { top: topPadding } },
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            backgroundColor: bgSecondary,
                            titleColor: textColor,
                            bodyColor: textColor,
                            borderColor: borderColor,
                            borderWidth: 1,
                        },
                        annotation: { annotations },
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            ticks: { color: textSecondary },
                            grid: { color: borderColor },
                        },
                        x: {
                            ticks: { color: textSecondary, maxRotation: 45, minRotation: 45, autoSkip: false, callback: labelCallback },
                            grid: { color: borderColor },
                        },
                    },
                },
            });
        });

        if (_themeObserver) _themeObserver.disconnect();
        _themeObserver = new MutationObserver(() => renderTimeline());
        _themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    }

    // Preloads an <img> per unique art URL so annotation `content` can be a
    // real HTMLImageElement (chartjs-plugin-annotation draws it directly,
    // it does not fetch URLs itself). Missing/broken art falls back to the
    // site's generic note-icon placeholder rather than skipping the marker.
    const _artImageCache = new Map();
    function _loadReleaseArt(releasesA, releasesB) {
        const urls = new Set();
        for (const rels of [...releasesA.values(), ...releasesB.values()]) {
            rels.forEach(r => urls.add(r.art || getFallbackImageUrl()));
        }
        return Promise.all([...urls].map(url => {
            if (_artImageCache.has(url)) return Promise.resolve();
            return new Promise(resolve => {
                const img = new Image();
                img.onload = () => { _artImageCache.set(url, img); resolve(); };
                // marker just gets skipped below
                img.onerror = () => resolve();
                img.src = url;
            });
        }));
    }

    const MARKER_SIZE = 22;
    const MARKER_GAP = 4;

    // One image per release, stacked vertically when a bucket holds more
    // than one (e.g. two albums the same year in Yearly mode). Hovering any
    // image in a stack shows a single joint tooltip listing every title in
    // that stack, since at this granularity they're indistinguishable in
    // time — a lone per-image tooltip would misleadingly single one out.
    function _buildReleaseAnnotations(buckets, releasesA, releasesB) {
        const annotations = {};
        let n = 0;
        buckets.forEach((bucket, i) => {
            [['a', releasesA.get(bucket.key)], ['b', releasesB.get(bucket.key)]].forEach(([side, rels]) => {
                if (!rels) return;
                const jointTitle = rels.map(r => r.title).join('\n');
                const baseOffset = side === 'a' ? -(MARKER_SIZE / 2 + 2) : -(MARKER_SIZE + MARKER_SIZE / 2 + 6);
                rels.forEach((r, stackIndex) => {
                    const img = _artImageCache.get(r.art || getFallbackImageUrl());
                    if (!img) return;
                    const stackOffset = stackIndex * (MARKER_SIZE + MARKER_GAP);
                    annotations[`release_${n++}`] = {
                        type: 'label',
                        xValue: i,
                        yValue: 0,
                        yAdjust: side === 'a' ? baseOffset - stackOffset : baseOffset - stackOffset,
                        width: MARKER_SIZE,
                        height: MARKER_SIZE,
                        content: img,
                        borderWidth: 2,
                        borderColor: side === 'a' ? getCSSColor('--primary') : getCSSColor('--text-tertiary'),
                        borderRadius: 4,
                        click: () => navigate({ view: 'release', id: r.id }),
                        enter(ctx) {
                            ctx.element.options.width = MARKER_SIZE + 8;
                            ctx.element.options.height = MARKER_SIZE + 8;
                            ctx.chart.canvas.style.cursor = 'pointer';
                            _showMarkerTooltip(ctx.chart.canvas, ctx.element, jointTitle);
                            ctx.chart.update('none');
                        },
                        leave(ctx) {
                            ctx.element.options.width = MARKER_SIZE;
                            ctx.element.options.height = MARKER_SIZE;
                            ctx.chart.canvas.style.cursor = '';
                            _hideMarkerTooltip();
                            ctx.chart.update('none');
                        },
                    };
                });
            });
        });
        return annotations;
    }

    let _markerTooltipEl = null;
    function _showMarkerTooltip(canvas, element, title) {
        if (!_markerTooltipEl) {
            _markerTooltipEl = document.createElement('div');
            _markerTooltipEl.className = 'compare-marker-tooltip';
            document.body.appendChild(_markerTooltipEl);
        }
        _markerTooltipEl.textContent = title;
        const rect = canvas.getBoundingClientRect();
        const cx = element.centerX ?? element.x;
        const cy = element.centerY ?? element.y;
        _markerTooltipEl.style.left = `${rect.left + cx}px`;
        _markerTooltipEl.style.top = `${rect.top + cy - 24}px`;
        _markerTooltipEl.hidden = false;
    }
    function _hideMarkerTooltip() {
        if (_markerTooltipEl) _markerTooltipEl.hidden = true;
    }

    function renderTopTracks(artistId, containerId) {
        const el = document.getElementById(containerId);
        if (!el) return;
        const safeId = artistId.replace(/'/g, "''");
        const result = _db.exec(`
            SELECT t.title, r.id, COUNT(l.id) as n, r.slug
            FROM track_artists ta
            JOIN tracks t ON t.id = ta.track_id AND t.hidden = 0
            JOIN releases r ON r.id = t.release_id AND r.hidden = 0
            LEFT JOIN listens l ON l.track_id = t.id
            WHERE ta.artist_id = '${safeId}' AND ta.role IN (${PRIMARY_ROLES_SQL})
            GROUP BY t.id
            HAVING n > 0
            ORDER BY n DESC
            LIMIT 8
        `)[0];

        if (!result || result.values.length === 0) {
            el.innerHTML = `<p class="compare-empty-inline">No listens yet.</p>`;
            return;
        }

        const max = Math.max(...result.values.map(([, , n]) => n));
        el.innerHTML = result.values.map(([title, releaseId, n, releaseSlug], i) => {
            const pct = Math.round((n / max) * 100);
            return `
                <a href="${releaseHref(releaseId, releaseSlug)}" class="compare-track-row">
                    <span class="compare-track-rank">${i + 1}</span>
                    <span class="compare-track-title">${escapeHtml(title)}</span>
                    <div class="compare-track-bar-track">
                        <div class="compare-track-bar-fill" style="width:${pct}%"></div>
                    </div>
                    <span class="compare-track-count">${formatNumber(n)}</span>
                </a>`;
        }).join('');
    }

    function renderSharedGenres(idA, idB) {
        const section = document.getElementById('compareGenresSection');
        const tagsEl = document.getElementById('compareGenresTags');
        if (!section || !tagsEl) return;

        const genresFor = artistId => {
            const safeId = artistId.replace(/'/g, "''");
            const result = _db.exec(`
                SELECT DISTINCT g.aoty_id, g.name
                FROM release_genres rg
                JOIN genres g ON g.aoty_id = rg.aoty_genre_id
                WHERE rg.release_id IN (
                    SELECT DISTINCT t.release_id
                    FROM track_artists ta
                    JOIN tracks t ON ta.track_id = t.id
                    JOIN releases r ON r.id = t.release_id AND r.hidden = 0
                    WHERE ta.artist_id = '${safeId}' AND ta.role IN (${PRIMARY_ROLES_SQL}) AND t.hidden = 0
                )
            `)[0];
            const map = new Map();
            (result ? result.values : []).forEach(([id, name]) => map.set(id, name));
            return map;
        };

        const mapA = genresFor(idA);
        const mapB = genresFor(idB);
        const shared = [...mapA.keys()].filter(id => mapB.has(id));

        if (shared.length === 0) return;

        tagsEl.innerHTML = shared.map(id =>
            `<a href="?view=genre&id=${encodeURIComponent(id)}" class="genre-tag">${escapeHtml(mapA.get(id))}</a>`
        ).join('');
        section.removeAttribute('hidden');
    }

    return { mount, unmount };
})();
