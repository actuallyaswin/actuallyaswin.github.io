const ViewStats = (() => {
    let _db = null;

    function mount(container, db, params) {
        _db = db;
        document.title = 'Stats | Aswin Sivaraman';

        container.innerHTML = `
            <header>
                <h1>Stats</h1>
            </header>
            <div id="statsContent"></div>
        `;

        _render();
    }

    function unmount() { _db = null; }

    // ── Cache access ─────────────────────────────────────────────────────────
    // Every section below is precomputed by `mdb.py stats refresh` (run as
    // part of `just db-checkpoint`) into the `stats_cache` table — this page
    // used to run ~20 live aggregate queries per load (several full table
    // scans), measured at ~12s of blocked JS/WASM time via a HAR capture.
    // Rendering now just parses pre-baked JSON; there is no live SQL here
    // except the one-time cache lookup itself.
    function _cache(key) {
        const res = _db.exec('SELECT value_json FROM stats_cache WHERE key = ?', [key])[0];
        return res ? JSON.parse(res.values[0][0]) : null;
    }

    // ── Drill-down rendering ─────────────────────────────────────────────────
    // Drill-down rows/cards are precomputed too (see mdb.py's _drill_artists/
    // _drill_albums) — opening a chevron only ever toggles CSS, no query runs.
    function _drillPanel(rows, kind, id) {
        if (!rows || !rows.length) return '';
        const cards = rows.map(([rid, name, img, n]) => {
            const href = kind === 'artist' ? `?view=artist&id=${encodeURIComponent(rid)}`
                                            : `?view=release&id=${encodeURIComponent(rid)}`;
            const thumb = img || getFallbackImageUrl();
            return `<a href="${href}" class="lang-expand-card">
                <div class="lang-expand-thumb${kind === 'artist' ? ' rounded' : ''}" style="background-image:url('${thumb}')"></div>
                <div class="lang-expand-name">${escapeHtml(name)}</div>
                <div class="lang-expand-count">${formatNumber(n)} plays</div>
            </a>`;
        }).join('');
        return `<div class="lang-expand" id="${id}">${cards}</div>`;
    }

    let _expandSeq = 0;

    // Wraps a row's inner cells with a chevron + (optionally) a hidden
    // drill-down panel of top-4 artist/album cards. `drillRows` empty ⇒ a
    // dimmed, non-interactive chevron (for visual consistency) with no click
    // wiring. Uses event delegation (see _wireDrillDowns) rather than inline
    // onclick, matching the artist.js Pulse accordion.
    function _rowWithDrill(rowInnerHtml, drillRows, kind) {
        const id = `lde${++_expandSeq}`;
        const panelHtml = _drillPanel(drillRows, kind, id);
        const clickable = panelHtml ? ' lang-row-clickable' : '';
        const chevronClass = panelHtml ? 'lang-chevron' : 'lang-chevron disabled';
        const dataAttr = panelHtml ? ` data-drill-id="${id}"` : '';
        return `<div class="lang-row${clickable}"${dataAttr}>
                ${rowInnerHtml}
                <span class="${chevronClass}">▶</span>
            </div>
            ${panelHtml}`;
    }

    // Event delegation for every drill-down row on the page — attached once
    // per _render() call on the container, rather than one listener per row.
    function _wireDrillDowns(container) {
        container.addEventListener('click', e => {
            const row = e.target.closest('.lang-row-clickable');
            if (!row || !container.contains(row)) return;
            const panel = document.getElementById(row.dataset.drillId);
            const chevron = row.querySelector('.lang-chevron');
            if (!panel || !chevron) return;
            const isOpen = panel.classList.toggle('open');
            chevron.classList.toggle('expanded', isOpen);
        });
    }

    // ── Bar row renderers ─────────────────────────────────────────────────────
    // Both take the cached items array of { label, n, drill } where `drill`
    // is the eagerly-precomputed top-4 array (or [], for a zero-listen row).
    // They differ only in bar styling; _rowWithDrill handles the shared
    // chevron/panel markup.

    const CATEGORY_COLORS = ['#87ae73', '#c9a227', '#67a1fd', '#c97ba5', '#9aa0a6', '#e0685f'];

    // Many-category breakdowns (language, era, country, release type, labels):
    // opacity-graded bars in one shared color. `formatLabel` optionally renders
    // custom markup for the label cell (e.g. a flag icon) instead of escaped text.
    function _breakdownRows(items, formatLabel) {
        const total = items.reduce((s, it) => s + it.n, 0);
        const max   = Math.max(...items.map(it => it.n), 1);
        return items.map(({ label, n, drill }) => {
            const pct     = total ? ((n / total) * 100).toFixed(1) : '0.0';
            const opacity = (0.35 + 0.65 * (n / max)).toFixed(2);
            const labelHtml = formatLabel ? formatLabel(label) : escapeHtml(String(label));
            const rowHtml = `
                <span class="lang-code">${labelHtml}</span>
                <div class="lang-bar-track">
                    <div class="lang-bar-fill" style="width:${pct}%;background:var(--primary);opacity:${opacity}"></div>
                </div>
                <span class="lang-count">${formatNumber(n)}</span>
                <span class="lang-pct">${pct}%</span>`;
            return _rowWithDrill(rowHtml, drill, drill?.length ? _drillKindFor(items) : null);
        }).join('');
    }

    // Low-cardinality breakdowns (2-6 values: gender, artist type, explicit,
    // popularity tier): one color per row, each bar sized to its own share.
    function _coloredRows(items) {
        const total = items.reduce((s, it) => s + it.n, 0);
        return items.map(({ label, n, drill }, i) => {
            const pct   = total ? ((n / total) * 100).toFixed(1) : '0.0';
            const color = CATEGORY_COLORS[i % CATEGORY_COLORS.length];
            const rowHtml = `
                <span class="lang-code">
                    <span class="segbar-legend-dot" style="background:${color}"></span>
                    ${escapeHtml(String(label))}
                </span>
                <div class="lang-bar-track">
                    <div class="lang-bar-fill" style="width:${pct}%;background:${color}"></div>
                </div>
                <span class="lang-count">${formatNumber(n)}</span>
                <span class="lang-pct">${pct}%</span>`;
            return _rowWithDrill(rowHtml, drill, drill?.length ? _drillKindFor(items) : null);
        }).join('');
    }

    // Renders [label, value] pairs as big-number stat-card tiles (reuses the
    // existing .stats/.stat-card pattern from the home/top-* views) — used
    // when a section is a handful of headline numbers, not a distribution.
    function _statCards(items) {
        const cards = items.map(([label, value]) => `
            <div class="stat-card">
                <div class="stat-value">${escapeHtml(String(value))}</div>
                <div class="stat-label">${escapeHtml(label)}</div>
            </div>`).join('');
        return `<div class="stats">${cards}</div>`;
    }

    // ── Section copy ───────────────────────────────────────────────────────────
    // Single source of truth for each section's title + description text, so
    // renaming a section only means editing one entry here instead of hunting
    // down the matching <h3> call sites separately.
    const SECTIONS = {
        language: {
            title: 'Language Breakdown',
            kind: 'release',
        },
        gender: {
            title: 'Artist Gender',
            desc: "Excluding bands and groups.",
            kind: 'artist',
        },
        artistType: {
            title: 'Artist Type',
            kind: 'artist',
        },
        era: {
            title: 'Release Era',
            desc: 'By decade the album came out.',
            kind: 'release',
        },
        country: {
            title: 'Artist Country',
            desc: 'Showing the top 12.',
            kind: 'artist',
        },
        releaseType: {
            title: 'Release Type',
            kind: 'release',
        },
        recency: {
            title: 'Release Recency',
            desc: 'How old the music you play is, relative to when it came out.',
            kind: 'release',
        },
        explicit: {
            title: 'Explicit Content',
            kind: 'artist',
        },
        popularity: {
            title: 'Mainstream vs. Deep Cuts',
            desc: "Per Spotify's artist popularity score.",
            kind: 'artist',
        },
        labels: {
            title: 'Top Labels',
            desc: 'Most-played record labels.',
        },
        completion: {
            title: 'Album Completion',
            desc: 'Albums you keep coming back to but have never finished.',
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
            <div class="diversity-stub">
                <i data-lucide="construction" style="width:20px;height:20px;color:var(--text-tertiary)"></i>
                <span>${escapeHtml(note)}</span>
            </div>
        </section>`;
    }

    // Wraps rendered bar rows in a <section>.
    function _section(key, rowsHtml) {
        return `<section class="stat-section">
            ${_sectionHeader(key)}
            <div class="lang-list has-drilldown">${rowsHtml}</div>
        </section>`;
    }

    // ── Sections ────────────────────────────────────────────────────────────────
    // Each is a thin cache lookup + render — all data (including drill-downs)
    // came from `mdb.py stats refresh`; see that command for the source query
    // this section's numbers are computed from.

    function _drillSection(key, style, formatLabel) {
        const items = _cache(key);
        if (!items || !items.length) return _emptySection(key, 'No data yet — run `mdb stats refresh`.');
        items._kind = SECTIONS[key].kind;
        const rowsHtml = style === 'colored' ? _coloredRows(items) : _breakdownRows(items, formatLabel);
        return _section(key, rowsHtml);
    }

    function _languageSection() { return _drillSection('language', 'breakdown'); }
    function _genderSection()   { return _drillSection('gender', 'colored'); }
    function _artistTypeSection() { return _drillSection('artistType', 'colored'); }
    function _decadeSection()   { return _drillSection('era', 'breakdown'); }
    function _releaseTypeSection() { return _drillSection('releaseType', 'breakdown'); }
    function _recencySection()  { return _drillSection('recency', 'breakdown'); }
    function _explicitSection() { return _drillSection('explicit', 'colored'); }
    function _popularitySection() { return _drillSection('popularity', 'colored'); }

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

    function _labelSection() {
        const items = _cache('labels');
        if (!items || !items.length) return _emptySection('labels', 'No data yet — run `mdb stats refresh`.');
        return _section('labels', _breakdownRows(items));
    }

    function _completionSection() {
        const rows = _cache('completion');
        if (!rows || !rows.length) return _emptySection('completion', 'No data yet — run `mdb stats refresh`.');

        const max = Math.max(...rows.map(r => r.listens), 1);
        const rowsHtml = rows.map(({ id, title, total, heard, listens }) => {
            const pct     = total ? ((heard / total) * 100).toFixed(0) : '0';
            const opacity = (0.35 + 0.65 * (listens / max)).toFixed(2);
            const href    = `?view=release&id=${encodeURIComponent(id)}`;
            // .lang-list is a fixed 5-column grid; every row (display:contents)
            // must supply exactly 5 children or subsequent rows' columns drift.
            // This section has no chevron, so the 5th cell is an empty spacer.
            return `<div class="lang-row lang-row-static">
                <a class="lang-code lang-code-link" href="${href}">${escapeHtml(title)}</a>
                <div class="lang-bar-track">
                    <div class="lang-bar-fill" style="width:${pct}%;background:var(--primary);opacity:${opacity}"></div>
                </div>
                <span class="lang-count">${heard}/${total}</span>
                <span class="lang-pct">${pct}%</span>
                <span></span>
            </div>`;
        }).join('');
        return _section('completion', rowsHtml);
    }

    function _relistenedSection() {
        const rows = _cache('relistened');
        if (!rows || !rows.length) return _emptySection('relistened', 'No data yet — run `mdb stats refresh`.');

        const cards = rows.map(({ id, title, artist, art_url, release_id, n }) => {
            const href  = release_id ? `?view=release&id=${encodeURIComponent(release_id)}` : '#';
            const thumb = art_url || getFallbackImageUrl();
            const sub   = artist ? `${escapeHtml(artist)} · ${formatNumber(n)} plays` : `${formatNumber(n)} plays`;
            return `<a href="${href}" class="lang-expand-card lang-expand-card-wide">
                <div class="lang-expand-thumb" style="background-image:url('${thumb}')"></div>
                <div class="lang-expand-name">${escapeHtml(title)}</div>
                <div class="lang-expand-count">${sub}</div>
            </a>`;
        }).join('');
        return `<section class="stat-section">
            ${_sectionHeader('relistened')}
            <div class="lang-expand lang-expand-static">${cards}</div>
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

    function _certSpotlightSection() {
        const rows = _cache('cert');
        if (!rows || !rows.length) return _emptySection('cert', 'No data yet — run `mdb certs refresh`.');

        const pills = rows.map(({ id, name, cert }) => `
                <a href="?view=artist&id=${encodeURIComponent(id)}" class="badge-cert badge-cert-${cert}"
                   title="${escapeHtml(CERT_LABELS[cert] || cert)}" style="text-decoration:none;width:auto;margin:0.15rem;display:inline-flex;gap:0.35rem">
                    ${escapeHtml(name)}
                </a>`).join('');
        return `<section class="stat-section">
            ${_sectionHeader('cert')}
            <div style="display:flex;flex-wrap:wrap;margin-top:0.75rem">${pills}</div>
        </section>`;
    }

    // ── Stats for Nerds (moved here from views/home.js so its dataset-wide
    // computations get the same precomputed-cache treatment as everything
    // else on this page — the "every year artists" query in particular
    // scans all of listens/track_artists on every home-page load otherwise) ──
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

        const everyYearHtml = n.every_year_artists.length ? `
            <div class="lang-expand-static" style="margin-top:1rem">
                ${n.every_year_artists.map(a => `
                    <a href="?view=artist&id=${encodeURIComponent(a.id)}" class="lang-expand-card">
                        <div class="lang-expand-name" style="font-size:0.75rem">${escapeHtml(a.name)}</div>
                    </a>`).join('')}
            </div>` : '';

        return `<section class="stat-section">
            ${_sectionHeader('nerd')}
            ${_statCards(cards)}
            ${everyYearHtml}
        </section>`;
    }

    // ── Main render ────────────────────────────────────────────────────────────
    function _render() {
        const el = document.getElementById('statsContent');
        if (!el) return;

        el.innerHTML = `
            <div class="insights-group">
                <div class="insights-grid insights-grid--thirds">
                    ${_genderSection()}
                    ${_artistTypeSection()}
                    ${_popularitySection()}
                </div>
                <div class="insights-grid insights-grid--halves">
                    ${_decadeSection()}
                    ${_countrySection()}
                </div>
            </div>

            <div class="insights-group">
                <div class="insights-grid insights-grid--halves">
                    ${_languageSection()}
                    ${_recencySection()}
                </div>
                <div class="insights-grid insights-grid--thirds">
                    ${_releaseTypeSection()}
                    ${_explicitSection()}
                    ${_vinylOverlapCard()}
                </div>
                <div class="insights-grid insights-grid--halves">
                    ${_labelSection()}
                    ${_completionSection()}
                </div>
            </div>

            <div class="insights-group">
                ${_relistenedSection()}
            </div>

            <div class="insights-group">
                ${_certSpotlightSection()}
            </div>

            <div class="insights-group">
                ${_nerdSection()}
            </div>
        `;

        _wireDrillDowns(el);
        lucide.createIcons();
    }

    return { mount, unmount };
})();
