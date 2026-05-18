const ViewStats = (() => {
    let _db   = null;
    let _year = 'all';

    function mount(container, db, params) {
        _db = db;
        document.title = 'aswin.db/music – Stats';

        container.innerHTML = `
            <header>
                <h1>Stats</h1>
            </header>
            <div class="page-controls">
                <div class="control-block">
                    <span class="control-block-label">Year</span>
                    <div class="sort-controls">
                        <select id="statsYear" class="year-filter-select">
                            <option value="all">All time</option>
                        </select>
                    </div>
                </div>
            </div>
            <div id="statsContent"></div>
        `;

        _populateYears();
        document.getElementById('statsYear')?.addEventListener('change', e => {
            _year = e.target.value;
            _render();
        });
        _render();
    }

    function unmount() { _db = null; }

    // ── Year selector ──────────────────────────────────────────────────────────
    function _populateYears() {
        const sel = document.getElementById('statsYear');
        const res = _db.exec(`SELECT DISTINCT year FROM listens WHERE year IS NOT NULL ORDER BY year DESC`)[0];
        if (res) res.values.forEach(([yr]) => {
            const o = document.createElement('option');
            o.value = yr; o.textContent = yr;
            sel.appendChild(o);
        });
    }

    // ── SQL helper ─────────────────────────────────────────────────────────────
    function _q(sql) {
        const r = _db.exec(sql)[0];
        return r ? r.values : [];
    }

    function _where() {
        return _year !== 'all' ? `WHERE year = ${parseInt(_year)}` : '';
    }

    // ── Bar chart renderer ─────────────────────────────────────────────────────
    function _barChart(data, labels, opts = {}) {
        const max   = Math.max(...data.map(d => d || 0), 1);
        const height = opts.height || 120;
        const bars = data.map((v, i) => {
            const pct = Math.round(((v || 0) / max) * 100);
            return `<div class="stat-bar-wrap">
                <div class="stat-bar" style="height:${height}px">
                    <div class="stat-bar-fill" style="height:${pct}%;background:var(--primary)"></div>
                </div>
                <div class="stat-bar-label">${escapeHtml(labels[i])}</div>
            </div>`;
        }).join('');
        return `<div class="stat-bars">${bars}</div>`;
    }

    // ── Punchcard renderer ─────────────────────────────────────────────────────
    function _punchcard(data) {
        // data: [[h, d, n], ...] where d=0(Sun)..6(Sat), h=0..23
        const grid = Array.from({length:7}, () => new Array(24).fill(0));
        data.forEach(([h, d, n]) => { grid[d][h] = n; });
        const max = Math.max(...grid.flat(), 1);
        const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];

        let html = '<div class="punchcard">';
        // Hour labels row
        html += '<div class="punchcard-row"><div class="punchcard-day-label"></div>';
        for (let h = 0; h < 24; h++)
            html += `<div class="punchcard-hour-label">${h % 6 === 0 ? h : ''}</div>`;
        html += '</div>';

        for (let d = 0; d < 7; d++) {
            html += `<div class="punchcard-row"><div class="punchcard-day-label">${dayNames[d]}</div>`;
            for (let h = 0; h < 24; h++) {
                const pct = grid[d][h] / max;
                const tip = `${dayNames[d]} ${h}:00 — ${grid[d][h].toLocaleString()} plays`;
                html += `<div class="punchcard-cell" title="${tip}" style="opacity:${0.08 + pct * 0.92}"></div>`;
            }
            html += '</div>';
        }
        html += '</div>';
        return html;
    }

    // ── Diversity section renderer ─────────────────────────────────────────────
    function _stubSection(title, desc, dataNote) {
        return `<section class="stat-section">
            <h2>${escapeHtml(title)}</h2>
            <p class="rec-desc">${escapeHtml(desc)}</p>
            <div class="diversity-stub">
                <i data-lucide="construction" style="width:20px;height:20px;color:var(--text-tertiary)"></i>
                <span>${escapeHtml(dataNote)}</span>
            </div>
        </section>`;
    }

    function _languageSection() {
        const w   = _year !== 'all' ? `AND l.year = ${parseInt(_year)}` : '';
        const res = _q(`
            SELECT t.language, COUNT(l.id) n
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            WHERE t.language IS NOT NULL ${w}
            GROUP BY t.language
            ORDER BY n DESC
            LIMIT 12
        `);
        if (!res.length) return _stubSection(
            'Language Breakdown',
            'Most-listened languages by play count.',
            'Requires track language tags — run the language tagger to populate.'
        );

        const total = res.reduce((s, [, n]) => s + n, 0);
        const rows  = res.map(([lang, n]) => {
            const pct = ((n / total) * 100).toFixed(1);
            return `<div class="lang-row">
                <span class="lang-code">${escapeHtml(lang || '?')}</span>
                <div class="lang-bar-track">
                    <div class="lang-bar-fill" style="width:${pct}%;background:var(--primary)"></div>
                </div>
                <span class="lang-count">${formatNumber(n)}</span>
                <span class="lang-pct">${pct}%</span>
            </div>`;
        }).join('');

        return `<section class="stat-section">
            <h2>Language Breakdown</h2>
            <p class="rec-desc">Most-listened languages by play count.</p>
            <div class="lang-list">${rows}</div>
        </section>`;
    }

    // ── Main render ────────────────────────────────────────────────────────────
    function _render() {
        const w   = _where();
        const el  = document.getElementById('statsContent');
        if (!el) return;

        // ── By hour ────────────────────────────────────────────────────────────
        const hourData  = new Array(24).fill(0);
        _q(`SELECT CAST(strftime('%H',datetime(timestamp,'unixepoch')) AS INTEGER) h, COUNT(*) n
            FROM listens ${w} GROUP BY h`)
            .forEach(([h, n]) => { hourData[h] = n; });

        // ── By day of week ─────────────────────────────────────────────────────
        const dayData  = new Array(7).fill(0);
        const dayLabels = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
        _q(`SELECT CAST(strftime('%w',datetime(timestamp,'unixepoch')) AS INTEGER) d, COUNT(*) n
            FROM listens ${w} GROUP BY d`)
            .forEach(([d, n]) => { dayData[d] = n; });

        // ── By month ───────────────────────────────────────────────────────────
        const monthData   = new Array(12).fill(0);
        const monthLabels = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        _q(`SELECT CAST(strftime('%m',datetime(timestamp,'unixepoch')) AS INTEGER) m, COUNT(*) n
            FROM listens ${w} GROUP BY m`)
            .forEach(([m, n]) => { monthData[m - 1] = n; });

        // ── Punchcard ──────────────────────────────────────────────────────────
        const pcData = _q(`
            SELECT CAST(strftime('%H',datetime(timestamp,'unixepoch')) AS INTEGER) h,
                   CAST(strftime('%w',datetime(timestamp,'unixepoch')) AS INTEGER) d,
                   COUNT(*) n
            FROM listens ${w}
            GROUP BY h, d
        `);

        el.innerHTML = `
            <section class="stat-section">
                <h2>By Hour of Day</h2>
                <p class="rec-desc">Play count distribution across the 24-hour clock.</p>
                ${_barChart(hourData, hourData.map((_, i) => i % 6 === 0 ? String(i) : ''))}
            </section>

            <section class="stat-section">
                <h2>By Day of Week</h2>
                <p class="rec-desc">Total plays per day.</p>
                ${_barChart(dayData, dayLabels, { height: 100 })}
            </section>

            <section class="stat-section">
                <h2>By Month</h2>
                <p class="rec-desc">Seasonal listening patterns.</p>
                ${_barChart(monthData, monthLabels, { height: 100 })}
            </section>

            <section class="stat-section">
                <h2>Punchcard</h2>
                <p class="rec-desc">Hour × day intensity — darker cells mean more plays.</p>
                ${_punchcard(pcData)}
            </section>

            ${_languageSection()}

            ${_stubSection(
                'Gender Representation',
                'Breakdown of top-played artists by gender.',
                'Requires MusicBrainz artist gender enrichment (mdb enrich artists).'
            )}

            ${_stubSection(
                'Artist Type',
                'Solo artists, groups, orchestras, DJs, and more.',
                'Requires MusicBrainz artist type enrichment (mdb enrich artists).'
            )}
        `;

        lucide.createIcons();
    }

    return { mount, unmount };
})();
