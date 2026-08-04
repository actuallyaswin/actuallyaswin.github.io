const ViewHome = (() => {
    let _db = null;
    let _abortController = null;

    function mount(container, db, params) {
        _db = db;
        _abortController = new AbortController();
        document.title = 'Music | Aswin Sivaraman';

        container.innerHTML = `
            <header>
                <h1>Music</h1>
                <p class="subtitle">
                    Explore data through the years:
                    <a href="?view=year" class="year-link" id="yearRange">Loading...</a>
                </p>
            </header>

            <div class="stats" id="stats">
                <div class="stat-card">
                    <div class="stat-value" id="statListens">-</div>
                    <div class="stat-label">Total Listens</div>
                </div>
                <a href="?view=top&type=artists" class="stat-card">
                    <div class="stat-value" id="statArtists">-</div>
                    <div class="stat-label">Artists</div>
                </a>
                <a href="?view=top&type=albums" class="stat-card">
                    <div class="stat-value" id="statReleases">-</div>
                    <div class="stat-label">Releases</div>
                </a>
                <a href="?view=top&type=tracks" class="stat-card">
                    <div class="stat-value" id="statTracks">-</div>
                    <div class="stat-label">Tracks</div>
                </a>
            </div>


            <div class="stats-row">
                <section id="weeklyReleasesSection" hidden>
                    <h2>Top Releases This Month</h2>
                    <div id="weeklyReleasesCollage"></div>
                </section>
                <section id="homeRecentPlaysSection" hidden>
                    <div class="section-header">
                        <h2>Recent Plays</h2>
                        <a href="?view=history" class="home-see-all">See all →</a>
                    </div>
                    <div class="recent-plays-list" id="homeRecentPlaysList"></div>
                </section>
                <section class="nerds-section">
                    <div class="section-header">
                        <h2>Stats for Nerds</h2>
                        <a href="?view=stats" class="home-see-all">See all →</a>
                    </div>
                    <dl class="nerds-list">
                        <div class="nerds-row">
                            <dt>Days of Scrobbling</dt>
                            <dd id="nerdDays">—</dd>
                        </div>
                        <div class="nerds-row">
                            <dt>Avg. Listens / Day</dt>
                            <dd id="nerdAvgDay">—</dd>
                        </div>
                        <div class="nerds-row">
                            <dt>Total Listening Time</dt>
                            <dd id="nerdTotalTime">—</dd>
                        </div>
                        <div class="nerds-row">
                            <dt>Most Active Month</dt>
                            <dd id="nerdPeakMonth">—</dd>
                        </div>
                        <div class="nerds-row">
                            <dt>One hit wonders <span class="tooltip-wrap" data-tooltip="Heard exactly once"><i data-lucide="info"></i></span></dt>
                            <dd id="nerdOneHit">—</dd>
                        </div>
                        <div class="nerds-row">
                            <dt>Every year artists</dt>
                            <dd id="nerdEveryYear">—</dd>
                        </div>
                        <div class="nerds-accordion" id="nerdEveryYearPanel" hidden>
                            <ul id="nerdEveryYearList"></ul>
                        </div>
                        <div class="nerds-row">
                            <dt>Eddington number <span class="tooltip-wrap" data-tooltip="N tracks each heard N+ times"><i data-lucide="info"></i></span></dt>
                            <dd id="nerdEddington">—</dd>
                        </div>
                        <div class="nerds-row">
                            <dt>Artist cut over point <span class="tooltip-wrap" data-tooltip="N artists each with N+ scrobbles"><i data-lucide="info"></i></span></dt>
                            <dd id="nerdArtistCutover">—</dd>
                        </div>
                    </dl>
                </section>
                <section id="genreCommitsSection" hidden>
                    <div class="commits-header">
                        <h2>Taste Over Time</h2>
                        <button id="colorModeToggle" class="commits-mode-btn"></button>
                    </div>
                    <div class="commits-grid" id="commitsGrid"></div>
                </section>
            </div>

            <section id="homeRecsSection" hidden>
                <div class="section-header">
                    <h2>Recommendations</h2>
                    <a href="?view=recommendations" class="home-see-all">See all →</a>
                </div>
                <div class="disc-grid" id="homeRecsGrid"></div>
            </section>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        loadYearRange();
        loadStats();
        loadNerdsStats();
        loadWeeklyReleases();
        loadHomeRecentPlays();
        loadRecommendationsPreview();
        loadGenreCommits();
    }

    function unmount() {
        if (_abortController) {
            _abortController.abort();
            _abortController = null;
        }
    }

    function loadYearRange() {
        const yearRange = _db.exec(`
            SELECT MIN(year) as min_year, MAX(year) as max_year
            FROM listens WHERE year IS NOT NULL
        `)[0];

        if (yearRange && yearRange.values[0][0] !== null) {
            const [minYear, maxYear] = yearRange.values[0];
            const el = document.getElementById('yearRange');
            if (el) {
                el.textContent = `${minYear} – ${maxYear}`;
                el.href = `?view=year&year=${maxYear}`;
            }
        }
    }

    function loadStats() {
        const stats = _db.exec(`
            SELECT
                (SELECT COUNT(*) FROM listens l
                 JOIN tracks t ON l.track_id = t.id
                 WHERE t.hidden = 0) as total_listens,
                (SELECT COUNT(*) FROM artists WHERE hidden = 0) as total_artists,
                (SELECT COUNT(*) FROM releases WHERE hidden = 0) as total_releases,
                (SELECT COUNT(*) FROM tracks WHERE hidden = 0) as total_tracks
        `)[0];

        const [totalListens, totalArtists, totalReleases, totalTracks] = stats.values[0];
        document.getElementById('statListens').textContent = formatNumber(totalListens);
        document.getElementById('statArtists').textContent = formatNumber(totalArtists);
        document.getElementById('statReleases').textContent = formatNumber(totalReleases);
        document.getElementById('statTracks').textContent = formatNumber(totalTracks);
    }


    // All values here come from `mdb.py stats refresh`'s `nerd` cache entry —
    // see views/stats.js's `_cache()`/`_nerdSection()` for the same source.
    function loadNerdsStats() {
        const res = _db.exec('SELECT value_json FROM stats_cache WHERE key = ?', ['nerd'])[0];
        if (!res) return;
        const n = JSON.parse(res.values[0][0]);

        const daysEl = document.getElementById('nerdDays');
        if (daysEl) daysEl.textContent = `${formatNumber(n.active_days)} (${n.active_pct}%)`;
        const avgEl = document.getElementById('nerdAvgDay');
        if (avgEl) avgEl.textContent = n.avg_per_day;

        const totalHrs = n.total_hours;
        const days = Math.floor(totalHrs / 24);
        const remHrs = totalHrs % 24;
        const timeEl = document.getElementById('nerdTotalTime');
        if (timeEl) timeEl.textContent = days > 0 ? `${formatNumber(days)} days, ${remHrs} hrs` : `${formatNumber(totalHrs)} hrs`;

        const peakEl = document.getElementById('nerdPeakMonth');
        if (peakEl) peakEl.textContent = n.peak_month ? `${n.peak_month} · ${formatNumber(n.peak_month_count)}` : '—';

        const ohwPct = n.one_hit_wonders_total ? ((n.one_hit_wonders / n.one_hit_wonders_total) * 100).toFixed(1) : '0';
        const ohwEl = document.getElementById('nerdOneHit');
        if (ohwEl) ohwEl.textContent = `${formatNumber(n.one_hit_wonders)} (${ohwPct}%)`;

        const artists = n.every_year_artists;
        if (artists.length > 0) {
            const ddEl = document.getElementById('nerdEveryYear');
            const panel = document.getElementById('nerdEveryYearPanel');
            const list  = document.getElementById('nerdEveryYearList');
            if (ddEl) {
                ddEl.innerHTML = `${artists.length}
                    <button class="nerds-accordion-btn" id="nerdEveryYearToggle" title="Show artists (${n.every_year_total_years} years)">
                        <i data-lucide="square-menu"></i>
                    </button>`;
                lucide.createIcons({ nodes: [ddEl] });
                document.getElementById('nerdEveryYearToggle')?.addEventListener('click', () => {
                    const open = panel.hidden;
                    panel.hidden = !open;
                    document.getElementById('nerdEveryYearToggle')?.classList.toggle('nerds-accordion-btn-open', open);
                });
            }
            if (list) {
                list.innerHTML = artists.map(({ id, name }) =>
                    `<li><a href="?view=artist&id=${encodeURIComponent(id)}">${escapeHtml(name)}</a></li>`
                ).join('');
            }
        }

        const eddEl = document.getElementById('nerdEddington');
        if (eddEl) eddEl.textContent = formatNumber(n.eddington);
        const cutEl = document.getElementById('nerdArtistCutover');
        if (cutEl) cutEl.textContent = formatNumber(n.artist_cutover);

        lucide.createIcons();
    }


    function loadWeeklyReleases() {
        const thirtyDaysAgo = Math.floor(Date.now() / 1000) - 30 * 86400;
        const result = _db.exec(`
            SELECT r.id, r.title, COALESCE(r.album_art_thumb_url, r.album_art_url) as album_art_url, a.name as artist_name, COUNT(l.id) as plays
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN releases r ON t.release_id = r.id
            LEFT JOIN artists a ON r.primary_artist_id = a.id
            WHERE l.timestamp >= ${thirtyDaysAgo}
            AND t.hidden = 0 AND r.hidden = 0
            GROUP BY r.id
            ORDER BY plays DESC
            LIMIT 16
        `)[0];

        const container = document.getElementById('weeklyReleasesCollage');
        const section = document.getElementById('weeklyReleasesSection');
        if (!container || !section || !result || result.values.length === 0) return;

        const n = 4;
        container.className = 'collage-grid';
        container.style.gridTemplateColumns = `repeat(${n}, 1fr)`;

        result.values.forEach(([id, title, albumArtUrl, artistName]) => {
            const card = document.createElement('a');
            card.className = 'image-card';
            card.href = `?view=release&id=${encodeURIComponent(id)}`;
            card.title = title + (artistName ? ` · ${artistName}` : '');
            const imgSrc = albumArtUrl || getFallbackImageUrl();
            card.innerHTML = `<div class="image-card-img" style="background-image: url('${cssUrl(imgSrc)}')"></div>`;
            container.appendChild(card);
        });

        section.removeAttribute('hidden');
    }

    function loadHomeRecentPlays() {
        const result = _db.exec(`
            SELECT
                t.title,
                COALESCE(r.album_art_thumb_url, r.album_art_url) as album_art_url,
                a.name as artist_name,
                l.timestamp,
                r.id as release_id,
                r.title as release_title
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            LEFT JOIN releases r ON t.release_id = r.id
            LEFT JOIN artists a ON r.primary_artist_id = a.id
            WHERE t.hidden = 0
            ORDER BY l.timestamp DESC
            LIMIT 10
        `)[0];

        const section = document.getElementById('homeRecentPlaysSection');
        const list = document.getElementById('homeRecentPlaysList');
        if (!section || !list || !result || result.values.length === 0) return;

        const now = Date.now() / 1000;
        list.innerHTML = result.values.map(([trackTitle, albumArtUrl, artistName, timestamp, releaseId, releaseTitle]) => {
            const imgSrc = albumArtUrl || getFallbackImageUrl();
            let dateStr;
            const diff = now - timestamp;
            if (diff < 3600)        dateStr = `${Math.floor(diff / 60)}m ago`;
            else if (diff < 86400)  dateStr = `${Math.floor(diff / 3600)}h ago`;
            else if (diff < 604800) dateStr = `${Math.floor(diff / 86400)}d ago`;
            else {
                const d = new Date(timestamp * 1000);
                dateStr = d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
            }
            const subtitleParts = [
                artistName   ? `<i data-lucide="user" style="width: 12px; height: 12px;"></i> ${escapeHtml(artistName)}` : null,
                releaseTitle ? `<i data-lucide="disc-album" style="width: 12px; height: 12px;"></i> ${escapeHtml(releaseTitle)}` : null,
            ].filter(Boolean).join(' · ');
            return `
                <div class="recent-play-row">
                    <div class="recent-play-thumb" style="background-image: url('${cssUrl(imgSrc)}')"></div>
                    <div class="recent-play-info">
                        <div class="recent-play-name">${escapeHtml(trackTitle)}</div>
                        ${subtitleParts ? `<div class="recent-play-album">${subtitleParts}</div>` : ''}
                    </div>
                    <span class="recent-play-date">${dateStr}</span>
                </div>
            `;
        }).join('');

        section.removeAttribute('hidden');
    }

    function loadRecommendationsPreview() {
        // Counts come from constants.js: SHELF_DESKTOP, SHELF_MOBILE
        const section = document.getElementById('homeRecsSection');
        const grid    = document.getElementById('homeRecsGrid');
        if (!section || !grid) return;

        const now   = Math.floor(Date.now() / 1000);
        const seed  = _db.exec('SELECT COUNT(*) FROM listens')[0].values[0][0];
        const since = now - 90 * 86400;

        const result = _db.exec(`
            SELECT r.id, r.title, COALESCE(r.album_art_thumb_url, r.album_art_url), a.name, r.release_year,
                   COUNT(l.id) plays
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN releases r ON r.id = t.release_id
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE l.timestamp >= ${since}
              AND r.hidden = 0 AND t.hidden = 0 AND t.variant_section IS NULL
            GROUP BY r.id ORDER BY plays DESC LIMIT 30
        `)[0];
        if (!result || !result.values.length) return;

        const pool   = [...result.values].sort((a, b) => (a[0] < b[0] ? -1 : 1));
        const start  = seed % pool.length;
        const picked = [];
        for (let i = 0; i < Math.min(SHELF_DESKTOP, pool.length); i++)
            picked.push(pool[(start + i) % pool.length]);

        grid.innerHTML = picked.map(([id, title, art, artist, year]) => {
            const img = art
                ? `<div class="disc-card-img" style="background-image:url('${cssUrl(art)}')"></div>`
                : `<div class="disc-card-img" style="background:var(--bg-tertiary)"></div>`;
            const sub = [artist, year].filter(Boolean).join(' · ');
            return `<a class="disc-card" href="?view=release&id=${encodeURIComponent(id)}">
                ${img}
                <div class="disc-card-meta"><div class="disc-card-info">
                    <div class="disc-card-title">${escapeHtml(title || '')}</div>
                    <div class="disc-card-sub">${escapeHtml(sub)}</div>
                </div></div></a>`;
        }).join('');

        section.removeAttribute('hidden');
    }

    function loadGenreCommits() {
        let colorMode = 'top';

        const result = _db.exec(`
            SELECT year, month, listen_count, color_hex, top_genre_color_hex, dominant_genre, genres_json
            FROM monthly_genre_profile
            ORDER BY year, month
        `)[0];

        const section = document.getElementById('genreCommitsSection');
        const grid    = document.getElementById('commitsGrid');
        if (!section || !grid || !result || result.values.length === 0) return;

        const profileMap = {};
        result.values.forEach(([year, month, count, blendedColor, topColor, dominant, genresJson]) => {
            profileMap[`${year}-${month}`] = {
                year, month, count, dominant,
                blendedColor, topColor,
                genres: genresJson ? JSON.parse(genresJson) : [],
            };
        });

        const allYears  = result.values.map(r => r[0]);
        const minYear   = Math.min(...allYears);
        const maxYear   = Math.max(...allYears);
        const nYears    = maxYear - minYear + 1;
        const now       = new Date();
        const curYear   = now.getFullYear();
        const curMonth  = now.getMonth() + 1;

        // Transposed: months = rows (Y-axis), years = columns (X-axis)
        // Columns: month-label + one per year
        grid.style.gridTemplateColumns = `2rem repeat(${nYears}, var(--commit-cell-w, 24px))`;

        const MONTHS_SHORT = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        const MONTHS_LONG  = ['January','February','March','April','May','June',
                              'July','August','September','October','November','December'];

        // Header row: blank corner + year labels
        const corner = document.createElement('div');
        corner.className = 'commit-month-label';
        grid.appendChild(corner);
        for (let year = minYear; year <= maxYear; year++) {
            const el = document.createElement('div');
            el.className = 'commit-year-label';
            el.textContent = '\u2019' + String(year).slice(2); // '11, '12, …
            grid.appendChild(el);
        }

        // Month rows
        for (let month = 1; month <= 12; month++) {
            const mLabel = document.createElement('div');
            mLabel.className = 'commit-month-label';
            mLabel.textContent = MONTHS_SHORT[month - 1];
            grid.appendChild(mLabel);

            for (let year = minYear; year <= maxYear; year++) {
                const cell     = document.createElement('div');
                cell.className = 'commit-cell';
                const isFuture = year > curYear || (year === curYear && month > curMonth);
                const profile  = profileMap[`${year}-${month}`];

                if (isFuture) {
                    cell.classList.add('commit-future');
                } else if (!profile || profile.count === 0) {
                    cell.classList.add('commit-empty');
                } else {
                    cell.classList.add('commit-has-data');
                    cell.dataset.year         = year;
                    cell.dataset.month        = month;
                    cell.dataset.count        = profile.count;
                    cell.dataset.genres       = JSON.stringify(profile.genres);
                    cell.dataset.blendedColor = profile.blendedColor;
                    cell.dataset.topColor     = profile.topColor;
                    cell.style.backgroundColor = profile.topColor;
                }

                grid.appendChild(cell);
            }
        }

        // Floating tooltip
        const tooltip = document.createElement('div');
        tooltip.className = 'commit-tooltip';
        document.body.appendChild(tooltip);

        function showTooltip(cell, e) {
            const year   = cell.dataset.year;
            const month  = parseInt(cell.dataset.month);
            const count  = parseInt(cell.dataset.count);
            const genres = JSON.parse(cell.dataset.genres || '[]');

            const genreRows = genres.slice(0, 5).map(g =>
                `<div class="ctt-genre">` +
                `<span class="ctt-dot" style="background:${escapeHtml(g.color)}"></span>` +
                `<span class="ctt-name">${escapeHtml(g.genre)}</span>` +
                `<span class="ctt-pct">${g.pct}%</span>` +
                `</div>`
            ).join('');

            tooltip.innerHTML =
                `<div class="ctt-header">${MONTHS_LONG[month - 1]} ${year}</div>` +
                `<div class="ctt-count">${formatNumber(count)} listens</div>` +
                (genreRows ? `<div class="ctt-genres">${genreRows}</div>` : '');

            positionTooltip(e);
            tooltip.style.display = 'block';
        }

        function positionTooltip(e) {
            const x = e.clientX + 14;
            const y = e.clientY - tooltip.offsetHeight / 2;
            tooltip.style.left = Math.min(x, window.innerWidth  - tooltip.offsetWidth  - 12) + 'px';
            tooltip.style.top  = Math.max(8, Math.min(y, window.innerHeight - tooltip.offsetHeight - 8)) + 'px';
        }

        grid.addEventListener('mouseover', e => {
            const cell = e.target.closest('.commit-has-data');
            if (!cell) { tooltip.style.display = 'none'; return; }
            showTooltip(cell, e);
        });
        grid.addEventListener('mousemove', e => {
            if (tooltip.style.display === 'none') return;
            if (!e.target.closest('.commit-has-data')) { tooltip.style.display = 'none'; return; }
            positionTooltip(e);
        });
        grid.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });

        grid.addEventListener('touchstart', e => {
            const cell = e.target.closest('.commit-has-data');
            if (!cell) return;
            e.preventDefault();
            const touch = e.touches[0];
            showTooltip(cell, { clientX: touch.clientX, clientY: touch.clientY });
        }, { passive: false });
        document.addEventListener('touchstart', e => {
            if (!grid.contains(e.target)) tooltip.style.display = 'none';
        }, { signal: _abortController.signal });

        // Color mode toggle
        const toggleBtn = document.getElementById('colorModeToggle');
        function applyColorMode(mode) {
            colorMode = mode;
            const isTop = mode === 'top';
            toggleBtn.innerHTML = isTop
                ? `<i data-lucide="circle-dot"></i> top`
                : `<i data-lucide="layers"></i> blended`;
            grid.querySelectorAll('.commit-has-data').forEach(cell => {
                cell.style.backgroundColor = isTop ? cell.dataset.topColor : cell.dataset.blendedColor;
            });
        }
        applyColorMode(colorMode);
        toggleBtn.addEventListener('click', () => applyColorMode(colorMode === 'top' ? 'blended' : 'top'));

        section.removeAttribute('hidden');
    }

    return { mount, unmount };
})();
