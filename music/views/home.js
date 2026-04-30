const ViewHome = (() => {
    let _db = null;
    let _abortController = null;

    function mount(container, db, params) {
        _db = db;
        document.title = 'aswin.db/music';

        container.innerHTML = `
            <header>
                <div class="site-logo">
                    <span class="logo-main">aswin.db</span><span class="logo-slash">/</span><span class="logo-accent">music</span>
                </div>
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
                <a href="?view=top-artists" class="stat-card">
                    <div class="stat-value" id="statArtists">-</div>
                    <div class="stat-label">Artists</div>
                </a>
                <a href="?view=top-albums" class="stat-card">
                    <div class="stat-value" id="statReleases">-</div>
                    <div class="stat-label">Releases</div>
                </a>
                <a href="?view=top-tracks" class="stat-card">
                    <div class="stat-value" id="statTracks">-</div>
                    <div class="stat-label">Tracks</div>
                </a>
            </div>

            <div class="search-section">
                <div class="search-col">
                    <input type="text" id="searchInput" class="search-input" placeholder="Search artists..." autocomplete="off">
                    <div id="searchResults" class="search-results"></div>
                </div>
                <div class="search-col">
                    <input type="text" id="releaseSearchInput" class="search-input" placeholder="Search releases..." autocomplete="off">
                    <div id="releaseSearchResults" class="search-results"></div>
                </div>
            </div>

            <div class="stats-row">
                <section id="weeklyReleasesSection" hidden>
                    <h2>Top Releases This Week</h2>
                    <div id="weeklyReleasesCollage"></div>
                </section>
                <section id="homeRecentPlaysSection" hidden>
                    <h2>Recent Plays</h2>
                    <div class="recent-plays-list" id="homeRecentPlaysList"></div>
                </section>
                <section class="nerds-section">
                    <h2>Stats for Nerds</h2>
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

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        loadYearRange();
        loadStats();
        loadNerdsStats();
        setupSearch();
        setupReleaseSearch();
        loadWeeklyReleases();
        loadHomeRecentPlays();
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

    function attachKeyNav(inputEl, resultsEl) {
        inputEl.addEventListener('keydown', e => {
            if (!resultsEl.classList.contains('active')) return;
            const items = Array.from(resultsEl.querySelectorAll('a.search-result-item'));
            if (!items.length) return;
            const focused = resultsEl.querySelector('.keyboard-focused');
            const idx = focused ? items.indexOf(focused) : -1;

            if (e.key === 'ArrowDown') {
                e.preventDefault();
                const next = items[idx + 1] ?? items[0];
                focused?.classList.remove('keyboard-focused');
                next.classList.add('keyboard-focused');
                next.scrollIntoView({ block: 'nearest' });
            } else if (e.key === 'ArrowUp') {
                e.preventDefault();
                if (idx <= 0) {
                    focused?.classList.remove('keyboard-focused');
                    inputEl.focus();
                } else {
                    const prev = items[idx - 1];
                    focused?.classList.remove('keyboard-focused');
                    prev.classList.add('keyboard-focused');
                    prev.scrollIntoView({ block: 'nearest' });
                }
            } else if (e.key === 'Enter' && focused) {
                e.preventDefault();
                focused.click();
            } else if (e.key === 'Escape') {
                resultsEl.classList.remove('active');
                focused?.classList.remove('keyboard-focused');
            }
        });
    }

    function loadNerdsStats() {
        const MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

        // Days of Scrobbling + Avg Listens/Day
        const daysResult = _db.exec(`
            SELECT
                COUNT(DISTINCT date(l.timestamp, 'unixepoch')) as active_days,
                CAST(julianday(date(MAX(l.timestamp), 'unixepoch'))
                     - julianday(date(MIN(l.timestamp), 'unixepoch')) + 1 AS INTEGER) as total_days,
                COUNT(l.id) as total_listens
            FROM listens l
            JOIN tracks t ON l.track_id = t.id WHERE t.hidden = 0
        `)[0];
        if (daysResult) {
            const [active, total, totalListens] = daysResult.values[0];
            const pct = total > 0 ? ((active / total) * 100).toFixed(1) : '0';
            const el = document.getElementById('nerdDays');
            if (el) el.textContent = `${formatNumber(active)} (${pct}%)`;
            const avgEl = document.getElementById('nerdAvgDay');
            if (avgEl && active > 0) avgEl.textContent = (totalListens / active).toFixed(1);
        }

        // Total Listening Time
        const timeResult = _db.exec(`
            SELECT SUM(COALESCE(l.ms_played, t.duration_ms)) as total_ms
            FROM listens l
            JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
        `)[0];
        if (timeResult) {
            const totalMs = timeResult.values[0][0] || 0;
            const totalHrs = Math.floor(totalMs / 3600000);
            const days = Math.floor(totalHrs / 24);
            const remHrs = totalHrs % 24;
            const el = document.getElementById('nerdTotalTime');
            if (el) el.textContent = days > 0 ? `${formatNumber(days)} days, ${remHrs} hrs` : `${formatNumber(totalHrs)} hrs`;
        }

        // Most Active Month
        const peakResult = _db.exec(`
            SELECT strftime('%Y-%m', datetime(l.timestamp, 'unixepoch')) as ym,
                   COUNT(l.id) as cnt
            FROM listens l
            JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
            GROUP BY ym
            ORDER BY cnt DESC
            LIMIT 1
        `)[0];
        if (peakResult) {
            const [ym, cnt] = peakResult.values[0];
            const [year, month] = ym.split('-');
            const label = `${MONTH_NAMES[parseInt(month) - 1]} ${year}`;
            const el = document.getElementById('nerdPeakMonth');
            if (el) el.textContent = `${label} · ${formatNumber(cnt)}`;
        }

        // One hit wonders
        const ohwResult = _db.exec(`
            WITH artist_counts AS (
                SELECT ta.artist_id, COUNT(l.id) as play_count
                FROM listens l
                JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
                JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
                JOIN artists a ON ta.artist_id = a.id AND a.hidden = 0
                GROUP BY ta.artist_id
            )
            SELECT
                SUM(CASE WHEN play_count = 1 THEN 1 ELSE 0 END) as ohw,
                COUNT(*) as total
            FROM artist_counts
        `)[0];
        if (ohwResult) {
            const [ohw, total] = ohwResult.values[0];
            const pct = total > 0 ? ((ohw / total) * 100).toFixed(1) : '0';
            const el = document.getElementById('nerdOneHit');
            if (el) el.textContent = `${formatNumber(ohw)} (${pct}%)`;
        }

        // Every year artists
        const eyResult = _db.exec(`
            WITH artist_years AS (
                SELECT ta.artist_id,
                       strftime('%Y', datetime(l.timestamp, 'unixepoch')) AS yr
                FROM listens l
                JOIN tracks t ON l.track_id = t.id
                JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
                WHERE t.hidden = 0
                GROUP BY ta.artist_id, yr
            ),
            total_years AS (
                SELECT COUNT(DISTINCT strftime('%Y', datetime(l.timestamp, 'unixepoch'))) AS n
                FROM listens l
                JOIN tracks t ON l.track_id = t.id
                WHERE t.hidden = 0
            ),
            every_year AS (
                SELECT artist_id, COUNT(DISTINCT yr) AS yrs
                FROM artist_years
                GROUP BY artist_id
                HAVING yrs = (SELECT n FROM total_years)
            )
            SELECT a.id, a.name, (SELECT n FROM total_years) AS total_yrs
            FROM every_year ey
            JOIN artists a ON a.id = ey.artist_id
            WHERE a.hidden = 0
            ORDER BY a.name
        `)[0];
        if (eyResult && eyResult.values.length > 0) {
            const totalYrs = eyResult.values[0][2];
            const artists = eyResult.values.map(([id, name]) => ({ id, name }));
            const ddEl = document.getElementById('nerdEveryYear');
            const panel = document.getElementById('nerdEveryYearPanel');
            const list  = document.getElementById('nerdEveryYearList');
            if (ddEl) {
                ddEl.innerHTML = `${artists.length}
                    <button class="nerds-accordion-btn" id="nerdEveryYearToggle" title="Show artists (${totalYrs} years)">
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

        // Eddington number (tracks)
        const eddResult = _db.exec(`
            WITH track_counts AS (
                SELECT l.track_id, COUNT(l.id) as play_count
                FROM listens l
                JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
                GROUP BY l.track_id
            ),
            ranked AS (
                SELECT play_count,
                       ROW_NUMBER() OVER (ORDER BY play_count DESC) as rank
                FROM track_counts
            )
            SELECT MAX(rank) as eddington
            FROM ranked
            WHERE play_count >= rank
        `)[0];
        if (eddResult) {
            const el = document.getElementById('nerdEddington');
            if (el) el.textContent = formatNumber(eddResult.values[0][0]);
        }

        // Artist cut over point
        const cutResult = _db.exec(`
            WITH artist_counts AS (
                SELECT ta.artist_id, COUNT(l.id) as play_count
                FROM listens l
                JOIN tracks t ON l.track_id = t.id AND t.hidden = 0
                JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
                JOIN artists a ON ta.artist_id = a.id AND a.hidden = 0
                GROUP BY ta.artist_id
            ),
            ranked AS (
                SELECT play_count,
                       ROW_NUMBER() OVER (ORDER BY play_count DESC) as rank
                FROM artist_counts
            )
            SELECT MAX(rank) as cutover
            FROM ranked
            WHERE play_count >= rank
        `)[0];
        if (cutResult) {
            const el = document.getElementById('nerdArtistCutover');
            if (el) el.textContent = formatNumber(cutResult.values[0][0]);
        }

        lucide.createIcons();
    }

    function setupSearch() {
        const searchInput = document.getElementById('searchInput');
        const searchResults = document.getElementById('searchResults');
        let debounceTimer;

        _abortController = new AbortController();
        const signal = _abortController.signal;

        searchInput.addEventListener('input', e => {
            clearTimeout(debounceTimer);
            const query = e.target.value.trim();
            if (query.length < 2) {
                searchResults.classList.remove('active');
                return;
            }
            debounceTimer = setTimeout(() => performSearch(query), 300);
        });

        attachKeyNav(searchInput, searchResults);

        document.addEventListener('click', e => {
            if (searchInput && !searchInput.contains(e.target) && searchResults && !searchResults.contains(e.target)) {
                searchResults.classList.remove('active');
            }
        }, { signal });
    }

    function performSearch(query) {
        const searchResults = document.getElementById('searchResults');
        if (!searchResults) return;

        const safeQ = query.replace(/'/g, "''");
        const result = _db.exec(`
            SELECT
                a.id,
                a.name,
                COUNT(l.id) as total_listens,
                (SELECT aa.alias FROM artist_aliases aa
                 WHERE aa.artist_id = a.id AND lower(aa.alias) LIKE lower('%${safeQ}%')
                 LIMIT 1) as matched_alias
            FROM artists a
            LEFT JOIN track_artists ta ON a.id = ta.artist_id AND ta.role = 'main'
            LEFT JOIN tracks t ON ta.track_id = t.id AND t.hidden = 0
            LEFT JOIN listens l ON t.id = l.track_id
            WHERE (a.name LIKE '%${safeQ}%'
                   OR a.id IN (SELECT artist_id FROM artist_aliases WHERE lower(alias) LIKE lower('%${safeQ}%')))
            AND a.hidden = 0
            GROUP BY a.id
            ORDER BY total_listens DESC
            LIMIT 10
        `)[0];

        searchResults.innerHTML = '';

        if (!result || result.values.length === 0) {
            searchResults.innerHTML = '<div class="search-result-item">No results found</div>';
            searchResults.classList.add('active');
            return;
        }

        result.values.forEach(([id, name, totalListens, matchedAlias]) => {
            const item = document.createElement('a');
            item.className = 'search-result-item';
            item.href = `?view=artist&id=${encodeURIComponent(id)}`;
            const showAlias = matchedAlias && name.toLowerCase().indexOf(query.toLowerCase()) === -1;
            item.innerHTML = `
                <div class="search-result-main">
                    <span class="search-result-name">${escapeHtml(name)}${showAlias ? ` <span class="search-result-alias">· ${escapeHtml(matchedAlias)}</span>` : ''}</span>
                    <span class="search-result-count">${formatNumber(totalListens)}</span>
                </div>
            `;
            searchResults.appendChild(item);
        });

        searchResults.classList.add('active');
    }

    function setupReleaseSearch() {
        const searchInput = document.getElementById('releaseSearchInput');
        const searchResults = document.getElementById('releaseSearchResults');
        let debounceTimer;

        const signal = _abortController.signal;

        searchInput.addEventListener('input', e => {
            clearTimeout(debounceTimer);
            const query = e.target.value.trim();
            if (query.length < 2) {
                searchResults.classList.remove('active');
                return;
            }
            debounceTimer = setTimeout(() => performReleaseSearch(query), 300);
        });

        attachKeyNav(searchInput, searchResults);

        document.addEventListener('click', e => {
            if (searchInput && !searchInput.contains(e.target) && searchResults && !searchResults.contains(e.target)) {
                searchResults.classList.remove('active');
            }
        }, { signal });
    }

    function performReleaseSearch(query) {
        const searchResults = document.getElementById('releaseSearchResults');
        if (!searchResults) return;

        const result = _db.exec(`
            SELECT
                r.id,
                r.title,
                r.release_year,
                a.name,
                COUNT(l.id) as total_listens
            FROM releases r
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            LEFT JOIN tracks t ON t.release_id = r.id AND t.hidden = 0
            LEFT JOIN listens l ON l.track_id = t.id
            WHERE r.title LIKE '%${query.replace(/'/g, "''")}%'
            AND r.hidden = 0
            AND NOT EXISTS (SELECT 1 FROM release_variants rv WHERE rv.variant_id = r.id)
            GROUP BY r.id
            ORDER BY total_listens DESC
            LIMIT 10
        `)[0];

        searchResults.innerHTML = '';

        if (!result || result.values.length === 0) {
            searchResults.innerHTML = '<div class="search-result-item">No results found</div>';
            searchResults.classList.add('active');
            return;
        }

        result.values.forEach(([id, title, year, artistName, totalListens]) => {
            const item = document.createElement('a');
            item.className = 'search-result-item';
            item.href = `?view=release&id=${encodeURIComponent(id)}`;
            item.innerHTML = `
                <div class="search-result-main">
                    <span class="search-result-name">${escapeHtml(title)}</span>
                    <span class="search-result-count">${formatNumber(totalListens)}</span>
                </div>
                <div class="search-result-meta">${escapeHtml(artistName || 'Various Artists')}${year ? ` · ${year}` : ''}</div>
            `;
            searchResults.appendChild(item);
        });

        searchResults.classList.add('active');
    }

    function loadWeeklyReleases() {
        const sevenDaysAgo = Math.floor(Date.now() / 1000) - 7 * 86400;
        const result = _db.exec(`
            SELECT r.id, r.title, COALESCE(r.album_art_thumb_url, r.album_art_url) as album_art_url, a.name as artist_name, COUNT(l.id) as plays
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN releases r ON t.release_id = r.id
            LEFT JOIN artists a ON r.primary_artist_id = a.id
            WHERE l.timestamp >= ${sevenDaysAgo}
            AND t.hidden = 0 AND r.hidden = 0
            GROUP BY r.id
            ORDER BY plays DESC
            LIMIT 9
        `)[0];

        const container = document.getElementById('weeklyReleasesCollage');
        const section = document.getElementById('weeklyReleasesSection');
        if (!container || !section || !result || result.values.length === 0) return;

        const n = result.values.length <= 4 ? 2 : 3;
        container.className = 'collage-grid';
        container.style.gridTemplateColumns = `repeat(${n}, 1fr)`;

        result.values.forEach(([id, title, albumArtUrl, artistName]) => {
            const card = document.createElement('a');
            card.className = 'image-card';
            card.href = `?view=release&id=${encodeURIComponent(id)}`;
            card.title = title + (artistName ? ` · ${artistName}` : '');
            const imgSrc = albumArtUrl || getFallbackImageUrl();
            card.innerHTML = `<div class="image-card-img" style="background-image: url('${imgSrc}')"></div>`;
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
                    <div class="recent-play-thumb" style="background-image: url('${imgSrc}')"></div>
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
        });

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
