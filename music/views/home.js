const ViewHome = (() => {
    let _db = null;
    let _abortController = null;

    // Home intro prose. Every ${...} is a value or link built by loadIntro();
    // everything else is plain text and safe to reword in place.
    const INTRO = ({ since, listens, artists, releases, tracks, listeningTime, find }) => `
        <p>
            I have been actively logging (almost) all the music I have listened to
            ${since}. I enjoy browsing the history to see who my most-played artists
            and albums were, and are: it lets me revisit old gems and keep track of
            whatever I am currently obsessed with. Scrobbles come from
            <a href="https://www.last.fm" target="_blank" rel="noopener noreferrer">Last.fm</a>,
            and album metadata from
            <a href="https://musicbrainz.org" target="_blank" rel="noopener noreferrer">MusicBrainz</a>.
        </p>
        <p>
            As of today: ${listens} across ${artists}, ${releases}, and ${tracks}${
                listeningTime ? `, adding up to roughly ${listeningTime} of music` : ''}.
            ${find ? `My latest find was ${find}.` : ''}
        </p>
    `;

    function mount(container, db, params) {
        _db = db;
        _abortController = new AbortController();
        setPageTitle('Music');

        container.innerHTML = `
            <header class="home-header">
                <h1>Music</h1>
            </header>

            <section class="home-intro" id="homeIntro"></section>


            <div class="stats-row">
                <section id="weeklyReleasesSection" hidden>
                    <div class="section-header">
                        <h2>Top Releases This Month</h2>
                    </div>
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

        loadIntro();
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

    function loadIntro() {
        const el = document.getElementById('homeIntro');
        if (!el) return;

        const counts = _db.exec(`
            SELECT
                (SELECT COUNT(*) FROM listens l
                 JOIN tracks t ON l.track_id = t.id
                 WHERE t.hidden = 0) as total_listens,
                (SELECT COUNT(*) FROM artists WHERE hidden = 0) as total_artists,
                (SELECT COUNT(*) FROM releases WHERE hidden = 0) as total_releases,
                (SELECT COUNT(*) FROM tracks WHERE hidden = 0) as total_tracks
        `)[0];
        if (!counts) return;
        const [listens, artists, releases, tracks] = counts.values[0];

        const stat = (href, n, noun) =>
            `<a href="${href}"><strong>${formatNumber(n)}</strong> ${noun}</a>`;

        const years = _db.exec(
            'SELECT MIN(year), MAX(year) FROM listens WHERE year IS NOT NULL'
        )[0];
        let since = 'for years';
        if (years && years.values[0][0] !== null) {
            const [first, latest] = years.values[0];
            since = `<a href="?view=year&year=${latest}">since ${first}</a>`;
        }

        const nerd = _db.exec('SELECT value_json FROM stats_cache WHERE key = ?', ['nerd'])[0];
        let listeningTime = null;
        if (nerd) {
            const days = Math.round(JSON.parse(nerd.values[0][0]).total_hours / 24);
            listeningTime = `<strong>${formatNumber(days)} days</strong>`;
        }

        // Singles and one-off compilations aren't "discoveries", and a release
        // needs a few plays before it counts as one.
        const found = _db.exec(`
            SELECT r.id, r.title, a.id, a.name, r.slug, a.slug
            FROM releases r
            LEFT JOIN artists a ON a.id = r.primary_artist_id
            WHERE r.hidden = 0
              AND lower(r.type) IN ('album', 'ep')
              AND r.stat_total_plays >= 5
              AND r.stat_first_listen_ts IS NOT NULL
            ORDER BY r.stat_first_listen_ts DESC
            LIMIT 1
        `)[0];
        let find = null;
        if (found) {
            const [rid, rtitle, aid, aname, rslug, aslug] = found.values[0];
            find = `<a href="${releaseHref(rid, rslug)}">${escapeHtml(rtitle)}</a>`;
            if (aname) {
                find += ` by <a href="${artistHref(aid, aslug)}">${escapeHtml(aname)}</a>`;
            }
        }

        el.innerHTML = INTRO({
            since,
            listens:  stat('?view=history', listens, 'listens'),
            artists:  stat('?view=top&type=artists', artists, 'artists'),
            releases: stat('?view=top&type=albums', releases, 'releases'),
            tracks:   stat('?view=top&type=tracks', tracks, 'tracks'),
            listeningTime,
            find,
        });
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
                list.innerHTML = artists.map(({ id, name, slug }) =>
                    `<li><a href="${artistHref(id, slug)}">${escapeHtml(name)}</a></li>`
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
            SELECT r.id, r.title, COALESCE(r.album_art_thumb_url, r.album_art_url) as album_art_url, a.name as artist_name, COUNT(l.id) as plays, r.slug
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

        result.values.forEach(([id, title, albumArtUrl, artistName, plays, slug]) => {
            const card = createImageCard({ href: releaseHref(id, slug), imageUrl: albumArtUrl });
            card.title = title + (artistName ? ` · ${artistName}` : '');
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
                r.title as release_title,
                r.slug as release_slug
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            LEFT JOIN releases r ON t.release_id = r.id
            LEFT JOIN artists a ON r.primary_artist_id = a.id
            WHERE t.hidden = 0
            ORDER BY l.timestamp DESC
            LIMIT 40
        `)[0];

        const section = document.getElementById('homeRecentPlaysSection');
        const list = document.getElementById('homeRecentPlaysList');
        if (!section || !list || !result || result.values.length === 0) return;

        // Collapse consecutive plays from the same release into one row —
        // otherwise an album played straight through reads as a stuck/glitched
        // list rather than genuine recent activity.
        const groups = [];
        result.values.forEach(([trackTitle, albumArtUrl, artistName, timestamp, releaseId, releaseTitle, releaseSlug]) => {
            const last = groups[groups.length - 1];
            const key = releaseId || `track:${trackTitle}`;
            if (last && last.key === key) {
                last.count += 1;
                last.tracks.push(trackTitle);
            } else {
                groups.push({
                    key, trackTitle, albumArtUrl, artistName, timestamp,
                    releaseId, releaseTitle, releaseSlug, count: 1, tracks: [trackTitle],
                });
            }
        });

        list.innerHTML = groups.slice(0, 10).map(g => {
            const imgSrc = g.albumArtUrl || getFallbackImageUrl();
            const dateStr = formatTimeAgo(g.timestamp);
            const nameHtml = g.count > 1
                ? `${g.count} tracks from ${escapeHtml(g.releaseTitle || 'this release')}`
                : escapeHtml(g.trackTitle);
            const subtitleParts = [
                g.artistName ? `<i data-lucide="user" style="width: 12px; height: 12px;"></i> ${escapeHtml(g.artistName)}` : null,
                (g.releaseTitle && g.count === 1) ? `<i data-lucide="disc-album" style="width: 12px; height: 12px;"></i> ${escapeHtml(g.releaseTitle)}` : null,
            ].filter(Boolean).join(' · ');
            const tag = g.releaseId ? 'a' : 'div';
            const hrefAttr = g.releaseId ? ` href="${releaseHref(g.releaseId, g.releaseSlug)}"` : '';
            return `
                <${tag} class="recent-play-row"${hrefAttr}>
                    <div class="recent-play-thumb" style="background-image: url('${cssUrl(imgSrc)}')"></div>
                    <div class="recent-play-info">
                        <div class="recent-play-name">${nameHtml}</div>
                        ${subtitleParts ? `<div class="recent-play-album">${subtitleParts}</div>` : ''}
                    </div>
                    <span class="recent-play-date">${dateStr}</span>
                </${tag}>
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
                   COUNT(l.id) plays, r.slug
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

        grid.innerHTML = picked.map(([id, title, art, artist, year, plays, slug]) => {
            const img = art
                ? `<div class="disc-card-img" style="background-image:url('${cssUrl(art)}')"></div>`
                : `<div class="disc-card-img" style="background:var(--bg-tertiary)"></div>`;
            const sub = [artist, year].filter(Boolean).join(' · ');
            return `<a class="disc-card" href="${releaseHref(id, slug)}">
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
            // '11, '12, …
            el.textContent = '\u2019' + String(year).slice(2);
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
