const ViewArtist = (() => {
    let _db = null;
    let _artistId = null;
    let _currentChart = null;
    let _chartData = { monthly: null, yearly: null, monthlyRaw: null };
    let _chartState = { granularity: 'monthly', type: 'distribution' };
    let _discSort = 'date'; // 'date' | 'listens'
    let _discData = { own: null, collabs: null };
    let _themeObserver = null;

    const CHART_ENABLED = false;
    const HERO_ENABLED = false;

    function mount(container, db, params) {
        _db = db;
        _artistId = params.id;
        _currentChart = null;
        _chartData = { monthly: null, yearly: null, monthlyRaw: null };
        _discData = { own: null, collabs: null };

        if (!_artistId) {
            navigate({ view: 'home' });
            return;
        }

        container.innerHTML = `
            <div id="artistHero" class="artist-hero" hidden></div>

            <header id="artistHeader" class="artist-header-layout release-header-grid">
                <div class="artist-photo-container">
                    <div class="artist-photo" id="artistPhoto">
                        <svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
                            <circle cx="50" cy="50" r="50" fill="#1e293b"/>
                            <text x="50" y="60" text-anchor="middle" font-size="40" fill="#475569">♪</text>
                        </svg>
                    </div>
                    <div class="artist-badges" id="artistBadges"></div>
                </div>
                <div class="artist-info-container">
                    <h1 id="artistName">Loading...</h1>
                    <p id="artistAka" class="release-artist" hidden></p>
                    <dl id="artistStatsTable" class="release-stats-table" hidden></dl>
                </div>
                <nav id="artistLinkPills" class="release-link-pills"></nav>
            </header>

            <div class="stats-row">
                <section class="pulse-section" id="pulseSection" hidden>
                    <h2>Timeline</h2>
                    <div class="pulse-rows" id="pulseRows"></div>
                </section>
                <section id="recentPlaysSection" hidden>
                    <h2>Recent Plays</h2>
                    <div class="recent-plays-list" id="recentPlaysList"></div>
                </section>
            </div>

            <section class="disc-section">
                <div class="section-header">
                    <h2>Discography</h2>
                    <div class="sort-controls">
                        <span class="disc-sort-label">Sort by</span>
                        <button class="sort-btn${_discSort === 'date' ? ' active' : ''}" data-disc-sort="date">Release Date</button>
                        <button class="sort-btn${_discSort === 'listens' ? ' active' : ''}" data-disc-sort="listens">Listens</button>
                    </div>
                </div>
                <div id="discographyContainer">
                    <div class="loading">Loading discography…</div>
                </div>
            </section>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        loadArtistInfo();
        loadArtistBadges();
        loadDiscography();
        loadListeningHistory();
        loadRecentPlays();
        setupDiscSort();
    }

    function unmount() {
        if (_currentChart) {
            _currentChart.destroy();
            _currentChart = null;
        }
        if (_themeObserver) {
            _themeObserver.disconnect();
            _themeObserver = null;
        }
    }

    function loadArtistInfo() {
        const safeId = _artistId.replace(/'/g, "''");

        const result = _db.exec(`
            SELECT
                a.name,
                COALESCE(a.image_thumb_url, a.image_url) AS image_url,
                a.image_url AS image_full_url,
                a.hero_image_url,
                COUNT(DISTINCT CASE WHEN t.hidden = 0 THEN t.id END) as unique_tracks,
                COUNT(CASE WHEN t.hidden = 0 THEN l.id END) as total_plays,
                COUNT(DISTINCT CASE WHEN t.hidden = 0 THEN t.release_id END) as total_releases,
                a.spotify_id,
                a.mbid,
                a.aoty_id,
                a.aoty_url
            FROM artists a
            LEFT JOIN track_artists ta ON a.id = ta.artist_id AND ta.role = 'main'
            LEFT JOIN tracks t ON ta.track_id = t.id
            LEFT JOIN listens l ON t.id = l.track_id
            WHERE a.id = '${safeId}' AND (a.hidden IS NULL OR a.hidden = 0)
            GROUP BY a.id
        `)[0];

        if (!result || result.values.length === 0) {
            const el = document.getElementById('artistName');
            if (el) el.textContent = 'Artist not found';
            return;
        }

        const [name, imageUrl, imageFullUrl, heroImageUrl, uniqueTracks, totalPlays, totalReleases,
               spotifyId, mbid, aotyId, aotyUrl] = result.values[0];

        const extLinks = new Map();
        try {
            const linksResult = _db.exec(`
                SELECT service, link_value
                FROM external_links
                WHERE entity_type = 0 AND entity_id = '${safeId}'
            `)[0];
            if (linksResult) linksResult.values.forEach(([svc, val]) => extLinks.set(svc, val));
        } catch (_) {}
        const wikiPageId = extLinks.get(0) || null;

        // Aliases
        const aliasResult = _db.exec(`
            SELECT alias, alias_type, language FROM artist_aliases
            WHERE artist_id = '${safeId}'
            ORDER BY sort_order, alias_type
        `)[0];
        const aliases = aliasResult ? aliasResult.values : [];
        const nativeScript    = aliases.find(([, t]) => t === 'native_script');
        const transliteration = aliases.find(([, t, l]) => t === 'transliteration' && l === 'en');
        const pastNames       = aliases.filter(([, t]) => t === 'past_name').map(([a]) => a);

        const nameEl = document.getElementById('artistName');
        const isNonLatin = s => /[^ -]/.test(s);
        if (nativeScript) {
            nameEl.innerHTML = `${escapeHtml(nativeScript[0])} <span class="artist-romanized">(${escapeHtml(name)})</span>`;
        } else if (isNonLatin(name) && transliteration) {
            nameEl.innerHTML = `${escapeHtml(name)} <span class="artist-romanized">(${escapeHtml(transliteration[0])})</span>`;
        } else {
            nameEl.textContent = name;
        }

        // Past names go into the stats table (plain text row), not prominent display
        // akaEl left hidden intentionally

        document.title = `aswin.db/music - ${name}`;

        // First/last listen
        const timeResult = _db.exec(`
            SELECT MIN(l.timestamp), MAX(l.timestamp)
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
            WHERE ta.artist_id = '${safeId}' AND t.hidden = 0
        `)[0];
        const [firstTs, lastTs] = timeResult?.values[0] ?? [null, null];

        // ── Stats table ───────────────────────────────────────────────────────
        const statsEl = document.getElementById('artistStatsTable');
        if (statsEl) {
            const fmtTs = ts => {
                if (!ts) return null;
                return new Date(ts * 1000).toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
            };
            const rows = [];
            if (totalPlays > 0)    rows.push(['Plays',       formatNumber(totalPlays)]);
            if (uniqueTracks > 0)  rows.push(['Tracks',      formatNumber(uniqueTracks)]);
            if (totalReleases > 0) rows.push(['Releases',    formatNumber(totalReleases)]);
            if (firstTs)           rows.push(['First heard', fmtTs(firstTs)]);
            if (lastTs)            rows.push(['Last played', formatRelativeTime(lastTs)]);

            const makeCell = ([lbl, val]) =>
                `<div class="rst-row"><dt class="rst-label">${lbl}</dt><dd class="rst-value">${val}</dd></div>`;
            let html = '';
            for (let i = 0; i < rows.length; i += 2)
                html += `<div class="rst-pair">${makeCell(rows[i])}${rows[i+1] ? makeCell(rows[i+1]) : ''}</div>`;
            statsEl.innerHTML = html;
            statsEl.removeAttribute('hidden');
        }

        // ── Photo ─────────────────────────────────────────────────────────────
        if (imageUrl) {
            const photoEl = document.getElementById('artistPhoto');
            photoEl.innerHTML = `<img src="${imageUrl}" alt="${escapeHtml(name)}">`;
            photoEl.classList.add('has-art');
            photoEl.addEventListener('click', () => _openArtModal(imageFullUrl || imageUrl));
        }

        if (HERO_ENABLED && heroImageUrl) {
            const heroEl = document.getElementById('artistHero');
            if (heroEl) {
                heroEl.removeAttribute('hidden');
                heroEl.innerHTML = `<img class="artist-hero-img" src="${heroImageUrl}" alt="">`;
                document.getElementById('artistHeader').classList.add('has-hero');
            }
        }

        // ── Members / Member Of → stats table rows ────────────────────────────
        const _appendFullRow = (label, html) => {
            const tbl = document.getElementById('artistStatsTable');
            if (!tbl) return;
            const row = document.createElement('div');
            row.className = 'rst-row rst-genres-row';
            row.innerHTML = `<dt class="rst-label">${label}</dt><dd class="rst-value">${html}</dd>`;
            tbl.appendChild(row);
            tbl.removeAttribute('hidden');
        };

        // ── Also known as → stats table (plain comma-separated, above Members) ──
        if (pastNames.length > 0) {
            _appendFullRow('Also known as', escapeHtml(pastNames.join(', ')));
        }

        const membersResult = _db.exec(`
            SELECT a.id, a.name FROM artist_members am
            JOIN artists a ON a.id = am.member_artist_id
            WHERE am.group_artist_id = '${safeId}'
            ORDER BY am.sort_order, a.name
        `)[0];
        if (membersResult?.values.length) {
            _appendFullRow('Members',
                membersResult.values.map(([mid, mname]) =>
                    `<a href="?view=artist&id=${encodeURIComponent(mid)}" class="stat-genre-tag is-primary">${escapeHtml(mname)}</a>`
                ).join(''));
        }

        const memberOfResult = _db.exec(`
            SELECT a.id, a.name FROM artist_members am
            JOIN artists a ON a.id = am.group_artist_id
            WHERE am.member_artist_id = '${safeId}'
            ORDER BY a.name
        `)[0];
        if (memberOfResult?.values.length) {
            _appendFullRow('Member of',
                memberOfResult.values.map(([gid, gname]) =>
                    `<a href="?view=artist&id=${encodeURIComponent(gid)}" class="stat-genre-tag is-primary">${escapeHtml(gname)}</a>`
                ).join(''));
        }

        // ── Genres → stats table ──────────────────────────────────────────────
        const genreResult = _db.exec(`
            SELECT g.aoty_id, g.name, COUNT(DISTINCT rg.release_id) as freq
            FROM release_genres rg
            JOIN genres g ON rg.aoty_genre_id = g.aoty_id
            WHERE rg.release_id IN (
                SELECT DISTINCT t.release_id
                FROM track_artists ta
                JOIN tracks t ON ta.track_id = t.id
                JOIN releases r ON r.id = t.release_id AND r.hidden = 0
                WHERE ta.artist_id = '${safeId}' AND ta.role = 'main' AND t.hidden = 0
            )
            GROUP BY g.aoty_id
            ORDER BY freq DESC
            LIMIT 8
        `)[0];
        if (genreResult?.values.length) {
            _appendFullRow('Genre',
                genreResult.values.map(([gid, gname]) =>
                    `<a href="?view=genre&id=${encodeURIComponent(gid)}" class="stat-genre-tag is-primary">${escapeHtml(gname)}</a>`
                ).join(''));
        }

        // ── Link pills ────────────────────────────────────────────────────────
        const SVG_EXT = `<svg class="pill-ext" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>`;
        const pill = (svc, href, pname, sub) => {
            const icon = svc === 'aoty'
                ? `<img src="images/links/aoty-icon.png" class="pill-aoty-img">`
                : `<span class="pill-mask"></span>`;
            return `<a href="${href}" target="_blank" rel="noopener" class="release-link-pill pill-${svc}">` +
                `<span class="pill-icon">${icon}</span>` +
                `<span class="pill-text"><span class="pill-service-name">${pname}</span>` +
                (sub ? `<span class="pill-sub">${sub}</span>` : '') +
                `</span>${SVG_EXT}</a>`;
        };

        const pillsEl = document.getElementById('artistLinkPills');
        if (pillsEl) {
            let phtml = '';
            if (spotifyId)          phtml += pill('spotify',     `https://open.spotify.com/artist/${spotifyId}`, 'Spotify');
            if (extLinks.get(4))    phtml += pill('deezer',      `https://www.deezer.com/artist/${extLinks.get(4)}`, 'Deezer');
            if (extLinks.get(5))    phtml += pill('tidal',       `https://tidal.com/browse/artist/${extLinks.get(5)}`, 'Tidal');
            if (extLinks.get(7))    phtml += pill('beatport',    `https://www.beatport.com/artist/-/${extLinks.get(7)}`, 'Beatport');
            if (extLinks.get(6))    phtml += pill('bandcamp',    extLinks.get(6), 'Bandcamp');
            if (mbid)               phtml += pill('musicbrainz', `https://musicbrainz.org/artist/${mbid}`, 'MusicBrainz');
            if (wikiPageId)         phtml += pill('wikipedia',   `https://en.wikipedia.org/wiki/?curid=${wikiPageId}`, 'Wikipedia');
            const resolvedAotyUrl = aotyUrl || (aotyId ? `https://www.albumoftheyear.org/artist/${aotyId}/` : null);
            if (resolvedAotyUrl)    phtml += pill('aoty',        resolvedAotyUrl, 'AOTY');
            if (extLinks.get(8))    phtml += pill('genius',      `https://genius.com/artists/${extLinks.get(8)}`, 'Genius');
            if (phtml) pillsEl.innerHTML = phtml;
            else pillsEl.style.display = 'none';
        }
    }

    function loadDiscography() {
        const safeId = _artistId.replace(/'/g, "''");

        const ownResult = _db.exec(`
            SELECT
                r.id,
                r.title,
                r.release_date,
                r.type,
                r.type_secondary,
                COALESCE(r.album_art_thumb_url, r.album_art_url) as album_art_url,
                (SELECT COUNT(*) FROM tracks t WHERE t.release_id = r.id AND t.hidden = 0
                 AND t.variant_section IS NULL
                 AND (t.duration_ms IS NULL OR t.duration_ms >= 30000)) as total_tracks,
                (SELECT COUNT(DISTINCT t.id) FROM tracks t
                 WHERE t.release_id = r.id AND t.hidden = 0
                 AND t.variant_section IS NULL
                 AND (t.duration_ms IS NULL OR t.duration_ms >= 30000)
                 AND EXISTS (SELECT 1 FROM listens l WHERE l.track_id = t.id)) as listened_tracks,
                (SELECT COUNT(*) FROM tracks t JOIN listens l ON t.id = l.track_id
                 WHERE t.release_id = r.id AND t.hidden = 0
                 AND t.variant_section IS NULL) as total_listens
            FROM releases r
            WHERE r.primary_artist_id = '${safeId}'
            AND r.hidden = 0        `)[0];

        const collabResult = _db.exec(`
            SELECT
                r.id,
                r.title,
                r.release_date,
                r.type,
                r.type_secondary,
                COALESCE(r.album_art_thumb_url, r.album_art_url) as album_art_url,
                (SELECT COUNT(*) FROM tracks t WHERE t.release_id = r.id AND t.hidden = 0
                 AND t.variant_section IS NULL
                 AND (t.duration_ms IS NULL OR t.duration_ms >= 30000)) as total_tracks,
                (SELECT COUNT(DISTINCT t.id) FROM tracks t
                 WHERE t.release_id = r.id AND t.hidden = 0
                 AND t.variant_section IS NULL
                 AND (t.duration_ms IS NULL OR t.duration_ms >= 30000)
                 AND EXISTS (SELECT 1 FROM listens l WHERE l.track_id = t.id)) as listened_tracks,
                (SELECT COUNT(*) FROM tracks t JOIN listens l ON t.id = l.track_id
                 WHERE t.release_id = r.id AND t.hidden = 0
                 AND t.variant_section IS NULL) as total_listens,
                (SELECT a2.name FROM artists a2 WHERE a2.id = r.primary_artist_id) as primary_artist_name
            FROM releases r
            JOIN release_artists ra ON ra.release_id = r.id
            WHERE ra.artist_id = '${safeId}' AND ra.role = 'main'
            AND r.primary_artist_id != '${safeId}'
            AND r.hidden = 0        `)[0];

        _discData.own = ownResult ? ownResult.values : [];
        _discData.collabs = collabResult ? collabResult.values : [];

        renderDiscography();
    }

    const _DISC_GROUPS = [
        { key: 'album',       label: 'Albums',              test: r => r.type === 'album' && !r.typeSecondary },
        { key: 'ep',          label: 'EPs',                 test: r => r.type === 'ep' },
        { key: 'single',      label: 'Singles',             test: r => r.type === 'single' },
        { key: 'compilation', label: 'Compilations',        test: r => r.typeSecondary === 'compilation' },
        { key: 'soundtrack',  label: 'Soundtracks',         test: r => r.typeSecondary === 'soundtrack' },
        { key: 'live',        label: 'Live',                test: r => r.typeSecondary === 'live' },
        { key: 'remix',       label: 'Remixes & DJ-Mixes',  test: r => r.typeSecondary === 'remix' || r.typeSecondary === 'dj-mix' },
        { key: 'mixtape',     label: 'Mixtapes',            test: r => r.typeSecondary === 'mixtape' },
        { key: 'other',       label: 'Other',               test: () => true },
    ];

    function _discDonutColor(pct) {
        if (pct <= 0)   return 'var(--border)';
        if (pct < 0.5)  return '#3b82f6';
        if (pct < 0.75) return '#f59e0b';
        if (pct < 1.0)  return '#f97316';
        return '#22c55e';
    }

    function _makeDiscCard(row, collab) {
        const [id, title, releaseDate, type, typeSecondary, albumArtUrl,
               totalTracks, listenedTracks, totalListens, primaryArtistName] = row;

        const pct   = totalTracks > 0 ? listenedTracks / totalTracks : 0;
        const pctInt = Math.round(pct * 100);
        const color = _discDonutColor(pct);

        let subParts = [];
        const year = releaseDate ? releaseDate.slice(0, 4) : null;
        if (year) subParts.push(year);
        if (collab && primaryArtistName) subParts.push(escapeHtml(primaryArtistName));
        else if (totalListens > 0) subParts.push(`${formatNumber(totalListens)} plays`);

        const card = document.createElement('a');
        card.className = 'disc-card' + (totalListens === 0 ? ' unplayed' : '');
        card.href = `?view=release&id=${encodeURIComponent(id)}`;

        const imgSrc = albumArtUrl || getFallbackImageUrl();
        const tooltipText = totalTracks > 0 ? `${listenedTracks} / ${totalTracks} tracks` : '';

        card.innerHTML = `
            <div class="disc-card-img" style="background-image: url('${imgSrc}')"></div>
            <div class="disc-card-meta">
                <div class="disc-card-info">
                    <div class="disc-card-title">${escapeHtml(title)}</div>
                    <div class="disc-card-sub">${subParts.join(' · ')}</div>
                </div>
                ${totalTracks > 0 ? `<div class="donut-wrap" style="--p:${pctInt};--c:${color}" data-tooltip="${tooltipText}"><div class="donut"></div></div>` : ''}
            </div>
        `;
        return card;
    }

    function _renderDiscGroup(container, label, rows, collab) {
        if (!rows || rows.length === 0) return;

        const group = document.createElement('div');
        group.className = 'disc-group';

        const h3 = document.createElement('h3');
        h3.textContent = label;
        group.appendChild(h3);

        const grid = document.createElement('div');
        grid.className = 'disc-grid';
        rows.forEach(row => grid.appendChild(_makeDiscCard(row, collab)));
        group.appendChild(grid);

        container.appendChild(group);
    }

    function renderDiscography() {
        const container = document.getElementById('discographyContainer');
        if (!container) return;
        container.innerHTML = '';

        const sortFn = _discSort === 'listens'
            ? (a, b) => b[8] - a[8]
            : (a, b) => (b[2] || '').localeCompare(a[2] || '');

        const own = [...(_discData.own || [])].sort(sortFn);
        const collabs = [...(_discData.collabs || [])].sort(sortFn);

        if (own.length === 0 && collabs.length === 0) {
            container.innerHTML = '<div class="loading">No releases found</div>';
            return;
        }

        // Assign each release to the first matching group
        const groupBuckets = _DISC_GROUPS.map(() => []);
        own.forEach(row => {
            const obj = { type: row[3], typeSecondary: row[4] };
            const idx = _DISC_GROUPS.findIndex(g => g.test(obj));
            if (idx >= 0) groupBuckets[idx].push(row);
        });

        _DISC_GROUPS.forEach(({ label }, i) => {
            _renderDiscGroup(container, label, groupBuckets[i], false);
        });

        _renderDiscGroup(container, 'Collaborations', collabs, true);
    }

    function setupDiscSort() {
        setupToggleGroup('[data-disc-sort]', btn => {
            _discSort = btn.dataset.discSort;
            renderDiscography();
        });
    }

    function loadRecentPlays() {
        const safeId = _artistId.replace(/'/g, "''");
        const result = _db.exec(`
            SELECT
                t.title,
                COALESCE(r.album_art_thumb_url, r.album_art_url) as album_art_url,
                r.title as release_title,
                l.timestamp
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            LEFT JOIN releases r ON t.release_id = r.id
            WHERE t.id IN (
                SELECT DISTINCT track_id FROM track_artists
                WHERE artist_id = '${safeId}' AND role = 'main'
            )
            AND t.hidden = 0
            ORDER BY l.timestamp DESC
            LIMIT 10
        `)[0];

        const section = document.getElementById('recentPlaysSection');
        const list = document.getElementById('recentPlaysList');
        if (!section || !list || !result || result.values.length === 0) return;

        const now = Date.now() / 1000;
        list.innerHTML = result.values.map(([trackTitle, albumArtUrl, releaseTitle, timestamp]) => {
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
            const subtitle = releaseTitle
                ? `<i data-lucide="disc-album" style="width: 12px; height: 12px;"></i> ${escapeHtml(releaseTitle)}`
                : null;
            return `
                <div class="recent-play-row">
                    <div class="recent-play-thumb" style="background-image: url('${imgSrc}')"></div>
                    <div class="recent-play-info">
                        <div class="recent-play-name">${escapeHtml(trackTitle)}</div>
                        ${subtitle ? `<div class="recent-play-album">${subtitle}</div>` : ''}
                    </div>
                    <span class="recent-play-date">${dateStr}</span>
                </div>
            `;
        }).join('');

        section.removeAttribute('hidden');
    }

    function loadArtistBadges() {
        const safeId = _artistId.replace(/'/g, "''");
        const badgesEl = document.getElementById('artistBadges');
        if (!badgesEl) return;

        const playsResult = _db.exec(`
            SELECT COUNT(*) FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
            WHERE ta.artist_id = '${safeId}' AND t.hidden = 0
        `)[0];
        const totalPlays = playsResult ? playsResult.values[0][0] : 0;

        let certTier = null;
        if (totalPlays >= 1000)     certTier = 'diamond';
        else if (totalPlays >= 500) certTier = 'platinum';
        else if (totalPlays >= 250) certTier = 'gold';

        const certLabels = { gold: 'Gold — 250+ plays', platinum: 'Platinum — 500+ plays', diamond: 'Diamond — 1,000+ plays' };
        if (certTier) badgesEl.innerHTML = `<span class="badge-cert badge-cert-${certTier}" title="${certLabels[certTier]}">${certTier}</span>`;

        // Peak years → compact stats row instead of badge tower.
        // Two-step ranking: first find which years this artist has plays,
        // then rank only across those years — avoids a full cross-artist scan.
        const medalResult = _db.exec(`
            WITH artist_years AS (
                SELECT l.year
                FROM listens l
                JOIN tracks t ON l.track_id = t.id
                JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
                WHERE ta.artist_id = '${safeId}' AND t.hidden = 0
                GROUP BY l.year
            ),
            year_plays AS (
                SELECT ta.artist_id, l.year, COUNT(*) as plays
                FROM listens l
                JOIN tracks t ON l.track_id = t.id
                JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
                WHERE t.hidden = 0 AND l.year IN (SELECT year FROM artist_years)
                GROUP BY ta.artist_id, l.year
            ),
            ranked AS (
                SELECT artist_id, year,
                    RANK() OVER (PARTITION BY year ORDER BY plays DESC) as rnk
                FROM year_plays
            )
            SELECT year, rnk FROM ranked
            WHERE artist_id = '${safeId}' AND rnk <= 3
            ORDER BY rnk, year ASC
        `)[0];

        if (medalResult?.values.length) {
            const tierClass = { 1: 'gold', 2: 'silver', 3: 'bronze' };
            // Sort by year ascending so pills read chronologically
            const pills = [...medalResult.values]
                .sort(([ya], [yb]) => ya - yb)
                .map(([year, rnk]) => {
                    const yy = `'${String(year).slice(2)}`;
                    return `<span class="peak-year-pill peak-year-${tierClass[rnk]}" title="#${rnk} in ${year}">${yy}</span>`;
                }).join('');

            const statsEl = document.getElementById('artistStatsTable');
            if (statsEl) {
                const row = document.createElement('div');
                row.className = 'rst-pair';
                row.innerHTML = `<div class="rst-row" style="grid-column:1/-1;border-right:none">` +
                    `<dt class="rst-label">Peak years</dt>` +
                    `<dd class="rst-value" style="display:flex;flex-wrap:wrap;gap:0.2rem;justify-content:flex-end;font-weight:normal">${pills}</dd></div>`;
                // Insert before Members/Genre rows (before first rst-genres-row, or at end)
                const firstFull = statsEl.querySelector('.rst-genres-row');
                if (firstFull) statsEl.insertBefore(row, firstFull);
                else statsEl.appendChild(row);
                statsEl.removeAttribute('hidden');
            }
        }
    }

    function renderPulse(yearlyValues) {
        const pulseEl = document.getElementById('pulseSection');
        const rowsEl = document.getElementById('pulseRows');
        if (!pulseEl || !rowsEl || !yearlyValues || yearlyValues.length === 0) return;

        const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        const max = Math.max(...yearlyValues.map(([, count]) => count));

        const monthlyByYear = new Map();
        if (_chartData.monthlyRaw) {
            _chartData.monthlyRaw.forEach(([year, month, count]) => {
                if (!monthlyByYear.has(year)) monthlyByYear.set(year, new Map());
                monthlyByYear.get(year).set(month, count);
            });
        }

        rowsEl.innerHTML = yearlyValues.map(([year, count]) => {
            const pct = Math.round((count / max) * 100);
            return `
                <div class="pulse-row" data-year="${year}">
                    <span class="pulse-year">${year}</span>
                    <span class="pulse-count">${formatNumber(count)}</span>
                    <div class="pulse-bar-track">
                        <div class="pulse-bar-fill" style="width: ${pct}%"></div>
                    </div>
                    <span class="pulse-chevron">▶</span>
                </div>
                <div class="pulse-monthly" id="pulse-monthly-${year}" style="display:none"></div>
            `;
        }).join('');

        rowsEl.addEventListener('click', e => {
            const row = e.target.closest('.pulse-row');
            if (!row) return;
            const year = parseInt(row.dataset.year);
            const monthlyEl = document.getElementById(`pulse-monthly-${year}`);
            if (!monthlyEl) return;

            const isExpanded = row.classList.contains('expanded');
            if (isExpanded) {
                monthlyEl.style.display = 'none';
                row.classList.remove('expanded');
                return;
            }

            if (!monthlyEl.innerHTML) {
                const monthMap = monthlyByYear.get(year) || new Map();
                const monthMax = Math.max(...[...monthMap.values()], 1);
                monthlyEl.innerHTML = Array.from({ length: 12 }, (_, i) => {
                    const m = i + 1;
                    const c = monthMap.get(m) || 0;
                    const p = Math.round((c / monthMax) * 100);
                    return `
                        <div class="pulse-month-row">
                            <span class="pulse-month-name">${monthNames[i]}</span>
                            <span class="pulse-month-count">${c > 0 ? formatNumber(c) : ''}</span>
                            <div class="pulse-month-bar-track">
                                <div class="pulse-month-bar-fill" style="width: ${p}%"></div>
                            </div>
                        </div>
                    `;
                }).join('');
            }

            monthlyEl.style.display = '';
            row.classList.add('expanded');
        });

        pulseEl.removeAttribute('hidden');
    }

    function loadListeningHistory() {
        const safeId = _artistId.replace(/'/g, "''");

        const monthlyResult = _db.exec(`
            SELECT l.year, l.month, COUNT(*) as listen_count
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
            WHERE ta.artist_id = '${safeId}' AND t.hidden = 0
            GROUP BY l.year, l.month
            ORDER BY l.year, l.month
        `)[0];

        const yearlyResult = _db.exec(`
            SELECT l.year, COUNT(*) as listen_count
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
            WHERE ta.artist_id = '${safeId}' AND t.hidden = 0
            GROUP BY l.year
            ORDER BY l.year
        `)[0];

        if ((!monthlyResult || monthlyResult.values.length === 0) &&
            (!yearlyResult || yearlyResult.values.length === 0)) {
            return;
        }

        if (monthlyResult && monthlyResult.values.length > 0) {
            _chartData.monthly    = buildMonthlyChartData(monthlyResult.values);
            _chartData.monthlyRaw = monthlyResult.values;
        }

        if (yearlyResult && yearlyResult.values.length > 0) {
            _chartData.yearly = buildYearlyChartData(yearlyResult.values);
            renderPulse(yearlyResult.values);
        }
    }

    function buildMonthlyChartData(values) {
        const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const years = values.map(([year]) => year);
        const minYear = Math.min(...years);
        const maxYear = Math.max(...years);
        const dataMap = new Map();
        values.forEach(([year, month, count]) => dataMap.set(`${year}-${month}`, count));
        const labels = [], data = [];
        for (let year = minYear; year <= maxYear; year++) {
            for (let month = 1; month <= 12; month++) {
                labels.push(`${monthNames[month - 1]} ${year}`);
                data.push(dataMap.get(`${year}-${month}`) || 0);
            }
        }
        return { labels, data };
    }

    function buildYearlyChartData(values) {
        const years = values.map(([year]) => year);
        const minYear = Math.min(...years);
        const maxYear = Math.max(...years);
        const dataMap = new Map();
        values.forEach(([year, count]) => dataMap.set(year, count));
        const labels = [], data = [];
        for (let year = minYear; year <= maxYear; year++) {
            labels.push(year.toString());
            data.push(dataMap.get(year) || 0);
        }
        return { labels, data };
    }

    return { mount, unmount };

    function _openArtModal(url) {
        const existing = document.getElementById('artModal');
        if (existing) existing.remove();
        const modal = document.createElement('div');
        modal.id = 'artModal';
        modal.className = 'art-modal';
        modal.innerHTML = `<div class="art-modal-inner"><img src="${url}" alt=""></div>`;
        document.body.appendChild(modal);
        const close = () => modal.remove();
        modal.addEventListener('click', close);
        document.addEventListener('keydown', function onKey(e) {
            if (e.key === 'Escape') { close(); document.removeEventListener('keydown', onKey); }
        });
    }
})();
