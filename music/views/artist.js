const ViewArtist = (() => {
    let _db = null;
    let _artistId = null;
    let _currentChart = null;
    let _chartData = { monthly: null, yearly: null, monthlyRaw: null };
    let _chartState = { granularity: 'monthly', type: 'distribution' };
    // 'date' | 'listens'
    let _discSort = 'date';
    // 'grid' | 'list'
    let _discView = localStorage.getItem('artistDiscView') || 'grid';
    let _discData = { own: null, collabs: null };
    let _themeObserver = null;
    let _artistName = null;
    let _hasListens = false;

    const CHART_ENABLED = false;
    // Experimental: small release-art markers on the Timeline's year rows.
    // Flagged off by default — flip to true to preview, revert if it doesn't
    // earn its keep.
    const TIMELINE_RELEASE_MARKERS = false;

    function mount(container, db, params) {
        _db = db;
        _currentChart = null;
        _chartData = { monthly: null, yearly: null, monthlyRaw: null };
        _discData = { own: null, collabs: null };
        _artistName = null;
        _hasListens = false;

        // Accept either a slug or a raw ULID in either param — resolve to the
        // canonical id up front so every query below (all keyed on the real
        // id) keeps working unchanged, then canonicalize the address bar to
        // the slug-based URL for clean copy-paste.
        const key = params.slug || params.id;
        if (!key) {
            navigate({ view: 'home' });
            return;
        }
        const safeKey = String(key).replace(/'/g, "''");
        const resolved = db.exec(`SELECT id, slug FROM artists WHERE slug = '${safeKey}' OR id = '${safeKey}' LIMIT 1`)[0];
        if (resolved) {
            const [realId, slug] = resolved.values[0];
            _artistId = realId;
            if (slug && params.id && !params.slug) {
                history.replaceState({ view: 'artist', slug }, '', `?view=artist&slug=${encodeURIComponent(slug)}`);
            }
        } else {
            // No match — keep the original key so downstream queries fail the
            // same way they always did, surfacing "Artist not found".
            _artistId = key;
        }

        container.innerHTML = `
            <nav class="genre-breadcrumb" id="artistBreadcrumb">
                <a href="?" class="bc-home"><i data-lucide="home"></i></a>
                <i data-lucide="chevron-right" class="bc-sep"></i>
                <span class="bc-current" id="artistBreadcrumbName">Loading…</span>
                <a class="artist-compare-link" id="artistCompareLink" hidden>
                    <i data-lucide="git-compare"></i> Compare
                </a>
            </nav>

            <div id="artistHero" class="artist-hero" hidden></div>

            <header id="artistHeader" class="entity-header entity-header-grid">
                <div class="artist-photo-container">
                    <div class="artist-photo" id="artistPhoto">
                        <svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
                            <circle cx="50" cy="50" r="50" fill="#20232c"/>
                            <text x="50" y="60" text-anchor="middle" font-size="40" fill="#767c85">♪</text>
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
                    <div class="sort-controls">
                        <button class="sort-btn${_discView === 'grid' ? ' active' : ''}" data-disc-view="grid" title="Art view" aria-label="Art view"><i data-lucide="layout-grid"></i></button>
                        <button class="sort-btn${_discView === 'list' ? ' active' : ''}" data-disc-view="list" title="List view" aria-label="List view"><i data-lucide="list"></i></button>
                    </div>
                </div>
                <div id="discographyContainer">
                    ${renderLoading("Loading discography…")}
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
                a.stat_unique_tracks,
                a.stat_total_plays,
                a.stat_total_releases,
                a.spotify_id,
                a.mbid,
                a.aoty_id,
                a.aoty_url,
                a.stat_first_listen_ts,
                a.stat_last_listen_ts,
                a.stat_drift_days
            FROM artists a
            WHERE a.id = '${safeId}' AND (a.hidden IS NULL OR a.hidden = 0)
        `)[0];

        if (!result || result.values.length === 0) {
            const el = document.getElementById('artistName');
            if (el) el.textContent = 'Artist not found';
            return;
        }

        const [name, imageUrl, imageFullUrl, uniqueTracksRaw, totalPlaysRaw, totalReleasesRaw,
               spotifyId, mbid, aotyId, aotyUrl, firstTs, lastTs, driftDays] = result.values[0];
        const uniqueTracks   = uniqueTracksRaw || 0;
        const totalPlays     = totalPlaysRaw || 0;
        const totalReleases  = totalReleasesRaw || 0;


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

        _artistName = name;
        _hasListens = totalPlays > 0;

        const breadcrumbNameEl = document.getElementById('artistBreadcrumbName');
        if (breadcrumbNameEl) breadcrumbNameEl.textContent = name;

        const compareLinkEl = document.getElementById('artistCompareLink');
        if (compareLinkEl) {
            compareLinkEl.href = `?view=compare&a=${encodeURIComponent(_artistId)}`;
            compareLinkEl.removeAttribute('hidden');
        }

        const nameEl = document.getElementById('artistName');
        const isNonLatin = s => /[^ -]/.test(s);
        // Primary name first, native/transliterated form in parens — matches
        // release.js's title rendering (e.g. "Disgaea 7: Vows of the
        // Virtueless (魔界戦記ディスガイア7)"), rather than leading with the
        // native script.
        const secondaryLabel = nativeScript ? nativeScript[0] : (isNonLatin(name) && transliteration ? transliteration[0] : null);
        if (secondaryLabel) {
            nameEl.innerHTML = `${escapeHtml(name)} <span class="artist-romanized">(${escapeHtml(secondaryLabel)})</span>`;
        } else {
            nameEl.textContent = name;
        }

        // Past names go in the stats table rather than the header.
        setPageTitle(name);

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
            if (driftDays !== null && totalPlays >= 3) rows.push(['Drift', driftDays < 1
                ? 'Same-day repeats'
                : `${driftDays.toFixed(1)} days between plays`]);

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
            SELECT a.id, a.name, a.slug FROM artist_members am
            JOIN artists a ON a.id = am.member_artist_id
            WHERE am.group_artist_id = '${safeId}'
            ORDER BY am.sort_order, a.name
        `)[0];
        if (membersResult?.values.length) {
            _appendFullRow('Members',
                membersResult.values.map(([mid, mname, mslug]) =>
                    `<a href="${artistHref(mid, mslug)}" class="stat-genre-tag is-primary">${escapeHtml(mname)}</a>`
                ).join(''));
        }

        const memberOfResult = _db.exec(`
            SELECT a.id, a.name, a.slug FROM artist_members am
            JOIN artists a ON a.id = am.group_artist_id
            WHERE am.member_artist_id = '${safeId}'
            ORDER BY a.name
        `)[0];
        if (memberOfResult?.values.length) {
            _appendFullRow('Member of',
                memberOfResult.values.map(([gid, gname, gslug]) =>
                    `<a href="${artistHref(gid, gslug)}" class="stat-genre-tag is-primary">${escapeHtml(gname)}</a>`
                ).join(''));
        }

        // ── Collaborators → stats table (either direction, deduped) ────────────
        const collabResult = _db.exec(`
            SELECT DISTINCT a.id, a.name, a.slug
            FROM artist_relations ar
            JOIN artists a ON a.id = (
                CASE WHEN ar.from_artist_id = '${safeId}' THEN ar.to_artist_id ELSE ar.from_artist_id END
            )
            WHERE ar.relation_type = 'collaboration'
              AND ('${safeId}' IN (ar.from_artist_id, ar.to_artist_id))
              AND a.hidden = 0
            ORDER BY a.name
        `)[0];
        if (collabResult?.values.length) {
            _appendFullRow('Collaborators',
                collabResult.values.map(([cid, cname, cslug]) =>
                    `<a href="${artistHref(cid, cslug)}" class="stat-genre-tag is-primary">${escapeHtml(cname)}</a>`
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
                WHERE ta.artist_id = '${safeId}' AND ta.role IN (${PRIMARY_ROLES_SQL}) AND t.hidden = 0
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
        const pillsEl = document.getElementById('artistLinkPills');
        if (pillsEl) {
            let phtml = '';
            if (spotifyId)          phtml += renderLinkPill('spotify',     `https://open.spotify.com/artist/${spotifyId}`, 'Spotify');
            if (extLinks.get(4))    phtml += renderLinkPill('deezer',      `https://www.deezer.com/artist/${extLinks.get(4)}`, 'Deezer');
            if (extLinks.get(5))    phtml += renderLinkPill('tidal',       `https://tidal.com/browse/artist/${extLinks.get(5)}`, 'Tidal');
            if (extLinks.get(7))    phtml += renderLinkPill('beatport',    `https://www.beatport.com/artist/-/${extLinks.get(7)}`, 'Beatport');
            if (extLinks.get(13))   phtml += renderLinkPill('traxsource',  extLinks.get(13), 'Traxsource');
            if (extLinks.get(6))    phtml += renderLinkPill('bandcamp',    extLinks.get(6), 'Bandcamp');
            if (mbid)               phtml += renderLinkPill('musicbrainz', `https://musicbrainz.org/artist/${mbid}`, 'MusicBrainz');
            if (wikiPageId)         phtml += renderLinkPill('wikipedia',   wikipediaHref(wikiPageId), 'Wikipedia');
            const resolvedAotyUrl = aotyUrl || (aotyId ? `https://www.albumoftheyear.org/artist/${aotyId}/` : null);
            if (resolvedAotyUrl)    phtml += renderLinkPill('aoty',        resolvedAotyUrl, 'AOTY');
            if (extLinks.get(8))    phtml += renderLinkPill('genius',      `https://genius.com/artists/${extLinks.get(8)}`, 'Genius');
            if (extLinks.get(11))   phtml += renderLinkPill('rym',             extLinks.get(11), 'RateYourMusic');
            if (extLinks.get(12))   phtml += renderLinkPill('residentadvisor', extLinks.get(12), 'Resident Advisor');
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
                (SELECT COUNT(*) FROM tracks t JOIN listens l ON t.id = l.track_id
                 WHERE t.release_id = r.id AND t.hidden = 0
                 AND t.variant_section IS NULL) as total_listens,
                NULL as primary_artist_name,
                r.slug
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
                (SELECT COUNT(*) FROM tracks t JOIN listens l ON t.id = l.track_id
                 WHERE t.release_id = r.id AND t.hidden = 0
                 AND t.variant_section IS NULL) as total_listens,
                (SELECT a2.name FROM artists a2 WHERE a2.id = r.primary_artist_id) as primary_artist_name,
                r.slug
            FROM releases r
            JOIN release_artists ra ON ra.release_id = r.id
            WHERE ra.artist_id = '${safeId}' AND ra.role = 'main'
            AND r.primary_artist_id != '${safeId}'
            AND r.hidden = 0        `)[0];

        // Group edit/length variants (radio edit, extended mix, ...) of the
        // same recording per release — hearing any one counts as heard.
        const releaseIds = [
            ...(ownResult ? ownResult.values.map(r => r[0]) : []),
            ...(collabResult ? collabResult.values.map(r => r[0]) : []),
        ];
        const completionByRelease = new Map();
        if (releaseIds.length > 0) {
            const idList = releaseIds.map(id => `'${id.replace(/'/g, "''")}'`).join(',');
            const trackRowsResult = _db.exec(`
                SELECT t.release_id, t.title,
                       EXISTS (SELECT 1 FROM listens l WHERE l.track_id = t.id) as heard
                FROM tracks t
                WHERE t.release_id IN (${idList}) AND t.hidden = 0
                  AND t.variant_section IS NULL
                  AND (t.duration_ms IS NULL OR t.duration_ms >= 30000)
            `)[0];
            if (trackRowsResult) {
                const groupsByRelease = new Map();
                trackRowsResult.values.forEach(([releaseId, trackTitle, heard]) => {
                    if (!groupsByRelease.has(releaseId)) groupsByRelease.set(releaseId, new Map());
                    const groups = groupsByRelease.get(releaseId);
                    const key = sameSongKey(trackTitle);
                    groups.set(key, groups.get(key) || !!heard);
                });
                groupsByRelease.forEach((groups, releaseId) => {
                    completionByRelease.set(releaseId, {
                        total: groups.size,
                        heard: [...groups.values()].filter(Boolean).length,
                    });
                });
            }
        }
        const withCompletion = row => {
            const c = completionByRelease.get(row[0]) || { total: 0, heard: 0 };
            return [...row, c.total, c.heard];
        };

        _discData.own = ownResult ? ownResult.values.map(withCompletion) : [];
        _discData.collabs = collabResult ? collabResult.values.map(withCompletion) : [];

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

    function _makeDiscCard(row, collab) {
        const [id, title, releaseDate, type, typeSecondary, albumArtUrl,
               totalListens, primaryArtistName, slug, totalTracks, listenedTracks] = row;

        let subParts = [];
        const year = releaseDate ? releaseDate.slice(0, 4) : null;
        if (year) subParts.push(year);
        if (collab && primaryArtistName) subParts.push(escapeHtml(primaryArtistName));
        else if (totalListens > 0) subParts.push(`${formatNumber(totalListens)} plays`);

        const card = document.createElement('a');
        card.className = 'disc-card' + (totalListens === 0 ? ' unplayed' : '');
        card.href = releaseHref(id, slug);

        const imgSrc = albumArtUrl || getFallbackImageUrl();

        card.innerHTML = `
            <div class="disc-card-img" style="background-image: url('${cssUrl(imgSrc)}')"></div>
            <div class="disc-card-meta">
                <div class="disc-card-info">
                    <div class="disc-card-title">${escapeHtml(title)}</div>
                    <div class="disc-card-sub">${subParts.join(' · ')}</div>
                </div>
                ${donutHtml(listenedTracks, totalTracks)}
            </div>
        `;
        return card;
    }

    function _makeDiscListRow(row, collab) {
        const [id, title, releaseDate, type, typeSecondary, albumArtUrl,
               totalListens, primaryArtistName, slug, totalTracks, listenedTracks] = row;

        const year   = releaseDate ? releaseDate.slice(0, 4) : '';

        const rowEl = document.createElement('a');
        rowEl.className = 'disc-list-row' + (totalListens === 0 ? ' unplayed' : '');
        rowEl.href = releaseHref(id, slug);

        const imgSrc = albumArtUrl || getFallbackImageUrl();
        const subLabel = collab && primaryArtistName ? escapeHtml(primaryArtistName) : '';

        rowEl.innerHTML = `
            <div class="disc-list-img" style="background-image: url('${cssUrl(imgSrc)}')"></div>
            <div class="disc-list-info">
                <div class="disc-list-title">${escapeHtml(title)}</div>
                ${subLabel ? `<div class="disc-list-artist">${subLabel}</div>` : ''}
            </div>
            <div class="disc-list-right">
                <span class="disc-list-year">${year}</span>
                <span class="disc-list-frac">${totalTracks > 0 ? `${listenedTracks}/${totalTracks}` : ''}</span>
                ${donutHtml(listenedTracks, totalTracks, { small: true })}
            </div>
        `;
        return rowEl;
    }

    function _renderDiscGroup(container, label, rows, collab) {
        if (!rows || rows.length === 0) return;

        const group = document.createElement('div');
        group.className = 'disc-group';

        const h3 = document.createElement('h3');
        h3.textContent = label;
        group.appendChild(h3);

        if (_discView === 'list') {
            const list = document.createElement('div');
            list.className = 'disc-list';
            rows.forEach(row => list.appendChild(_makeDiscListRow(row, collab)));
            group.appendChild(list);
        } else {
            const grid = document.createElement('div');
            grid.className = 'disc-grid';
            rows.forEach(row => grid.appendChild(_makeDiscCard(row, collab)));
            group.appendChild(grid);
        }

        container.appendChild(group);
    }

    function renderDiscography() {
        const container = document.getElementById('discographyContainer');
        if (!container) return;
        container.innerHTML = '';

        const sortFn = _discSort === 'listens'
            ? (a, b) => b[6] - a[6]
            : (a, b) => (b[2] || '').localeCompare(a[2] || '');

        const own = [...(_discData.own || [])].sort(sortFn);
        const collabs = [...(_discData.collabs || [])].sort(sortFn);

        if (own.length === 0 && collabs.length === 0) {
            const name = _artistName || 'This artist';
            container.innerHTML = _hasListens
                ? `<div class="empty-state">
                       <i data-lucide="disc-3" class="app-error-icon"></i>
                       <div class="empty-state-title">No album data yet for ${escapeHtml(name)}</div>
                       <p class="empty-state-hint">Scrobbles exist, but no release metadata has been matched
                           to this artist yet. Their raw listens still show up in History.</p>
                       <a class="back-button" href="?view=history&amp;q=${encodeURIComponent(name)}">
                           View in History</a>
                   </div>`
                : `<div class="empty-state">
                       <i data-lucide="disc-3" class="app-error-icon"></i>
                       <div class="empty-state-title">No releases catalogued for ${escapeHtml(name)}</div>
                       <p class="empty-state-hint">This artist is tracked (e.g. from a canonical list) but has
                           no album data or listens on file yet.</p>
                   </div>`;
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
        setupToggleGroup('[data-disc-view]', btn => {
            _discView = btn.dataset.discView;
            localStorage.setItem('artistDiscView', _discView);
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
                l.timestamp,
                r.id as release_id,
                r.slug as release_slug
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            LEFT JOIN releases r ON t.release_id = r.id
            WHERE (
                t.id IN (
                    SELECT DISTINCT track_id FROM track_artists
                    WHERE artist_id = '${safeId}' AND role IN (${PRIMARY_ROLES_SQL})
                )
                OR r.primary_artist_id = '${safeId}'
            )
            AND t.hidden = 0
            ORDER BY l.timestamp DESC
            LIMIT 40
        `)[0];

        const section = document.getElementById('recentPlaysSection');
        const list = document.getElementById('recentPlaysList');
        if (!section || !list || !result || result.values.length === 0) return;

        // Collapse consecutive plays from the same release into one row —
        // otherwise an album played straight through reads as a stuck/glitched
        // list rather than genuine recent activity.
        const groups = [];
        result.values.forEach(([trackTitle, albumArtUrl, releaseTitle, timestamp, releaseId, releaseSlug]) => {
            const last = groups[groups.length - 1];
            const key = releaseId || `track:${trackTitle}`;
            if (last && last.key === key) {
                last.count += 1;
                last.tracks.push(trackTitle);
            } else {
                groups.push({ key, trackTitle, albumArtUrl, releaseTitle, timestamp, releaseId, releaseSlug, count: 1, tracks: [trackTitle] });
            }
        });

        list.innerHTML = groups.slice(0, 10).map(g => {
            const imgSrc = g.albumArtUrl || getFallbackImageUrl();
            const dateStr = formatTimeAgo(g.timestamp);
            const nameHtml = g.count > 1
                ? `${g.count} tracks from ${escapeHtml(g.releaseTitle || 'this release')}`
                : escapeHtml(g.trackTitle);
            const subtitle = (g.releaseTitle && g.count === 1)
                ? `<i data-lucide="disc-album" style="width: 12px; height: 12px;"></i> ${escapeHtml(g.releaseTitle)}`
                : null;
            const tag = g.releaseId ? 'a' : 'div';
            const hrefAttr = g.releaseId ? ` href="${releaseHref(g.releaseId, g.releaseSlug)}"` : '';
            return `
                <${tag} class="recent-play-row"${hrefAttr}>
                    <div class="recent-play-thumb" style="background-image: url('${cssUrl(imgSrc)}')"></div>
                    <div class="recent-play-info">
                        <div class="recent-play-name">${nameHtml}</div>
                        ${subtitle ? `<div class="recent-play-album">${subtitle}</div>` : ''}
                    </div>
                    <span class="recent-play-date">${dateStr}</span>
                </${tag}>
            `;
        }).join('');

        section.removeAttribute('hidden');
    }

    // Cert tier comes from `artists.cert` (`mdb.py certs refresh`); peak years
    // from `artist_year_medals` (`mdb.py stats refresh`). Neither is computed
    // here — the peak-year ranking is a dataset-wide scan.
    function loadArtistBadges() {
        const safeId = _artistId.replace(/'/g, "''");
        const badgesEl = document.getElementById('artistBadges');
        if (!badgesEl) return;

        const certResult = _db.exec(`SELECT cert, secondary_type FROM artists WHERE id = '${safeId}'`)[0];
        const certTier = certResult ? certResult.values[0][0] : null;
        const secondaryType = certResult ? certResult.values[0][1] : null;

        const certLabels = { gold: 'Gold — 250+ plays', platinum: 'Platinum — 500+ plays', diamond: 'Diamond — 1,000+ plays' };
        // Full tooltip text vs. the short word shown on the badge itself.
        const typeLabels = { supergroup: 'Supergroup — members are established artists in their own right', virtual: 'Virtual — an animated, synthetic, or fictional performer', standup: 'Stand-up comic — comedy albums/specials, not music' };
        const typeDisplay = { standup: 'stand-up' };
        const typeIcons = {
            supergroup: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>',
            virtual: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/><line x1="8" y1="16" x2="8" y2="16"/><line x1="16" y1="16" x2="16" y2="16"/></svg>',
            standup: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/><path d="M19 10v2a7 7 0 0 1-14 0v-2"/><line x1="12" y1="19" x2="12" y2="23"/><line x1="8" y1="23" x2="16" y2="23"/></svg>',
        };

        let badgesHtml = '';
        if (secondaryType && typeLabels[secondaryType]) {
            badgesHtml += `<span class="badge-type badge-type-${secondaryType}" title="${typeLabels[secondaryType]}">${typeIcons[secondaryType]}${typeDisplay[secondaryType] || secondaryType}</span>`;
        }
        if (certTier) badgesHtml += `<span class="badge-cert badge-cert-${certTier}" title="${certLabels[certTier]}">${certTier}</span>`;
        badgesEl.innerHTML = badgesHtml;

        // Peak years → compact stats row instead of badge tower.
        const medalResult = _db.exec(`
            SELECT year, rank FROM artist_year_medals
            WHERE artist_id = '${safeId}'
            ORDER BY rank, year ASC
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
            JOIN track_artists ta ON t.id = ta.track_id AND ta.role IN (${PRIMARY_ROLES_SQL})
            WHERE ta.artist_id = '${safeId}' AND t.hidden = 0
            GROUP BY l.year, l.month
            ORDER BY l.year, l.month
        `)[0];

        const yearlyResult = _db.exec(`
            SELECT l.year, COUNT(*) as listen_count
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN track_artists ta ON t.id = ta.track_id AND ta.role IN (${PRIMARY_ROLES_SQL})
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
        // Build the <img> via the DOM rather than innerHTML: the URL comes from
        // scraped third-party metadata, and a quote in it would break out of
        // the attribute.
        const inner = document.createElement('div');
        inner.className = 'art-modal-inner';
        const img = document.createElement('img');
        img.src = url;
        img.alt = '';
        inner.appendChild(img);
        modal.appendChild(inner);
        document.body.appendChild(modal);

        // AbortController so click-to-dismiss tears down the keydown handler
        // too, rather than leaking one per image opened.
        const ac = new AbortController();
        const close = () => { ac.abort(); modal.remove(); };
        modal.addEventListener('click', close, { signal: ac.signal });
        document.addEventListener('keydown', e => {
            if (e.key === 'Escape') close();
        }, { signal: ac.signal });
    }
})();
