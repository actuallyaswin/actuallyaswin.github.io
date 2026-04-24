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
            <div class="site-header">
                <a href="?" class="site-logo-small">
                    <span class="logo-main">aswin.db</span><span class="logo-slash">/</span><span class="logo-accent">music</span>
                </a>
                <a href="javascript:history.back()" class="back-button">← Back</a>
            </div>

            <div id="artistHero" class="artist-hero" hidden></div>

            <header id="artistHeader" class="artist-header-layout">
                <div class="artist-photo-container">
                    <div class="artist-photo" id="artistPhoto">
                        <svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
                            <circle cx="50" cy="50" r="50" fill="#1e293b"/>
                            <text x="50" y="60" text-anchor="middle" font-size="40" fill="#475569">♪</text>
                        </svg>
                    </div>
                </div>
                <div class="artist-info-container">
                    <div class="artist-name-row">
                        <h1 id="artistName">Loading...</h1>
                        <p id="artistAka" class="artist-aka" hidden></p>
                    </div>
                    <div id="artistMembers" class="artist-members" hidden></div>
                    <div id="artistMemberOf" class="artist-members" hidden></div>
                    <p id="artistGenres" class="genre-list"></p>
                    <div id="artistLinks" class="release-links"></div>
                    <div class="stats-compact" id="artistStats">
                        <div class="stat-item">
                            <span class="stat-value" id="totalPlays">-</span>
                            <span class="stat-label">plays</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-value" id="uniqueTracks">-</span>
                            <span class="stat-label">tracks</span>
                        </div>
                        <div class="stat-item">
                            <span class="stat-value" id="totalReleases">-</span>
                            <span class="stat-label">releases</span>
                        </div>
                        <div class="stat-item" id="lastPlayedStat" hidden>
                            <span class="stat-value" id="lastPlayedValue">-</span>
                            <span class="stat-label">last played</span>
                        </div>
                    </div>
                    <div class="artist-badges" id="artistBadges"></div>
                </div>
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
                a.image_url,
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

        const [name, imageUrl, heroImageUrl, uniqueTracks, totalPlays, totalReleases,
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
        const wikiPageId = extLinks.get(0) || null;  // EL_SVC_WIKIPEDIA

        // Load aliases and update name display
        const aliasResult = _db.exec(`
            SELECT alias, alias_type FROM artist_aliases
            WHERE artist_id = '${safeId}'
            ORDER BY sort_order, alias_type
        `)[0];
        const aliases = aliasResult ? aliasResult.values : [];
        const nativeScript = aliases.find(([, t]) => t === 'native_script');
        const pastNames    = aliases.filter(([, t]) => t === 'past_name').map(([a]) => a);

        const nameEl = document.getElementById('artistName');
        if (nativeScript) {
            nameEl.innerHTML = `${escapeHtml(nativeScript[0])} <span class="artist-romanized">(${escapeHtml(name)})</span>`;
        } else {
            nameEl.textContent = name;
        }

        const akaEl = document.getElementById('artistAka');
        if (akaEl && pastNames.length > 0) {
            akaEl.textContent = `formerly ${pastNames.join(', ')}`;
            akaEl.removeAttribute('hidden');
        }

        document.getElementById('totalPlays').textContent = formatNumber(totalPlays);
        document.getElementById('uniqueTracks').textContent = formatNumber(uniqueTracks);
        document.getElementById('totalReleases').textContent = formatNumber(totalReleases);
        document.title = `aswin.db/music - ${name}`;

        const lastTsResult = _db.exec(`
            SELECT MAX(l.timestamp) FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
            WHERE ta.artist_id = '${safeId}' AND t.hidden = 0
        `)[0];
        const lastTs = lastTsResult && lastTsResult.values[0][0];
        if (lastTs) {
            const el = document.getElementById('lastPlayedStat');
            if (el) {
                document.getElementById('lastPlayedValue').textContent = formatRelativeTime(lastTs);
                el.removeAttribute('hidden');
            }
        }

        if (imageUrl) {
            document.getElementById('artistPhoto').innerHTML = `<img src="${imageUrl}" alt="${escapeHtml(name)}">`;
        }

        const heroEl = document.getElementById('artistHero');
        if (HERO_ENABLED && heroImageUrl && heroEl) {
            heroEl.removeAttribute('hidden');
            heroEl.innerHTML = `<img class="artist-hero-img" src="${heroImageUrl}" alt="">`;
            document.getElementById('artistHeader').classList.add('has-hero');
        }

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

        const genresEl = document.getElementById('artistGenres');
        if (genresEl && genreResult && genreResult.values.length > 0) {
            genresEl.innerHTML = renderGenreTags(genreResult.values.map(([id, name]) => [id, name, 1]));
        }

        const linksEl = document.getElementById('artistLinks');
        if (linksEl) {
            const links = [];
            if (spotifyId) {
                links.push({ href: `https://open.spotify.com/artist/${spotifyId}`, service: 'spotify', label: 'Spotify' });
            }
            if (mbid) {
                links.push({ href: `https://musicbrainz.org/artist/${mbid}`, service: 'musicbrainz', label: 'MusicBrainz' });
            }
            const resolvedAotyUrl = aotyUrl || (aotyId ? `https://www.albumoftheyear.org/artist/${aotyId}/` : null);
            if (resolvedAotyUrl) {
                links.push({ href: resolvedAotyUrl, service: 'aoty', label: 'Album of the Year' });
            }
            if (wikiPageId) {
                links.push({ href: wikiPageId, service: 'wikipedia', label: 'Wikipedia' });
            }
            const deezerId = extLinks.get(4) || null;
            if (deezerId) {
                links.push({ href: `https://www.deezer.com/artist/${deezerId}`, service: 'deezer', label: 'Deezer' });
            }
            const tidalId = extLinks.get(5) || null;
            if (tidalId) {
                links.push({ href: `https://tidal.com/browse/artist/${tidalId}`, service: 'tidal', label: 'Tidal' });
            }
            const bandcampUrl = extLinks.get(6) || null;
            if (bandcampUrl) {
                links.push({ href: bandcampUrl, service: 'bandcamp', label: 'Bandcamp' });
            }
            const beatportId = extLinks.get(7) || null;
            if (beatportId) {
                links.push({ href: `https://www.beatport.com/artist/-/${beatportId}`, service: 'beatport', label: 'Beatport' });
            }
            linksEl.innerHTML = links.map(({ href, service, label }) => {
                const icon = service === 'aoty'
                    ? `<img src="images/links/aoty-icon.png" alt="${label}" style="width:20px;height:20px;object-fit:contain;display:block">`
                    : `<span class="link-icon-mask" style="--icon-url: url('images/links/${service}.svg')"></span>`;
                return `<a href="${href}" target="_blank" rel="noopener" class="release-link-icon" data-service="${service}" title="${label}">${icon}</a>`;
            }).join('');
        }

        // Members of this group (supergroup display)
        const membersResult = _db.exec(`
            SELECT a.id, a.name FROM artist_members am
            JOIN artists a ON a.id = am.member_artist_id
            WHERE am.group_artist_id = '${safeId}'
            ORDER BY am.sort_order, a.name
        `)[0];
        const membersEl = document.getElementById('artistMembers');
        if (membersEl && membersResult && membersResult.values.length > 0) {
            membersEl.innerHTML = '<span class="artist-members-label">Members</span>' +
                membersResult.values.map(([mid, mname]) =>
                    `<a href="?view=artist&id=${encodeURIComponent(mid)}" class="artist-member-chip">${escapeHtml(mname)}</a>`
                ).join('');
            membersEl.removeAttribute('hidden');
        }

        // Groups this artist belongs to
        const memberOfResult = _db.exec(`
            SELECT a.id, a.name FROM artist_members am
            JOIN artists a ON a.id = am.group_artist_id
            WHERE am.member_artist_id = '${safeId}'
            ORDER BY a.name
        `)[0];
        const memberOfEl = document.getElementById('artistMemberOf');
        if (memberOfEl && memberOfResult && memberOfResult.values.length > 0) {
            memberOfEl.innerHTML = '<span class="artist-members-label">Member of</span>' +
                memberOfResult.values.map(([gid, gname]) =>
                    `<a href="?view=artist&id=${encodeURIComponent(gid)}" class="artist-member-chip">${escapeHtml(gname)}</a>`
                ).join('');
            memberOfEl.removeAttribute('hidden');
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
                r.album_art_url,
                (SELECT COUNT(*) FROM tracks t WHERE t.release_id = r.id AND t.hidden = 0
                 AND (t.duration_ms IS NULL OR t.duration_ms >= 30000)) as total_tracks,
                (SELECT COUNT(DISTINCT t.id) FROM tracks t
                 WHERE t.release_id = r.id AND t.hidden = 0
                 AND (t.duration_ms IS NULL OR t.duration_ms >= 30000)
                 AND EXISTS (SELECT 1 FROM listens l WHERE l.track_id = t.id)) as listened_tracks,
                (SELECT COUNT(*) FROM tracks t JOIN listens l ON t.id = l.track_id
                 WHERE t.release_id = r.id AND t.hidden = 0) as total_listens
            FROM releases r
            WHERE r.primary_artist_id = '${safeId}'
            AND r.hidden = 0
            AND NOT EXISTS (SELECT 1 FROM release_variants rv WHERE rv.variant_id = r.id)
        `)[0];

        const collabResult = _db.exec(`
            SELECT
                r.id,
                r.title,
                r.release_date,
                r.type,
                r.type_secondary,
                r.album_art_url,
                (SELECT COUNT(*) FROM tracks t WHERE t.release_id = r.id AND t.hidden = 0
                 AND (t.duration_ms IS NULL OR t.duration_ms >= 30000)) as total_tracks,
                (SELECT COUNT(DISTINCT t.id) FROM tracks t
                 WHERE t.release_id = r.id AND t.hidden = 0
                 AND (t.duration_ms IS NULL OR t.duration_ms >= 30000)
                 AND EXISTS (SELECT 1 FROM listens l WHERE l.track_id = t.id)) as listened_tracks,
                (SELECT COUNT(*) FROM tracks t JOIN listens l ON t.id = l.track_id
                 WHERE t.release_id = r.id AND t.hidden = 0) as total_listens,
                (SELECT a2.name FROM artists a2 WHERE a2.id = r.primary_artist_id) as primary_artist_name
            FROM releases r
            JOIN release_artists ra ON ra.release_id = r.id
            WHERE ra.artist_id = '${safeId}' AND ra.role = 'main'
            AND r.primary_artist_id != '${safeId}'
            AND r.hidden = 0
            AND NOT EXISTS (SELECT 1 FROM release_variants rv WHERE rv.variant_id = r.id)
        `)[0];

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
                r.album_art_url,
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
            SELECT COUNT(*) as plays
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
            WHERE ta.artist_id = '${safeId}' AND t.hidden = 0
        `)[0];

        const totalPlays = playsResult ? playsResult.values[0][0] : 0;
        let certTier = null;
        if (totalPlays >= 1000) certTier = 'diamond';
        else if (totalPlays >= 500) certTier = 'platinum';
        else if (totalPlays >= 250) certTier = 'gold';

        const medalResult = _db.exec(`
            WITH all_yearly AS (
                SELECT ta.artist_id, l.year, COUNT(*) as plays
                FROM listens l
                JOIN tracks t ON l.track_id = t.id
                JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
                WHERE t.hidden = 0
                GROUP BY ta.artist_id, l.year
            ),
            ranked AS (
                SELECT artist_id, year,
                    RANK() OVER (PARTITION BY year ORDER BY plays DESC) as rnk
                FROM all_yearly
            )
            SELECT year, rnk
            FROM ranked
            WHERE artist_id = '${safeId}' AND rnk <= 3
            ORDER BY year ASC
        `)[0];

        const fragments = [];

        const certLabels = {
            gold: 'Gold — 250+ plays',
            platinum: 'Platinum — 500+ plays',
            diamond: 'Diamond — 1,000+ plays',
        };

        if (certTier) {
            fragments.push(`<span class="badge-cert badge-cert-${certTier}" title="${certLabels[certTier]}">${certTier}</span>`);
        }

        if (medalResult && medalResult.values.length > 0) {
            const streakCount = medalResult.values.length;
            fragments.push(`<span class="badge-streak" title="${streakCount} year${streakCount > 1 ? 's' : ''} in top 3">★ ${streakCount}</span>`);
            const rankLabel = { 1: '#1', 2: '#2', 3: '#3' };
            const rankText  = { 1: 'Most', 2: '2nd most', 3: '3rd most' };
            const tierClass = { 1: 'gold', 2: 'silver', 3: 'bronze' };
            medalResult.values.forEach(([year, rnk]) => {
                fragments.push(`
                    <span class="badge-medal badge-medal-${tierClass[rnk]}" title="${rankText[rnk]} played artist in ${year}">
                        <span class="medal-rank">${rankLabel[rnk]}</span>
                        <span class="medal-year">${year}</span>
                    </span>
                `);
            });
        }

        badgesEl.innerHTML = fragments.join('');
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
})();
