const ViewRelease = (() => {
    let _db = null;
    let _releaseId = null;
    let _primaryArtistId = null;
    let _releaseType = null;
    let _currentChart = null;
    let _chartData = { monthly: null, yearly: null, monthlyRaw: null };
    let _chartState = { granularity: 'monthly', type: 'distribution' };
    let _themeObserver = null;

    // Set to true to re-enable the Chart.js listening history chart
    const CHART_ENABLED = false;

    // Set to false to disable per-track audio feature bars (Energy, Mood, Dance)
    const AUDIO_FEATURES_ENABLED = true;

    function mount(container, db, params) {
        _db = db;
        _releaseId = params.id;
        _primaryArtistId = null;
        _releaseType = null;
        _currentChart = null;
        _chartData = { monthly: null, yearly: null, monthlyRaw: null };

        if (!_releaseId) {
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

            <header id="releaseHeader" class="artist-header-layout">
                <div class="artist-photo-container">
                    <div class="artist-photo" id="albumArt">
                        <svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
                            <rect width="100" height="100" fill="#1e293b"/>
                            <text x="50" y="60" text-anchor="middle" font-size="40" fill="#475569">♪</text>
                        </svg>
                    </div>
                </div>
                <div class="artist-info-container">
                    <h1 id="releaseName">Loading...</h1>
                    <p class="release-artist">
                        by <span id="releaseArtist"></span> ·
                        <span id="releaseYear"></span> ·
                        <span id="releaseType"></span>
                    </p>
                    <p id="releaseAka" class="release-aka" hidden></p>
                    <p id="releaseGenres" class="genre-list"></p>
                        <div id="releaseLinks" class="release-links"></div>
                </div>
            </header>

            ${CHART_ENABLED ? `
            <section class="chart-container">
                <div class="chart-header">
                    <h3 class="chart-title">Listening History Over Time</h3>
                    <div class="chart-controls">
                        <div class="control-group">
                            <button class="control-btn${_chartState.granularity === 'monthly' ? ' active' : ''}" data-granularity="monthly">Monthly</button>
                            <button class="control-btn${_chartState.granularity === 'yearly' ? ' active' : ''}" data-granularity="yearly">Yearly</button>
                        </div>
                        <div class="control-group">
                            <button class="control-btn${_chartState.type === 'distribution' ? ' active' : ''}" data-type="distribution">Distribution</button>
                            <button class="control-btn${_chartState.type === 'cumulative' ? ' active' : ''}" data-type="cumulative">Cumulative</button>
                        </div>
                    </div>
                </div>
                <canvas id="historyChart"></canvas>
            </section>
            ` : ''}

            <div class="stats-row">
                <section class="pulse-section" id="pulseSection">
                    <h2>Timeline</h2>
                    <div class="pulse-rows" id="pulseRows"></div>
                </section>

                <section class="tracks-section">
                    <h2>Tracks</h2>
                    <div class="tracklist" id="trackList">
                        <div class="loading">Loading tracks...</div>
                    </div>
                    <div id="variantsSection"></div>
                </section>
            </div>

            <div id="sourcesSection"></div>

            <footer>
                <p>Powered by <a href="https://github.com/sql-js/sql.js" target="_blank">sql.js</a></p>
            </footer>
        `;

        lucide.createIcons();

        loadReleaseInfo();
        loadReleaseAliases();
        loadTracks();
        loadListeningHistory();
        loadVariants();
        loadSources();
        loadCanonicalBacklink();
        if (CHART_ENABLED) setupChartControls();
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

    function loadReleaseInfo() {
        const safeId = _releaseId.replace(/'/g, "''");

        const result = _db.exec(`
            SELECT
                r.title,
                r.release_year,
                r.type,
                r.type_secondary,
                r.album_art_url,
                COUNT(DISTINCT CASE WHEN t.hidden = 0 THEN t.id END) as tracks_listened,
                COUNT(CASE WHEN t.hidden = 0 THEN l.id END)          as total_plays,
                r.spotify_id,
                r.release_group_mbid,
                r.mbid,
                r.aoty_url,
                r.aoty_id,
                r.aoty_score_critic,
                r.aoty_score_user,
                r.aoty_ratings_critic,
                r.aoty_ratings_user,
                r.primary_artist_id,
                r.wikipedia_url,
                r.apple_music_id
            FROM releases r
            LEFT JOIN tracks t ON t.release_id = r.id
            LEFT JOIN listens l ON l.track_id = t.id
            WHERE r.id = '${safeId}'
            GROUP BY r.id
        `)[0];

        if (!result || result.values.length === 0) {
            const el = document.getElementById('releaseName');
            if (el) el.textContent = 'Release not found';
            return;
        }

        const [title, year, type, typeSecondary, albumArtUrl, tracksListened, totalPlays,
               spotifyId, releaseGroupMbid, mbid, aotyUrl, aotyId,
               aotyScoreCritic, aotyScoreUser, aotyRatingsCritic, aotyRatingsUser,
               primaryArtistId, wikipediaUrl, appleMusicId] = result.values[0];
        const typeLabel = [type, typeSecondary].filter(Boolean).join(' / ');

        _primaryArtistId = primaryArtistId || null;
        _releaseType = type || null;

        const artistResult = _db.exec(`
            SELECT DISTINCT a.name, a.id
            FROM releases r
            JOIN artists a ON (
                a.id = r.primary_artist_id
                OR a.id IN (SELECT artist_id FROM release_artists WHERE release_id = r.id AND role = 'main')
            )
            WHERE r.id = '${safeId}' AND a.id IS NOT NULL
            ORDER BY (r.primary_artist_id = a.id) DESC, a.name
        `)[0];

        document.getElementById('releaseName').textContent = title || 'Unknown Release';
        document.getElementById('releaseYear').textContent = year || 'Unknown year';
        document.getElementById('releaseType').textContent = typeLabel || 'Unknown type';
        document.title = `aswin.db/music - ${title || 'Release'}`;

        if (albumArtUrl) {
            const albumArtDiv = document.getElementById('albumArt');
            albumArtDiv.style.backgroundImage = `url(${albumArtUrl})`;
            albumArtDiv.style.backgroundSize = 'cover';
            albumArtDiv.style.backgroundPosition = 'center';
            albumArtDiv.innerHTML = '';
        }

        if (artistResult && artistResult.values.length > 0) {
            const artistSpan = document.getElementById('releaseArtist');
            const links = artistResult.values.map(([name, id]) =>
                `<a href="?view=artist&id=${encodeURIComponent(id)}" style="color:${getCSSColor('--primary')};text-decoration:none">${escapeHtml(name)}</a>`
            );
            const joined = links.length === 1 ? links[0]
                : links.length === 2 ? `${links[0]} and ${links[1]}`
                : `${links.slice(0, -1).join(', ')}, and ${links[links.length - 1]}`;
            artistSpan.innerHTML = joined;
        } else {
            document.getElementById('releaseArtist').textContent = 'Various Artists';
        }

        const genreResult = _db.exec(`
            SELECT g.aoty_id, g.name, rg.is_primary
            FROM release_genres rg
            JOIN genres g ON rg.aoty_genre_id = g.aoty_id
            WHERE rg.release_id = '${safeId}'
            ORDER BY rg.is_primary DESC, g.name
        `)[0];

        const genresEl = document.getElementById('releaseGenres');
        if (genresEl && genreResult && genreResult.values.length > 0) {
            genresEl.innerHTML = renderGenreTags(genreResult.values);
        }

        const linksEl = document.getElementById('releaseLinks');
        if (linksEl) {
            const iconLinks = [];
            if (spotifyId) {
                iconLinks.push(`<a href="https://open.spotify.com/album/${spotifyId}" target="_blank" rel="noopener" class="release-link-icon" data-service="spotify" title="Spotify"><span class="link-icon-mask" style="--icon-url: url('images/spotify.svg')"></span></a>`);
            }
            if (releaseGroupMbid) {
                iconLinks.push(`<a href="https://musicbrainz.org/release-group/${releaseGroupMbid}" target="_blank" rel="noopener" class="release-link-icon" data-service="musicbrainz" title="MusicBrainz"><span class="link-icon-mask" style="--icon-url: url('images/musicbrainz.svg')"></span></a>`);
            } else if (mbid) {
                iconLinks.push(`<a href="https://musicbrainz.org/release/${mbid}" target="_blank" rel="noopener" class="release-link-icon" data-service="musicbrainz" title="MusicBrainz"><span class="link-icon-mask" style="--icon-url: url('images/musicbrainz.svg')"></span></a>`);
            }
            const resolvedAotyUrl = aotyUrl || (aotyId ? `https://www.albumoftheyear.org/album/${aotyId}/` : null);
            if (resolvedAotyUrl) {
                iconLinks.push(`<a href="${resolvedAotyUrl}" target="_blank" rel="noopener" class="release-link-icon" data-service="aoty" title="Album of the Year"><img src="images/aoty.png" alt="Album of the Year"></a>`);
            }
            if (wikipediaUrl) {
                iconLinks.push(`<a href="${wikipediaUrl}" target="_blank" rel="noopener" class="release-link-icon" data-service="wikipedia" title="Wikipedia"><span class="link-icon-mask" style="--icon-url: url('images/wikipedia.svg')"></span></a>`);
            }
            if (appleMusicId) {
                iconLinks.push(`<a href="https://music.apple.com/album/${appleMusicId}" target="_blank" rel="noopener" class="release-link-icon" data-service="applemusic" title="Apple Music"><span class="link-icon-mask" style="--icon-url: url('images/applemusic.svg')"></span></a>`);
            }

            const scoreColor = n => `hsl(${Math.round(Math.min(n, 100) * 1.2)}, 65%, 40%)`;
            const scores = [];
            if (aotyScoreCritic != null) {
                scores.push(`<span class="release-link-score" title="Critic Score${aotyRatingsCritic != null ? ` (${formatNumber(aotyRatingsCritic)} reviews)` : ''}"><i data-lucide="message-square-warning" style="color:${scoreColor(aotyScoreCritic)}"></i> <span style="color:${scoreColor(aotyScoreCritic)}">${aotyScoreCritic}</span></span>`);
            }
            if (aotyScoreUser != null) {
                scores.push(`<span class="release-link-score" title="User Score${aotyRatingsUser != null ? ` (${formatNumber(aotyRatingsUser)} ratings)` : ''}"><i data-lucide="message-square-heart" style="color:${scoreColor(aotyScoreUser)}"></i> <span style="color:${scoreColor(aotyScoreUser)}">${Number(aotyScoreUser).toFixed(1)}</span></span>`);
            }

            let html = iconLinks.join('');
            if (iconLinks.length > 0 && scores.length > 0) html += `<span class="release-links-sep">·</span>`;
            html += scores.join(`<span class="release-links-sep">·</span>`);
            linksEl.innerHTML = html;
            lucide.createIcons();
        }
    }

    function loadReleaseAliases() {
        const safeId = _releaseId.replace(/'/g, "''");
        const result = _db.exec(`
            SELECT alias, is_definitive
            FROM release_aliases
            WHERE release_id = '${safeId}'
            ORDER BY is_definitive DESC, alias
        `)[0];

        const el = document.getElementById('releaseAka');
        if (!el || !result || result.values.length === 0) return;

        const parts = result.values.map(([alias, isDef]) =>
            isDef ? `<strong>${escapeHtml(alias)}</strong>` : escapeHtml(alias)
        );
        el.innerHTML = `Also known as ${parts.join(', ')}`;
        el.removeAttribute('hidden');
    }

    function formatDuration(ms) {
        if (!ms) return '?:??';
        const totalSec = Math.floor(ms / 1000);
        const min = Math.floor(totalSec / 60);
        const sec = String(totalSec % 60).padStart(2, '0');
        return `${min}:${sec}`;
    }

    // ── Track matching ──────────────────────────────────────────────────────────

    function _normTitle(s) {
        return (s || '').toLowerCase().replace(/[^a-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
    }

    // ── Shared tracklist renderer ───────────────────────────────────────────────

    function _renderTracklist(container, tracks, showPlayCounts, opts = {}) {
        const { showTrackArtists = false, artistsByTrack = new Map(), primaryArtistId = null } = opts;
        container.innerHTML = '';
        if (!tracks.length) {
            container.innerHTML = '<div class="tracklist-empty">No tracks found</div>';
            return;
        }

        if (showPlayCounts) {
            const colHeader = document.createElement('div');
            colHeader.className = 'tracklist-col-header';
            colHeader.innerHTML = `
                <span class="tracklist-num"></span>
                <div class="tracklist-info"></div>
                <div class="tracklist-plays"><i data-lucide="headphones"></i></div>
            `;
            container.appendChild(colHeader);
        }

        const maxDisc   = Math.max(...tracks.map(t => t.discNumber || 1));
        const multiDisc = maxDisc > 1;
        let currentDisc = null;

        tracks.forEach((t, i) => {
            const disc = t.discNumber || 1;
            if (multiDisc && disc !== currentDisc) {
                currentDisc = disc;
                const header = document.createElement('div');
                header.className = 'tracklist-disc-header';
                header.textContent = `Disc ${disc}`;
                container.appendChild(header);
            }
            const row        = document.createElement('div');
            const displayNum = t.trackNumber != null ? t.trackNumber : (i + 1);
            const playsCell  = showPlayCounts
                ? `<div class="tracklist-plays">${(t.playCount > 0) ? formatNumber(t.playCount) : '—'}</div>`
                : '<div class="tracklist-plays">—</div>';

            let afHtml = '';
            if (AUDIO_FEATURES_ENABLED && t.audioFeaturesJson) {
                try {
                    const af = JSON.parse(t.audioFeaturesJson);
                    const features = [['Energy', af.energy], ['Mood', af.valence], ['Dance', af.danceability]];
                    const bars = features.filter(([, val]) => val != null).map(([label, val]) => `
                        <div class="af-bar-row">
                            <span class="af-bar-label">${label}</span>
                            <div class="af-bar-track"><div class="af-bar-fill" style="width:${Math.round(val * 100)}%"></div></div>
                        </div>`).join('');
                    if (bars) afHtml = `<div class="af-bars">${bars}</div>`;
                } catch (e) { /* ignore malformed JSON */ }
            }

            const bpmHtml = t.tempoBpm != null ? `<span class="track-bpm">♩ ${Math.round(t.tempoBpm)}</span>` : '';

            // Per-track artist credits
            let trackArtistsHtml = '';
            if (artistsByTrack.has(t.id)) {
                const artists = artistsByTrack.get(t.id);
                const mainArtists = showTrackArtists ? artists.filter(a => a.role === 'main' && a.id !== primaryArtistId) : [];
                const featArtists = artists.filter(a => a.role === 'featured');

                const parts = [];
                if (mainArtists.length > 0) {
                    const mainLinks = mainArtists.map(a =>
                        `<a href="?view=artist&id=${encodeURIComponent(a.id)}" class="tracklist-artist-link">${escapeHtml(a.name)}</a>`
                    );
                    const mainStr = mainLinks.length <= 2
                        ? mainLinks.join(' and ')
                        : mainLinks.slice(0, -1).join(', ') + ', and ' + mainLinks[mainLinks.length - 1];
                    parts.push(mainStr);
                }
                if (featArtists.length > 0) {
                    const featLinks = featArtists.map(a =>
                        `<a href="?view=artist&id=${encodeURIComponent(a.id)}" class="tracklist-artist-link">${escapeHtml(a.name)}</a>`
                    );
                    const featStr = featLinks.length <= 2
                        ? featLinks.join(' and ')
                        : featLinks.slice(0, -1).join(', ') + ', and ' + featLinks[featLinks.length - 1];
                    parts.push(`<span class="tracklist-feat">feat. ${featStr}</span>`);
                }
                if (parts.length > 0) {
                    trackArtistsHtml = `<div class="tracklist-track-artists">${parts.join(' · ')}</div>`;
                }
            }

            row.className = 'tracklist-row' + (trackArtistsHtml ? ' has-track-artists' : '');
            row.innerHTML = `
                <span class="tracklist-num">${displayNum}</span>
                <div class="tracklist-info">
                    <div class="tracklist-title-row">
                        <div class="tracklist-name">${escapeHtml(t.title)}</div>
                        <div class="tracklist-duration">${formatDuration(t.durationMs)}</div>
                    </div>
                    ${trackArtistsHtml}
                    ${afHtml}
                </div>
                <div class="tracklist-plays">${bpmHtml}${(showPlayCounts && t.playCount > 0) ? formatNumber(t.playCount) : '—'}</div>
            `;
            container.appendChild(row);
        });
        lucide.createIcons();
    }

    // ── Main tracklist ──────────────────────────────────────────────────────────

    function loadTracks() {
        const safeId = _releaseId.replace(/'/g, "''");

        const result = _db.exec(`
            SELECT t.title, t.id, t.track_number, t.disc_number, t.duration_ms, t.isrc,
                   COUNT(l.id) as play_count, t.tempo_bpm, t.audio_features
            FROM tracks t
            LEFT JOIN listens l ON l.track_id = t.id
            WHERE t.release_id = '${safeId}' AND t.hidden = 0
            GROUP BY t.id
            ORDER BY t.disc_number, t.track_number, t.title
        `)[0];

        const container = document.getElementById('trackList');
        if (!container) return;

        const tracks = (result ? result.values : []).map(
            ([title, id, trackNumber, discNumber, durationMs, isrc, playCount, tempoBpm, audioFeaturesJson]) =>
                ({ title, id, trackNumber, discNumber, durationMs, isrc, playCount, tempoBpm, audioFeaturesJson })
        );

        const showTrackArtists = (_releaseType === 'compilation' || _primaryArtistId === null);

        let artistsByTrack = new Map();
        if (tracks.length > 0) {
            const taResult = _db.exec(`
                SELECT ta.track_id, a.id, a.name, ta.role
                FROM track_artists ta
                JOIN artists a ON a.id = ta.artist_id
                WHERE ta.track_id IN (
                    SELECT id FROM tracks WHERE release_id = '${safeId}' AND hidden = 0
                )
                ORDER BY ta.track_id,
                         CASE ta.role WHEN 'main' THEN 0 ELSE 1 END,
                         a.name
            `)[0];

            if (taResult) {
                taResult.values.forEach(([trackId, artistId, artistName, role]) => {
                    if (!artistsByTrack.has(trackId)) artistsByTrack.set(trackId, []);
                    artistsByTrack.get(trackId).push({ id: artistId, name: artistName, role });
                });
            }
        }

        _renderTracklist(container, tracks, true, { showTrackArtists, artistsByTrack, primaryArtistId: _primaryArtistId });
    }

    // ── Release variants ────────────────────────────────────────────────────────

    function loadVariants() {
        const safeId = _releaseId.replace(/'/g, "''");

        const varResult = _db.exec(`
            SELECT rv.variant_id, rv.variant_type, r.title, r.album_art_url, r.release_year
            FROM release_variants rv
            JOIN releases r ON r.id = rv.variant_id
            WHERE rv.canonical_id = '${safeId}' AND r.hidden = 0
            ORDER BY rv.sort_order, r.release_year
        `)[0];

        if (!varResult || varResult.values.length === 0) return;

        // Fetch canonical tracks once for all variant comparisons
        const canonResult = _db.exec(`
            SELECT t.title, t.isrc
            FROM tracks t
            WHERE t.release_id = '${safeId}' AND t.hidden = 0
        `)[0];
        const canonTracks = (canonResult ? canonResult.values : [])
            .map(([title, isrc]) => ({ title, isrc }));

        const section = document.getElementById('variantsSection');
        if (!section) return;

        // Accumulate shown tracks across variants so the same track isn't shown twice
        const shownIsrcs  = new Set(canonTracks.map(t => t.isrc).filter(Boolean));
        const shownTitles = new Set(canonTracks.map(t => _normTitle(t.title)));

        for (const [variantId, variantType, variantTitle, artUrl, year] of varResult.values) {
            const safeVarId = variantId.replace(/'/g, "''");

            const vtResult = _db.exec(`
                SELECT t.title, t.id, t.track_number, t.disc_number, t.duration_ms, t.isrc
                FROM tracks t
                WHERE t.release_id = '${safeVarId}' AND t.hidden = 0
                ORDER BY t.disc_number, t.track_number, t.title
            `)[0];

            const variantTracks = (vtResult ? vtResult.values : []).map(
                ([title, id, trackNumber, discNumber, durationMs, isrc]) =>
                    ({ title, id, trackNumber, discNumber, durationMs, isrc })
            );

            const exclusive = variantTracks.filter(t =>
                !(t.isrc && shownIsrcs.has(t.isrc)) && !shownTitles.has(_normTitle(t.title))
            );

            // Add this variant's exclusive tracks to the cumulative shown set
            exclusive.forEach(t => {
                if (t.isrc) shownIsrcs.add(t.isrc);
                shownTitles.add(_normTitle(t.title));
            });

            const wrap     = document.createElement('section');
            wrap.className = 'variant-section';

            const typeBadge = variantType
                ? `<span class="variant-type-badge">${escapeHtml(variantType.toUpperCase())}</span>`
                : '';
            const artStyle = artUrl
                ? `background-image: url('${artUrl}'); background-size: cover; background-position: center;`
                : '';

            wrap.innerHTML = `
                <div class="variant-header">
                    <div class="variant-art" style="${artStyle}"></div>
                    <div class="variant-header-info">
                        <h3>
                            <a href="?view=release&id=${encodeURIComponent(variantId)}">
                                ${escapeHtml(variantTitle)}
                            </a>
                        </h3>
                        <span class="variant-meta">
                            ${typeBadge}
                            ${year ? `<span class="variant-year">${year}</span>` : ''}
                        </span>
                    </div>
                </div>
                <div class="tracklist variant-tracklist" id="vt-${variantId}"></div>
            `;
            section.appendChild(wrap);

            const trackContainer = document.getElementById(`vt-${variantId}`);
            if (exclusive.length === 0) {
                trackContainer.innerHTML = '<div class="tracklist-empty">No additional tracks</div>';
            } else {
                _renderTracklist(trackContainer, exclusive, false);
            }
        }

        lucide.createIcons();
    }

    // ── Compilation sources ─────────────────────────────────────────────────────

    function loadSources() {
        const safeId = _releaseId.replace(/'/g, "''");

        const result = _db.exec(`
            SELECT
                rs.source_id,
                rs.disc_number,
                r.title,
                r.album_art_url,
                r.release_year,
                (SELECT COUNT(*)
                 FROM listens l JOIN tracks t ON l.track_id = t.id
                 WHERE t.release_id = rs.source_id AND t.hidden = 0) AS total_listens,
                (SELECT COALESCE(SUM(t.duration_ms), 0) / 60000
                 FROM tracks t
                 WHERE t.release_id = rs.source_id AND t.hidden = 0)  AS total_minutes
            FROM release_sources rs
            JOIN releases r ON r.id = rs.source_id
            WHERE rs.compilation_id = '${safeId}'
            ORDER BY rs.disc_number
        `)[0];

        if (!result || result.values.length === 0) return;

        const section = document.getElementById('sourcesSection');
        if (!section) return;

        const heading       = document.createElement('h2');
        heading.textContent = 'Compiled From';
        section.appendChild(heading);

        const grid     = document.createElement('div');
        grid.className = 'wide-grid';

        for (const [sourceId, discNumber, title, artUrl, year, totalListens, totalMinutes]
                of result.values) {
            const discLabel = discNumber != null ? `Disc ${discNumber}` : null;
            const card = createWideCard({
                href:         `?view=release&id=${encodeURIComponent(sourceId)}`,
                imageUrl:     artUrl,
                name:         title,
                meta:         [year, discLabel].filter(Boolean).join(' · '),
                totalListens: totalListens || 0,
                totalMinutes: totalMinutes || 0,
            });
            grid.appendChild(card);
        }

        section.appendChild(grid);
        lucide.createIcons();
    }

    // ── Canonical backlink ──────────────────────────────────────────────────────

    // When this release is itself a variant, show a "This is a remaster of X" note.
    function loadCanonicalBacklink() {
        const safeId = _releaseId.replace(/'/g, "''");

        const result = _db.exec(`
            SELECT rv.canonical_id, r.title, rv.variant_type
            FROM release_variants rv
            JOIN releases r ON r.id = rv.canonical_id
            WHERE rv.variant_id = '${safeId}'
            LIMIT 1
        `)[0];

        if (!result || result.values.length === 0) return;

        const [canonicalId, canonicalTitle, variantType] = result.values[0];
        const typeLabel = variantType ? `${variantType} of` : 'edition of';

        const artistContainer = document.querySelector('.release-artist');
        if (!artistContainer) return;

        const p     = document.createElement('p');
        p.className = 'variant-backlink';
        p.innerHTML = `A ${escapeHtml(typeLabel)} <a href="?view=release&id=${encodeURIComponent(canonicalId)}">${escapeHtml(canonicalTitle)}</a>`;
        artistContainer.insertAdjacentElement('afterend', p);
    }

    // ── Listening history ───────────────────────────────────────────────────────

    function loadListeningHistory() {
        const safeId = _releaseId.replace(/'/g, "''");

        const monthlyResult = _db.exec(`
            SELECT l.year, l.month, COUNT(*) as listen_count
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            WHERE t.release_id = '${safeId}' AND t.hidden = 0
            GROUP BY l.year, l.month
            ORDER BY l.year, l.month
        `)[0];

        const yearlyResult = _db.exec(`
            SELECT l.year, COUNT(*) as listen_count
            FROM listens l
            JOIN tracks t ON l.track_id = t.id
            WHERE t.release_id = '${safeId}' AND t.hidden = 0
            GROUP BY l.year
            ORDER BY l.year
        `)[0];

        if ((!monthlyResult || monthlyResult.values.length === 0) &&
            (!yearlyResult  || yearlyResult.values.length  === 0)) {
            const rowsEl = document.getElementById('pulseRows');
            if (rowsEl) rowsEl.innerHTML = '<p class="no-data">No listening history yet.</p>';
            const el = document.querySelector('.chart-container');
            if (el) el.innerHTML = '<div class="loading">No listening history found</div>';
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

        if (CHART_ENABLED) renderChart();
    }

    function renderPulse(yearlyValues) {
        const pulseEl = document.getElementById('pulseSection');
        const rowsEl  = document.getElementById('pulseRows');
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
            const year      = parseInt(row.dataset.year);
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
    }

    function buildMonthlyChartData(values) {
        const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
        const years      = values.map(([year]) => year);
        const minYear    = Math.min(...years);
        const maxYear    = Math.max(...years);
        const dataMap    = new Map();
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
        const years   = values.map(([year]) => year);
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

    function setupChartControls() {
        setupToggleGroup('[data-granularity]', btn => {
            _chartState.granularity = btn.dataset.granularity;
            renderChart();
        });

        setupToggleGroup('[data-type]', btn => {
            _chartState.type = btn.dataset.type;
            renderChart();
        });

        _themeObserver = new MutationObserver(() => {
            if (_currentChart) renderChart();
        });
        _themeObserver.observe(document.documentElement, {
            attributes: true,
            attributeFilter: ['data-theme']
        });
    }

    function renderChart() {
        const data = _chartData[_chartState.granularity];
        if (!data) return;

        const primaryColor  = getCSSColor('--primary');
        const chartBg       = getCSSColor('--chart-bg');
        const chartBgSolid  = getCSSColor('--chart-bg-solid');
        const bgSecondary   = getCSSColor('--bg-secondary');
        const textColor     = getCSSColor('--text');
        const textSecondary = getCSSColor('--text-secondary');
        const borderColor   = getCSSColor('--border');

        let chartValues = [...data.data];
        if (_chartState.type === 'cumulative') {
            chartValues = data.data.reduce((acc, val, idx) => {
                acc.push(idx === 0 ? val : acc[idx - 1] + val);
                return acc;
            }, []);
        }

        const skipFactor    = Math.max(1, Math.ceil(data.labels.length / 15));
        const labelCallback = (value, index) => index % skipFactor === 0 ? data.labels[index] : '';

        if (_currentChart) _currentChart.destroy();

        const ctx = document.getElementById('historyChart');
        if (!ctx) return;

        _currentChart = new Chart(ctx.getContext('2d'), {
            type: _chartState.type === 'cumulative' ? 'line' : 'bar',
            data: {
                labels: data.labels,
                datasets: [{
                    label: _chartState.type === 'cumulative' ? 'Total Listens' : 'Listens per Period',
                    data: chartValues,
                    backgroundColor: _chartState.type === 'cumulative' ? chartBg : chartBgSolid,
                    borderColor: primaryColor,
                    borderWidth: _chartState.type === 'cumulative' ? 3 : 1,
                    fill: _chartState.type === 'cumulative',
                    tension: 0.4
                }]
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
                        borderColor: borderColor,
                        borderWidth: 1
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: { color: textSecondary, stepSize: 1 },
                        grid:  { color: borderColor }
                    },
                    x: {
                        ticks: {
                            color: textSecondary,
                            maxRotation: 45,
                            minRotation: 45,
                            autoSkip: false,
                            callback: labelCallback
                        },
                        grid: { color: borderColor }
                    }
                }
            }
        });
    }

    return { mount, unmount };
})();
