const ViewAdmin = (() => {
    const ROW_H = 44;
    const BUFFER = 12;

    let _db = null;
    let _allRows = [];
    let _rows = [];
    let _pending = new Map();  // id -> {track_id?, hidden?, _trackName?, _artistName?, _artistId?, _releaseName?, _releaseId?}
    let _fileHandle = null;
    let _trackOpts = [];
    let _matchFilter = 'unmatched';
    let _filters = {};
    let _scrollEl = null;
    let _activeTs = null;  // { instance, cell, textEl }
    let _container = null;

    // -- Auth --

    async function _hashPin(pin, salt) {
        const enc = new TextEncoder();
        const key = await crypto.subtle.importKey('raw', enc.encode(pin), { name: 'PBKDF2' }, false, ['deriveBits']);
        const bits = await crypto.subtle.deriveBits(
            { name: 'PBKDF2', hash: 'SHA-256', salt: enc.encode(salt), iterations: 100000 },
            key, 256
        );
        return Array.from(new Uint8Array(bits)).map(b => b.toString(16).padStart(2, '0')).join('');
    }

    function _getStoredPin() {
        try {
            const res = _db.exec("SELECT key, value FROM settings WHERE key IN ('admin_pin_hash','admin_pin_salt')");
            if (!res.length || !res[0].values.length) return null;
            const map = Object.fromEntries(res[0].values);
            return map.admin_pin_hash && map.admin_pin_salt ? map : null;
        } catch { return null; }
    }

    // -- Data --

    function _loadRows() {
        const res = _db.exec(`
            SELECT l.id, l.timestamp, l.source,
                   l.raw_track_name, l.raw_artist_name, l.raw_album_name,
                   l.ms_played, COALESCE(l.skipped,0) as skipped,
                   COALESCE(l.hidden,0) as hidden, l.track_id,
                   t.title as matched_track_name, t.language as matched_track_language,
                   r.id as matched_release_id, r.title as matched_release_name,
                   a.id as matched_artist_id, a.name as matched_artist_name
            FROM listens l
            LEFT JOIN tracks t ON l.track_id = t.id
            LEFT JOIN releases r ON t.release_id = r.id
            LEFT JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
            LEFT JOIN artists a ON ta.artist_id = a.id
            ORDER BY l.timestamp DESC
        `);
        if (!res.length) return [];
        const cols = res[0].columns;
        return res[0].values.map(row => Object.fromEntries(cols.map((c, i) => [c, row[i]])));
    }

    function _loadTrackOpts() {
        const res = _db.exec(`
            SELECT t.id, t.title, r.id as release_id, r.title as release_title,
                   a.id as artist_id, a.name as artist_name
            FROM tracks t
            LEFT JOIN releases r ON t.release_id = r.id
            LEFT JOIN track_artists ta ON t.id = ta.track_id AND ta.role = 'main'
            LEFT JOIN artists a ON ta.artist_id = a.id
            WHERE t.hidden = 0 AND (r.hidden IS NULL OR r.hidden = 0)
            ORDER BY a.name, r.title, t.disc_number, t.track_number
        `);
        if (!res.length) return [];
        return res[0].values.map(([id, title, releaseId, releaseTitle, artistId, artistName]) => ({
            value: String(id),
            text: title,
            releaseId: releaseId ? String(releaseId) : null,
            releaseTitle: releaseTitle || '',
            artistId: artistId ? String(artistId) : null,
            artistName: artistName || '',
            // search surface: "track — release artist"
            searchText: `${title} ${releaseTitle || ''} ${artistName || ''}`.toLowerCase(),
        }));
    }

    // -- Filtering --

    function _applyFilters() {
        _rows = _allRows.filter(row => {
            const pending = _pending.get(row.id) || {};
            const trackId = 'track_id' in pending ? pending.track_id : row.track_id;

            if (_matchFilter === 'matched' && !trackId) return false;
            if (_matchFilter === 'unmatched' && trackId) return false;

            for (const [col, val] of Object.entries(_filters)) {
                if (!val) continue;
                const cell = String(row[col] ?? '').toLowerCase();
                if (!cell.includes(val.toLowerCase())) return false;
            }
            return true;
        });

        const countEl = document.getElementById('adminRowCount');
        if (countEl) countEl.textContent = `${_rows.length.toLocaleString()} listens`;
        if (_scrollEl) _scrollEl.scrollTop = 0;
        _renderWindow();
    }

    let _raf = null;

    // -- Virtual scroll --

    function _renderWindow() {
        if (!_scrollEl) return;
        // Capture scrollTop before any DOM mutations — reflow from innerHTML='' can shift it
        const scrollTop = _scrollEl.scrollTop;
        const start = Math.max(0, Math.floor(scrollTop / ROW_H) - BUFFER);
        const end = Math.min(_rows.length, start + Math.ceil(_scrollEl.clientHeight / ROW_H) + BUFFER * 2);

        const tbody = document.getElementById('adminTbody');
        tbody.innerHTML = '';
        for (let i = start; i < end; i++) tbody.appendChild(_buildRow(_rows[i]));

        // Update spacers after content so total table height stays stable during mutation
        document.getElementById('adminSpacerTop').style.height = `${start * ROW_H}px`;
        document.getElementById('adminSpacerBot').style.height = `${(_rows.length - end) * ROW_H}px`;
    }

    function _scheduleRender() {
        if (_raf) cancelAnimationFrame(_raf);
        _raf = requestAnimationFrame(() => { _raf = null; _renderWindow(); });
    }

    // -- Formatting --

    function _fmtTs(ts) {
        return new Date(ts * 1000).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    }

    function _fmtMs(ms) {
        if (!ms) return '—';
        const s = Math.floor(ms / 1000);
        return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`;
    }

    // -- Row building --

    function _buildRow(row) {
        const pending = _pending.get(row.id) || {};
        const trackId = 'track_id' in pending ? pending.track_id : row.track_id;
        const hidden = 'hidden' in pending ? pending.hidden : row.hidden;
        const isMatched = !!trackId;

        const displayTrack = pending._trackName ?? row.matched_track_name ?? '';
        const displayArtist = pending._artistName ?? row.matched_artist_name ?? '';
        const artistId = pending._artistId ?? row.matched_artist_id ?? null;
        const displayRelease = pending._releaseName ?? row.matched_release_name ?? '';
        const releaseId = pending._releaseId ?? row.matched_release_id ?? null;

        const artistLink = artistId
            ? `<a href="?view=artist&id=${artistId}" class="admin-link">${escapeHtml(displayArtist)}</a>`
            : (displayArtist ? escapeHtml(displayArtist) : '<span class="admin-empty">—</span>');

        const releaseLink = releaseId
            ? `<a href="?view=release&id=${releaseId}" class="admin-link">${escapeHtml(displayRelease)}</a>`
            : (displayRelease ? escapeHtml(displayRelease) : '<span class="admin-empty">—</span>');

        const tr = document.createElement('tr');
        tr.dataset.id = row.id;
        tr.className = `admin-row${hidden ? ' admin-row--hidden' : ''}${!isMatched ? ' admin-row--unmatched' : ''}`;

        tr.innerHTML = `
            <td class="admin-cell admin-cell--ts">${_fmtTs(row.timestamp)}</td>
            <td class="admin-cell"><span class="admin-source admin-source--${row.source}">${row.source}</span></td>
            <td class="admin-cell admin-cell--raw" title="${escapeHtml(row.raw_track_name || '')}">${escapeHtml(row.raw_track_name || '—')}</td>
            <td class="admin-cell admin-cell--raw" title="${escapeHtml(row.raw_artist_name || '')}">${escapeHtml(row.raw_artist_name || '—')}</td>
            <td class="admin-cell admin-cell--raw" title="${escapeHtml(row.raw_album_name || '')}">${escapeHtml(row.raw_album_name || '—')}</td>
            <td class="admin-cell admin-cell--num">${_fmtMs(row.ms_played)}</td>
            <td class="admin-cell admin-cell--center">${row.skipped ? '<span class="admin-skip" title="Skipped">↩</span>' : ''}</td>
            <td class="admin-cell admin-cell--track-select" data-row-id="${row.id}" title="Click to edit match">
                <span class="admin-select-text">${displayTrack
                    ? `${escapeHtml(displayTrack)}<i data-lucide="pencil" class="admin-edit-icon"></i>`
                    : '<span class="admin-match-prompt">match…</span>'
                }</span>
            </td>
            <td class="admin-cell admin-cell--lang">${row.matched_track_language ? `<span class="admin-lang">${escapeHtml(row.matched_track_language)}</span>` : ''}</td>
            <td class="admin-cell admin-cell--derived">${artistLink}</td>
            <td class="admin-cell admin-cell--derived">${releaseLink}</td>
            <td class="admin-cell admin-cell--action">
                <button class="admin-btn admin-btn--eject" data-id="${row.id}" title="Unmatch" ${!isMatched ? 'disabled' : ''}>
                    <i data-lucide="disc-3"></i>
                </button>
            </td>
            <td class="admin-cell admin-cell--action">
                <button class="admin-btn admin-btn--hide${hidden ? ' is-active' : ''}" data-id="${row.id}" title="${hidden ? 'Unhide' : 'Hide'}">
                    <i data-lucide="${hidden ? 'eye' : 'eye-off'}"></i>
                </button>
            </td>
        `;

        return tr;
    }

    // -- Track match modal --

    function _closeActive() {
        if (!_activeTs) return;
        _activeTs.instance.destroy();
        document.getElementById('adminMatchModal')?.remove();
        _activeTs = null;
    }

    function _activateTrackSelect(cell, rowId) {
        _closeActive();

        const row = _allRows.find(r => r.id === rowId);
        if (!row) return;

        const pending = _pending.get(rowId) || {};
        const currentTrackId = 'track_id' in pending ? pending.track_id : row.track_id;

        // Count siblings with the same raw triplet
        const siblings = _allRows.filter(r =>
            r.raw_track_name === row.raw_track_name &&
            r.raw_artist_name === row.raw_artist_name &&
            r.raw_album_name === row.raw_album_name
        );
        const siblingCount = siblings.length;

        // Build modal
        const modal = document.createElement('div');
        modal.id = 'adminMatchModal';
        modal.className = 'admin-match-modal';
        modal.innerHTML = `
            <div class="admin-match-modal-inner">
                <div class="admin-match-modal-header">
                    <span class="admin-match-modal-raw">${escapeHtml(row.raw_track_name || '')}
                        ${row.raw_artist_name ? `<span class="admin-match-modal-artist">· ${escapeHtml(row.raw_artist_name)}</span>` : ''}
                    </span>
                    <button class="admin-match-modal-close" title="Cancel">✕</button>
                </div>
                <select id="adminMatchSelect"></select>
                ${siblingCount > 1 ? `
                <label class="admin-match-modal-bulk">
                    <input type="checkbox" id="adminMatchBulk" checked>
                    Apply to all ${siblingCount} listens with this exact raw track · artist · album
                </label>` : ''}
                <p class="admin-match-modal-hint">Type at least 2 characters to search</p>
            </div>
        `;
        document.body.appendChild(modal);

        modal.querySelector('.admin-match-modal-close').addEventListener('click', _closeActive);

        const sel = document.getElementById('adminMatchSelect');
        const instance = new TomSelect(sel, {
            valueField: 'value',
            labelField: 'text',
            searchField: ['text'],
            maxOptions: 50,
            create: false,
            closeAfterSelect: true,
            openOnFocus: false,
            placeholder: 'Search by track, release, or artist…',
            load(query, callback) {
                if (query.length < 2) return callback([]);
                const q = query.toLowerCase();
                callback(_trackOpts.filter(o => o.searchText.includes(q)).slice(0, 50));
            },
            render: {
                option(data) {
                    const sub = [data.releaseTitle, data.artistName].filter(Boolean).join(' · ');
                    return `<div class="admin-ts-option">
                        <span class="admin-ts-option-title">${escapeHtml(data.text)}</span>
                        ${sub ? `<span class="admin-ts-option-sub">${escapeHtml(sub)}</span>` : ''}
                    </div>`;
                },
                no_results() {
                    return `<div class="admin-ts-option admin-ts-option-sub" style="padding:10px">No results</div>`;
                },
            },
            items: currentTrackId ? [String(currentTrackId)] : [],
            onItemAdd(value) {
                const opt = _trackOpts.find(o => o.value === value);
                if (!opt) return;
                const change = {
                    track_id: value,
                    _trackName: opt.text,
                    _artistId: opt.artistId,
                    _artistName: opt.artistName,
                    _releaseId: opt.releaseId,
                    _releaseName: opt.releaseTitle,
                };

                const bulk = document.getElementById('adminMatchBulk');
                const applyBulk = bulk ? bulk.checked : false;
                const targets = applyBulk ? siblings : [row];

                for (const r of targets) {
                    _pending.set(r.id, { ...(_pending.get(r.id) || {}), ...change });
                }

                _markDirty();
                _closeActive();
                // Refresh all affected visible rows
                for (const r of targets) _refreshRow(r.id);
            },
        });

        instance.focus();
        _activeTs = { instance };
    }

    function _refreshRow(rowId) {
        const tr = document.querySelector(`tr[data-id="${rowId}"]`);
        if (!tr) return;
        const row = _allRows.find(r => r.id === rowId);
        if (!row) return;
        tr.replaceWith(_buildRow(row));
    }

    function _markDirty() {
        const btn = document.getElementById('adminSaveBtn');
        if (btn) btn.classList.add('is-dirty');
    }

    // -- Actions --

    function _handleEject(id) {
        const p = _pending.get(id) || {};
        _pending.set(id, {
            ...p,
            track_id: null, _trackName: null,
            _artistId: null, _artistName: null,
            _releaseId: null, _releaseName: null,
        });
        _markDirty();
        _refreshRow(id);
    }

    function _handleHide(id) {
        const row = _allRows.find(r => r.id === id);
        if (!row) return;
        const p = _pending.get(id) || {};
        const current = 'hidden' in p ? p.hidden : row.hidden;
        _pending.set(id, { ...p, hidden: current ? 0 : 1 });
        _markDirty();
        _refreshRow(id);
    }

    // -- Save --

    async function _save() {
        const saveBtn = document.getElementById('adminSaveBtn');
        if (saveBtn) saveBtn.textContent = 'Saving…';

        try {
            for (const [id, changes] of _pending) {
                if ('track_id' in changes) {
                    _db.run('UPDATE listens SET track_id=? WHERE id=?', [changes.track_id, id]);
                }
                if ('hidden' in changes) {
                    _db.run('UPDATE listens SET hidden=? WHERE id=?', [changes.hidden, id]);
                }
            }
            _pending.clear();

            _allRows = _loadRows();
            _applyFilters();

            const data = _db.export();
            const blob = new Blob([data], { type: 'application/octet-stream' });

            if (window.showSaveFilePicker) {
                try {
                    if (!_fileHandle) {
                        _fileHandle = await window.showSaveFilePicker({
                            suggestedName: 'master.sqlite',
                            types: [{ description: 'SQLite Database', accept: { 'application/octet-stream': ['.sqlite'] } }],
                        });
                    }
                    const writable = await _fileHandle.createWritable();
                    await writable.write(blob);
                    await writable.close();
                    if (saveBtn) { saveBtn.textContent = 'Saved'; saveBtn.classList.remove('is-dirty'); }
                    setTimeout(() => { if (saveBtn) saveBtn.textContent = 'Save'; }, 1500);
                    return;
                } catch (e) {
                    if (e.name === 'AbortError') { if (saveBtn) saveBtn.textContent = 'Save'; return; }
                }
            }

            const url = URL.createObjectURL(blob);
            Object.assign(document.createElement('a'), { href: url, download: 'master.sqlite' }).click();
            URL.revokeObjectURL(url);
            if (saveBtn) { saveBtn.textContent = 'Downloaded'; saveBtn.classList.remove('is-dirty'); }
            setTimeout(() => { if (saveBtn) saveBtn.textContent = 'Save'; }, 1500);
        } catch (err) {
            console.error('Save failed', err);
            if (saveBtn) saveBtn.textContent = 'Save failed';
        }
    }

    // -- Table mount --

    function _mountTable(container) {
        document.title = 'aswin.db/music – Admin';

        _allRows = _loadRows();
        _trackOpts = _loadTrackOpts();

        const filterCols = [
            ['raw_track_name', 'raw track'],
            ['raw_artist_name', 'raw artist'],
            ['raw_album_name', 'raw album'],
            ['matched_track_name', 'matched track'],
            ['matched_artist_name', 'matched artist'],
            ['matched_release_name', 'matched release'],
        ];

        container.innerHTML = `
            <div class="admin-wrap">
                <div class="admin-toolbar">
                    <div class="admin-toolbar-left">
                        <a href="?" class="back-button">← Home</a>
                        <div class="admin-toggle-group">
                            <button class="admin-toggle-btn${_matchFilter === 'all' ? ' is-active' : ''}" data-filter="all">All</button>
                            <button class="admin-toggle-btn${_matchFilter === 'matched' ? ' is-active' : ''}" data-filter="matched">Matched</button>
                            <button class="admin-toggle-btn${_matchFilter === 'unmatched' ? ' is-active' : ''}" data-filter="unmatched">Unmatched</button>
                        </div>
                        <span id="adminRowCount" class="admin-row-count"></span>
                    </div>
                    <div class="admin-toolbar-right">
                        <button id="adminSaveBtn" class="admin-save-btn">Save</button>
                    </div>
                </div>
                <div class="admin-filter-bar">
                    ${filterCols.map(([col, label]) => `
                        <input class="admin-filter-input" data-col="${col}" placeholder="${label}…" value="${escapeHtml(_filters[col] || '')}">
                    `).join('')}
                </div>
                <div class="admin-scroll" id="adminScroll">
                    <table class="admin-table">
                        <thead>
                            <tr>
                                <th>Date</th>
                                <th>Src</th>
                                <th>Raw Track</th>
                                <th>Raw Artist</th>
                                <th>Raw Album</th>
                                <th>Dur</th>
                                <th title="Skipped">↩</th>
                                <th>Matched Track</th>
                                <th>Lang</th>
                                <th>Artist</th>
                                <th>Release</th>
                                <th></th>
                                <th></th>
                            </tr>
                        </thead>
                        <tbody><tr><td id="adminSpacerTop" colspan="13" style="padding:0;height:0"></td></tr></tbody>
                        <tbody id="adminTbody"></tbody>
                        <tbody><tr><td id="adminSpacerBot" colspan="13" style="padding:0;height:0"></td></tr></tbody>
                    </table>
                </div>
            </div>
        `;

        _scrollEl = document.getElementById('adminScroll');
        _applyFilters();

        _scrollEl.addEventListener('scroll', _scheduleRender, { passive: true });

        container.querySelector('.admin-toggle-group').addEventListener('click', e => {
            const btn = e.target.closest('[data-filter]');
            if (!btn) return;
            _matchFilter = btn.dataset.filter;
            container.querySelectorAll('.admin-toggle-btn').forEach(b => b.classList.toggle('is-active', b === btn));
            _applyFilters();
        });

        container.querySelector('.admin-filter-bar').addEventListener('input', e => {
            const col = e.target.dataset.col;
            if (col) { _filters[col] = e.target.value; _applyFilters(); }
        });

        container.addEventListener('click', e => {
            if (e.target.closest('.admin-btn--eject:not([disabled])')) {
                _handleEject(Number(e.target.closest('.admin-btn--eject').dataset.id));
                return;
            }
            if (e.target.closest('.admin-btn--hide')) {
                _handleHide(Number(e.target.closest('.admin-btn--hide').dataset.id));
                return;
            }
            const trackCell = e.target.closest('.admin-cell--track-select');
            if (trackCell && !e.target.closest('#adminMatchModal')) {
                _activateTrackSelect(trackCell, Number(trackCell.dataset.rowId));
                return;
            }
            if (_activeTs && !e.target.closest('#adminMatchModal')) {
                _closeActive();
            }
        });

        document.getElementById('adminSaveBtn').addEventListener('click', _save);

        document.addEventListener('keydown', function onKey(e) {
            if (e.key === 'Escape') _closeActive();
        });
    }

    // -- Auth mount --

    function _mountAuth(container) {
        document.title = 'aswin.db/music – Admin';
        const noPinSet = !_getStoredPin();

        container.innerHTML = `
            <div class="admin-auth">
                <div class="admin-auth-card">
                    <h2>Admin</h2>
                    ${noPinSet
                        ? `<p class="admin-auth-hint">No PIN configured. Run:<br>
                           <code>python mdb.py admin-pin</code></p>`
                        : `<p class="admin-auth-hint">Enter PIN to continue.</p>
                           <form id="pinForm">
                               <input type="password" id="pinInput" class="admin-pin-input" placeholder="PIN" autofocus autocomplete="current-password">
                               <button type="submit" class="admin-pin-submit">Unlock</button>
                           </form>
                           <p id="pinError" class="admin-auth-error" hidden>Incorrect PIN.</p>`
                    }
                </div>
            </div>
        `;

        if (noPinSet) return;

        document.getElementById('pinForm').addEventListener('submit', async e => {
            e.preventDefault();
            const pin = document.getElementById('pinInput').value;
            const stored = _getStoredPin();
            const hash = await _hashPin(pin, stored.admin_pin_salt);
            if (hash === stored.admin_pin_hash) {
                sessionStorage.setItem('admin_authed', '1');
                _mountTable(container);
            } else {
                document.getElementById('pinError').hidden = false;
                document.getElementById('pinInput').value = '';
                document.getElementById('pinInput').focus();
            }
        });
    }

    // -- Public --

    function mount(container, db) {
        _container = container;
        _db = db;
        _pending.clear();
        _fileHandle = null;
        _activeTs = null;
        _filters = {};
        _matchFilter = 'unmatched';

        if (sessionStorage.getItem('admin_authed') === '1') {
            _mountTable(container);
        } else {
            _mountAuth(container);
        }
    }

    function unmount() {
        _closeActive();
        _scrollEl = null;
        _container = null;
    }

    return { mount, unmount };
})();
