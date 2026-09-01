// Collage/export tool — extracted from views/top.js, which was the single
// heaviest control surface on the site (grid shape/aspect presets, theme
// picker, topster tiers, PNG export) crammed into one Display mode. Reads
// its card data from views/collage-handoff.js rather than running any query
// itself, so any browsing view (Top today; Browse/a genre page later) can
// feed it without this file knowing about entities/SQL at all.
const ViewCollage = (() => {
    let _payload = null;
    let gridShape = { rows: 3, cols: 3 };
    // 'quilt' | 'captioned' | 'topster'
    let collageTheme = 'quilt';
    let topsterCount = 36;
    let _ac = null;

    const _GRID_PRESETS = [3, 4, 5, 6, 7, 10];
    const GRID_OPTIONS = _GRID_PRESETS.map(n => ({ value: n, label: `${n}×${n}` }));
    const _ASPECT_PRESETS = [
        { key: 'square',  label: 'Square',   icon: 'square',    ratio: 1 },
        { key: 'portrait',label: 'Portrait', icon: 'rectangle-vertical', ratio: 4 / 5 },
        { key: 'story',   label: 'Story',    icon: 'smartphone', ratio: 9 / 16 },
    ];
    const _COLLAGE_THEMES = [
        { key: 'quilt',     label: 'Quilt',     icon: 'grid-3x3' },
        { key: 'captioned', label: 'Captioned', icon: 'type' },
        { key: 'topster',   label: 'Topster',   icon: 'list' },
    ];
    const _TOPSTER_COUNTS = [10, 22, 36, 43, 50];
    const TOPSTER_COUNT_OPTIONS = _TOPSTER_COUNTS.map(n => ({ value: n, label: String(n) }));

    // Reproduces the Last.fm-community "Topster" step-pyramid: exactly 3
    // tiers, cols fixed at 5/6/7. Tiers 1-2 cap at 2 rows each (10, then 12
    // cells); tier 3 absorbs whatever's left at 7 cols, growing rows as
    // needed. Verified against the 5 supported counts: 10→[10], 22→[10,12],
    // 36→[10,12,14] (tier 3 @ 2 rows), 43→[10,12,21] (tier 3 @ 3 rows),
    // 50→[10,12,28] (tier 3 @ 4 rows).
    function _computeTopsterTiers(n) {
        const tiers = [];
        let remaining = n;
        for (const cols of [5, 6]) {
            if (remaining <= 0) break;
            const count = Math.min(remaining, cols * 2);
            tiers.push({ count, cols, rows: Math.ceil(count / cols) });
            remaining -= count;
        }
        if (remaining > 0) {
            tiers.push({ count: remaining, cols: 7, rows: Math.ceil(remaining / 7) });
        }
        return tiers;
    }

    function _dropdownSpecs() {
        return {
            topsterCount: {
                label: 'Count', options: TOPSTER_COUNT_OPTIONS, getValue: () => topsterCount,
                onPick: value => {
                    topsterCount = parseInt(value, 10);
                    _syncUrl(); _renderCollage(); _updateGridButtonStates();
                },
            },
            gridFixed: {
                label: 'Grid', options: GRID_OPTIONS,
                getValue: () => (gridShape.rows === gridShape.cols ? gridShape.cols : null),
                onPick: value => {
                    const n = parseInt(value, 10);
                    gridShape = { rows: n, cols: n };
                    _syncUrl(); _renderCollage(); _updateGridButtonStates();
                },
            },
        };
    }

    function mount(container, db, params) {
        _payload = getCollagePayload();
        setPageTitle('Collage');

        if (params.grid && /^\d+x\d+$/.test(params.grid)) {
            const [rows, cols] = params.grid.split('x').map(Number);
            if (rows >= 1 && rows <= 10 && cols >= 1 && cols <= 10) gridShape = { rows, cols };
        }
        if (params.theme && ['quilt','captioned','topster'].includes(params.theme)) collageTheme = params.theme;
        if (params.topsterCount && _TOPSTER_COUNTS.includes(+params.topsterCount)) topsterCount = +params.topsterCount;

        if (!_payload) {
            container.innerHTML = `
                <header><h1>Collage</h1></header>
                ${renderEmptyState(
                    'Nothing to collage yet',
                    'Open Top or Browse, pick a list of albums or artists, then use "Make a collage" to send them here.',
                    'grid-3x3'
                )}
            `;
            lucide.createIcons();
            return;
        }

        container.innerHTML = _renderShell();
        _ac = new AbortController();
        setupDropdowns(container, _dropdownSpecs(), _ac.signal);
        _setupControls();
        _renderCollage();
    }

    function unmount() {
        _ac?.abort();
        _ac = null;
    }

    function _renderShell() {
        return `
            <header>
                <h1>Collage</h1>
                <p class="subtitle">${escapeHtml(_payload.title || '')} · ${formatNumber(_payload.cards.length)} items</p>
                <a href="${_payload.backHref || '?view=top'}" class="back-button" style="margin-top:1rem">← Back</a>
            </header>
            <div class="page-controls">${_collageControlsHtml()}</div>
            <div id="collageContainer" class="image-grid"></div>
        `;
    }

    function _collageControlsHtml() {
        const themeHtml = `
            <div class="control-block">
                <span class="control-block-label">Theme</span>
                <div class="sort-controls">
                    ${_COLLAGE_THEMES.map(t => `<button class="sort-btn${collageTheme === t.key ? ' active' : ''}" data-collage-theme="${t.key}" title="${t.label}"><i data-lucide="${t.icon}"></i>${t.label}</button>`).join('')}
                </div>
            </div>`;

        if (collageTheme === 'topster') {
            return `
            ${themeHtml}
            <div class="control-block">
                <span class="control-block-label">Count</span>
                <div class="sort-controls">
                    ${dropdownHtml('topsterCount', 'Count', TOPSTER_COUNT_OPTIONS, () => topsterCount)}
                </div>
            </div>
            <div class="control-block">
                <span class="control-block-label">Export</span>
                <div class="sort-controls" style="gap:0.5rem;align-items:center">
                    <button class="sort-btn" id="collageDownloadBtn" title="Download Image"><i data-lucide="download"></i></button>
                </div>
            </div>`;
        }

        return `
            ${themeHtml}
            <div class="control-block">
                <span class="control-block-label">Grid</span>
                <div class="sort-controls">
                    ${dropdownHtml('gridFixed', 'Grid', GRID_OPTIONS, () => (gridShape.rows === gridShape.cols ? gridShape.cols : null))}
                </div>
            </div>
            <div class="control-block">
                <span class="control-block-label">Shape</span>
                <div class="sort-controls">
                    ${_ASPECT_PRESETS.map(p => `<button class="sort-btn" data-grid-aspect="${p.key}" title="${p.label}"><i data-lucide="${p.icon}"></i>${p.label}</button>`).join('')}
                    <button class="sort-btn" data-grid-custom title="Custom">Custom</button>
                </div>
            </div>
            <div class="control-block" id="customGridBlock" style="display:none">
                <span class="control-block-label">Rows × Cols</span>
                <div class="sort-controls">
                    <select id="customRows">${Array.from({length:10},(_,i)=>i+1).map(n=>`<option value="${n}">${n}</option>`).join('')}</select>
                    <select id="customCols">${Array.from({length:10},(_,i)=>i+1).map(n=>`<option value="${n}">${n}</option>`).join('')}</select>
                </div>
            </div>
            <div class="control-block">
                <span class="control-block-label">Export</span>
                <div class="sort-controls" style="gap:0.5rem;align-items:center">
                    <button class="sort-btn" id="collageDownloadBtn" title="Download Image"><i data-lucide="download"></i></button>
                </div>
            </div>`;
    }

    function _syncUrl() {
        const p = new URLSearchParams({ view: 'collage', theme: collageTheme });
        if (collageTheme === 'topster') p.set('topsterCount', topsterCount);
        else p.set('grid', `${gridShape.rows}x${gridShape.cols}`);
        history.replaceState(Object.fromEntries(p), '', '?' + p.toString());
    }

    function _setupControls() {
        document.querySelectorAll('[data-collage-theme]').forEach(btn => btn.addEventListener('click', () => {
            collageTheme = btn.dataset.collageTheme;
            _syncUrl();
            _rerenderControls();
        }));
        document.querySelectorAll('[data-grid-aspect]').forEach(btn => btn.addEventListener('click', () => {
            const preset = _ASPECT_PRESETS.find(p => p.key === btn.dataset.gridAspect);
            const approxCellCount = gridShape.rows * gridShape.cols || 25;
            gridShape = _nearestGridForRatio(preset.ratio, approxCellCount);
            _syncUrl(); _renderCollage(); _updateGridButtonStates();
        }));
        document.getElementById('customGridBlock') && (() => {
            const rowsSel = document.getElementById('customRows');
            const colsSel = document.getElementById('customCols');
            rowsSel.value = gridShape.rows; colsSel.value = gridShape.cols;
            const onChange = () => {
                gridShape = { rows: parseInt(rowsSel.value), cols: parseInt(colsSel.value) };
                _syncUrl(); _renderCollage(); _updateGridButtonStates();
            };
            rowsSel.addEventListener('change', onChange);
            colsSel.addEventListener('change', onChange);
        })();
        document.querySelector('[data-grid-custom]')?.addEventListener('click', () => {
            const block = document.getElementById('customGridBlock');
            if (block) block.style.display = block.style.display === 'none' ? '' : 'none';
        });

        document.getElementById('collageDownloadBtn')?.addEventListener('click', async e => {
            const btn = e.currentTarget;
            const showLabels = collageTheme === 'captioned';
            const cards = _payload.cards;
            let cells, rows, cols;
            if (collageTheme === 'topster') {
                const tiers = _computeTopsterTiers(Math.min(topsterCount, cards.length));
                cols = Math.max(...tiers.map(t => t.cols));
                rows = tiers.reduce((s, t) => s + t.rows, 0);
                cells = cards.slice(0, topsterCount).map(f => ({
                    imageUrl: f.imageUrl || getFallbackImageUrl(),
                    label: f.artistName ? `${f.artistName} - ${f.label || ''}` : (f.label || ''),
                }));
            } else {
                cells = cards.slice(0, gridShape.rows * gridShape.cols).map(f => ({
                    imageUrl: f.imageUrl || getFallbackImageUrl(),
                    label: f.label || '',
                }));
                rows = gridShape.rows; cols = gridShape.cols;
            }
            btn.disabled = true;
            btn.innerHTML = '<i data-lucide="loader-2" class="spin"></i>';
            lucide.createIcons({ root: btn });
            try {
                await CollageExport.exportCollage({
                    rows, cols, cells, showLabels,
                    theme: collageTheme,
                    tiers: collageTheme === 'topster' ? _computeTopsterTiers(Math.min(topsterCount, cards.length)) : null,
                    filenamePrefix: _payload.filenamePrefix || 'collage',
                });
            } finally {
                btn.disabled = false;
                btn.innerHTML = '<i data-lucide="download"></i>';
                lucide.createIcons({ root: btn });
            }
        });
    }

    // Re-renders just the controls row (theme switch changes which controls
    // are shown — Grid/Shape vs. Count) without tearing down the whole page.
    function _rerenderControls() {
        const container = document.getElementById('view-container');
        if (!container) return;
        const controlsRow = container.querySelector('.page-controls');
        if (controlsRow) controlsRow.innerHTML = _collageControlsHtml();
        setupDropdowns(container, _dropdownSpecs(), _ac.signal);
        _setupControls();
        _renderCollage();
    }

    function _updateGridButtonStates() {
        const container = document.getElementById('view-container');
        if (!container) return;
        refreshDropdownTrigger(container, 'gridFixed', GRID_OPTIONS, () =>
            (gridShape.rows === gridShape.cols ? gridShape.cols : null));
        refreshDropdownTrigger(container, 'topsterCount', TOPSTER_COUNT_OPTIONS, () => topsterCount);
    }

    function _renderCollage() {
        const container = document.getElementById('collageContainer');
        if (!container) return;
        if (collageTheme === 'topster') return _renderTopster(container);

        container.innerHTML = '';
        container.className = `collage-grid${collageTheme === 'captioned' ? ' collage-grid-captioned' : ''}`;
        container.style.gridTemplateColumns = `repeat(${gridShape.cols}, 1fr)`;

        const show = gridShape.rows * gridShape.cols;
        _payload.cards.forEach((f, i) => {
            const card = createImageCard({
                href: f.href,
                imageUrl: f.imageUrl,
                collageLabel: collageTheme === 'captioned' ? (f.label || '') : null
            });
            if (i >= show) card.style.display = 'none';
            container.appendChild(card);
        });
    }

    // Renders the "Topster" theme: black background, step-pyramid tiers
    // (largest tiles first) on the left, monospace "Artist - Title" list
    // grouped by the same tier boundaries on the right — mirrors the
    // Last.fm-community chart format referenced in this feature's design.
    function _renderTopster(container) {
        container.innerHTML = '';
        container.className = 'topster-layout';
        container.style.gridTemplateColumns = '';

        const cards = _payload.cards;
        const items = cards.slice(0, Math.min(topsterCount, cards.length));
        const tiers = _computeTopsterTiers(items.length);

        const gridEl = document.createElement('div');
        gridEl.className = 'topster-grid';
        const maxCols = Math.max(...tiers.map(t => t.cols));
        // fixed total width so narrower-column tiers get bigger cells, wider-column tiers get smaller ones — the shrinking-tile hierarchy from the reference format
        gridEl.style.width = `${maxCols * 130}px`;
        const listEl = document.createElement('div');
        listEl.className = 'topster-list';

        let idx = 0;
        tiers.forEach(tier => {
            const tierEl = document.createElement('div');
            tierEl.className = 'topster-tier';
            tierEl.style.gridTemplateColumns = `repeat(${tier.cols}, 1fr)`;
            const listBlock = document.createElement('div');
            listBlock.className = 'topster-list-block';
            for (let i = 0; i < tier.count && idx < items.length; i++, idx++) {
                const f = items[idx];
                const card = createImageCard({ href: f.href, imageUrl: f.imageUrl, extraClass: 'topster-cell' });
                tierEl.appendChild(card);

                const line = document.createElement('div');
                line.className = 'topster-list-line';
                line.textContent = f.artistName ? `${f.artistName} - ${f.label || ''}` : (f.label || '');
                listBlock.appendChild(line);
            }
            gridEl.appendChild(tierEl);
            listEl.appendChild(listBlock);
        });

        container.appendChild(gridEl);
        container.appendChild(listEl);
    }

    return { mount, unmount };
})();
