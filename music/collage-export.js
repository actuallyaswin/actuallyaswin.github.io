// Composes the currently-visible Collage-mode cells into an off-screen
// canvas and downloads it as a PNG. Entity-agnostic: only ever receives
// {imageUrl, label} pairs plus grid dimensions — no knowledge of
// artists/albums/tracks. Supports three themes (see views/top.js's
// collageTheme state): 'quilt' (plain grid), 'captioned' (grid + bottom-bar
// label), 'topster' (black bg, step-pyramid tiers + monospace sidebar list
// — see _exportTopster). A cell whose image fails to load (confirmed real
// case: coverartarchive.org returns 403 to plain fetches, unlike Spotify/
// Apple Music's permissive CORS) gets the same fallback placeholder tile
// used elsewhere on the site, and the export continues rather than aborting.
const CollageExport = (() => {
    const CELL_PX = 300;

    async function _loadCellImage(url) {
        try {
            const res = await fetch(url, { mode: 'cors' });
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const blob = await res.blob();
            return await createImageBitmap(blob);
        } catch (e) {
            return null;  // caller draws the fallback tile
        }
    }

    function _drawLabel(ctx, x, y, label) {
        const barHeight = CELL_PX * 0.22;
        ctx.fillStyle = 'rgba(0,0,0,0.6)';
        ctx.fillRect(x, y + CELL_PX - barHeight, CELL_PX, barHeight);
        ctx.fillStyle = '#ffffff';
        ctx.font = '600 16px sans-serif';
        ctx.textAlign = 'left';
        ctx.textBaseline = 'top';
        const padding = 10;
        const maxWidth = CELL_PX - padding * 2;
        let text = label || '';
        while (ctx.measureText(text).width > maxWidth && text.length > 1) {
            text = text.slice(0, -2) + '…';
        }
        ctx.fillText(text, x + padding, y + CELL_PX - barHeight + padding);
    }

    // Draws a "cover-fit" bitmap into an arbitrary-sized square cell (used
    // by both the plain grid themes at CELL_PX and Topster's smaller,
    // per-tier cell sizes).
    function _drawCoverSized(ctx, bitmap, x, y, size) {
        const scale = Math.max(size / bitmap.width, size / bitmap.height);
        const sw = size / scale, sh = size / scale;
        const sx = (bitmap.width - sw) / 2, sy = (bitmap.height - sh) / 2;
        ctx.drawImage(bitmap, sx, sy, sw, sh, x, y, size, size);
    }

    function _drawFallbackTileSized(ctx, x, y, size) {
        ctx.fillStyle = '#20232c';
        ctx.fillRect(x, y, size, size);
        ctx.fillStyle = '#767c85';
        ctx.font = `${Math.round(size * 0.27)}px sans-serif`;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText('♪', x + size / 2, y + size / 2 + size * 0.03);
    }

    async function exportCollage({ rows, cols, cells, showLabels, theme, tiers, filenamePrefix }) {
        if (theme === 'topster') return _exportTopster({ cells, tiers, filenamePrefix });

        const canvas = document.createElement('canvas');
        canvas.width = cols * CELL_PX;
        canvas.height = rows * CELL_PX;
        const ctx = canvas.getContext('2d');

        for (let i = 0; i < cells.length; i++) {
            const row = Math.floor(i / cols);
            const col = i % cols;
            if (row >= rows) break;
            const x = col * CELL_PX, y = row * CELL_PX;
            const bitmap = await _loadCellImage(cells[i].imageUrl);
            if (bitmap) { _drawCoverSized(ctx, bitmap, x, y, CELL_PX); bitmap.close(); }
            else _drawFallbackTileSized(ctx, x, y, CELL_PX);
            if (showLabels) _drawLabel(ctx, x, y, cells[i].label);
        }

        _download(canvas, `${filenamePrefix}-${cols}x${rows}`);
    }

    // Renders the "Topster" theme: black background, step-pyramid tiers
    // (largest tiles first, per `tiers` — see views/top.js's
    // _computeTopsterTiers) on the left, monospace "Artist - Title" text
    // grouped by the same tier boundaries on the right. Mirrors the
    // Last.fm-community Topster chart format.
    async function _exportTopster({ cells, tiers, filenamePrefix }) {
        const PADDING = 30;
        const GAP = 40;
        const maxCols = Math.max(...tiers.map(t => t.cols));
        const gridWidth = maxCols * 130;  // matches the live preview's fixed grid width
        const LINE_H = 22;
        const BLOCK_GAP = 24;
        const FONT = '15px ui-monospace, "SF Mono", Menlo, Consolas, monospace';

        // Measure text width to size the sidebar and truncate long lines,
        // using a throwaway canvas context (font metrics only, no drawing).
        const measureCtx = document.createElement('canvas').getContext('2d');
        measureCtx.font = FONT;
        const maxLineWidth = Math.max(
            ...cells.map(c => measureCtx.measureText(c.label || '').width)
        );
        const sidebarWidth = Math.min(Math.max(maxLineWidth + 20, 300), 900);

        const gridHeight = tiers.reduce((sum, t) => sum + (gridWidth / t.cols) * t.rows, 0);
        const listHeight = tiers.reduce((sum, t) => sum + t.count * LINE_H, 0) + (tiers.length - 1) * BLOCK_GAP;
        const canvasHeight = Math.max(gridHeight, listHeight) + PADDING * 2;
        const canvasWidth = PADDING * 2 + gridWidth + GAP + sidebarWidth;

        const canvas = document.createElement('canvas');
        canvas.width = canvasWidth;
        canvas.height = canvasHeight;
        const ctx = canvas.getContext('2d');
        ctx.fillStyle = '#000000';
        ctx.fillRect(0, 0, canvasWidth, canvasHeight);

        let idx = 0;
        let gridY = PADDING;
        let listY = PADDING;
        const listX = PADDING + gridWidth + GAP;

        for (const tier of tiers) {
            const cellSize = gridWidth / tier.cols;
            for (let i = 0; i < tier.count && idx < cells.length; i++, idx++) {
                const col = i % tier.cols;
                const row = Math.floor(i / tier.cols);
                const x = PADDING + col * cellSize;
                const y = gridY + row * cellSize;
                const bitmap = await _loadCellImage(cells[idx].imageUrl);
                if (bitmap) { _drawCoverSized(ctx, bitmap, x, y, cellSize); bitmap.close(); }
                else _drawFallbackTileSized(ctx, x, y, cellSize);

                ctx.fillStyle = '#ffffff';
                ctx.font = FONT;
                ctx.textAlign = 'left';
                ctx.textBaseline = 'top';
                let text = cells[idx].label || '';
                const maxTextWidth = sidebarWidth - 10;
                while (ctx.measureText(text).width > maxTextWidth && text.length > 1) {
                    text = text.slice(0, -2) + '…';
                }
                ctx.fillText(text, listX, listY + i * LINE_H);
            }
            gridY += cellSize * tier.rows;
            listY += tier.count * LINE_H + BLOCK_GAP;
        }

        _download(canvas, `${filenamePrefix}-topster`);
    }

    function _download(canvas, name) {
        canvas.toBlob(blob => {
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            const today = new Date().toISOString().slice(0, 10);
            a.href = url;
            a.download = `${name}-${today}.png`;
            a.click();
            URL.revokeObjectURL(url);
        }, 'image/png');
    }

    return { exportCollage };
})();
