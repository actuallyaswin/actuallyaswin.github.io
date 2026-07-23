// Composes the currently-visible Collage-mode cells into an off-screen
// canvas and downloads it as a PNG. Entity-agnostic: only ever receives
// {imageUrl, label} pairs plus grid dimensions — no knowledge of
// artists/albums/tracks. A cell whose image fails to load (confirmed real
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

    function _drawFallbackTile(ctx, x, y) {
        ctx.fillStyle = '#20232c';
        ctx.fillRect(x, y, CELL_PX, CELL_PX);
        ctx.fillStyle = '#767c85';
        ctx.font = '80px sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText('♪', x + CELL_PX / 2, y + CELL_PX / 2 + 10);
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

    async function exportCollage({ rows, cols, cells, showLabels, filenamePrefix }) {
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
            if (bitmap) ctx.drawImage(bitmap, x, y, CELL_PX, CELL_PX);
            else _drawFallbackTile(ctx, x, y);
            if (showLabels) _drawLabel(ctx, x, y, cells[i].label);
        }

        const blob = await new Promise(resolve => canvas.toBlob(resolve, 'image/png'));
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        const today = new Date().toISOString().slice(0, 10);
        a.href = url;
        a.download = `${filenamePrefix}-${cols}x${rows}-${today}.png`;
        a.click();
        URL.revokeObjectURL(url);
    }

    return { exportCollage };
})();
