// Shared "recent plays" list rendering — collapses consecutive plays from the
// same release into one row (otherwise an album played straight through reads
// as a stuck/glitched list rather than genuine recent activity), then renders
// each group as a row. Used by views/home.js (all artists, shows artist name)
// and views/artist.js (one artist, artist name redundant).

// plays: [{ trackTitle, albumArtUrl, artistName, timestamp, releaseId, releaseTitle, releaseSlug }]
function groupConsecutivePlays(plays) {
    const groups = [];
    plays.forEach(p => {
        const last = groups[groups.length - 1];
        const key = p.releaseId || `track:${p.trackTitle}`;
        if (last && last.key === key) {
            last.count += 1;
            last.tracks.push(p.trackTitle);
        } else {
            groups.push({ ...p, key, count: 1, tracks: [p.trackTitle] });
        }
    });
    return groups;
}

function renderRecentPlayRow(g, { showArtist = false } = {}) {
    const imgSrc = g.albumArtUrl || getFallbackImageUrl();
    const dateStr = formatTimeAgo(g.timestamp);
    const nameHtml = g.count > 1
        ? `${g.count} tracks from ${escapeHtml(g.releaseTitle || 'this release')}`
        : escapeHtml(g.trackTitle);
    const subtitleParts = [
        showArtist && g.artistName ? `<i data-lucide="user" style="width: 12px; height: 12px;"></i> ${escapeHtml(g.artistName)}` : null,
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
}
