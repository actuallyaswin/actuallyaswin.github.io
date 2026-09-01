// Shared "Pulse" section renderer — a yearly listen-count bar list where each
// row expands into a monthly breakdown on click. Used identically by
// views/artist.js and views/release.js; monthlyRaw is passed in explicitly
// rather than read from either view's own chart-data state, so this stays a
// pure DOM-writing function with no dependency on either view's module scope.
function renderPulse(yearlyValues, monthlyRaw) {
    const pulseEl = document.getElementById('pulseSection');
    const rowsEl  = document.getElementById('pulseRows');
    if (!pulseEl || !rowsEl || !yearlyValues || yearlyValues.length === 0) return;

    const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const max = Math.max(...yearlyValues.map(([, count]) => count));

    const monthlyByYear = new Map();
    if (monthlyRaw) {
        monthlyRaw.forEach(([year, month, count]) => {
            if (!monthlyByYear.has(year)) monthlyByYear.set(year, new Map());
            monthlyByYear.get(year).set(month, count);
        });
    }

    rowsEl.innerHTML = yearlyValues.map(([year, count]) => {
        const pct = Math.round((count / max) * 100);
        return `
            <div class="pulse-row" data-year="${year}" role="button" tabindex="0" aria-expanded="false">
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

    const toggleRow = row => {
        const year      = parseInt(row.dataset.year);
        const monthlyEl = document.getElementById(`pulse-monthly-${year}`);
        if (!monthlyEl) return;

        const isExpanded = row.classList.contains('expanded');
        if (isExpanded) {
            monthlyEl.style.display = 'none';
            row.classList.remove('expanded');
            row.setAttribute('aria-expanded', 'false');
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
        row.setAttribute('aria-expanded', 'true');
    };

    rowsEl.addEventListener('click', e => {
        const row = e.target.closest('.pulse-row');
        if (row) toggleRow(row);
    });
    rowsEl.addEventListener('keydown', e => {
        if (e.key !== 'Enter' && e.key !== ' ') return;
        const row = e.target.closest('.pulse-row');
        if (!row) return;
        e.preventDefault();
        toggleRow(row);
    });

    // No-ops harmlessly on release.js's pulseSection, which doesn't render
    // with the `hidden` attribute in the first place; required on artist.js's.
    pulseEl.removeAttribute('hidden');
}
