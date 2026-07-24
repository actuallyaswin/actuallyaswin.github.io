// Reads a CSS custom property's computed value at call time. Shared by
// music/artist.js, music/release.js, and music/views/release.js for
// theme-aware Chart.js coloring — originally lived in music/theme.js.
function getCSSColor(variable) {
    return getComputedStyle(document.documentElement).getPropertyValue(variable).trim();
}
