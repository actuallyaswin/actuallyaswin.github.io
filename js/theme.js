// Theme Management
// The initial theme is applied by a blocking inline script in <head> (see
// _layouts/default.html) to avoid a flash of the wrong theme; this file only
// handles the toggle and the OS-preference listener.
// Shares the data-theme attribute contract with music/theme.js:
// no attribute = dark (default), data-theme="light" = explicit override.

function currentTheme() {
    return document.documentElement.getAttribute('data-theme') === 'light' ? 'light' : 'dark';
}

// persist=false for OS-preference changes: following the system isn't an
// explicit choice, and writing it would pin the value and kill the listener below.
function applyTheme(theme, persist) {
    if (theme === 'light') {
        document.documentElement.setAttribute('data-theme', 'light');
    } else {
        document.documentElement.removeAttribute('data-theme');
    }
    if (persist) {
        try {
            localStorage.setItem('theme', theme);
        } catch (e) {}
    }
    updateThemeToggleIcon(theme);
}

function updateThemeToggleIcon(theme) {
    const toggle = document.getElementById('themeToggle');
    const sunIcon = document.getElementById('sun-icon');
    const moonIcon = document.getElementById('moon-icon');
    if (toggle && sunIcon && moonIcon) {
        sunIcon.style.display = theme === 'light' ? 'block' : 'none';
        moonIcon.style.display = theme === 'light' ? 'none' : 'block';
        toggle.setAttribute('aria-label', `Switch to ${theme === 'light' ? 'dark' : 'light'} mode`);
    }
}

function toggleTheme() {
    applyTheme(currentTheme() === 'light' ? 'dark' : 'light', true);
}

(function initTheme() {
    // The attribute is already set; just sync the toggle icon.
    updateThemeToggleIcon(currentTheme());

    const toggle = document.getElementById('themeToggle');
    if (toggle) {
        toggle.addEventListener('click', toggleTheme);
    }

    if (window.matchMedia) {
        window.matchMedia('(prefers-color-scheme: light)').addEventListener('change', (e) => {
            let saved = null;
            try {
                saved = localStorage.getItem('theme');
            } catch (err) {}
            if (!saved) {
                applyTheme(e.matches ? 'light' : 'dark', false);
            }
        });
    }
})();
