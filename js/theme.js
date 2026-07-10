// Theme Management
// Detects system preference and handles theme toggle
// Shares the data-theme attribute contract with music/theme.js:
// no attribute = dark (default), data-theme="light" = explicit override.

function getInitialTheme() {
    const savedTheme = localStorage.getItem('theme');
    if (savedTheme) {
        return savedTheme;
    }

    if (window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches) {
        return 'light';
    }

    return 'dark';
}

function applyTheme(theme) {
    if (theme === 'light') {
        document.documentElement.setAttribute('data-theme', 'light');
    } else {
        document.documentElement.removeAttribute('data-theme');
    }
    localStorage.setItem('theme', theme);
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
    const currentTheme = document.documentElement.getAttribute('data-theme') === 'light' ? 'light' : 'dark';
    const newTheme = currentTheme === 'light' ? 'dark' : 'light';
    applyTheme(newTheme);
}

(function initTheme() {
    const theme = getInitialTheme();
    applyTheme(theme);

    if (window.matchMedia) {
        window.matchMedia('(prefers-color-scheme: light)').addEventListener('change', e => {
            if (!localStorage.getItem('theme')) {
                applyTheme(e.matches ? 'light' : 'dark');
            }
        });
    }
})();
