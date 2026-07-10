// Initializes Mermaid.js (loaded from CDN in the layout) and renders any
// ```mermaid fenced code blocks left untouched by Rouge on the page.
// Mermaid's own theme follows the site's existing data-theme attribute so
// diagrams aren't jarring against the active light/dark mode.
(function initMermaid() {
    if (typeof mermaid === 'undefined') return;

    const isLight = document.documentElement.getAttribute('data-theme') === 'light';
    mermaid.initialize({
        startOnLoad: false,
        theme: isLight ? 'default' : 'dark'
    });

    const blocks = document.querySelectorAll('pre code.language-mermaid');
    blocks.forEach((block) => {
        const container = document.createElement('div');
        container.className = 'mermaid';
        container.textContent = block.textContent;
        block.closest('pre').replaceWith(container);
    });

    if (blocks.length > 0) {
        mermaid.run({ querySelector: '.mermaid' });
    }
})();
