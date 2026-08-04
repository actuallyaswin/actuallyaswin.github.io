// Renders ```mermaid fenced code blocks. Mermaid itself (~3.3 MB) is loaded
// lazily via dynamic import only when a diagram is actually present — it used
// to be a blocking <script> on every page, including the homepage and blog
// index, neither of which has ever contained a diagram.
// Mermaid's theme follows the site's data-theme attribute.
(async function initMermaid() {
    const blocks = document.querySelectorAll('pre code.language-mermaid');
    if (blocks.length === 0) return;

    let mermaid;
    try {
        const mod = await import('https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.esm.min.mjs');
        mermaid = mod.default;
    } catch (e) {
        // Leave the fenced code block as-is; a readable source listing is a
        // better failure mode than an empty container.
        console.error('Mermaid failed to load; leaving diagram source visible.', e);
        return;
    }

    const isLight = document.documentElement.getAttribute('data-theme') === 'light';
    mermaid.initialize({
        startOnLoad: false,
        theme: isLight ? 'default' : 'dark'
    });

    blocks.forEach((block) => {
        const container = document.createElement('div');
        container.className = 'mermaid';
        container.textContent = block.textContent;
        block.closest('pre').replaceWith(container);
    });

    mermaid.run({ querySelector: '.mermaid' });
})();
