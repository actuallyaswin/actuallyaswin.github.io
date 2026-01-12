// Database configuration
const DB_CONFIG = {
    // Database version - update this when you create a new release
    version: 'v1.0',

    // GitHub repository info
    repo: 'actuallyaswin/actuallyaswin.github.io',

    // Database filename
    filename: 'listening_history.sqlite',

    // Local path (for testing)
    get localPath() {
        return this.filename;
    },

    // CDN URL (for production)
    get cdnUrl() {
        return `https://github.com/${this.repo}/releases/download/music-db-${this.version}/${this.filename}`;
    },

    // Fetch database with local fallback
    async fetchDatabase() {
        // Try local file first (for development)
        try {
            const localResponse = await fetch(this.localPath);
            if (localResponse.ok) {
                console.log('Loading database from local file');
                return await localResponse.arrayBuffer();
            }
        } catch (e) {
            // Local fetch failed, will try CDN
        }

        // Fall back to CDN
        console.log('Loading database from CDN');
        const cdnResponse = await fetch(this.cdnUrl);
        if (!cdnResponse.ok) {
            throw new Error(`Failed to load database: ${cdnResponse.statusText}`);
        }
        return await cdnResponse.arrayBuffer();
    }
};
