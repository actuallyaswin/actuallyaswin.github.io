// Database configuration
const DB_CONFIG = {
    // Database filenames
    filename: 'listening_history.sqlite',
    overridesFilename: 'listening_history_overrides.sqlite',

    // Fetch main database
    async fetchDatabase() {
        const response = await fetch(this.filename);
        if (!response.ok) {
            throw new Error(`Failed to load database: ${response.statusText}`);
        }
        console.log('Loading database from GitHub Pages');
        return await response.arrayBuffer();
    },

    // Fetch overrides database (optional, may not exist)
    async fetchOverridesDatabase() {
        try {
            const response = await fetch(this.overridesFilename);
            if (response.ok) {
                console.log('Loading overrides database from GitHub Pages');
                return await response.arrayBuffer();
            }
        } catch (e) {
            // Overrides don't exist, that's ok
        }

        console.log('No overrides database found (this is OK)');
        return null;
    }
};
