/**
 * Reactive state store with pub/sub.
 */

// ── Application state ────────────────────────────────────────────────────
const state = {
    // Current route
    route: 'dashboard',

    // Data
    jobs: [],
    products: [],
    pagination: { page: 1, total: 0, pages: 0 },
    stats: null,
    selected: null,  // selected product for modal

    // Loading flags
    loading: false,
    batchLoading: false,
    searchLoading: false,
    sessionLoading: false,

    // Scrape form
    scrape: {
        platform: 'taobao', keyword: '', maxProducts: 100, maxPages: 10,
        startPage: 1, language: 'en', includeDetails: false, mode: 'keyword',
        categoryId: '',
    },

    // Product filters
    filters: {
        platform: '', keyword: '', shopName: '', detailsScraped: '',
        minQuality: '', page: 1, limit: 20,
    },

    // Search
    search: { query: '', page: 0, size: 20 },
    searchResults: null,

    // Sessions
    sessionStatus: null,
    proxyStatus: null,
    qrLogin: { active: false, platform: null, qrImage: null, status: null, polling: false },
    cookieImport: { platform: 'taobao', text: '' },

    // Batch details
    batch: {
        mode: 'pending', platform: '', limit: '', keyword: '', categoryName: '',
        minQuality: '', language: 'en', delayMin: 5000, delayMax: 12000,
    },

    // Migration
    migrationLog: [],
    migrationRunning: false,

    // Category defaults
    categories: {
        taobao: ['电脑','手机','女装','男装','食品','家电','美妆'],
        tmall:  ['女装','男装','美妆','食品'],
        '1688': ['电子产品','服装','食品','家居'],
    },
};

// ── Product registry (for safe lookup by itemId) ─────────────────────────
const REGISTRY = new Map();

export function registerProducts(list) {
    (list || []).forEach(p => { if (p.itemId) REGISTRY.set(p.itemId, p); });
}

export function getProduct(itemId) {
    return REGISTRY.get(itemId);
}

// ── Pub/sub for re-rendering ─────────────────────────────────────────────
const listeners = new Set();

export function subscribe(fn) {
    listeners.add(fn);
    return () => listeners.delete(fn);
}

export function notify() {
    for (const fn of listeners) fn(state);
}

// ── Export state object ──────────────────────────────────────────────────
export default state;
