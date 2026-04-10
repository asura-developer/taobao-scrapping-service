/**
 * Client-side hash router.
 * Routes: #/dashboard, #/scrape, #/jobs, etc.
 */

import state, { notify } from './state.js';

// ── Route metadata ───────────────────────────────────────────────────────
export const ROUTES = {
    dashboard:   { title: 'Dashboard',           sub: 'System overview and quick stats' },
    scrape:      { title: 'New Scrape Job',      sub: 'Configure and launch a new data collection task' },
    batch:       { title: 'Batch Details',       sub: 'Scrape product details in bulk' },
    jobs:        { title: 'Job Queue',           sub: 'Monitor active and completed scrape jobs' },
    products:    { title: 'Product Catalog',     sub: 'Browse and manage scraped product data' },
    search:      { title: 'Search',              sub: 'Full-text search across products' },
    categories:  { title: 'Categories',          sub: 'Browse and discover product categories' },
    compare:     { title: 'Comparison',          sub: 'Cross-platform price comparison' },
    prices:      { title: 'Price Tracking',      sub: 'Monitor price changes and trends' },
    scheduler:   { title: 'Scheduler',           sub: 'Manage automated scrape schedules' },
    retry:       { title: 'Retry Queue',         sub: 'Monitor and manage failed job retries' },
    logs:        { title: 'Logs',                sub: 'View application logs' },
    webhooks:    { title: 'Webhooks',            sub: 'Manage webhook endpoints' },
    images:      { title: 'Images',              sub: 'Download and manage product images' },
    sessions:    { title: 'Session Manager',     sub: 'Monitor login sessions and cookie health' },
    settings:    { title: 'Settings',            sub: 'API keys, migrations, and debug tools' },
};

/**
 * Navigate to a route.
 */
export function navigate(route) {
    window.location.hash = `#/${route}`;
}

/**
 * Get current route from hash.
 */
function getRouteFromHash() {
    const hash = window.location.hash.replace(/^#\/?/, '');
    return hash && ROUTES[hash] ? hash : 'dashboard';
}

/**
 * Called on hash change — updates state and triggers callbacks.
 */
let onRouteChangeCallbacks = [];

export function onRouteChange(fn) {
    onRouteChangeCallbacks.push(fn);
}

function handleHashChange() {
    const route = getRouteFromHash();
    if (state.route !== route) {
        state.route = route;
        notify();
        for (const fn of onRouteChangeCallbacks) fn(route);
    }
}

/**
 * Initialize router — listen for hash changes.
 */
export function initRouter() {
    window.addEventListener('hashchange', handleHashChange);
    // Set initial route from hash
    state.route = getRouteFromHash();
}
