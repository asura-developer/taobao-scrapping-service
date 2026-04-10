/**
 * Sidebar navigation component.
 */

import { navigate } from '../router.js';
import state from '../state.js';

const NAV_STRUCTURE = [
    {
        section: 'Operations',
        items: [
            { id: 'dashboard', icon: '\u25a3', label: 'Dashboard' },
            { id: 'scrape',    icon: '\uff0b', label: 'New Job' },
            { id: 'batch',     icon: '\ud83d\udccb', label: 'Batch Details' },
            { id: 'jobs',      icon: '\u2699', label: 'Jobs' },
        ],
    },
    {
        section: 'Data',
        items: [
            { id: 'products',   icon: '\ud83d\udce6', label: 'Products' },
            { id: 'search',     icon: '\ud83d\udd0d', label: 'Search' },
            { id: 'categories', icon: '\ud83d\uddc2', label: 'Categories' },
            { id: 'compare',    icon: '\u2696',  label: 'Comparison' },
        ],
    },
    {
        section: 'Monitoring',
        items: [
            { id: 'prices',    icon: '\ud83d\udcc8', label: 'Price Tracking' },
            { id: 'scheduler', icon: '\ud83d\udd52', label: 'Scheduler' },
            { id: 'retry',     icon: '\ud83d\udd04', label: 'Retry Queue' },
            { id: 'logs',      icon: '\ud83d\udcdd', label: 'Logs' },
        ],
    },
    {
        section: 'Integrations',
        items: [
            { id: 'webhooks', icon: '\ud83d\udd17', label: 'Webhooks' },
            { id: 'images',   icon: '\ud83d\uddbc', label: 'Images' },
        ],
    },
    {
        section: 'System',
        items: [
            { id: 'sessions', icon: '\ud83d\udd11', label: 'Sessions' },
            { id: 'settings', icon: '\u2699\ufe0f', label: 'Settings' },
        ],
    },
];

/**
 * Render sidebar HTML.
 */
export function renderSidebar() {
    const nav = NAV_STRUCTURE.map(group => {
        const items = group.items.map(item => {
            const active = state.route === item.id ? ' active' : '';
            return `<a class="nav-item${active}" href="#/${item.id}" data-route="${item.id}">
                <span class="nav-icon">${item.icon}</span> ${item.label}
            </a>`;
        }).join('');
        return `<div class="nav-section">${group.section}</div>${items}`;
    }).join('');

    return `
    <aside class="sidebar">
        <div class="sidebar-brand">
            <div class="brand-icon">\ud83d\udd77</div>
            <div><div class="brand-name">Scraper OS</div><div class="brand-sub">Data Pipeline</div></div>
        </div>
        <nav class="sidebar-nav">${nav}</nav>
        <div class="sidebar-footer">
            <div class="status-pill"><div class="status-dot"></div> System Online</div>
        </div>
    </aside>`;
}

/**
 * Update sidebar active state without full re-render.
 */
export function updateSidebarActive(route) {
    document.querySelectorAll('.nav-item').forEach(el => {
        el.classList.toggle('active', el.dataset.route === route);
    });
}
