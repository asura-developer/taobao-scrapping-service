/**
 * Centralized API module with auth, error handling, toast notifications.
 */

import { showToast } from './components/toast.js';

const API = window.location.origin + '/api';

export function getApiKey() {
    return localStorage.getItem('scraper_api_key') || '';
}

export function setApiKey(key) {
    if (key) localStorage.setItem('scraper_api_key', key);
    else localStorage.removeItem('scraper_api_key');
}

/**
 * Main API fetch wrapper.
 * @param {string} path - API path (e.g. '/scraper/jobs')
 * @param {object} opts - fetch options
 * @param {object} extra - { silent: bool, toast: bool }
 * @returns {Promise<object>}
 */
export async function apiFetch(path, opts = {}, { silent = false, toast = true } = {}) {
    const key = getApiKey();
    if (key) {
        opts.headers = { ...(opts.headers || {}), 'X-API-Key': key };
    }
    try {
        const r = await fetch(`${API}${path}`, opts);
        const data = await r.json();
        if (!r.ok && !silent && toast) {
            showToast(data.error || data.detail || `Request failed (${r.status})`, 'error');
        }
        return { ...data, _status: r.status };
    } catch (e) {
        if (!silent && toast) {
            showToast(`Network error: ${e.message}`, 'error');
        }
        return { success: false, error: e.message, _status: 0 };
    }
}

/**
 * POST JSON helper.
 */
export async function apiPost(path, body, extra) {
    return apiFetch(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
    }, extra);
}

/**
 * DELETE helper.
 */
export async function apiDelete(path, extra) {
    return apiFetch(path, { method: 'DELETE' }, extra);
}

/**
 * Open a download URL in a new tab with auth params.
 */
export function apiDownload(path) {
    window.open(`${API}${path}`, '_blank');
}
