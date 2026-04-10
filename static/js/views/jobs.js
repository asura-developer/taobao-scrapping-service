/**
 * Jobs view — monitor active and completed scrape jobs.
 */

import state from '../state.js';
import { apiFetch, apiDelete } from '../api.js';
import { esc, fmtDate, platformBadge, statusBadge, loadingState } from '../components/helpers.js';
import { showToast } from '../components/toast.js';

let autoRefresh = null;

export async function onEnter() {
    await fetchJobs();
    autoRefresh = setInterval(fetchJobs, 5000);
}

export function onLeave() {
    if (autoRefresh) { clearInterval(autoRefresh); autoRefresh = null; }
}

async function fetchJobs() {
    const d = await apiFetch('/scraper/jobs', {}, { silent: true, toast: false });
    if (d.success) {
        state.jobs = d.data || [];
        renderView();
    }
}

async function cancelJob(jobId) {
    await apiDelete(`/scraper/job/${jobId}`);
    showToast('Job cancelled', 'info');
    fetchJobs();
}

window._cancelJob = cancelJob;
window._refreshJobs = fetchJobs;

export function renderView() {
    const root = document.getElementById('view-root');
    if (!root) return;

    const header = `<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px">
        <span style="font-family:var(--mono);font-size:12px;color:var(--muted)">${state.jobs.length} JOBS</span>
        <button class="btn btn-ghost btn-sm" onclick="window._refreshJobs()">\u21bb Refresh</button>
    </div>`;

    if (!state.jobs.length) {
        root.innerHTML = header + `<div class="card"><div class="empty"><div class="empty-icon">\ud83d\udced</div><div class="empty-text">No jobs yet \u2014 start a scrape</div></div></div>`;
        return;
    }

    root.innerHTML = header + `<div class="job-list fade-in">${state.jobs.map(j => {
        const query = j.searchType === 'keyword' ? j.searchParams?.keyword : j.searchParams?.categoryName;
        const progress = j.progress?.productsScraped || 0;
        const total = j.searchParams?.maxProducts || 100;
        const pct = Math.min(100, Math.round((progress / total) * 100));
        return `<div class="job-row">
            <div>${platformBadge(j.platform)}</div>
            <div>
                <div class="job-name">${esc(query || '\u2014')}</div>
                <div class="job-id">${esc(j.jobId)}</div>
                ${j.status === 'running' ? `
                <div class="progress-track"><div class="progress-fill" style="width:${pct}%"></div></div>
                <div class="progress-label">${progress} / ${total} products \u00b7 Page ${j.progress?.currentPage || 0}</div>` : ''}
                ${j.error ? `<div style="font-size:11px;color:var(--red-l);margin-top:4px;font-family:var(--mono)">${esc(j.error.substring(0, 120))}</div>` : ''}
            </div>
            <div>${statusBadge(j.status)}</div>
            <div class="job-meta">
                ${j.status === 'completed' ? `<span style="color:var(--green-l);font-family:var(--mono);font-size:11px">${j.results?.totalProducts || 0} saved</span>` : ''}
                ${j.status === 'running' ? `<button class="btn btn-danger btn-sm" onclick="window._cancelJob('${esc(j.jobId)}')">Cancel</button>` : ''}
                <div style="margin-top:4px">${fmtDate(j.createdAt)}</div>
            </div>
        </div>`;
    }).join('')}</div>`;
}
