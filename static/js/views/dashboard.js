/**
 * Dashboard view — overview with stats, recent jobs, session health.
 */

import { apiFetch } from '../api.js';
import { esc, statCard, loadingState, platformBadge, statusBadge, fmtDate } from '../components/helpers.js';

let dashData = null;
let dashLoading = false;

export async function onEnter() {
    dashLoading = true;
    render();
    try {
        const [stats, jobs, retryStats, sessions] = await Promise.all([
            apiFetch('/products/stats/summary', {}, { silent: true, toast: false }),
            apiFetch('/scraper/jobs', {}, { silent: true, toast: false }),
            apiFetch('/retry/stats', {}, { silent: true, toast: false }),
            apiFetch('/scraper/session-status', {}, { silent: true, toast: false }),
        ]);
        dashData = {
            stats: stats.success ? stats.data : null,
            jobs: jobs.success ? (jobs.data || []).slice(0, 5) : [],
            retry: retryStats.success ? retryStats.data : null,
            sessions: sessions.success ? sessions.data : null,
        };
    } catch (e) {
        dashData = null;
    }
    dashLoading = false;
    render();
}

function render() {
    const root = document.getElementById('view-root');
    if (!root) return;

    if (dashLoading && !dashData) {
        root.innerHTML = loadingState('Loading dashboard\u2026');
        return;
    }

    if (!dashData) {
        root.innerHTML = `<div class="card"><div class="empty"><div class="empty-icon">\u25a3</div><div class="empty-text">Could not load dashboard</div></div></div>`;
        return;
    }

    const s = dashData.stats || {};
    const jobs = dashData.jobs || [];
    const retry = dashData.retry || {};

    // Stat cards
    const statsRow = `<div class="grid-4">
        ${statCard('Total Products', s.totalProducts || 0)}
        ${statCard('With Details', s.productsWithDetails || 0)}
        ${statCard('Active Jobs', jobs.filter(j => j.status === 'running').length)}
        ${statCard('Retry Queue', retry.pending || 0)}
    </div>`;

    // Session health pills
    const sess = dashData.sessions;
    const sessionHealth = sess ? `<div class="card" style="margin-top:14px">
        <div class="card-title">Session Health</div>
        <div style="display:flex;gap:12px;flex-wrap:wrap">
            ${renderSessionPill('Taobao/Tmall', sess.taobao_tmall)}
            ${renderSessionPill('1688', sess['1688'])}
        </div>
    </div>` : '';

    // Recent jobs
    const jobsBlock = `<div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
            <div class="card-title" style="margin-bottom:0">Recent Jobs</div>
            <a href="#/jobs" class="btn btn-ghost btn-sm">View All \u2192</a>
        </div>
        ${jobs.length ? `<div class="job-list">${jobs.map(j => {
            const query = j.searchType === 'keyword' ? j.searchParams?.keyword : j.searchParams?.categoryName;
            return `<div class="job-row" style="grid-template-columns:auto 1fr auto auto">
                <div>${platformBadge(j.platform)}</div>
                <div>
                    <div class="job-name">${esc(query || '\u2014')}</div>
                    <div class="job-id">${esc(j.jobId)}</div>
                </div>
                <div>${statusBadge(j.status)}</div>
                <div class="job-meta">${fmtDate(j.createdAt)}</div>
            </div>`;
        }).join('')}</div>` : '<div class="empty" style="padding:20px"><div class="empty-text">No jobs yet</div></div>'}
    </div>`;

    // Platform breakdown
    const platformBlock = s.byPlatform?.length ? `<div class="card">
        <div class="card-title">By Platform</div>
        ${s.byPlatform.map(i => `<div class="detail-row">
            <span>${platformBadge(i._id)}</span>
            <span style="font-family:var(--mono);font-size:13px">${(i.count || 0).toLocaleString()}</span>
        </div>`).join('')}
    </div>` : '';

    root.innerHTML = `<div class="fade-in">
        ${statsRow}
        ${sessionHealth}
        <div class="grid-2" style="margin-top:14px">
            ${jobsBlock}
            ${platformBlock}
        </div>
    </div>`;
}

function renderSessionPill(label, info) {
    if (!info) return '';
    const colors = { ok: 'var(--green-l)', expiring: 'var(--amber)', expired: 'var(--red-l)', missing: 'var(--red-l)' };
    const color = colors[info.status] || 'var(--muted)';
    return `<div style="display:flex;align-items:center;gap:8px;padding:8px 14px;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);border-left:3px solid ${color}">
        <div style="width:8px;height:8px;border-radius:50%;background:${color};flex-shrink:0"></div>
        <div>
            <div style="font-size:12px;font-weight:500">${label}</div>
            <div style="font-family:var(--mono);font-size:10px;color:${color}">${(info.status || 'unknown').toUpperCase()}</div>
        </div>
    </div>`;
}

export function renderView() {
    render();
}
