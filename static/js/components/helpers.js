/**
 * Shared helper functions — escaping, formatting, badges.
 */

export function esc(s) {
    return String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;')
        .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

export function fmtDate(d) {
    if (!d) return '\u2014';
    return new Date(d).toLocaleString('en-US', {
        month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
    });
}

export function qualityColor(q) {
    return q >= 80 ? 'var(--green-l)' : q >= 50 ? 'var(--amber)' : 'var(--red-l)';
}

export function platformBadge(p) {
    return `<span class="badge badge-${esc(p || '')}">${esc(p || '?')}</span>`;
}

export function statusBadge(s) {
    return `<span class="badge badge-${esc(s || 'pending')}">${esc(s || 'pending')}</span>`;
}

export function emptyState(icon, text) {
    return `<div class="card"><div class="empty"><div class="empty-icon">${icon}</div><div class="empty-text">${text}</div></div></div>`;
}

export function loadingState(text = 'Loading\u2026') {
    return emptyState('\u23F3', text);
}

/**
 * Render a stat card.
 */
export function statCard(label, value, sub = '') {
    return `<div class="stat-card">
        <div class="stat-label">${esc(label)}</div>
        <div class="stat-value">${typeof value === 'number' ? value.toLocaleString() : esc(String(value))}</div>
        ${sub ? `<div class="stat-sub">${sub}</div>` : ''}
    </div>`;
}

/**
 * Render pagination controls.
 */
export function renderPagination(current, total, totalItems, onPrev, onNext) {
    return `<div class="pagination">
        <button class="btn btn-ghost btn-sm" onclick="${onPrev}" ${current <= 1 ? 'disabled' : ''}>\u2190 Prev</button>
        <span class="page-info">Page ${current} \u00b7 ${(totalItems || 0).toLocaleString()} total</span>
        <button class="btn btn-ghost btn-sm" onclick="${onNext}" ${current >= total ? 'disabled' : ''}>Next \u2192</button>
    </div>`;
}

/**
 * Build a section block (for modals etc).
 */
export function section(title, content) {
    return `<div class="modal-section">
        <div class="modal-section-title">${title}</div>
        ${content}
    </div>`;
}
