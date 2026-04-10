/**
 * Toast notification system.
 */

let container = null;

function ensureContainer() {
    if (!container) {
        container = document.createElement('div');
        container.className = 'toast-container';
        document.body.appendChild(container);
    }
    return container;
}

const ICONS = {
    success: '\u2705',
    error: '\u274c',
    info: '\u2139\ufe0f',
    warn: '\u26a0\ufe0f',
};

/**
 * Show a toast notification.
 * @param {string} message
 * @param {'success'|'error'|'info'|'warn'} type
 * @param {number} duration - ms before auto-dismiss (0 = manual)
 */
export function showToast(message, type = 'info', duration = 4000) {
    const c = ensureContainer();
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `
        <span class="toast-icon">${ICONS[type] || ICONS.info}</span>
        <span class="toast-msg">${message}</span>
        <button class="toast-close" onclick="this.parentElement.remove()">\u00d7</button>
    `;
    c.appendChild(toast);

    if (duration > 0) {
        setTimeout(() => {
            toast.classList.add('removing');
            setTimeout(() => toast.remove(), 250);
        }, duration);
    }

    return toast;
}
