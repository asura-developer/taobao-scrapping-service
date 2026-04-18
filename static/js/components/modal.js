/**
 * Generic modal system.
 */

let modalRoot = null;

function getRoot() {
    if (!modalRoot) {
        modalRoot = document.getElementById('modal-root');
    }
    return modalRoot;
}

function renderActions(actions) {
    if (!actions?.length) return '';
    return `<div class="modal-footer">
        ${actions.map((action, index) => `
            <button
                class="btn ${action.className || action.variant || 'btn-ghost'}"
                id="${action.id || `modal-action-${index}`}"
                type="${action.type || 'button'}"
                ${action.form ? `form="${action.form}"` : ''}
                data-modal-action="${index}"
                ${action.disabled ? 'disabled' : ''}
            >${action.label || 'OK'}</button>
        `).join('')}
    </div>`;
}

function formValues(form) {
    return Object.fromEntries(new FormData(form).entries());
}

/**
 * Open a modal with content HTML.
 * Inputs, links, and buttons inside content remain interactive. For callback-based
 * interactions, pass actions and/or onSubmit.
 *
 * @param {object} opts - { title, content, maxWidth, onClose, actions, onMount, onSubmit, submitFormId, closeOnOverlay }
 */
export function openModal({
    title = '',
    content = '',
    maxWidth = '780px',
    onClose,
    actions = [],
    onMount,
    onSubmit,
    submitFormId,
    closeOnOverlay = true,
} = {}) {
    const root = getRoot();
    root.innerHTML = `
    <div class="modal-overlay" id="modal-overlay">
        <div class="modal" style="max-width:${maxWidth}">
            <div class="modal-header">
                <div class="modal-title">${title}</div>
                <button class="modal-close" id="modal-close-btn">\u00d7</button>
            </div>
            <div class="modal-body">${content}</div>
            ${renderActions(actions)}
        </div>
    </div>`;

    const overlay = document.getElementById('modal-overlay');
    const closeBtn = document.getElementById('modal-close-btn');

    const close = () => {
        closeModal();
        if (onClose) onClose();
    };

    overlay.addEventListener('click', e => { if (closeOnOverlay && e.target === overlay) close(); });
    closeBtn.addEventListener('click', close);

    actions.forEach((action, index) => {
        const button = root.querySelector(`[data-modal-action="${index}"]`);
        if (!button || !action.onClick) return;
        button.addEventListener('click', async (event) => {
            const result = await action.onClick({ event, root, close });
            if (action.close === true && result !== false) close();
        });
    });

    if (onSubmit) {
        const form = submitFormId
            ? document.getElementById(submitFormId)
            : root.querySelector('form');
        if (form) {
            form.addEventListener('submit', async (event) => {
                event.preventDefault();
                const result = await onSubmit({
                    event,
                    form,
                    formData: new FormData(form),
                    values: formValues(form),
                    root,
                    close,
                });
                if (result === true) close();
            });
        }
    }

    if (onMount) onMount({ root, close });
}

/**
 * Close the current modal.
 */
export function closeModal() {
    const root = getRoot();
    root.innerHTML = '';
}

/**
 * Update just the modal body content without recreating the overlay/chrome.
 * Useful for dynamic content that changes during polling (e.g. QR login).
 * @param {string} html - New HTML for the modal body
 */
export function updateModalBody(html) {
    const body = document.querySelector('#modal-root .modal-body');
    if (body) body.innerHTML = html;
}

/**
 * Set raw HTML in the modal root (for custom modals like product detail).
 */
export function setModalHTML(html) {
    const root = getRoot();
    root.innerHTML = html;
}

/**
 * Confirm dialog — returns a Promise that resolves true/false.
 */
export function confirm(title, message) {
    return new Promise(resolve => {
        const root = getRoot();
        root.innerHTML = `
        <div class="confirm-overlay" id="confirm-overlay">
            <div class="confirm-box">
                <div class="confirm-title">${title}</div>
                <div class="confirm-msg">${message}</div>
                <div class="confirm-actions">
                    <button class="btn btn-ghost" id="confirm-cancel">Cancel</button>
                    <button class="btn btn-danger" id="confirm-ok">Confirm</button>
                </div>
            </div>
        </div>`;

        const cleanup = (result) => { root.innerHTML = ''; resolve(result); };
        document.getElementById('confirm-cancel').addEventListener('click', () => cleanup(false));
        document.getElementById('confirm-ok').addEventListener('click', () => cleanup(true));
        document.getElementById('confirm-overlay').addEventListener('click', e => {
            if (e.target.id === 'confirm-overlay') cleanup(false);
        });
    });
}
