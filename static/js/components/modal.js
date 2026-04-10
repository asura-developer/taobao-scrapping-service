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

/**
 * Open a modal with content HTML.
 * @param {object} opts - { title, content, maxWidth, onClose }
 */
export function openModal({ title = '', content = '', maxWidth = '780px', onClose } = {}) {
    const root = getRoot();
    root.innerHTML = `
    <div class="modal-overlay" id="modal-overlay">
        <div class="modal" style="max-width:${maxWidth}">
            <div class="modal-header">
                <div class="modal-title">${title}</div>
                <button class="modal-close" id="modal-close-btn">\u00d7</button>
            </div>
            <div class="modal-body">${content}</div>
        </div>
    </div>`;

    const overlay = document.getElementById('modal-overlay');
    const closeBtn = document.getElementById('modal-close-btn');

    const close = () => {
        closeModal();
        if (onClose) onClose();
    };

    overlay.addEventListener('click', e => { if (e.target === overlay) close(); });
    closeBtn.addEventListener('click', close);
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
