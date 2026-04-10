import { apiDelete, apiFetch, apiPost } from './api.js';
import { openModal, closeModal, updateModalBody } from './components/modal.js';
import { esc } from './components/helpers.js';

let activeFlow = null;

function renderQrModal(state) {
    if (state.status === 'loading') {
        return `<div class="qr-status qr-status-loading qr-pulse">Preparing a fresh login session...</div>`;
    }

    let html = '';
    if (state.qrImage) {
        html += `<div class="qr-container"><img src="${state.qrImage}" /></div>`;
    }

    if (state.status === 'waiting') {
        html += `<div class="qr-status qr-status-waiting qr-pulse">Scan the QR code to start this job</div>`;
    } else if (state.status === 'success') {
        html += `<div class="qr-status qr-status-success">${esc(state.message || 'Login successful')}</div>`;
    } else if (state.status === 'expired') {
        html += `<div class="qr-status qr-status-expired">${esc(state.message || 'QR code expired')}</div>`;
    } else if (state.status === 'error') {
        html += `<div class="qr-status qr-status-error">${esc(state.error || 'Login failed')}</div>`;
    }

    return html;
}

async function cancelActiveFlow() {
    const flow = activeFlow;
    if (!flow || flow.cancelled) return;
    flow.cancelled = true;
    if (flow.platform) {
        try {
            await apiPost(`/scraper/qr-login/${flow.platform}/cancel`, {}, { silent: true, toast: false });
        } catch (e) {}
    }
}

async function pollQrStatus(flow) {
    while (!flow.cancelled) {
        const res = await apiFetch(`/scraper/qr-login/${flow.platform}/status`, {}, { silent: true, toast: false });
        if (flow.cancelled) return false;

        if (!res.success) {
            flow.status = 'error';
            flow.error = res.error || 'Failed to check QR login status';
            updateModalBody(renderQrModal(flow));
            return false;
        }

        flow.status = res.data?.status || 'waiting';
        flow.message = res.data?.message || '';
        if (res.data?.qrImage) flow.qrImage = res.data.qrImage;
        updateModalBody(renderQrModal(flow));

        if (flow.status === 'success') return true;
        if (flow.status === 'expired') return false;

        await new Promise(resolve => setTimeout(resolve, 3000));
    }
    return false;
}

export async function requireFreshQrLogin(platform) {
    if (activeFlow) {
        return false;
    }

    const flow = {
        platform,
        status: 'loading',
        qrImage: null,
        message: '',
        error: '',
        cancelled: false,
    };
    activeFlow = flow;

    try {
        await apiDelete(`/scraper/clear-cookies/${platform}`, { silent: true, toast: false });

        openModal({
            title: `QR Code Login - ${platform}`,
            content: renderQrModal(flow),
            maxWidth: '420px',
            onClose: () => {
                cancelActiveFlow();
            },
        });

        const start = await apiPost(`/scraper/qr-login/${platform}`, {}, { silent: true, toast: false });
        if (!start.success || !start.data) {
            flow.status = 'error';
            flow.error = start.error || 'Failed to start QR login';
            updateModalBody(renderQrModal(flow));
            return false;
        }

        flow.status = 'waiting';
        flow.qrImage = start.data.qrImage;
        updateModalBody(renderQrModal(flow));

        const ok = await pollQrStatus(flow);
        if (!ok) return false;

        await new Promise(resolve => setTimeout(resolve, 900));
        closeModal();
        return true;
    } finally {
        activeFlow = null;
    }
}
