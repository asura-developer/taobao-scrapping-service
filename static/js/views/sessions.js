/**
 * Sessions view — login sessions, cookies, QR login, proxies.
 */

import state from '../state.js';
import { apiFetch, apiPost, apiDelete } from '../api.js';
import { esc } from '../components/helpers.js';
import { showToast } from '../components/toast.js';
import { openModal, closeModal, updateModalBody } from '../components/modal.js';

export async function onEnter() {
    await fetchSessionStatus();
}

async function fetchSessionStatus() {
    state.sessionLoading = true;
    renderView();
    try {
        const [sess, prx] = await Promise.all([
            apiFetch('/scraper/session-status', {}, { silent: true, toast: false }),
            apiFetch('/scraper/proxy-status', {}, { silent: true, toast: false }),
        ]);
        if (sess.success) state.sessionStatus = sess.data;
        if (prx.success) state.proxyStatus = prx.data;
    } catch (e) {}
    state.sessionLoading = false;
    renderView();
}

async function importCookies() {
    const plat = state.cookieImport.platform;
    const text = state.cookieImport.text.trim();
    if (!text) return showToast('Paste cookies JSON first', 'warn');
    let cookies;
    try { cookies = JSON.parse(text); } catch (e) { return showToast('Invalid JSON: ' + e.message, 'error'); }
    if (!Array.isArray(cookies)) return showToast('Cookies must be a JSON array', 'error');
    const res = await apiPost('/scraper/import-cookies', { platform: plat, cookies });
    if (res.success) {
        state.cookieImport.text = '';
        showToast(res.message || 'Cookies imported!', 'success');
        fetchSessionStatus();
    } else { showToast(res.error || 'Import failed', 'error'); }
}

async function clearCookies(platform) {
    const res = await apiDelete(`/scraper/clear-cookies/${platform}`);
    if (res.success) { showToast('Cookies cleared', 'success'); fetchSessionStatus(); }
    else showToast(res.error || 'Failed', 'error');
}

async function exportCookies(platform) {
    const res = await apiFetch(`/scraper/export-cookies/${platform}`, {}, { silent: true, toast: false });
    if (res.success && res.data?.cookies) {
        const blob = new Blob([JSON.stringify(res.data.cookies, null, 2)], { type: 'application/json' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = platform === '1688' ? 'cookies-1688.json' : 'cookies.json';
        a.click();
    } else { showToast(res.error || 'No cookies found', 'error'); }
}

function renderQrModalBody() {
    const qr = state.qrLogin;
    if (qr.status === 'loading') {
        return `<div class="qr-status qr-status-loading qr-pulse">Starting browser...</div>`;
    }
    let html = '';
    if (qr.qrImage) {
        html += `<div class="qr-container"><img src="${qr.qrImage}" /></div>`;
    }
    if (qr.status === 'waiting') {
        html += `<div class="qr-status qr-status-waiting qr-pulse">Scan the QR code with your mobile app</div>`;
    } else if (qr.status === 'success') {
        html += `<div class="qr-status qr-status-success">${esc(qr.message || 'Login successful!')}</div>`;
    } else if (qr.status === 'expired') {
        html += `<div class="qr-status qr-status-expired">${esc(qr.message || 'QR code expired')}</div>`;
        html += `<div style="text-align:center;margin-top:12px"><button class="btn btn-primary btn-sm" onclick="window._startQrLogin('${esc(qr.platform)}')">Retry</button></div>`;
    } else if (qr.status === 'error') {
        html += `<div class="qr-status qr-status-error">${esc(qr.error || 'Error')}</div>`;
    }
    return html;
}

async function startQrLogin(platform) {
    state.qrLogin = { active: true, platform, qrImage: null, status: 'loading', polling: false };
    openModal({
        title: `QR Code Login \u2014 ${platform}`,
        content: renderQrModalBody(),
        maxWidth: '420px',
        onClose: () => cancelQrLogin(),
    });
    const res = await apiPost(`/scraper/qr-login/${platform}`, {});
    if (res.success && res.data) {
        state.qrLogin.qrImage = res.data.qrImage;
        state.qrLogin.status = 'waiting';
        state.qrLogin.polling = true;
        updateModalBody(renderQrModalBody());
        pollQrStatus(platform);
    } else {
        state.qrLogin.status = 'error';
        state.qrLogin.error = res.error || 'Failed to start QR login';
        updateModalBody(renderQrModalBody());
    }
}

async function pollQrStatus(platform) {
    if (!state.qrLogin.polling) return;
    try {
        const res = await apiFetch(`/scraper/qr-login/${platform}/status`, {}, { silent: true, toast: false });
        if (!state.qrLogin.polling) return;
        if (res.success && res.data) {
            state.qrLogin.status = res.data.status;
            if (res.data.qrImage) state.qrLogin.qrImage = res.data.qrImage;
            if (res.data.status === 'success') {
                state.qrLogin.polling = false;
                state.qrLogin.message = res.data.message;
                updateModalBody(renderQrModalBody());
                setTimeout(() => {
                    closeModal();
                    state.qrLogin = { active: false, platform: null, qrImage: null, status: null, polling: false };
                    fetchSessionStatus();
                }, 2000);
                return;
            }
            if (res.data.status === 'expired') {
                state.qrLogin.polling = false;
                state.qrLogin.message = 'QR code expired. Click Retry to try again.';
                updateModalBody(renderQrModalBody());
                return;
            }
        }
        updateModalBody(renderQrModalBody());
        setTimeout(() => pollQrStatus(platform), 3000);
    } catch (e) {
        state.qrLogin.polling = false;
        state.qrLogin.status = 'error';
        state.qrLogin.error = e.message;
        updateModalBody(renderQrModalBody());
    }
}

async function cancelQrLogin() {
    const wasPlatform = state.qrLogin.platform;
    state.qrLogin.polling = false;
    state.qrLogin = { active: false, platform: null, qrImage: null, status: null, polling: false };
    if (wasPlatform) {
        try { await apiPost(`/scraper/qr-login/${wasPlatform}/cancel`, {}); } catch (e) {}
    }
}

async function setGateway() {
    const input = document.getElementById('gateway-input');
    const sticky = document.getElementById('gateway-sticky');
    if (!input?.value.trim()) return showToast('Enter a gateway proxy URL', 'warn');
    const res = await apiPost('/scraper/proxy/gateway', { url: input.value.trim(), sticky: sticky?.checked || false });
    if (res.success) { input.value = ''; if (sticky) sticky.checked = false; showToast('Gateway set', 'success'); fetchSessionStatus(); }
    else showToast(res.error || 'Failed', 'error');
}

async function addProxy() {
    const input = document.getElementById('proxy-input');
    if (!input?.value.trim()) return showToast('Enter a proxy URL', 'warn');
    const res = await apiPost('/scraper/proxy', { proxy: input.value.trim() });
    if (res.success) { input.value = ''; showToast('Proxy added', 'success'); fetchSessionStatus(); }
    else showToast(res.error || 'Failed', 'error');
}

async function removeProxy(hostPort) {
    const res = await apiDelete(`/scraper/proxy/${encodeURIComponent(hostPort)}`);
    if (res.success) { showToast('Proxy removed', 'success'); fetchSessionStatus(); }
    else showToast(res.error || 'Failed', 'error');
}

// Window bindings
window._refreshSessions = fetchSessionStatus;
window._importCookies = importCookies;
window._clearCookies = clearCookies;
window._exportCookies = exportCookies;
window._startQrLogin = startQrLogin;
window._cancelQrLogin = cancelQrLogin;
window._setGateway = setGateway;
window._addProxy = addProxy;
window._removeProxy = removeProxy;
window._setCookieImport = (key, val) => { state.cookieImport[key] = val; };

export function renderView() {
    const root = document.getElementById('view-root');
    if (!root) return;

    if (state.sessionLoading && !state.sessionStatus) {
        root.innerHTML = `<div class="card"><div class="empty"><div class="empty-icon">\u23f3</div><div class="empty-text">Checking sessions\u2026</div></div></div>`;
        return;
    }

    const statusColor = s => ({ ok: 'var(--green-l)', expiring: 'var(--amber)' }[s] || 'var(--red-l)');
    const statusIcon = s => ({ ok: '\u2705', expiring: '\u26a0\ufe0f', missing: '\ud83d\udd11' }[s] || '\u274c');

    const renderCard = (key, info) => {
        if (!info) return '';
        const isPair = key === 'taobao_tmall';
        const platforms = isPair
            ? `<span class="badge badge-taobao" style="margin-right:4px">taobao</span><span class="badge badge-tmall">tmall</span>`
            : `<span class="badge badge-1688">1688</span>`;
        const platKey = isPair ? 'taobao' : '1688';

        return `<div class="card" style="border-left:3px solid ${statusColor(info.status)}">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px">
        <div style="display:flex;align-items:center;gap:10px">
          <span style="font-size:22px">${statusIcon(info.status)}</span>
          <div>
            <div style="display:flex;gap:6px;align-items:center;margin-bottom:4px">${platforms}</div>
            <div style="font-family:var(--mono);font-size:10px;color:var(--muted)">${esc(info.file || '')}</div>
          </div>
        </div>
        <div style="text-align:right">
          <div style="font-family:var(--mono);font-size:11px;font-weight:600;color:${statusColor(info.status)}">${(info.status || '').toUpperCase()}</div>
          ${info.total ? `<div style="font-family:var(--mono);font-size:10px;color:var(--muted);margin-top:2px">${info.total} cookies</div>` : ''}
        </div>
      </div>
      <div style="background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);padding:12px 14px;margin-bottom:14px">
        <div style="font-size:12px;color:${statusColor(info.status)}">${esc(info.message || '')}</div>
        ${info.expiresInHours != null && info.status !== 'expired' ? `
        <div style="font-family:var(--mono);font-size:10px;color:var(--muted);margin-top:6px">
          Earliest expiry: <span style="color:${statusColor(info.status)}">${info.expiresInHours}h</span>
        </div>` : ''}
      </div>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn btn-ghost btn-sm" onclick="window._startQrLogin('${platKey}')">\ud83d\udcf1 QR Login</button>
        <button class="btn btn-ghost btn-sm" onclick="window._exportCookies('${platKey}')">\ud83d\udce4 Export</button>
        <button class="btn btn-ghost btn-sm" style="color:var(--red-l)" onclick="window._clearCookies('${platKey}')">\ud83d\uddd1 Clear</button>
        <button class="btn btn-ghost btn-sm" onclick="window._refreshSessions()">\u21bb Refresh</button>
      </div>
    </div>`;
    };

    const st = state.sessionStatus;

    // Proxy section
    const px = state.proxyStatus;
    let proxyBlock = '';
    if (px) {
        const statusBorder = px.enabled ? 'var(--green-l)' : 'var(--muted)';
        const statusI = px.enabled ? '\ud83d\udd00' : '\ud83d\udd13';
        const modeLabel = px.mode === 'gateway' ? 'Gateway' : px.mode === 'pool' ? 'Static Pool' : '';
        const statusTitle = px.enabled ? `Proxy Active \u2014 ${modeLabel}${px.stickySessions ? ' (sticky)' : ''}` : 'Proxy Rotation Disabled';
        const statusSub = px.enabled
            ? `${px.available} available / ${px.total} total / ${px.dead} dead`
            : 'No proxies configured';

        proxyBlock = `<div class="card" style="border-left:3px solid ${statusBorder}">
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px">
        <span style="font-size:20px">${statusI}</span>
        <div>
          <div style="font-size:13px;font-weight:500">${statusTitle}</div>
          <div style="font-family:var(--mono);font-size:10px;color:var(--muted)">${statusSub}</div>
        </div>
      </div>
      ${px.proxies?.length ? `<div style="overflow-x:auto">
        <table class="data-table">
          <thead><tr><th>HOST</th><th style="text-align:center">TYPE</th><th style="text-align:center">PROTO</th><th style="text-align:center">REQS</th><th style="text-align:center">SUCCESS</th><th style="text-align:center">STATUS</th><th></th></tr></thead>
          <tbody>${px.proxies.map(p => `<tr>
            <td style="font-family:var(--mono)">${esc(p.host)}</td>
            <td style="text-align:center"><span style="font-size:10px;padding:2px 6px;border-radius:3px;background:${p.type==='gateway'?'rgba(96,165,250,0.15);color:var(--blue-l)':'rgba(160,168,181,0.15);color:var(--text2)'}">${p.type}</span></td>
            <td style="text-align:center">${p.protocol}</td>
            <td style="text-align:center;font-family:var(--mono)">${p.requests}</td>
            <td style="text-align:center;font-family:var(--mono);color:${p.successRate>=80?'var(--green-l)':p.successRate>=50?'var(--amber)':'var(--red-l)'}">${p.successRate}%</td>
            <td style="text-align:center">${p.dead?'<span style="color:var(--red-l)">DEAD</span>':'<span style="color:var(--green-l)">OK</span>'}</td>
            <td><button class="btn btn-ghost btn-sm" style="color:var(--red-l);padding:2px 6px;font-size:10px" onclick="window._removeProxy('${esc(p.host)}')">\u2715</button></td>
          </tr>`).join('')}</tbody>
        </table>
      </div>` : ''}
      <div style="margin-top:14px;padding-top:14px;border-top:1px solid var(--border)">
        <div style="font-family:var(--mono);font-size:10px;color:var(--muted);margin-bottom:8px">ROTATING GATEWAY</div>
        <div style="display:flex;gap:8px;align-items:center;margin-bottom:6px">
          <input id="gateway-input" type="text" placeholder="http://user:pass@gate.smartproxy.com:7777"
            style="flex:1;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);padding:6px 10px;color:var(--text);font-family:var(--mono);font-size:11px" />
          <button class="btn btn-primary btn-sm" onclick="window._setGateway()">Set</button>
        </div>
        <div style="display:flex;align-items:center;gap:6px">
          <input id="gateway-sticky" type="checkbox" style="accent-color:var(--amber)" />
          <label style="font-size:11px;color:var(--text2);cursor:pointer">Sticky sessions</label>
        </div>
      </div>
      <div style="margin-top:14px;padding-top:14px;border-top:1px solid var(--border)">
        <div style="font-family:var(--mono);font-size:10px;color:var(--muted);margin-bottom:8px">ADD STATIC PROXY</div>
        <div style="display:flex;gap:8px;align-items:center">
          <input id="proxy-input" type="text" placeholder="protocol://user:pass@host:port"
            style="flex:1;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);padding:6px 10px;color:var(--text);font-family:var(--mono);font-size:11px" />
          <button class="btn btn-primary btn-sm" onclick="window._addProxy()">Add</button>
        </div>
      </div>
    </div>`;
    }

    root.innerHTML = `<div class="fade-in" style="max-width:640px;display:flex;flex-direction:column;gap:14px">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:2px">
    <span style="font-family:var(--mono);font-size:11px;color:var(--muted)">LOGIN SESSION HEALTH</span>
    <button class="btn btn-ghost btn-sm" onclick="window._refreshSessions()">\u21bb Refresh</button>
  </div>
  <div style="background:rgba(37,99,235,0.06);border:1px solid rgba(37,99,235,0.2);border-radius:var(--radius);padding:12px 16px;font-size:12px;color:var(--text2);line-height:1.7">
    <strong style="color:var(--text)">Cookie files:</strong><br>
    Taobao &amp; Tmall \u2192 <code style="font-family:var(--mono);color:var(--amber)">utils/cookies.json</code><br>
    1688 \u2192 <code style="font-family:var(--mono);color:var(--blue-l)">utils/cookies-1688.json</code>
  </div>
  ${st ? renderCard('taobao_tmall', st.taobao_tmall) : ''}
  ${st ? renderCard('1688', st['1688']) : ''}
  ${!st ? `<div class="card"><div class="empty"><div class="empty-icon">\ud83d\udd11</div><div class="empty-text">No session data \u2014 click Refresh</div></div></div>` : ''}
  <div style="margin-top:10px">
    <span style="font-family:var(--mono);font-size:11px;color:var(--muted)">IMPORT COOKIES</span>
  </div>
  <div class="card">
    <div style="font-size:12px;color:var(--text2);margin-bottom:12px;line-height:1.6">
      Paste cookies exported from a browser extension (e.g. <strong style="color:var(--text)">Cookie-Editor</strong>). Export as JSON array.
    </div>
    <div style="display:flex;gap:8px;margin-bottom:10px;align-items:center">
      <label style="font-family:var(--mono);font-size:10px;color:var(--muted)">PLATFORM</label>
      <select style="background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);padding:4px 8px;color:var(--text);font-size:12px"
              onchange="window._setCookieImport('platform',this.value)">
        <option value="taobao" ${state.cookieImport.platform==='taobao'?'selected':''}>Taobao / Tmall</option>
        <option value="1688" ${state.cookieImport.platform==='1688'?'selected':''}>1688</option>
      </select>
    </div>
    <textarea rows="6"
      placeholder='[{"name":"cookie_name","value":"cookie_value","domain":".taobao.com",...}]'
      style="width:100%;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);padding:10px;color:var(--text);font-family:var(--mono);font-size:11px;resize:vertical;margin-bottom:10px"
      oninput="window._setCookieImport('text',this.value)">${esc(state.cookieImport.text)}</textarea>
    <button class="btn btn-primary btn-sm" onclick="window._importCookies()">\ud83d\udce5 Import Cookies</button>
  </div>
  <div style="margin-top:10px">
    <span style="font-family:var(--mono);font-size:11px;color:var(--muted)">PROXY POOL</span>
  </div>
  ${proxyBlock || `<div class="card"><div class="empty"><div class="empty-text">No proxy data \u2014 click Refresh</div></div></div>`}
</div>`;
}
