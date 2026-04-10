/**
 * Batch Details view.
 */

import state from '../state.js';
import { apiPost } from '../api.js';
import { requireFreshQrLogin } from '../auth-flow.js';
import { esc } from '../components/helpers.js';
import { showToast } from '../components/toast.js';
import { navigate } from '../router.js';

export function onEnter() { renderView(); }

async function startBatchDetails() {
    if (state.batchLoading) return;
    state.batchLoading = true;
    renderView();
    try {
        const b = state.batch;
        const payload = {
            mode: b.mode,
            language: b.language,
            delayMin: parseInt(b.delayMin) || 5000,
            delayMax: parseInt(b.delayMax) || 12000,
        };
        if (b.platform) payload.platform = b.platform;
        if (b.limit) payload.limit = parseInt(b.limit);
        if (b.keyword) payload.keyword = b.keyword;
        if (b.categoryName) payload.categoryName = b.categoryName;
        if (b.minQuality) payload.minQuality = parseInt(b.minQuality);

        let d = await apiPost('/scraper/scrape-pending-details', payload, { toast: false });
        if (
            d._status === 409 &&
            b.platform &&
            b.platform !== 'alibaba' &&
            `${d.detail || d.error || d.message || ''}`.includes('Fresh QR login required')
        ) {
            const loggedIn = await requireFreshQrLogin(b.platform);
            if (!loggedIn) {
                showToast('Login was not completed. Batch detail job was not created.', 'warn');
                return;
            }
            d = await apiPost('/scraper/scrape-pending-details', payload, { toast: false });
        }

        if (d.success) {
            showToast('Batch detail scrape started!', 'success');
            navigate('jobs');
        } else {
            showToast(d.detail || d.error || d.message || 'Failed', 'error');
        }
    } catch (e) {
        showToast(e.message, 'error');
    }
    state.batchLoading = false;
    renderView();
}

window._startBatch = startBatchDetails;
window._batchState = (key, val) => { state.batch[key] = val; renderView(); };

export function renderView() {
    const root = document.getElementById('view-root');
    if (!root) return;

    const b = state.batch;
    const langOptions = [
        ['en','English'],['zh','Chinese (no translation)'],['th','Thai'],['ja','Japanese'],['ko','Korean'],['ru','Russian']
    ];

    root.innerHTML = `<div class="fade-in" style="max-width:580px">
  <div class="card">
    <div class="card-title">Mode</div>
    <div class="seg-group" style="width:100%;margin-bottom:20px">
      ${['pending','all','low'].map(m => `
      <button class="seg-btn${b.mode===m?' active':''}" style="flex:1"
        onclick="window._batchState('mode','${m}')">${m === 'pending' ? 'Pending' : m === 'all' ? 'All' : 'Low Quality'}</button>`).join('')}
    </div>
    <div style="padding:8px 12px;background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);font-size:12px;color:var(--text2);margin-bottom:20px;line-height:1.6">
      ${b.mode === 'pending' ? '\ud83d\udccb Only products that haven\'t been detail-scraped yet' :
        b.mode === 'all' ? '\ud83d\udd04 Re-scrape all products matching filters (overwrites existing details)' :
        '\u26a0 Only products with extraction quality below threshold'}
    </div>

    <div class="card-title">Platform Filter</div>
    <div class="seg-group" style="width:100%;margin-bottom:20px">
      <button class="seg-btn${!b.platform?' active':''}" style="flex:1"
        onclick="window._batchState('platform','')">All</button>
      ${['taobao','tmall','1688'].map(p => `
      <button class="seg-btn${b.platform===p?' active':''}" style="flex:1"
        onclick="window._batchState('platform','${p}')">${p}</button>`).join('')}
    </div>

    <div class="grid-2" style="margin-bottom:16px">
      <div class="form-group">
        <label>Keyword Filter</label>
        <input type="text" value="${esc(b.keyword)}" oninput="window._batchState('keyword',this.value)" placeholder="Filter by search keyword" />
      </div>
      <div class="form-group">
        <label>Category Filter</label>
        <input type="text" value="${esc(b.categoryName)}" oninput="window._batchState('categoryName',this.value)" placeholder="Filter by category name" />
      </div>
    </div>

    <div class="grid-2" style="margin-bottom:16px">
      <div class="form-group">
        <label>Limit (products)</label>
        <input type="number" value="${esc(b.limit)}" oninput="window._batchState('limit',this.value)" min="1" placeholder="All matching" />
      </div>
      <div class="form-group">
        <label>Min Quality</label>
        <select onchange="window._batchState('minQuality',this.value)">
          <option value="" ${!b.minQuality?'selected':''}>Default (50)</option>
          <option value="30" ${b.minQuality==='30'?'selected':''}>30%</option>
          <option value="50" ${b.minQuality==='50'?'selected':''}>50%</option>
          <option value="70" ${b.minQuality==='70'?'selected':''}>70%</option>
          <option value="80" ${b.minQuality==='80'?'selected':''}>80%</option>
        </select>
      </div>
    </div>

    <div class="card-title">Delay Between Requests</div>
    <div class="grid-2" style="margin-bottom:20px">
      <div class="form-group">
        <label>Min Delay (ms)</label>
        <input type="number" value="${b.delayMin}" min="1000" step="1000"
          oninput="window._batchState('delayMin',Math.max(1000,parseInt(this.value)||5000))" />
      </div>
      <div class="form-group">
        <label>Max Delay (ms)</label>
        <input type="number" value="${b.delayMax}" min="2000" step="1000"
          oninput="window._batchState('delayMax',Math.max(2000,parseInt(this.value)||12000))" />
      </div>
    </div>

    <div class="form-group" style="margin-bottom:20px">
      <label>Translate To</label>
      <select onchange="window._batchState('language',this.value)">
        ${langOptions.map(([v,l]) => `<option value="${v}"${b.language===v?' selected':''}>${l}</option>`).join('')}
      </select>
    </div>

    <button class="btn btn-primary btn-full" onclick="window._startBatch()" ${state.batchLoading?'disabled':''}>
      ${state.batchLoading ? '\u23f3 Starting\u2026' : '\u25b6 Start Batch Detail Scraping'}
    </button>
  </div>
</div>`;
}
