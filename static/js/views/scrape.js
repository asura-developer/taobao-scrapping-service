/**
 * New Scrape Job view.
 */

import state, { notify } from '../state.js';
import { apiFetch, apiPost } from '../api.js';
import { requireFreshQrLogin } from '../auth-flow.js';
import { esc } from '../components/helpers.js';
import { showToast } from '../components/toast.js';
import { navigate } from '../router.js';

export function onEnter() { renderView(); }

async function startScraping() {
    if (!state.scrape.keyword || state.loading) return;
    state.loading = true;
    renderView();
    try {
        const loggedIn = await requireFreshQrLogin(state.scrape.platform);
        if (!loggedIn) {
            showToast('Login was not completed. Job was not created.', 'warn');
            return;
        }

        const body = {
            platform: state.scrape.platform,
            keyword: state.scrape.keyword,
            maxProducts: state.scrape.maxProducts,
            maxPages: state.scrape.maxPages,
            startPage: state.scrape.startPage,
            language: state.scrape.language,
            includeDetails: state.scrape.includeDetails,
        };
        const d = await apiPost('/scraper/search', body);
        if (d.success) {
            showToast('Scrape job started!', 'success');
            navigate('jobs');
        } else {
            showToast(d.error || 'Failed to start job', 'error');
        }
    } catch (e) {
        showToast(e.message, 'error');
    } finally {
        state.loading = false;
        renderView();
    }
}

async function startCategoryScrape() {
    if (!state.scrape.categoryId || state.loading) return;
    state.loading = true;
    renderView();
    try {
        const loggedIn = await requireFreshQrLogin(state.scrape.platform);
        if (!loggedIn) {
            showToast('Login was not completed. Job was not created.', 'warn');
            return;
        }

        const body = {
            platform: state.scrape.platform,
            categoryId: state.scrape.categoryId,
            maxProducts: state.scrape.maxProducts,
            maxPages: state.scrape.maxPages,
            language: state.scrape.language,
            includeDetails: state.scrape.includeDetails,
        };
        const d = await apiPost('/scraper/category', body);
        if (d.success) {
            showToast('Category scrape started!', 'success');
            navigate('jobs');
        } else {
            showToast(d.error || 'Failed to start category scrape', 'error');
        }
    } catch (e) {
        showToast(e.message, 'error');
    } finally {
        state.loading = false;
        renderView();
    }
}

// Expose to window for inline event handlers
window._scrapeStart = startScraping;
window._scrapeStartCategory = startCategoryScrape;

export function renderView() {
    const root = document.getElementById('view-root');
    if (!root) return;

    const S = state.scrape;
    const cats = state.categories[S.platform] || [];

    const langOptions = [
        ['en','English'],['zh','Chinese'],['th','Thai'],['ja','Japanese'],['ko','Korean'],['ru','Russian']
    ];

    root.innerHTML = `<div class="fade-in" style="max-width:580px">
  <div class="card">
    <div class="card-title">Platform</div>
    <div class="seg-group" style="width:100%;margin-bottom:20px">
      ${['taobao','tmall','1688'].map(p => `
      <button class="seg-btn${S.platform===p?' active':''}" style="flex:1"
        onclick="window._scrapeState('platform','${p}')">${p}</button>`).join('')}
    </div>

    <div class="card-title" style="margin-top:0">Mode</div>
    <div class="seg-group" style="width:100%;margin-bottom:20px">
      <button class="seg-btn${S.mode==='keyword'?' active':''}" style="flex:1"
        onclick="window._scrapeState('mode','keyword')">Keyword Search</button>
      <button class="seg-btn${S.mode==='category'?' active':''}" style="flex:1"
        onclick="window._scrapeState('mode','category')">Category Scrape</button>
    </div>

    ${S.mode === 'keyword' ? `
    <div class="card-title" style="margin-top:0">Quick Category</div>
    <div class="chip-wrap">
      ${cats.map(c => `<button class="chip${S.keyword===c?' active':''}"
        onclick="window._scrapeState('keyword','${c}')">${c}</button>`).join('')}
    </div>
    <div class="form-group">
      <label>Keyword</label>
      <input type="text" value="${esc(S.keyword)}"
        oninput="window._scrapeState('keyword',this.value)"
        onkeydown="if(event.key==='Enter') window._scrapeStart()"
        placeholder="e.g. \u7535\u8111, wireless earbuds, baby formula" />
    </div>` : `
    <div class="form-group">
      <label>Category ID</label>
      <input type="text" value="${esc(S.categoryId)}"
        oninput="window._scrapeState('categoryId',this.value)"
        onkeydown="if(event.key==='Enter') window._scrapeStartCategory()"
        placeholder="Enter category ID" />
    </div>`}

    <div class="grid-2" style="margin-bottom:16px">
      <div class="form-group">
        <label>Max Products</label>
        <input type="number" value="${S.maxProducts}" min="1" max="1000"
          oninput="window._scrapeState('maxProducts',Math.max(1,parseInt(this.value)||100))" />
      </div>
      <div class="form-group">
        <label>Max Pages</label>
        <input type="number" value="${S.maxPages}" min="1" max="100"
          oninput="window._scrapeState('maxPages',Math.max(1,parseInt(this.value)||10))" />
      </div>
    </div>

    ${S.mode === 'keyword' ? `
    <div class="grid-2" style="margin-bottom:16px">
      <div class="form-group">
        <label>Start Page</label>
        <input type="number" value="${S.startPage}" min="1" max="100"
          oninput="window._scrapeState('startPage',Math.max(1,parseInt(this.value)||1))" />
      </div>
      <div class="form-group">
        <label>Language</label>
        <select onchange="window._scrapeState('language',this.value)">
          ${langOptions.map(([v,l]) => `<option value="${v}"${S.language===v?' selected':''}>${l}</option>`).join('')}
        </select>
      </div>
    </div>` : `
    <div class="form-group" style="margin-bottom:16px">
      <label>Language</label>
      <select onchange="window._scrapeState('language',this.value)">
        ${langOptions.map(([v,l]) => `<option value="${v}"${S.language===v?' selected':''}>${l}</option>`).join('')}
      </select>
    </div>`}

    <div class="form-group" style="margin-bottom:20px">
      <label style="display:flex;align-items:center;gap:8px;text-transform:none;cursor:pointer;font-weight:400">
        <input type="checkbox" ${S.includeDetails?'checked':''} onchange="window._scrapeState('includeDetails',this.checked)"
          style="width:auto;accent-color:var(--amber)" />
        <span style="font-size:13px;color:var(--text2)">Scrape product details <span style="color:var(--muted)">(slower)</span></span>
      </label>
    </div>

    ${S.mode === 'keyword' && !S.keyword ? `
    <div style="padding:10px 14px;background:rgba(240,165,0,0.06);border:1px solid rgba(240,165,0,0.2);border-radius:var(--radius);font-size:12px;color:#d97706;margin-bottom:16px;font-family:var(--mono)">
      \u26a0 Enter a keyword or select a category above
    </div>` : ''}

    ${S.mode === 'keyword' ?
      `<button class="btn btn-primary btn-full" onclick="window._scrapeStart()" ${state.loading||!S.keyword?'disabled':''}>
        ${state.loading ? '\u23f3 Starting\u2026' : '\u25b6 Start Scraping'}
      </button>` :
      `<button class="btn btn-primary btn-full" onclick="window._scrapeStartCategory()" ${state.loading||!S.categoryId?'disabled':''}>
        ${state.loading ? '\u23f3 Starting\u2026' : '\u25b6 Start Category Scrape'}
      </button>`}
  </div>
</div>`;
}

// Helper to update scrape state from inline handlers
window._scrapeState = (key, val) => {
    state.scrape[key] = val;
    renderView();
};
