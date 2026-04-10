/**
 * Search view — full-text product search.
 */

import state, { registerProducts } from '../state.js';
import { apiFetch } from '../api.js';
import { esc, platformBadge } from '../components/helpers.js';
import { openProductModal } from './product-detail.js';

let searchDebounce = null;

export function onEnter() { renderView(); }

function onSearchInput(val) {
    state.search.query = val;
    clearTimeout(searchDebounce);
    searchDebounce = setTimeout(() => { if (val.trim().length >= 2) executeSearch(0); }, 500);
}

async function executeSearch(page = 0) {
    const inp = document.getElementById('search-input');
    const query = inp ? inp.value.trim() : state.search.query.trim();
    if (!query) return;
    state.search.query = query;
    state.search.page = page;
    state.searchLoading = true;
    patchResults();
    try {
        const params = new URLSearchParams({ q: query, page: page + 1, limit: state.search.size });
        const d = await apiFetch(`/products/search/text?${params}`, {}, { silent: true, toast: false });
        if (d.success) {
            const products = d.data.products || [];
            registerProducts(products);
            state.searchResults = {
                items: products.map(p => ({ itemId: p.itemId, title: p.title, subtitle: `\u00a5${p.price||'?'} \u00b7 ${p.platform}`, platform: p.platform })),
                total: d.data.count || 0,
                query,
                hasMore: products.length >= state.search.size,
            };
        } else { state.searchResults = null; }
    } catch (e) { state.searchResults = null; }
    state.searchLoading = false;
    patchResults();
}

function patchResults() {
    const el = document.getElementById('search-results');
    if (el) el.innerHTML = buildResultsHTML();
}

function buildResultsHTML() {
    if (state.searchLoading) return `<div class="card"><div class="empty"><div class="empty-icon">\u23f3</div><div class="empty-text">Searching\u2026</div></div></div>`;
    const sr = state.searchResults;
    if (!sr) return `<div class="card"><div class="empty"><div class="empty-icon">\ud83d\udd0d</div><div class="empty-text">Enter a query to search</div></div></div>`;
    if (!sr.items.length) return `<div class="card"><div class="empty"><div class="empty-icon">\ud83e\udd37</div><div class="empty-text">No results for "${esc(sr.query)}"</div></div></div>`;

    return `
    <div class="search-result-list fade-in">
      ${sr.items.map(r => `
      <div class="search-result-item" onclick="window._openSearchResult('${esc(r.itemId)}')">
        <div class="result-icon">\ud83d\udce6</div>
        <div>
          <div style="font-size:13px;font-weight:500;color:var(--text);line-height:1.4">${esc(r.title)}</div>
          <div style="font-size:12px;color:var(--muted);margin-top:2px;font-family:var(--mono)">${esc(r.subtitle)}</div>
        </div>
        <div>${platformBadge(r.platform)}</div>
      </div>`).join('')}
    </div>
    ${sr.hasMore ? `
    <div class="pagination">
      <button class="btn btn-ghost btn-sm" onclick="window._searchPage(${state.search.page-1})" ${state.search.page<=0?'disabled':''}>\u2190 Prev</button>
      <span class="page-info">Page ${state.search.page+1}</span>
      <button class="btn btn-ghost btn-sm" onclick="window._searchPage(${state.search.page+1})">Next \u2192</button>
    </div>` : ''}`;
}

window._onSearchInput = onSearchInput;
window._executeSearch = executeSearch;
window._searchPage = executeSearch;
window._openSearchResult = (itemId) => openProductModal(itemId);
window._clearSearch = () => {
    state.search.query = '';
    state.searchResults = null;
    const inp = document.getElementById('search-input');
    if (inp) inp.value = '';
    patchResults();
};

export function renderView() {
    const root = document.getElementById('view-root');
    if (!root) return;

    root.innerHTML = `<div style="max-width:840px">
  <div class="search-row">
    <input id="search-input" class="search-input" type="text" autocomplete="off"
      value="${esc(state.search.query)}"
      placeholder="Search products by title, description, shop\u2026"
      oninput="window._onSearchInput(this.value)"
      onkeydown="if(event.key==='Enter') window._executeSearch(0)" />
    <button class="btn btn-primary" onclick="window._executeSearch(0)" ${state.searchLoading?'disabled':''}>
      ${state.searchLoading ? '\u23f3' : '\u2315 Search'}
    </button>
    ${state.search.query ? `<button class="btn btn-ghost" onclick="window._clearSearch()">\u2715</button>` : ''}
  </div>
  <div id="search-results">${buildResultsHTML()}</div>
</div>`;
}
