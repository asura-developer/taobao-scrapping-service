/**
 * Products view — browse and manage scraped products.
 */

import state, { registerProducts } from '../state.js';
import { apiFetch, apiPost, apiDelete, apiDownload } from '../api.js';
import { esc, platformBadge, qualityColor, loadingState } from '../components/helpers.js';
import { showToast } from '../components/toast.js';
import { openProductModal } from './product-detail.js';

export async function onEnter() {
    await fetchProducts();
}

async function fetchProducts() {
    state.loading = true;
    renderView();
    const p = new URLSearchParams();
    Object.entries(state.filters).forEach(([k, v]) => { if (v !== '' && v !== null) p.append(k, v); });
    try {
        const d = await apiFetch(`/products?${p}`, {}, { silent: true, toast: false });
        if (d.success) {
            state.products = d.data.products || [];
            state.pagination = d.data.pagination || { page: 1, total: 0, pages: 0 };
            registerProducts(state.products);
        }
    } catch (e) {}
    state.loading = false;
    renderView();
}

function exportProducts(format) {
    const p = new URLSearchParams();
    if (state.filters.platform) p.append('platform', state.filters.platform);
    if (state.filters.keyword) p.append('keyword', state.filters.keyword);
    if (state.filters.detailsScraped) p.append('detailsScraped', state.filters.detailsScraped);
    if (state.filters.minQuality) p.append('minQuality', state.filters.minQuality);
    apiDownload(`/products/export/${format}?${p}`);
}

async function scrapeDetails(itemId) {
    const d = await apiPost(`/scraper/details/${itemId}`, {});
    if (d.success) { showToast('Detail scrape started', 'success'); fetchProducts(); }
    else showToast(d.error || 'Failed', 'error');
}

async function deleteProduct(itemId) {
    const d = await apiDelete(`/products/${itemId}`);
    if (d.success) { showToast('Product deleted', 'success'); fetchProducts(); }
    else showToast(d.error || 'Failed', 'error');
}

// Expose to window
window._fetchProducts = fetchProducts;
window._exportProducts = exportProducts;
window._scrapeDetails = scrapeDetails;
window._deleteProduct = deleteProduct;
window._openProduct = (itemId) => openProductModal(itemId);
window._setFilter = (key, val) => { state.filters[key] = val; };
window._applyFilters = () => { state.filters.page = 1; fetchProducts(); };
window._clearFilters = () => {
    state.filters = { platform: '', keyword: '', shopName: '', detailsScraped: '', minQuality: '', page: 1, limit: 20 };
    fetchProducts();
};
window._prevPage = () => { state.filters.page--; fetchProducts(); };
window._nextPage = () => { state.filters.page++; fetchProducts(); };

export function renderView() {
    const root = document.getElementById('view-root');
    if (!root) return;

    const f = state.filters;
    const filtersHtml = `<div class="card" style="margin-bottom:14px">
  <div class="filter-row" style="margin-bottom:10px">
    <div class="form-group" style="min-width:130px">
      <label>Platform</label>
      <select onchange="window._setFilter('platform',this.value);window._applyFilters()">
        <option value="" ${!f.platform?'selected':''}>All</option>
        ${['taobao','tmall','1688'].map(p => `<option value="${p}" ${f.platform===p?'selected':''}>${p}</option>`).join('')}
      </select>
    </div>
    <div class="form-group" style="min-width:140px">
      <label>Details</label>
      <select onchange="window._setFilter('detailsScraped',this.value);window._applyFilters()">
        <option value="" ${f.detailsScraped===''?'selected':''}>All</option>
        <option value="true" ${f.detailsScraped==='true'?'selected':''}>With Details</option>
        <option value="false" ${f.detailsScraped==='false'?'selected':''}>Without</option>
      </select>
    </div>
    <div class="form-group" style="min-width:130px">
      <label>Min Quality</label>
      <select onchange="window._setFilter('minQuality',this.value);window._applyFilters()">
        <option value="" ${!f.minQuality?'selected':''}>Any</option>
        <option value="80">High (80%+)</option>
        <option value="50">Medium (50%+)</option>
      </select>
    </div>
    <div class="form-group">
      <label>&nbsp;</label>
      <button class="btn btn-ghost" onclick="window._clearFilters()">Clear</button>
    </div>
  </div>
  <div class="grid-2">
    <input type="text" placeholder="Search keyword\u2026" value="${esc(f.keyword)}"
      oninput="window._setFilter('keyword',this.value)"
      onkeydown="if(event.key==='Enter') window._applyFilters()" />
    <input type="text" placeholder="Shop name\u2026" value="${esc(f.shopName)}"
      oninput="window._setFilter('shopName',this.value)"
      onkeydown="if(event.key==='Enter') window._applyFilters()" />
  </div>
</div>`;

    if (state.loading) {
        root.innerHTML = filtersHtml + loadingState();
        return;
    }
    if (!state.products.length) {
        root.innerHTML = filtersHtml + `<div class="card"><div class="empty"><div class="empty-icon">\ud83d\udce6</div><div class="empty-text">No products found</div></div></div>`;
        return;
    }

    root.innerHTML = filtersHtml + `
<div class="product-grid fade-in">
  ${state.products.map(p => {
    const shop = p.shopInfo?.shopName || p.shopName || '';
    const q = p.extractionQuality;
    return `<div class="product-card" onclick="window._openProduct('${esc(p.itemId)}')">
      <img class="product-img" src="${esc(p.image || '')}" alt="" onerror="this.style.display='none'" loading="lazy"/>
      <div class="product-body">
        <div style="display:flex;gap:5px;flex-wrap:wrap">
          ${platformBadge(p.platform)}
          ${p.detailsScraped ? '<span class="badge badge-success">\u2713 Detail</span>' : ''}
        </div>
        <div class="product-title">${esc(p.title)}</div>
        <div class="product-price">\u00a5${esc(p.price || '\u2014')}</div>
        ${shop ? `<div class="product-meta">\ud83c\udfea ${esc(shop)}</div>` : ''}
        ${p.salesCount ? `<div class="product-meta">\ud83d\udce6 ${esc(p.salesCount)} sold</div>` : ''}
        ${q ? `<div>
          <div style="display:flex;justify-content:space-between;font-size:10px;font-family:var(--mono);color:var(--muted);margin-bottom:3px">
            <span>Quality</span><span style="color:${qualityColor(q)}">${q}%</span>
          </div>
          <div class="quality-bar"><div class="quality-fill" style="width:${q}%;background:${qualityColor(q)}"></div></div>
        </div>` : ''}
      </div>
      <div class="product-footer">
        ${!p.detailsScraped ? `<button class="btn btn-primary" onclick="event.stopPropagation();window._scrapeDetails('${esc(p.itemId)}')">+ Details</button>` : ''}
        <button class="btn btn-ghost" onclick="event.stopPropagation();window._openProduct('${esc(p.itemId)}')">View \u2192</button>
      </div>
    </div>`;
  }).join('')}
</div>
<div class="pagination">
  <button class="btn btn-ghost btn-sm" onclick="window._prevPage()" ${f.page<=1?'disabled':''}>\u2190 Prev</button>
  <span class="page-info">Page ${state.pagination.page || 1} \u00b7 ${(state.pagination.total || 0).toLocaleString()} total</span>
  <button class="btn btn-ghost btn-sm" onclick="window._nextPage()" ${f.page>=(state.pagination.pages||1)?'disabled':''}>Next \u2192</button>
</div>
<div style="display:flex;gap:8px;margin-top:10px;justify-content:flex-end">
  <button class="btn btn-ghost btn-sm" onclick="window._exportProducts('json')">Export JSON</button>
  <button class="btn btn-ghost btn-sm" onclick="window._exportProducts('csv')">Export CSV</button>
  <button class="btn btn-ghost btn-sm" onclick="window._exportProducts('excel')">Export Excel</button>
</div>`;
}
