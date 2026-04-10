/**
 * Product detail modal.
 */

import { getProduct } from '../state.js';
import { apiFetch } from '../api.js';
import { esc, fmtDate, platformBadge, qualityColor, section } from '../components/helpers.js';
import { setModalHTML } from '../components/modal.js';

export function openProductModal(itemId) {
    const p = getProduct(itemId);
    if (!p) { console.warn('Product not in registry:', itemId); return; }

    const d = p.detailedInfo || {};
    const dq = d.dataQuality || {};
    const q = p.extractionQuality;
    const shop = d.shopInfo || p.shopInfo || {};
    const sellerInfo = shop.sellerInfo || {};

    // Shop block
    const shopBlock = (shop.shopName || shop.shopLink) ? section('Shop', `
    <div style="background:var(--surface2);border:1px solid var(--border);border-radius:var(--radius);padding:14px 16px">
      ${shop.shopName ? `<div class="detail-row"><span class="detail-key">Name</span><span class="detail-val">${esc(shop.shopName)}</span></div>` : ''}
      ${shop.shopRating ? `<div class="detail-row"><span class="detail-key">Rating</span><span class="detail-val">${esc(shop.shopRating)} \u2b50</span></div>` : ''}
      ${sellerInfo.positiveFeedbackRate ? `<div class="detail-row"><span class="detail-key">Feedback</span><span class="detail-val">${sellerInfo.positiveFeedbackRate}%</span></div>` : ''}
      ${sellerInfo.averageDeliveryTime ? `<div class="detail-row"><span class="detail-key">Avg Ship</span><span class="detail-val">${esc(sellerInfo.averageDeliveryTime)}</span></div>` : ''}
      ${sellerInfo.serviceSatisfaction ? `<div class="detail-row"><span class="detail-key">Service</span><span class="detail-val">${sellerInfo.serviceSatisfaction}%</span></div>` : ''}
      ${shop.shopLink ? `<a href="${esc(shop.shopLink)}" target="_blank" rel="noopener"
        style="color:var(--amber);font-size:11px;font-family:var(--mono);text-decoration:none;display:inline-block;margin-top:8px">\u2197 Visit Shop</a>` : ''}
    </div>`) : '';

    const specsBlock = d.specifications && Object.keys(d.specifications).length ? section('Specifications', `
    <div class="spec-grid">
      ${Object.entries(d.specifications).map(([k,v]) => `
      <div class="spec-cell"><div class="spec-key">${esc(k)}</div><div class="spec-val">${esc(v)}</div></div>`).join('')}
    </div>`) : '';

    const variantsBlock = d.variants && Object.keys(d.variants).length ? section('Variants', `
    <div style="display:flex;flex-direction:column;gap:14px">
      ${Object.entries(d.variants).map(([type, opts]) => {
        const arr = Array.isArray(opts) ? opts : [];
        return `<div>
          <div style="font-size:11px;color:var(--muted);font-family:var(--mono);text-transform:uppercase;letter-spacing:0.06em;margin-bottom:7px">${esc(type)}</div>
          <div style="display:flex;flex-wrap:wrap;gap:5px">
            ${arr.slice(0, 24).map(o => {
              if (typeof o === 'string') return `<span class="variant-pill">${esc(o)}</span>`;
              if (o?.image) return `<img class="variant-img" src="${esc(o.image)}" title="${esc(o.value||'')}" loading="lazy" onerror="this.style.display='none'"/>`;
              return `<span class="variant-pill">${esc(o?.value || String(o))}</span>`;
            }).join('')}
            ${arr.length > 24 ? `<span style="color:var(--muted);font-size:12px;align-self:center;font-family:var(--mono)">+${arr.length-24}</span>` : ''}
          </div>
        </div>`;
      }).join('')}
    </div>`) : '';

    const imagesBlock = d.additionalImages?.length ? section(`Images (${d.additionalImages.length})`, `
    <div class="img-grid">
      ${d.additionalImages.map(img => `<img class="img-thumb" src="${esc(img)}" loading="lazy" onerror="this.style.display='none'"/>`).join('')}
    </div>`) : '';

    const guaranteesBlock = d.guarantees?.length ? section('Guarantees', `
    <div style="display:flex;flex-wrap:wrap;gap:6px">
      ${d.guarantees.map(g => `<span class="guarantee-chip">\u2713 ${esc(g)}</span>`).join('')}
    </div>`) : '';

    const descBlock = d.fullDescription ? section('Description', `
    <p style="font-size:13px;color:var(--text2);line-height:1.75">
      ${esc(d.fullDescription.substring(0, 600))}${d.fullDescription.length > 600 ? '\u2026' : ''}
    </p>`) : '';

    const qualityBlock = q ? section('Data Quality', `
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
      <span style="font-size:13px">Completeness</span>
      <span style="font-family:var(--mono);font-size:16px;font-weight:600;color:${qualityColor(q)}">${q}%</span>
    </div>
    <div class="quality-bar" style="height:5px;margin-bottom:14px">
      <div class="quality-fill" style="width:${q}%;background:${qualityColor(q)}"></div>
    </div>
    <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(90px,1fr));gap:4px;font-family:var(--mono);font-size:10px">
      ${['title','price','images','variants','specs','brand','reviews','description','salesVolume','shopName'].map(f => {
        const key = 'has' + f.charAt(0).toUpperCase() + f.slice(1);
        const ok = dq[key];
        return `<span style="color:${ok?'var(--green-l)':'var(--muted)'}">${ok?'\u2713':'\u2717'} ${f}</span>`;
      }).join('')}
    </div>`) : '';

    setModalHTML(`
<div class="modal-overlay" onclick="if(event.target===this) window._closeProductModal()">
  <div class="modal">
    <div class="modal-header">
      <div>
        <div class="modal-title">${esc(p.itemId)}</div>
        <div style="font-size:10px;color:var(--muted);font-family:var(--mono);margin-top:2px">${fmtDate(p.extractedAt||p.createdAt||p.updatedAt)}</div>
      </div>
      <button class="modal-close" onclick="window._closeProductModal()">\u00d7</button>
    </div>
    <div class="modal-body">
      <div class="grid-2" style="align-items:start;gap:20px;margin-bottom:22px">
        <div>
          <img src="${esc(p.image||'')}" alt="" onerror="this.style.display='none'"
            style="width:100%;border-radius:6px;background:var(--surface2);border:1px solid var(--border);aspect-ratio:1;object-fit:cover" loading="lazy"/>
          ${d.additionalImages?.length ? `
          <div class="img-grid" style="margin-top:6px;grid-template-columns:repeat(4,1fr)">
            ${d.additionalImages.slice(0,8).map(img => `<img class="img-thumb" src="${esc(img)}" loading="lazy" onerror="this.style.display='none'"/>`).join('')}
          </div>` : ''}
        </div>
        <div>
          <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px">
            ${platformBadge(p.platform)}
            ${p.detailsScraped ? '<span class="badge badge-success">\u2713 Detailed</span>' : ''}
          </div>
          <div style="font-size:15px;font-weight:600;line-height:1.45;margin-bottom:12px">${esc(d.fullTitle||p.title)}</div>
          <div style="font-family:var(--mono);font-size:22px;font-weight:600;color:var(--amber);margin-bottom:16px;letter-spacing:-0.02em">
            \u00a5${esc(p.price||'\u2014')}
            ${d.originalPrice ? `<span style="font-size:13px;color:var(--muted);text-decoration:line-through;margin-left:8px;font-weight:400">\u00a5${esc(d.originalPrice)}</span>` : ''}
          </div>
          <div>
            ${d.salesVolume||p.salesCount ? `<div class="detail-row"><span class="detail-key">Sales</span><span class="detail-val">${esc(d.salesVolume||p.salesCount)}</span></div>` : ''}
            ${d.rating ? `<div class="detail-row"><span class="detail-key">Rating</span><span class="detail-val">${esc(d.rating)} \u2605</span></div>` : ''}
            ${d.reviewCount ? `<div class="detail-row"><span class="detail-key">Reviews</span><span class="detail-val">${esc(d.reviewCount)}</span></div>` : ''}
            ${d.brand ? `<div class="detail-row"><span class="detail-key">Brand</span><span class="detail-val">${esc(d.brand)}</span></div>` : ''}
            ${p.categoryName ? `<div class="detail-row"><span class="detail-key">Category</span><span class="detail-val">${esc(p.categoryName)}</span></div>` : ''}
            ${p.location ? `<div class="detail-row"><span class="detail-key">Location</span><span class="detail-val">${esc(p.location)}</span></div>` : ''}
          </div>
          <a href="${esc(p.link||'#')}" target="_blank" rel="noopener"
            style="display:inline-flex;align-items:center;gap:6px;margin-top:14px;color:var(--amber);font-size:12px;font-family:var(--mono);text-decoration:none;border:1px solid rgba(240,165,0,0.25);padding:6px 12px;border-radius:var(--radius)">
            \u2197 View on Store
          </a>
        </div>
      </div>
      ${shopBlock}
      ${guaranteesBlock}
      ${specsBlock}
      ${variantsBlock}
      ${imagesBlock}
      ${descBlock}
      ${qualityBlock}
      <div class="modal-section">
        <div class="modal-section-title">Price History</div>
        <div id="price-history-container" style="font-size:12px;color:var(--muted)">Loading...</div>
      </div>
    </div>
  </div>
</div>`);

    // Fetch price history after modal renders
    fetchPriceHistory(p.itemId);
}

async function fetchPriceHistory(itemId) {
    const container = document.getElementById('price-history-container');
    if (!container) return;
    try {
        const d = await apiFetch('/prices/product/' + itemId, {}, { silent: true, toast: false });
        if (!d.success || !d.data?.length) {
            container.innerHTML = '<span style="color:var(--muted)">No price history yet</span>';
            return;
        }
        container.innerHTML = d.data.slice(0, 20).map(h => {
            const date = new Date(h.recordedAt).toLocaleDateString('en-US', {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'});
            return `<div class="detail-row">
              <span class="detail-key">${date}</span>
              <span class="detail-val" style="font-family:var(--mono)">
                \u00a5${h.price}${h.priceUsd ? ` <span style="color:var(--muted)">($${h.priceUsd})</span>` : ''}
              </span>
            </div>`;
        }).join('');
    } catch(e) {
        container.innerHTML = '<span style="color:var(--red-l)">Failed to load</span>';
    }
}

window._closeProductModal = () => {
    document.getElementById('modal-root').innerHTML = '';
};
