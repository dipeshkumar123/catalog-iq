/**
 * CatalogIQ Single Page Application (Vanilla JavaScript)
 *
 * Implements:
 * 1. Uses the shared RFC 4180 CSV parser (quoted fields, escaped quotes, CRLF/LF, BOM).
 * 2. Upload and live progress polling (>= 1s interval with adaptive backoff).
 * 3. Catalogue browsing with debounced search, category filtering, and pagination.
 * 4. Side-by-side product review and inline approval (PATCH /api/products/<sku>).
 * 5. Full keyboard accessibility and mobile responsiveness (down to 360px).
 */

(function () {
  'use strict';

  // State Management
  let currentJobId = null;
  let pollingTimer = null;
  let pollInterval = 1000;
  let unchangedPolls = 0;
  let lastProgressKey = '';

  let currentPage = 1;
  const pageSize = 20;
  let debounceSearchTimer = null;
  let currentReviewProduct = null;

  // DOM Elements
  const tabs = document.querySelectorAll('[role="tab"]');
  const panels = document.querySelectorAll('[role="tabpanel"]');

  const fileInput = document.getElementById('csv-file-input');
  const fileNameDisplay = document.getElementById('file-name-display');
  const btnUpload = document.getElementById('btn-upload');
  const uploadError = document.getElementById('upload-error');

  const progressSection = document.getElementById('progress-section');
  const jobStatusBadge = document.getElementById('job-status-badge');
  const jobStatusText = document.getElementById('job-status-text');
  const progressBarFill = document.getElementById('progress-bar-fill');
  const metricTotal = document.getElementById('metric-total');
  const metricDone = document.getElementById('metric-done');
  const metricFailed = document.getElementById('metric-failed');
  const metricCacheHits = document.getElementById('metric-cache-hits');
  const btnViewCatalogue = document.getElementById('btn-view-catalogue');

  const searchInput = document.getElementById('search-input');
  const categoryFilter = document.getElementById('category-filter');
  const productsTable = document.getElementById('products-table');
  const productsTbody = document.getElementById('products-tbody');
  const catalogueEmpty = document.getElementById('catalogue-empty');
  const btnPrevPage = document.getElementById('btn-prev-page');
  const btnNextPage = document.getElementById('btn-next-page');
  const pageInfo = document.getElementById('page-info');

  const reviewEmpty = document.getElementById('review-empty');
  const reviewContent = document.getElementById('review-content');
  const reviewErrorAlert = document.getElementById('review-error-alert');
  const rawSku = document.getElementById('raw-sku');
  const rawTitle = document.getElementById('raw-title');
  const rawDescription = document.getElementById('raw-description');
  const rawStatusContainer = document.getElementById('raw-status-container');
  const reviewForm = document.getElementById('review-form');
  const editTitle = document.getElementById('edit-title');
  const editCategory = document.getElementById('edit-category');
  const editTags = document.getElementById('edit-tags');
  const reviewFormError = document.getElementById('review-form-error');
  const btnCancelReview = document.getElementById('btn-cancel');

  const toast = document.getElementById('toast');

  async function apiFetch(url, options) {
    try {
      return await fetch(url, options);
    } catch (error) {
      throw new Error('Network error. Check your connection and try again.');
    }
  }

  // =========================================================================
  // 1. Shared RFC-4180 CSV Parser
  // =========================================================================

  const parseCSV = window.parseCSV;

  // =========================================================================
  // 2. Tab Navigation & Accessibility
  // =========================================================================

  function switchTab(targetId) {
    tabs.forEach(tab => {
      const isSelected = tab.getAttribute('aria-controls') === targetId;
      tab.setAttribute('aria-selected', isSelected ? 'true' : 'false');
      tab.tabIndex = isSelected ? 0 : -1;
      if (isSelected) {
        tab.classList.add('active');
        tab.focus();
      } else {
        tab.classList.remove('active');
      }
    });

    panels.forEach(panel => {
      if (panel.id === targetId) {
        panel.classList.remove('hidden');
        panel.classList.add('active');
      } else {
        panel.classList.add('hidden');
        panel.classList.remove('active');
      }
    });

    if (targetId === 'panel-catalogue') {
      fetchCatalogue();
    }
  }

  tabs.forEach((tab, index) => {
    tab.addEventListener('click', () => {
      switchTab(tab.getAttribute('aria-controls'));
    });

    tab.addEventListener('keydown', e => {
      let targetIndex = null;
      if (e.key === 'ArrowRight') {
        targetIndex = (index + 1) % tabs.length;
      } else if (e.key === 'ArrowLeft') {
        targetIndex = (index - 1 + tabs.length) % tabs.length;
      } else if (e.key === 'Home') {
        targetIndex = 0;
      } else if (e.key === 'End') {
        targetIndex = tabs.length - 1;
      }

      if (targetIndex !== null) {
        e.preventDefault();
        tabs[targetIndex].focus();
        switchTab(tabs[targetIndex].getAttribute('aria-controls'));
      }
    });
  });

  // =========================================================================
  // 3. Upload Flow & Live Progress Polling
  // =========================================================================

  let parsedProductsList = [];

  fileInput.addEventListener('change', () => {
    uploadError.classList.add('hidden');
    const file = fileInput.files[0];
    if (!file) {
      fileNameDisplay.textContent = 'No file selected';
      btnUpload.disabled = true;
      parsedProductsList = [];
      return;
    }

    fileNameDisplay.textContent = `${file.name} (${Math.round(file.size / 1024)} KB)`;
    const reader = new FileReader();
    reader.onload = e => {
      try {
        const text = e.target.result;
        parsedProductsList = parseCSV(text);
        if (parsedProductsList.length === 0) {
          showUploadError('The selected CSV file has no product rows.');
          btnUpload.disabled = true;
        } else {
          btnUpload.disabled = false;
        }
      } catch (err) {
        showUploadError(`Failed to parse CSV: ${err.message}`);
        btnUpload.disabled = true;
      }
    };
    reader.onerror = () => {
      showUploadError('Failed to read file from disk.');
      btnUpload.disabled = true;
    };
    reader.readAsText(file);
  });

  btnUpload.addEventListener('click', async () => {
    if (parsedProductsList.length === 0) return;

    // Validate that every product has sku and raw_title
    for (let idx = 0; idx < parsedProductsList.length; idx++) {
      const p = parsedProductsList[idx];
      if (!p.sku) {
        showUploadError(`Row ${idx + 1} is missing a SKU.`);
        return;
      }
      if (!p.raw_title) {
        showUploadError(`Product with SKU '${p.sku}' is missing a title.`);
        return;
      }
    }

    uploadError.classList.add('hidden');
    btnUpload.disabled = true;
    btnUpload.textContent = 'Submitting Job...';

    try {
      const res = await apiFetch('/api/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ products: parsedProductsList }),
      });

      const data = await res.json();
      if (!res.ok) {
        throw new Error(data.error || 'Failed to submit job.');
      }

      currentJobId = data.id;
      showToast(`Job ${currentJobId} created! Processing started.`);
      initProgress(data);
      startPolling();
    } catch (err) {
      showUploadError(err.message);
    } finally {
      btnUpload.disabled = false;
      btnUpload.textContent = 'Upload & Process';
    }
  });

  function showUploadError(msg) {
    uploadError.textContent = msg;
    uploadError.classList.remove('hidden');
  }

  function initProgress(job) {
    progressSection.classList.remove('hidden');
    btnViewCatalogue.disabled = true;
    updateProgressUI(job);
  }

  function updateProgressUI(job) {
    const total = job.total || 0;
    const done = job.done || 0;
    const failed = job.failed || 0;
    const cacheHits = job.cache_hits || 0;
    const processed = done + failed;
    const pct = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : 0;

    metricTotal.textContent = total;
    metricDone.textContent = done;
    metricFailed.textContent = failed;
    metricCacheHits.textContent = cacheHits;

    progressBarFill.style.width = `${pct}%`;
    progressBarFill.setAttribute('aria-valuenow', pct);

    jobStatusBadge.className = `badge badge-${job.status}`;
    jobStatusBadge.textContent = job.status;

    if (job.status === 'queued') {
      jobStatusText.textContent = `Queued (0/${total} processed)`;
    } else if (job.status === 'running') {
      jobStatusText.textContent = `Running (${processed}/${total}, ${failed} failed, ${cacheHits} cached)`;
    } else if (job.status === 'completed') {
      jobStatusText.textContent = `Completed (${done} done, ${failed} failed, ${cacheHits} cached)`;
      btnViewCatalogue.disabled = false;
    }
  }

  function startPolling() {
    stopPolling();
    pollInterval = 1000;
    unchangedPolls = 0;
    lastProgressKey = '';

    const executePoll = async () => {
      if (!currentJobId) return;

      try {
        const res = await apiFetch(`/api/jobs/${currentJobId}`);
        if (!res.ok) {
          const err = await res.json();
          throw new Error(err.error || 'Failed to poll job status.');
        }
        const job = await res.json();
        updateProgressUI(job);

        // Adaptive polling backoff: if progress hasn't changed in 5 ticks, back off to 2000ms
        const progressKey = `${job.done}-${job.failed}-${job.status}`;
        if (progressKey === lastProgressKey) {
          unchangedPolls++;
          if (unchangedPolls >= 5 && pollInterval < 2000) {
            pollInterval = 2000;
          }
        } else {
          unchangedPolls = 0;
          pollInterval = 1000;
          lastProgressKey = progressKey;
        }

        if (job.status === 'completed' || job.status === 'failed') {
          stopPolling();
          showToast(`Job ${job.id} has ${job.status}!`);
          return;
        }
      } catch (e) {
        console.error('Polling error:', e);
      }

      pollingTimer = setTimeout(executePoll, pollInterval);
    };

    pollingTimer = setTimeout(executePoll, pollInterval);
  }

  function stopPolling() {
    if (pollingTimer) {
      clearTimeout(pollingTimer);
      pollingTimer = null;
    }
  }

  btnViewCatalogue.addEventListener('click', () => {
    switchTab('panel-catalogue');
  });

  // =========================================================================
  // 4. Catalogue Browser (Search, Filter, Pagination)
  // =========================================================================

  searchInput.addEventListener('input', () => {
    clearTimeout(debounceSearchTimer);
    debounceSearchTimer = setTimeout(() => {
      currentPage = 1;
      fetchCatalogue();
    }, 300);
  });

  searchInput.addEventListener('keydown', e => {
    if (e.key === 'Escape') {
      clearTimeout(debounceSearchTimer);
      searchInput.value = '';
      currentPage = 1;
      fetchCatalogue();
    }
  });

  categoryFilter.addEventListener('change', () => {
    currentPage = 1;
    fetchCatalogue();
  });

  btnPrevPage.addEventListener('click', () => {
    if (currentPage > 1) {
      currentPage--;
      fetchCatalogue();
    }
  });

  btnNextPage.addEventListener('click', () => {
    currentPage++;
    fetchCatalogue();
  });

  async function fetchCatalogue() {
    const q = searchInput.value.trim();
    const category = categoryFilter.value;

    const params = new URLSearchParams({
      page: currentPage.toString(),
      page_size: pageSize.toString(),
    });
    if (q) params.set('q', q);
    if (category) params.set('category', category);

    try {
      const res = await apiFetch(`/api/products?${params.toString()}`);
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.error || 'Failed to fetch catalogue.');
      }
      const data = await res.json();
      renderCatalogue(data);
    } catch (err) {
      showToast(err.message, 'error');
    }
  }

  function renderCatalogue(data) {
    productsTbody.innerHTML = '';
    const items = data.items || [];
    const total = data.total || 0;
    const totalPages = Math.max(1, Math.ceil(total / pageSize));

    pageInfo.textContent = `Page ${data.page} of ${totalPages} (${total} total)`;
    btnPrevPage.disabled = data.page <= 1;
    btnNextPage.disabled = data.page >= totalPages;

    if (items.length === 0) {
      catalogueEmpty.classList.remove('hidden');
      productsTable.classList.add('hidden');
      return;
    }

    catalogueEmpty.classList.add('hidden');
    productsTable.classList.remove('hidden');

    items.forEach(product => {
      const tr = document.createElement('tr');
      tr.className = 'table-row-btn';
      tr.tabIndex = 0;
      tr.setAttribute('role', 'button');
      tr.setAttribute('aria-label', `Review product ${product.sku}`);

      // Helper to create escaped td
      const makeTd = text => {
        const td = document.createElement('td');
        td.textContent = text || '—';
        return td;
      };

      tr.appendChild(makeTd(product.sku));
      tr.appendChild(makeTd(product.clean_title || product.raw_title));
      tr.appendChild(makeTd(product.category));
      tr.appendChild(makeTd(product.brand));

      // Status cell with badge
      const tdStatus = document.createElement('td');
      const badge = document.createElement('span');
      badge.className = `badge badge-${product.status}`;
      badge.textContent = product.status;
      tdStatus.appendChild(badge);
      tr.appendChild(tdStatus);

      // Row click and Enter key
      tr.addEventListener('click', () => openProductReview(product.sku));
      tr.addEventListener('keydown', e => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          openProductReview(product.sku);
        }
      });

      productsTbody.appendChild(tr);
    });
  }

  // =========================================================================
  // 5. Product Review & Approval
  // =========================================================================

  async function openProductReview(sku) {
    try {
      const res = await apiFetch(`/api/products/${encodeURIComponent(sku)}`);
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.error || 'Failed to load product.');
      }
      currentReviewProduct = await res.json();
      populateReviewTab(currentReviewProduct);
      switchTab('panel-review');
      document.getElementById('review-heading').focus();
    } catch (err) {
      showToast(err.message, 'error');
    }
  }

  function populateReviewTab(product) {
    reviewEmpty.classList.add('hidden');
    reviewContent.classList.remove('hidden');
    reviewFormError.classList.add('hidden');

    // Error alert banner for failed products
    if (product.status === 'failed' && product.error) {
      reviewErrorAlert.textContent = `Pipeline Error: ${product.error}`;
      reviewErrorAlert.classList.remove('hidden');
    } else {
      reviewErrorAlert.classList.add('hidden');
    }

    // Left column: Raw metadata
    rawSku.textContent = product.sku;
    rawTitle.textContent = product.raw_title || '—';
    rawDescription.textContent = product.raw_description || 'None provided';

    rawStatusContainer.innerHTML = '';
    const badge = document.createElement('span');
    badge.className = `badge badge-${product.status}`;
    badge.textContent = product.status;
    rawStatusContainer.appendChild(badge);

    // Right column: Editable cleaned inputs
    editTitle.value = product.clean_title || product.raw_title || '';
    editCategory.value = product.category || 'Other';
    editTags.value = Array.isArray(product.tags) ? product.tags.join(', ') : '';

  }

  reviewForm.addEventListener('submit', async e => {
    e.preventDefault();
    if (!currentReviewProduct) return;

    const titleVal = editTitle.value.trim();
    const categoryVal = editCategory.value;
    const rawTagsVal = editTags.value;

    // Validate title
    if (!titleVal) {
      showReviewFormError('Clean title cannot be empty.');
      editTitle.focus();
      return;
    }

    // Parse and validate tags
    const tagsArr = rawTagsVal
      .split(',')
      .map(t => t.trim().toLowerCase())
      .filter(t => t.length > 0);

    if (tagsArr.length > 5) {
      showReviewFormError('Tags cannot exceed 5 items.');
      editTags.focus();
      return;
    }

    reviewFormError.classList.add('hidden');
    const btnApprove = document.getElementById('btn-approve');
    btnApprove.disabled = true;
    btnApprove.textContent = 'Saving...';

    try {
      const res = await apiFetch(`/api/products/${encodeURIComponent(currentReviewProduct.sku)}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          clean_title: titleVal,
          category: categoryVal,
          tags: tagsArr,
        }),
      });

      const updated = await res.json();
      if (!res.ok) {
        throw new Error(updated.error || 'Failed to approve product.');
      }

      const refreshed = await apiFetch(`/api/products/${encodeURIComponent(currentReviewProduct.sku)}`);
      if (!refreshed.ok) {
        const error = await refreshed.json();
        throw new Error(error.error || 'Product was approved but could not be refreshed.');
      }
      currentReviewProduct = await refreshed.json();
      populateReviewTab(currentReviewProduct);
      showToast('Product approved');
    } catch (err) {
      showReviewFormError(err.message);
    } finally {
      btnApprove.disabled = false;
      btnApprove.textContent = 'Approve Product';
    }
  });

  function showReviewFormError(msg) {
    reviewFormError.textContent = msg;
    reviewFormError.classList.remove('hidden');
  }

  btnCancelReview.addEventListener('click', () => {
    switchTab('panel-catalogue');
  });

  // =========================================================================
  // 6. Accessible Toast Notifications
  // =========================================================================

  let toastTimer = null;
  function showToast(message, type = 'info') {
    clearTimeout(toastTimer);
    toast.textContent = message;
    toast.className = `toast toast-${type}`;
    toast.classList.remove('hidden');

    toastTimer = setTimeout(() => {
      toast.classList.add('hidden');
    }, 3500);
  }

  // Global offline/network listener
  window.addEventListener('offline', () => {
    showToast('Network disconnected. Check your connection.', 'error');
  });

  console.log('CatalogIQ SPA loaded.');
})();
