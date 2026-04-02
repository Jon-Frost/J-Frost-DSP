// ══════════════════════════════════════════════════════════════════════════════
// UPLOAD MODAL — OPEN AND CLOSE THE FILE UPLOAD OVERLAY
// ══════════════════════════════════════════════════════════════════════════════

const uploadZone = document.getElementById('uploadZone');
const fileInput = document.getElementById('fileInput');
const uploadModal = document.getElementById('uploadModal');

function openUploadModal() {
    if (uploadModal) uploadModal.classList.add('active');
}

function closeUploadModal() {
    if (uploadModal) uploadModal.classList.remove('active');
}

// ══════════════════════════════════════════════════════════════════════════════
// FILE UPLOAD — XHR UPLOAD WITH PROGRESS BAR AND STATUS FEEDBACK
// ══════════════════════════════════════════════════════════════════════════════

function uploadFile(file) {
    // SEND THE FILE VIA XHR, UPDATE THE PROGRESS BAR, AND RELOAD ON SUCCESS
    const progress = document.getElementById('uploadProgress');
    const bar = document.getElementById('uploadBar');
    const status = document.getElementById('uploadStatus');
    const percent = document.getElementById('uploadPercent');

    if (!progress || !bar || !status || !percent) return;

    progress.style.display = 'block';

    const formData = new FormData();
    formData.append('file', file);

    const xhr = new XMLHttpRequest();
    xhr.open('POST', '/upload');
    xhr.upload.onprogress = (e) => {
        if (e.lengthComputable) {
            const p = Math.round((e.loaded / e.total) * 100);
            bar.style.width = p + '%';
            percent.textContent = p + '%';
        }
    };

    xhr.onload = () => {
        const resp = JSON.parse(xhr.responseText || '{}');
        if (xhr.status === 200 && resp.success) {
            status.textContent = resp.message;
            bar.style.width = '100%';
            bar.style.background = 'var(--success)';
            showToast(resp.message, 'success');
            setTimeout(() => location.reload(), 1200);
        } else {
            status.textContent = resp.error || 'Upload failed';
            bar.style.background = 'var(--danger)';
            showToast(resp.error || 'Upload failed', 'error');
        }
    };

    xhr.onerror = () => {
        status.textContent = 'Network error';
        showToast('Network error', 'error');
    };

    xhr.send(formData);
}

// ══════════════════════════════════════════════════════════════════════════════
// DELETE OPERATIONS — CONFIRM AND REMOVE DATASETS OR DASHBOARDS
// ══════════════════════════════════════════════════════════════════════════════

function deleteDataset(id, name) {
    // PROMPT FOR CONFIRMATION, THEN DELETE THE DATASET AND ALL LINKED DASHBOARDS
    if (!confirm(`Delete "${name}" and all associated dashboards?`)) return;
    fetch(`/api/dataset/${id}/delete`, { method: 'POST' })
        .then((r) => r.json())
        .then(() => {
            showToast('Dataset deleted', 'success');
            setTimeout(() => location.reload(), 600);
        });
}

function deleteDashboard(id, name) {
    // PROMPT FOR CONFIRMATION, THEN DELETE THE DASHBOARD RECORD
    if (!confirm(`Delete dashboard "${name}"?`)) return;
    fetch(`/api/dashboard/${id}/delete`, { method: 'POST' })
        .then((r) => r.json())
        .then(() => {
            showToast('Dashboard deleted', 'success');
            setTimeout(() => location.reload(), 600);
        });
}

// ══════════════════════════════════════════════════════════════════════════════
// EVENT BINDING — WIRE UP BUTTONS, CARDS, DRAG-AND-DROP ON DOM READY
// ══════════════════════════════════════════════════════════════════════════════

document.addEventListener('DOMContentLoaded', () => {
    // BIND UPLOAD MODAL OPEN/CLOSE BUTTONS
    document.querySelectorAll('.js-open-upload-modal').forEach((btn) => {
        btn.addEventListener('click', openUploadModal);
    });

    const closeBtn = document.querySelector('.js-close-upload-modal');
    if (closeBtn) closeBtn.addEventListener('click', closeUploadModal);

    // BIND DASHBOARD CARD CLICK-TO-OPEN AND DELETE BUTTONS
    document.querySelectorAll('.js-open-dashboard').forEach((card) => {
        card.addEventListener('click', () => {
            const href = card.dataset.href;
            if (href) window.location = href;
        });
    });

    document.querySelectorAll('.js-delete-dashboard').forEach((btn) => {
        btn.addEventListener('click', (event) => {
            event.stopPropagation();
            deleteDashboard(btn.dataset.dashboardId, btn.dataset.dashboardName || '');
        });
    });

    // BIND DATASET DELETE BUTTONS
    document.querySelectorAll('.js-delete-dataset').forEach((btn) => {
        btn.addEventListener('click', () => {
            deleteDataset(btn.dataset.datasetId, btn.dataset.datasetName || '');
        });
    });

    // SET UP DRAG-AND-DROP AND FILE INPUT HANDLERS ON THE UPLOAD ZONE
    if (uploadZone && fileInput) {
        ['dragenter', 'dragover'].forEach((e) => {
            uploadZone.addEventListener(e, (ev) => {
                ev.preventDefault();
                uploadZone.classList.add('dragover');
            });
        });

        ['dragleave', 'drop'].forEach((e) => {
            uploadZone.addEventListener(e, (ev) => {
                ev.preventDefault();
                uploadZone.classList.remove('dragover');
            });
        });

        uploadZone.addEventListener('drop', (e) => {
            if (e.dataTransfer.files.length) {
                fileInput.files = e.dataTransfer.files;
                uploadFile(e.dataTransfer.files[0]);
            }
        });

        fileInput.addEventListener('change', () => {
            if (fileInput.files.length) uploadFile(fileInput.files[0]);
        });
    }
});
