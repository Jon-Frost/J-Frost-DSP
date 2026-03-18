setTimeout(() => {
    document.querySelectorAll('.flash').forEach((el) => {
        el.style.opacity = '0';
        el.style.transform = 'translateX(100%)';
        setTimeout(() => el.remove(), 300);
    });
}, 4000);

window.showToast = function showToast(message, type = 'success') {
    const container = document.getElementById('flashContainer');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = `flash flash-${type}`;
    toast.textContent = message;
    container.appendChild(toast);

    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateX(100%)';
        setTimeout(() => toast.remove(), 300);
    }, 4000);
};
