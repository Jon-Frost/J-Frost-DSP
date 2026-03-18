document.addEventListener('DOMContentLoaded', () => {
    const grid = document.getElementById('chartGrid');
    const jsonTemplate = document.getElementById('chartsDataJson');
    if (!grid || !jsonTemplate) return;

    let chartsData = [];
    try {
        chartsData = JSON.parse((jsonTemplate.innerHTML || '[]').trim());
    } catch {
        chartsData = [];
    }

    chartsData.forEach((chart, i) => {
        const div = document.createElement('div');
        div.className = 'export-chart-cell';
        div.id = 'export_chart_' + i;
        grid.appendChild(div);

        Plotly.newPlot(div.id, chart.data, {
            ...chart.layout,
            autosize: true,
            height: 320,
        }, { responsive: true, displayModeBar: true, displaylogo: false });
    });
});
