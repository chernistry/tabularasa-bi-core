const plotTheme = {
    font: {
        family: "'Segoe UI', 'Helvetica Neue', Arial, sans-serif",
        size: 12,
        color: '#212529' // --text-color
    },
    paper_bgcolor: 'transparent', // Transparent background
    plot_bgcolor: 'transparent', // Transparent background
    margin: {
        l: 60,
        r: 30,
        b: 50,
        t: 50,
        pad: 4
    },
    title: {
        font: {
            size: 18,
            weight: '600',
            color: '#212529' // --text-color
        },
        x: 0.05,
        xanchor: 'left'
    },
    xaxis: {
        gridcolor: '#dee2e6', // --border-color
        linecolor: '#adb5bd',
        zeroline: false,
        tickfont: {
            color: '#6c757d' // --secondary-color
        }
    },
    yaxis: {
        gridcolor: '#dee2e6', // --border-color
        linecolor: '#adb5bd',
        zeroline: false,
        tickfont: {
            color: '#6c757d' // --secondary-color
        }
    },
    legend: {
        bgcolor: 'rgba(255, 255, 255, 0.7)',
        bordercolor: '#dee2e6', // --border-color
        borderwidth: 1
    },
    colorway: ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
};

// --- Shared Interaction Functions ---

function drawVerticalLine(chartId, pointIndex) {
    const chart = document.getElementById(chartId);
    if (!chart || !chart.data || !chart.data[0].x) return;

    const xValue = chart.data[0].x[pointIndex];
    if (xValue === undefined) return;

    const existingShapes = chart.layout.shapes ? chart.layout.shapes.filter(s => s.name !== 'selection-line') : [];

    Plotly.relayout(chart, {
        shapes: [...existingShapes, {
            type: 'line',
            name: 'selection-line',
            x0: xValue,
            y0: 0,
            x1: xValue,
            y1: 1,
            yref: 'paper',
            line: {
                color: 'var(--danger-color, #d62728)',
                width: 1.5,
                dash: 'dash'
            }
        }]
    });
}

function clearAllVerticalLines() {
    const charts = document.querySelectorAll('.chart-container');
    charts.forEach(chartEl => {
        if (chartEl.id && chartEl.layout && chartEl.layout.shapes) {
            const newShapes = chartEl.layout.shapes.filter(s => s.name !== 'selection-line');
            Plotly.relayout(chartEl.id, { shapes: newShapes });
        }
    });
}

// --- Modal Logic ---
function showModal(id) {
    const modal = document.getElementById(id);
    if(modal) modal.style.display = 'block';
}

function closeModal(id) {
    const modal = document.getElementById(id);
    if(modal) modal.style.display = 'none';
}

// Close modal if user clicks outside of it
window.addEventListener('click', function(event) {
    if (event.target.classList.contains('modal')) {
        event.target.style.display = 'none';
    }
}); 