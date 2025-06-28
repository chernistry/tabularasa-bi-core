const customPlotTheme = {
    // Set transparent backgrounds so the card background is visible
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: 'rgba(0,0,0,0)',

    // Define fonts and colors from the new design system
    font: {
        family: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif",
        size: 14,
        color: '#495057' // Corresponds to --color-text-secondary
    },

    // Title and legend styling
    title: {
        font: {
            color: '#212529', // Corresponds to --color-text-primary
            size: 18
        }
    },
    legend: {
        orientation: 'h',
        yanchor: 'bottom',
        y: 1.02,
        xanchor: 'right',
        x: 1
    },
    
    // Axis styling to match the theme
    xaxis: {
        gridcolor: '#DEE2E6', // Corresponds to --color-border
        linecolor: '#DEE2E6',
        zerolinecolor: '#DEE2E6',
        tickfont: {
            color: '#6c757d' // Corresponds to --color-secondary
        }
    },
    yaxis: {
        gridcolor: '#DEE2E6', // Corresponds to --color-border
        linecolor: '#DEE2E6',
        zerolinecolor: '#DEE2E6',
        tickfont: {
            color: '#6c757d' // Corresponds to --color-secondary
        }
    },

    // A modern colorway for traces
    colorway: ['#007BFF', '#6f42c1', '#28a745', '#dc3545', '#ffc107', '#17a2b8'], // Start with --color-primary

    // Default trace styling for a cleaner look
    bar: {
        marker: {
            line: {
                width: 0
            }
        }
    },
    scatter: {
        marker: {
            size: 8
        }
    },
    
    // Consistent margins for all plots
    margin: {
        l: 60,
        r: 30,
        t: 40,
        b: 50
    }
};

// To apply this theme, merge it with your specific layout options:
// Plotly.newPlot('your-div', data, { ...your_layout, ...customPlotTheme }, { responsive: true }); 