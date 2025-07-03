// DEPRECATED MOCK DATA – retained only for offline demo but NOT used in production.
// The backend now serves real data via FastAPI API endpoints. Future development must NOT
// rely on this file. At any point where data is unavailable, dashboards will display
// a user-friendly message rather than falling back to mocks.
//
// NOTE: All lines below are commented out intentionally. Keep the structure for reference.
// -----------------------------------------------------------------------------
// const mockData = {
//     campaign_performance: [ /* ... */ ],
//     performance_by_device: [ /* ... */ ],
//     performance_by_category: [ /* ... */ ],
//     kpis: { /* ... */ },
//     roi_trend: [ /* ... */ ]
// };
//
// function safeFetch(url) {
//     // Previously returned mock data when API errored; now always rejects.
//     return fetch(url).then(r => {
//         if (!r.ok) throw new Error(`HTTP ${r.status}`);
//         return r.json();
//     });
// } 