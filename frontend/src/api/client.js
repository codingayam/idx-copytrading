/**
 * API Client for IDX Copytrading
 */

const API_BASE = '/api';

/**
 * Generic fetch wrapper with error handling
 */
async function fetchApi(endpoint, options = {}) {
    const url = `${API_BASE}${endpoint}`;

    try {
        const response = await fetch(url, {
            ...options,
            headers: {
                'Content-Type': 'application/json',
                ...options.headers,
            },
        });

        if (!response.ok) {
            throw new Error(`API error: ${response.status} ${response.statusText}`);
        }

        return await response.json();
    } catch (error) {
        console.error(`API request failed: ${endpoint}`, error);
        throw error;
    }
}

/**
 * Build query string from params object
 */
function buildQuery(params) {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined && value !== null && value !== '') {
            query.append(key, value);
        }
    });
    return query.toString() ? `?${query.toString()}` : '';
}

// ==========================================
// API Functions
// ==========================================

export const api = {
    // Health Check
    async getHealth() {
        return fetchApi('/health');
    },

    // Brokers
    async getBrokers() {
        return fetchApi('/brokers');
    },

    async getBrokerAggregates(code, period = 'today') {
        return fetchApi(`/brokers/${code}/aggregates?period=${period}`);
    },

    async getBrokerTrades(code, { period = 'today', sort = 'netval', order = 'desc', page = 1, limit = 20 } = {}) {
        // Map camelCase to snake_case for API
        const sortFieldMap = { pctVolume: 'pct_volume' };
        const apiSort = sortFieldMap[sort] || sort;
        const query = buildQuery({ period, sort: apiSort, order, page, limit });
        return fetchApi(`/brokers/${code}/trades${query}`);
    },

    // Tickers
    async getTickers(limit = 100) {
        return fetchApi(`/tickers?limit=${limit}`);
    },

    async getTickerAggregates(symbol, period = 'today') {
        return fetchApi(`/tickers/${symbol}/aggregates?period=${period}`);
    },

    async getTickerBrokers(symbol, { period = 'today', sort = 'netval', order = 'desc', page = 1, limit = 20 } = {}) {
        // Map camelCase to snake_case for API
        const sortFieldMap = { pctVolume: 'pct_volume' };
        const apiSort = sortFieldMap[sort] || sort;
        const query = buildQuery({ period, sort: apiSort, order, page, limit });
        return fetchApi(`/tickers/${symbol}/brokers${query}`);
    },

    // Insights
    async getInsights(period = 'week', limit = 20) {
        return fetchApi(`/insights?period=${period}&limit=${limit}`);
    },
};

export default api;
