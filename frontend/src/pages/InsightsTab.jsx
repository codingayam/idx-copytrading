import { useState, useEffect } from 'react';
import api from '../api/client';
import { PeriodSelector } from '../components/PeriodSelector';
import { DataTable } from '../components/DataTable';

/**
 * Insights Tab - Top movers and market overview
 */
export function InsightsTab() {
    const [period, setPeriod] = useState('week');
    const [insights, setInsights] = useState(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        async function loadInsights() {
            setLoading(true);
            try {
                const data = await api.getInsights(period, 20);
                setInsights(data);
            } catch (err) {
                console.error('Failed to load insights:', err);
            } finally {
                setLoading(false);
            }
        }
        loadInsights();
    }, [period]);

    const columns = [
        {
            key: 'rank',
            label: '#',
            render: (val) => <span className="text-muted">{val}</span>
        },
        { key: 'symbol', label: 'Symbol', className: 'symbol' },
        { key: 'brokerCode', label: 'Broker', className: 'symbol' },
        { key: 'brokerName', label: 'Broker Name' },
        { key: 'netval', label: 'Net Value (M Rp)', type: 'netval', numeric: true },
        { key: 'bval', label: 'Buy Value', type: 'number', numeric: true },
        { key: 'sval', label: 'Sell Value', type: 'number', numeric: true },
    ];

    const marketStats = insights?.marketStats;

    return (
        <div className="animate-fade-in">
            <div className="page-header">
                <h1 className="page-title">Market Insights</h1>
                <div className="controls">
                    <PeriodSelector value={period} onChange={setPeriod} />
                </div>
            </div>

            {/* Market Stats */}
            {marketStats && (
                <div className="stats-grid">
                    <div className="stat-card">
                        <div className="stat-label">Market Date</div>
                        <div className="stat-value" style={{ fontSize: '1.25rem' }}>
                            {marketStats.date || '-'}
                        </div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">Total Buy Value</div>
                        <div className="stat-value positive">
                            {formatNumber(marketStats.totalBval)} M
                        </div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">Total Sell Value</div>
                        <div className="stat-value negative">
                            {formatNumber(marketStats.totalSval)} M
                        </div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">Active Symbols</div>
                        <div className="stat-value">{marketStats.activeSymbols?.toLocaleString() || '-'}</div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">Active Brokers</div>
                        <div className="stat-value">{marketStats.activeBrokers?.toLocaleString() || '-'}</div>
                    </div>
                </div>
            )}

            {/* Top Movers */}
            <div className="card">
                <div className="card-header">
                    <h3 className="card-title">Top Net Value Positions</h3>
                    <span className="text-secondary" style={{ fontSize: '0.875rem' }}>
                        Highest accumulation by broker-symbol pair
                    </span>
                </div>
                <DataTable
                    columns={columns}
                    data={insights?.topMovers || []}
                    loading={loading}
                />
            </div>

            {/* Info Box */}
            <div className="card" style={{ marginTop: 'var(--spacing-xl)' }}>
                <div className="card-body">
                    <h4 style={{ marginBottom: 'var(--spacing-md)' }}>📊 About This Data</h4>
                    <p className="text-secondary" style={{ lineHeight: 1.7 }}>
                        This dashboard tracks broker trading activity on the Indonesia Stock Exchange (IDX).
                        Data is crawled daily at 9 PM Jakarta time from NeoBDM.
                    </p>
                    <ul className="text-secondary" style={{ marginTop: 'var(--spacing-md)', paddingLeft: 'var(--spacing-lg)', lineHeight: 2 }}>
                        <li><strong>Net Value (Netval)</strong>: Buy value minus sell value. Positive = accumulation, negative = distribution.</li>
                        <li><strong>Bval / Sval</strong>: Total buying and selling value in Milyar Rupiah.</li>
                        <li><strong>Bavg / Savg</strong>: Volume-weighted average buy and sell price.</li>
                        <li><strong>% Volume</strong>: Broker's share of total trading volume for a symbol.</li>
                    </ul>
                </div>
            </div>
        </div>
    );
}

function formatNumber(num) {
    if (num === null || num === undefined) return '-';
    return num.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export default InsightsTab;
