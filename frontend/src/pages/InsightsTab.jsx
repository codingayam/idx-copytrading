import { useState, useEffect } from 'react';
import api from '../api/client';
import { PeriodSelector } from '../components/PeriodSelector';
import { DataTable } from '../components/DataTable';
import { Pagination } from '../components/Pagination';

const PAGE_SIZE = 10;

/**
 * Insights Tab - Top movers and market overview
 */
export function InsightsTab({ onNavigateToBroker, onNavigateToTicker }) {
    const [period, setPeriod] = useState('today');
    const [insights, setInsights] = useState(null);
    const [loading, setLoading] = useState(true);
    const [moverPage, setMoverPage] = useState(1);
    const [brokerPage, setBrokerPage] = useState(1);

    useEffect(() => {
        async function loadInsights() {
            setLoading(true);
            try {
                const data = await api.getInsights(period, 50); // Fetch more for client-side pagination
                setInsights(data);
                setMoverPage(1);
                setBrokerPage(1);
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
        {
            key: 'symbol',
            label: 'Symbol',
            className: 'symbol',
            render: (val) => (
                <span
                    className="clickable-link"
                    onClick={() => onNavigateToTicker?.(val)}
                    title="View ticker details"
                >
                    {val}
                </span>
            )
        },
        {
            key: 'brokerCode',
            label: 'Broker',
            className: 'symbol',
            render: (val) => (
                <span
                    className="clickable-link"
                    onClick={() => onNavigateToBroker?.(val)}
                    title="View broker details"
                >
                    {val}
                </span>
            )
        },
        { key: 'brokerName', label: 'Broker Name' },
        { key: 'netval', label: 'Net Value (M Rp)', type: 'netval', numeric: true },
        { key: 'bval', label: 'Buy Value (M Rp)', type: 'number', numeric: true },
        { key: 'sval', label: 'Sell Value (M Rp)', type: 'number', numeric: true },
    ];

    const brokerColumns = [
        {
            key: 'rank',
            label: '#',
            render: (val) => <span className="text-muted">{val}</span>
        },
        {
            key: 'brokerCode',
            label: 'Code',
            className: 'symbol',
            render: (val) => (
                <span
                    className="clickable-link"
                    onClick={() => onNavigateToBroker?.(val)}
                    title="View broker details"
                >
                    {val}
                </span>
            )
        },
        { key: 'brokerName', label: 'Broker Name' },
        { key: 'netval', label: 'Net Value (M Rp)', type: 'netval', numeric: true },
        { key: 'bval', label: 'Buy Value (M Rp)', type: 'number', numeric: true },
        { key: 'sval', label: 'Sell Value (M Rp)', type: 'number', numeric: true },
    ];

    const marketStats = insights?.marketStats;

    // Paginate data client-side
    const topMovers = insights?.topMovers || [];
    const topBrokers = insights?.topBrokers || [];

    const moverStart = (moverPage - 1) * PAGE_SIZE;
    const pagedMovers = topMovers.slice(moverStart, moverStart + PAGE_SIZE);
    const moverPages = Math.ceil(topMovers.length / PAGE_SIZE) || 1;

    const brokerStart = (brokerPage - 1) * PAGE_SIZE;
    const pagedBrokers = topBrokers.slice(brokerStart, brokerStart + PAGE_SIZE);
    const brokerPages = Math.ceil(topBrokers.length / PAGE_SIZE) || 1;

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
                    data={pagedMovers}
                    loading={loading}
                />
                {topMovers.length > PAGE_SIZE && (
                    <Pagination
                        page={moverPage}
                        pages={moverPages}
                        total={topMovers.length}
                        limit={PAGE_SIZE}
                        onPageChange={setMoverPage}
                    />
                )}
            </div>

            {/* Top Brokers */}
            <div className="card" style={{ marginTop: 'var(--spacing-xl)' }}>
                <div className="card-header">
                    <h3 className="card-title">Top Active Brokers</h3>
                    <span className="text-secondary" style={{ fontSize: '0.875rem' }}>
                        Highest net trading value by broker
                    </span>
                </div>
                <DataTable
                    columns={brokerColumns}
                    data={pagedBrokers}
                    loading={loading}
                />
                {topBrokers.length > PAGE_SIZE && (
                    <Pagination
                        page={brokerPage}
                        pages={brokerPages}
                        total={topBrokers.length}
                        limit={PAGE_SIZE}
                        onPageChange={setBrokerPage}
                    />
                )}
            </div>

            {/* Info Box */}
            <div className="card" style={{ marginTop: 'var(--spacing-xl)' }}>
                <div className="card-body">
                    <h4 style={{ marginBottom: 'var(--spacing-md)' }}>📊 About This Data</h4>
                    <p className="text-secondary" style={{ lineHeight: 1.7 }}>
                        This dashboard tracks broker trading activity on the Indonesia Stock Exchange (IDX).
                        Data is crawled daily at 9 PM SGT from NeoBDM.
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
