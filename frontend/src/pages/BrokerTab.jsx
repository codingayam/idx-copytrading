import { useState, useEffect, useCallback } from 'react';
import api from '../api/client';
import { PeriodSelector } from '../components/PeriodSelector';
import { DataTable } from '../components/DataTable';
import { Pagination } from '../components/Pagination';

/**
 * Broker Tab - View trading activity by broker
 */
export function BrokerTab() {
    const [brokers, setBrokers] = useState([]);
    const [selectedBroker, setSelectedBroker] = useState('');
    const [period, setPeriod] = useState('today');
    const [aggregates, setAggregates] = useState(null);
    const [trades, setTrades] = useState({ data: [], total: 0, pages: 1 });
    const [page, setPage] = useState(1);
    const [sort, setSort] = useState({ field: 'netval', order: 'desc' });
    const [loading, setLoading] = useState(false);
    const [brokersLoading, setBrokersLoading] = useState(true);

    // Load brokers on mount
    useEffect(() => {
        async function loadBrokers() {
            try {
                const data = await api.getBrokers();
                setBrokers(data);
                if (data.length > 0) {
                    setSelectedBroker(data[0].code);
                }
            } catch (err) {
                console.error('Failed to load brokers:', err);
            } finally {
                setBrokersLoading(false);
            }
        }
        loadBrokers();
    }, []);

    // Load data when broker, period, page, or sort changes
    useEffect(() => {
        if (!selectedBroker) return;

        async function loadData() {
            setLoading(true);
            try {
                const [aggData, tradesData] = await Promise.all([
                    api.getBrokerAggregates(selectedBroker, period),
                    api.getBrokerTrades(selectedBroker, {
                        period,
                        sort: sort.field,
                        order: sort.order,
                        page,
                        limit: 20,
                    }),
                ]);
                setAggregates(aggData.aggregates);
                setTrades(tradesData);
            } catch (err) {
                console.error('Failed to load broker data:', err);
            } finally {
                setLoading(false);
            }
        }
        loadData();
    }, [selectedBroker, period, page, sort]);

    const handleSort = useCallback((field) => {
        setSort((prev) => ({
            field,
            order: prev.field === field && prev.order === 'desc' ? 'asc' : 'desc',
        }));
        setPage(1);
    }, []);

    const handlePeriodChange = useCallback((newPeriod) => {
        setPeriod(newPeriod);
        setPage(1);
    }, []);

    const columns = [
        { key: 'symbol', label: 'Symbol', className: 'symbol' },
        { key: 'netval', label: 'Net Value (M Rp)', type: 'netval', numeric: true, sortable: true },
        { key: 'bval', label: 'Buy Value', type: 'number', numeric: true, sortable: true },
        { key: 'sval', label: 'Sell Value', type: 'number', numeric: true, sortable: true },
        { key: 'bavg', label: 'Avg Buy', type: 'number', numeric: true },
        { key: 'savg', label: 'Avg Sell', type: 'number', numeric: true },
    ];

    const selectedBrokerName = brokers.find((b) => b.code === selectedBroker)?.name || '';

    return (
        <div className="animate-fade-in">
            <div className="page-header">
                <h1 className="page-title">
                    Broker Analysis
                    {selectedBroker && <span className="text-secondary"> — {selectedBroker}</span>}
                </h1>
                <div className="controls">
                    <div className="control-group">
                        <label className="control-label">Broker:</label>
                        <select
                            value={selectedBroker}
                            onChange={(e) => {
                                setSelectedBroker(e.target.value);
                                setPage(1);
                            }}
                            disabled={brokersLoading}
                        >
                            {brokers.map((b) => (
                                <option key={b.code} value={b.code}>
                                    {b.code} - {b.name}
                                </option>
                            ))}
                        </select>
                    </div>
                    <PeriodSelector value={period} onChange={handlePeriodChange} />
                </div>
            </div>

            {/* Stats Cards */}
            {aggregates && (
                <div className="stats-grid">
                    <div className="stat-card">
                        <div className="stat-label">Total Net Value</div>
                        <div className={`stat-value ${aggregates.totalNetval >= 0 ? 'positive' : 'negative'}`}>
                            {aggregates.totalNetval >= 0 ? '+' : ''}{formatNumber(aggregates.totalNetval)} M
                        </div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">Total Buy Value</div>
                        <div className="stat-value">{formatNumber(aggregates.totalBval)} M</div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">Total Sell Value</div>
                        <div className="stat-value">{formatNumber(aggregates.totalSval)} M</div>
                    </div>
                    <div className="stat-card">
                        <div className="stat-label">Trade Count</div>
                        <div className="stat-value">{aggregates.tradeCount.toLocaleString()}</div>
                    </div>
                </div>
            )}

            {/* Trades Table */}
            <div className="card">
                <div className="card-header">
                    <h3 className="card-title">Trades by Symbol</h3>
                </div>
                <DataTable
                    columns={columns}
                    data={trades.data}
                    sortField={sort.field}
                    sortOrder={sort.order}
                    onSort={handleSort}
                    loading={loading}
                />
                {trades.total > 0 && (
                    <Pagination
                        page={page}
                        pages={trades.pages}
                        total={trades.total}
                        limit={20}
                        onPageChange={setPage}
                    />
                )}
            </div>
        </div>
    );
}

function formatNumber(num) {
    if (num === null || num === undefined) return '-';
    return num.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export default BrokerTab;
