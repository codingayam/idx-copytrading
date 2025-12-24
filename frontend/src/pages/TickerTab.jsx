import { useState, useEffect, useCallback } from 'react';
import api from '../api/client';
import { PeriodSelector } from '../components/PeriodSelector';
import { DataTable } from '../components/DataTable';
import { Pagination } from '../components/Pagination';

/**
 * Ticker Tab - View trading activity by symbol/ticker
 */
export function TickerTab() {
    const [tickers, setTickers] = useState([]);
    const [filteredTickers, setFilteredTickers] = useState([]);
    const [searchTerm, setSearchTerm] = useState('');
    const [selectedTicker, setSelectedTicker] = useState('');
    const [period, setPeriod] = useState('today');
    const [aggregates, setAggregates] = useState(null);
    const [brokers, setBrokers] = useState({ data: [], total: 0, pages: 1 });
    const [page, setPage] = useState(1);
    const [sort, setSort] = useState({ field: 'netval', order: 'desc' });
    const [loading, setLoading] = useState(false);
    const [tickersLoading, setTickersLoading] = useState(true);

    // Load tickers on mount
    useEffect(() => {
        async function loadTickers() {
            try {
                const data = await api.getTickers(2000);
                setTickers(data);
                setFilteredTickers(data.slice(0, 50));
                if (data.length > 0) {
                    setSelectedTicker(data[0].symbol);
                }
            } catch (err) {
                console.error('Failed to load tickers:', err);
            } finally {
                setTickersLoading(false);
            }
        }
        loadTickers();
    }, []);

    // Filter tickers based on search
    useEffect(() => {
        if (searchTerm) {
            const filtered = tickers.filter((t) =>
                t.symbol.toLowerCase().includes(searchTerm.toLowerCase())
            );
            setFilteredTickers(filtered.slice(0, 50));
        } else {
            setFilteredTickers(tickers.slice(0, 50));
        }
    }, [searchTerm, tickers]);

    // Load data when ticker, period, page, or sort changes
    useEffect(() => {
        if (!selectedTicker) return;

        async function loadData() {
            setLoading(true);
            try {
                const [aggData, brokersData] = await Promise.all([
                    api.getTickerAggregates(selectedTicker, period),
                    api.getTickerBrokers(selectedTicker, {
                        period,
                        sort: sort.field,
                        order: sort.order,
                        page,
                        limit: 20,
                    }),
                ]);
                setAggregates(aggData.aggregates);
                setBrokers(brokersData);
            } catch (err) {
                console.error('Failed to load ticker data:', err);
            } finally {
                setLoading(false);
            }
        }
        loadData();
    }, [selectedTicker, period, page, sort]);

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
        { key: 'brokerCode', label: 'Broker', className: 'symbol' },
        { key: 'brokerName', label: 'Name' },
        { key: 'netval', label: 'Net Value (M Rp)', type: 'netval', numeric: true, sortable: true },
        { key: 'bval', label: 'Buy Value (M Rp)', type: 'number', numeric: true, sortable: true },
        { key: 'sval', label: 'Sell Value (M Rp)', type: 'number', numeric: true, sortable: true },
        { key: 'bavg', label: 'Avg Buy', type: 'price', numeric: true },
        { key: 'savg', label: 'Avg Sell', type: 'price', numeric: true },
        { key: 'pctVolume', label: '% Volume', type: 'percent', numeric: true },
    ];

    return (
        <div className="animate-fade-in">
            <div className="page-header">
                <h1 className="page-title">
                    Ticker Analysis
                    {selectedTicker && <span className="text-secondary"> — {selectedTicker}</span>}
                </h1>
                <div className="controls">
                    <div className="control-group">
                        <label className="control-label">Search:</label>
                        <input
                            type="text"
                            placeholder="Search symbol..."
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value.toUpperCase())}
                            style={{ width: '120px' }}
                        />
                    </div>
                    <div className="control-group">
                        <label className="control-label">Symbol:</label>
                        <select
                            value={selectedTicker}
                            onChange={(e) => {
                                setSelectedTicker(e.target.value);
                                setPage(1);
                            }}
                            disabled={tickersLoading}
                        >
                            {filteredTickers.map((t) => (
                                <option key={t.symbol} value={t.symbol}>
                                    {t.symbol}
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
                        <div className="stat-label">Active Brokers</div>
                        <div className="stat-value">{brokers.total}</div>
                    </div>
                </div>
            )}

            {/* Brokers Table */}
            <div className="card">
                <div className="card-header">
                    <h3 className="card-title">Brokers Trading {selectedTicker}</h3>
                </div>
                <DataTable
                    columns={columns}
                    data={brokers.data}
                    sortField={sort.field}
                    sortOrder={sort.order}
                    onSort={handleSort}
                    loading={loading}
                />
                {brokers.total > 0 && (
                    <Pagination
                        page={page}
                        pages={brokers.pages}
                        total={brokers.total}
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

export default TickerTab;
