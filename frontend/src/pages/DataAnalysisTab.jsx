import { useState, useEffect, useCallback, useMemo } from 'react';
import api from '../api/client';
import { PeriodSelector } from '../components/PeriodSelector';

/**
 * Data Analysis Tab - Pivot table for broker-symbol cross-reference analysis
 * Users can select multiple brokers and symbols for analysis
 */
export function DataAnalysisTab() {
    const [pivotData, setPivotData] = useState(null);
    const [period, setPeriod] = useState('week');
    const [metric, setMetric] = useState('netval');
    const [loading, setLoading] = useState(false);

    // Available options
    const [allBrokers, setAllBrokers] = useState([]);
    const [allSymbols, setAllSymbols] = useState([]);
    const [brokersLoading, setBrokersLoading] = useState(true);
    const [symbolsLoading, setSymbolsLoading] = useState(true);

    // Selected items
    const [selectedBrokers, setSelectedBrokers] = useState([]);
    const [selectedSymbols, setSelectedSymbols] = useState([]);

    // Search filters
    const [brokerSearch, setBrokerSearch] = useState('');
    const [symbolSearch, setSymbolSearch] = useState('');

    // Load brokers and symbols on mount
    useEffect(() => {
        async function loadBrokers() {
            try {
                const data = await api.getBrokers();
                setAllBrokers(data);
            } catch (err) {
                console.error('Failed to load brokers:', err);
            } finally {
                setBrokersLoading(false);
            }
        }
        async function loadSymbols() {
            try {
                const data = await api.getTickers(2000);
                setAllSymbols(data);
            } catch (err) {
                console.error('Failed to load symbols:', err);
            } finally {
                setSymbolsLoading(false);
            }
        }
        loadBrokers();
        loadSymbols();
    }, []);

    // Filter options by search
    const filteredBrokers = useMemo(() => {
        if (!brokerSearch) return allBrokers;
        const search = brokerSearch.toLowerCase();
        return allBrokers.filter(b =>
            b.code.toLowerCase().includes(search) ||
            b.name.toLowerCase().includes(search)
        );
    }, [allBrokers, brokerSearch]);

    const filteredSymbols = useMemo(() => {
        if (!symbolSearch) return allSymbols;
        const search = symbolSearch.toUpperCase();
        return allSymbols.filter(s => s.symbol.includes(search));
    }, [allSymbols, symbolSearch]);

    // Load pivot data when selections change
    useEffect(() => {
        if (selectedBrokers.length === 0 || selectedSymbols.length === 0) {
            setPivotData(null);
            return;
        }

        async function loadData() {
            setLoading(true);
            try {
                const data = await api.getPivotData({
                    period,
                    metric,
                    brokers: selectedBrokers,
                    symbols: selectedSymbols,
                });
                setPivotData(data);
            } catch (err) {
                console.error('Failed to load pivot data:', err);
            } finally {
                setLoading(false);
            }
        }
        loadData();
    }, [selectedBrokers, selectedSymbols, period, metric]);

    const handlePeriodChange = useCallback((newPeriod) => {
        setPeriod(newPeriod);
    }, []);

    const toggleBroker = useCallback((code) => {
        setSelectedBrokers(prev =>
            prev.includes(code) ? prev.filter(b => b !== code) : [...prev, code]
        );
    }, []);

    const toggleSymbol = useCallback((symbol) => {
        setSelectedSymbols(prev =>
            prev.includes(symbol) ? prev.filter(s => s !== symbol) : [...prev, symbol]
        );
    }, []);

    const selectAllBrokers = useCallback(() => {
        setSelectedBrokers(filteredBrokers.map(b => b.code));
    }, [filteredBrokers]);

    const clearAllBrokers = useCallback(() => {
        setSelectedBrokers([]);
    }, []);

    const selectAllSymbols = useCallback(() => {
        setSelectedSymbols(filteredSymbols.map(s => s.symbol));
    }, [filteredSymbols]);

    const clearAllSymbols = useCallback(() => {
        setSelectedSymbols([]);
    }, []);

    const formatValue = (val) => {
        if (val === undefined || val === null) return '-';
        const absVal = Math.abs(val);
        if (absVal >= 1000) {
            return val.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 });
        }
        return val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    };

    const getCellClass = (val) => {
        if (val === undefined || val === null || val === 0) return 'cell-neutral';
        return val > 0 ? 'cell-positive' : 'cell-negative';
    };

    const metricLabels = {
        netval: 'Net Value',
        bval: 'Buy Value',
        sval: 'Sell Value',
    };

    return (
        <div className="animate-fade-in">
            <div className="page-header">
                <h1 className="page-title">
                    Data Analysis
                    <span className="text-secondary"> — Pivot Table</span>
                </h1>
                <div className="controls">
                    <div className="control-group">
                        <label className="control-label">Metric:</label>
                        <select
                            value={metric}
                            onChange={(e) => setMetric(e.target.value)}
                        >
                            <option value="netval">Net Value</option>
                            <option value="bval">Buy Value</option>
                            <option value="sval">Sell Value</option>
                        </select>
                    </div>
                    <PeriodSelector value={period} onChange={handlePeriodChange} />
                </div>
            </div>

            {/* Selection Panel */}
            <div className="pivot-selection-panel">
                {/* Broker Selection */}
                <div className="selection-box">
                    <div className="selection-header">
                        <h4>Brokers <span className="selection-count">({selectedBrokers.length} selected)</span></h4>
                        <div className="selection-actions">
                            <button className="btn-link" onClick={selectAllBrokers}>Select All</button>
                            <button className="btn-link" onClick={clearAllBrokers}>Clear</button>
                        </div>
                    </div>
                    <input
                        type="text"
                        placeholder="Search brokers..."
                        value={brokerSearch}
                        onChange={(e) => setBrokerSearch(e.target.value)}
                        className="selection-search"
                    />
                    <div className="selection-list">
                        {brokersLoading ? (
                            <div className="selection-loading">Loading...</div>
                        ) : (
                            filteredBrokers.map((broker) => (
                                <label key={broker.code} className="selection-item">
                                    <input
                                        type="checkbox"
                                        checked={selectedBrokers.includes(broker.code)}
                                        onChange={() => toggleBroker(broker.code)}
                                    />
                                    <span className="selection-code">{broker.code}</span>
                                    <span className="selection-name">{broker.name}</span>
                                </label>
                            ))
                        )}
                    </div>
                </div>

                {/* Symbol Selection */}
                <div className="selection-box">
                    <div className="selection-header">
                        <h4>Symbols <span className="selection-count">({selectedSymbols.length} selected)</span></h4>
                        <div className="selection-actions">
                            <button className="btn-link" onClick={selectAllSymbols}>Select All</button>
                            <button className="btn-link" onClick={clearAllSymbols}>Clear</button>
                        </div>
                    </div>
                    <input
                        type="text"
                        placeholder="Search symbols..."
                        value={symbolSearch}
                        onChange={(e) => setSymbolSearch(e.target.value.toUpperCase())}
                        className="selection-search"
                    />
                    <div className="selection-list">
                        {symbolsLoading ? (
                            <div className="selection-loading">Loading...</div>
                        ) : (
                            filteredSymbols.map((ticker) => (
                                <label key={ticker.symbol} className="selection-item">
                                    <input
                                        type="checkbox"
                                        checked={selectedSymbols.includes(ticker.symbol)}
                                        onChange={() => toggleSymbol(ticker.symbol)}
                                    />
                                    <span className="selection-code">{ticker.symbol}</span>
                                </label>
                            ))
                        )}
                    </div>
                </div>
            </div>

            {/* Pivot Table */}
            <div className="card">
                <div className="card-header">
                    <h3 className="card-title">
                        Broker × Symbol
                        <span className="text-secondary"> ({metricLabels[metric]} in M Rp)</span>
                    </h3>
                </div>

                {loading ? (
                    <div className="loading-container">
                        <div className="loading-spinner"></div>
                        <p>Loading pivot data...</p>
                    </div>
                ) : pivotData && pivotData.brokers.length > 0 && pivotData.symbols.length > 0 ? (
                    <div className="pivot-table-container">
                        <table className="pivot-table">
                            <thead>
                                <tr>
                                    <th className="pivot-header-cell sticky-col">Broker</th>
                                    {pivotData.symbols.map((col) => (
                                        <th key={col} className="pivot-header-cell">
                                            {col}
                                        </th>
                                    ))}
                                    <th className="pivot-header-cell pivot-total-col">Total</th>
                                </tr>
                            </thead>
                            <tbody>
                                {pivotData.brokers.map((broker) => (
                                    <tr key={broker}>
                                        <td className="pivot-row-header sticky-col">{broker}</td>
                                        {pivotData.symbols.map((symbol) => {
                                            const val = pivotData.data[broker]?.[symbol];
                                            return (
                                                <td
                                                    key={symbol}
                                                    className={`pivot-cell ${getCellClass(val)}`}
                                                >
                                                    {formatValue(val)}
                                                </td>
                                            );
                                        })}
                                        <td className={`pivot-cell pivot-total-col ${getCellClass(pivotData.totals.broker[broker])}`}>
                                            {formatValue(pivotData.totals.broker[broker])}
                                        </td>
                                    </tr>
                                ))}
                                {/* Column totals row */}
                                <tr className="pivot-totals-row">
                                    <td className="pivot-row-header sticky-col">Total</td>
                                    {pivotData.symbols.map((symbol) => (
                                        <td
                                            key={symbol}
                                            className={`pivot-cell ${getCellClass(pivotData.totals.symbol[symbol])}`}
                                        >
                                            {formatValue(pivotData.totals.symbol[symbol])}
                                        </td>
                                    ))}
                                    <td className="pivot-cell pivot-grand-total">
                                        {formatValue(
                                            Object.values(pivotData.totals.broker).reduce((a, b) => a + b, 0)
                                        )}
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                ) : (
                    <div className="empty-state">
                        <p>
                            {selectedBrokers.length === 0 && selectedSymbols.length === 0
                                ? 'Select brokers and symbols above to generate the pivot table.'
                                : selectedBrokers.length === 0
                                    ? 'Select at least one broker.'
                                    : 'Select at least one symbol.'}
                        </p>
                    </div>
                )}
            </div>

            {/* Legend */}
            <div className="pivot-legend">
                <span className="legend-item">
                    <span className="legend-color positive"></span> Positive (Net Buy)
                </span>
                <span className="legend-item">
                    <span className="legend-color negative"></span> Negative (Net Sell)
                </span>
                <span className="legend-item text-secondary">
                    Values in Million Rupiah (M Rp)
                </span>
            </div>
        </div>
    );
}

export default DataAnalysisTab;
