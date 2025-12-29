import { useState, useEffect, useCallback } from 'react';
import api from '../api/client';
import { PeriodSelector } from '../components/PeriodSelector';

/**
 * Data Analysis Tab - Pivot table for broker-symbol cross-reference analysis
 */
export function DataAnalysisTab() {
    const [pivotData, setPivotData] = useState(null);
    const [period, setPeriod] = useState('week');
    const [rowDimension, setRowDimension] = useState('broker');
    const [topN, setTopN] = useState(15);
    const [metric, setMetric] = useState('netval');
    const [loading, setLoading] = useState(false);

    // Load pivot data when filters change
    useEffect(() => {
        async function loadData() {
            setLoading(true);
            try {
                const data = await api.getPivotData({
                    rows: rowDimension,
                    period,
                    topN,
                    metric,
                });
                setPivotData(data);
            } catch (err) {
                console.error('Failed to load pivot data:', err);
            } finally {
                setLoading(false);
            }
        }
        loadData();
    }, [rowDimension, period, topN, metric]);

    const handlePeriodChange = useCallback((newPeriod) => {
        setPeriod(newPeriod);
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
                        <label className="control-label">Rows:</label>
                        <select
                            value={rowDimension}
                            onChange={(e) => setRowDimension(e.target.value)}
                        >
                            <option value="broker">Broker</option>
                            <option value="symbol">Symbol</option>
                        </select>
                    </div>
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
                    <div className="control-group">
                        <label className="control-label">Top N:</label>
                        <select
                            value={topN}
                            onChange={(e) => setTopN(Number(e.target.value))}
                        >
                            <option value={10}>10</option>
                            <option value={15}>15</option>
                            <option value={20}>20</option>
                            <option value={30}>30</option>
                            <option value={50}>50</option>
                        </select>
                    </div>
                    <PeriodSelector value={period} onChange={handlePeriodChange} />
                </div>
            </div>

            {/* Pivot Table */}
            <div className="card">
                <div className="card-header">
                    <h3 className="card-title">
                        {rowDimension === 'broker' ? 'Broker' : 'Symbol'} × {rowDimension === 'broker' ? 'Symbol' : 'Broker'}
                        <span className="text-secondary"> ({metricLabels[metric]} in M Rp)</span>
                    </h3>
                </div>

                {loading ? (
                    <div className="loading-container">
                        <div className="loading-spinner"></div>
                        <p>Loading pivot data...</p>
                    </div>
                ) : pivotData && pivotData.rows.length > 0 ? (
                    <div className="pivot-table-container">
                        <table className="pivot-table">
                            <thead>
                                <tr>
                                    <th className="pivot-header-cell sticky-col">
                                        {rowDimension === 'broker' ? 'Broker' : 'Symbol'}
                                    </th>
                                    {pivotData.columns.map((col) => (
                                        <th key={col} className="pivot-header-cell">
                                            {col}
                                        </th>
                                    ))}
                                    <th className="pivot-header-cell pivot-total-col">Total</th>
                                </tr>
                            </thead>
                            <tbody>
                                {pivotData.rows.map((rowKey) => (
                                    <tr key={rowKey}>
                                        <td className="pivot-row-header sticky-col">{rowKey}</td>
                                        {pivotData.columns.map((colKey) => {
                                            const val = pivotData.data[rowKey]?.[colKey];
                                            return (
                                                <td
                                                    key={colKey}
                                                    className={`pivot-cell ${getCellClass(val)}`}
                                                >
                                                    {formatValue(val)}
                                                </td>
                                            );
                                        })}
                                        <td className={`pivot-cell pivot-total-col ${getCellClass(pivotData.totals.row[rowKey])}`}>
                                            {formatValue(pivotData.totals.row[rowKey])}
                                        </td>
                                    </tr>
                                ))}
                                {/* Column totals row */}
                                <tr className="pivot-totals-row">
                                    <td className="pivot-row-header sticky-col">Total</td>
                                    {pivotData.columns.map((colKey) => (
                                        <td
                                            key={colKey}
                                            className={`pivot-cell ${getCellClass(pivotData.totals.column[colKey])}`}
                                        >
                                            {formatValue(pivotData.totals.column[colKey])}
                                        </td>
                                    ))}
                                    <td className="pivot-cell pivot-grand-total">
                                        {formatValue(
                                            Object.values(pivotData.totals.row).reduce((a, b) => a + b, 0)
                                        )}
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                ) : (
                    <div className="empty-state">
                        <p>No data available for the selected period.</p>
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
