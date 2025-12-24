/**
 * Reusable Data Table Component with sorting
 */
export function DataTable({ columns, data, sortField, sortOrder, onSort, loading }) {
    const handleSort = (field) => {
        if (onSort) {
            onSort(field);
        }
    };

    if (loading) {
        return (
            <div className="loading-container">
                <div className="loading-spinner"></div>
                <div className="loading-text">Loading data...</div>
            </div>
        );
    }

    if (!data || data.length === 0) {
        return (
            <div className="empty-state">
                <div className="empty-state-icon">📊</div>
                <div>No data available for this selection</div>
            </div>
        );
    }

    return (
        <div className="data-table-container">
            <table className="data-table">
                <thead>
                    <tr>
                        {columns.map((col) => (
                            <th
                                key={col.key}
                                className={`${col.numeric ? 'numeric' : ''} ${sortField === col.key ? 'sorted' : ''}`}
                                onClick={() => col.sortable && handleSort(col.key)}
                                style={{ cursor: col.sortable ? 'pointer' : 'default' }}
                            >
                                {col.label}
                                {sortField === col.key && (
                                    <span style={{ marginLeft: '4px' }}>
                                        {sortOrder === 'desc' ? '↓' : '↑'}
                                    </span>
                                )}
                            </th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    {data.map((row, index) => (
                        <tr key={row.id || index} className="animate-fade-in" style={{ animationDelay: `${index * 20}ms` }}>
                            {columns.map((col) => (
                                <td
                                    key={col.key}
                                    className={`${col.numeric ? 'numeric' : ''} ${col.className || ''}`}
                                >
                                    {col.render ? col.render(row[col.key], row) : formatValue(row[col.key], col)}
                                </td>
                            ))}
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}

/**
 * Format value based on column type
 */
function formatValue(value, col) {
    if (value === null || value === undefined) return '-';

    if (col.type === 'number') {
        return formatNumber(value);
    }

    if (col.type === 'netval') {
        const formatted = formatNumber(Math.abs(value));
        const className = value >= 0 ? 'text-positive' : 'text-negative';
        const prefix = value >= 0 ? '+' : '-';
        return <span className={className}>{prefix}{formatted}</span>;
    }

    if (col.type === 'percent') {
        return `${value.toFixed(2)}%`;
    }

    return value;
}

/**
 * Format number with abbreviation
 */
function formatNumber(num) {
    if (Math.abs(num) >= 1000) {
        return (num / 1000).toFixed(2) + 'T';  // Trillion Rp
    }
    if (Math.abs(num) >= 1) {
        return num.toFixed(2) + 'M';  // Milyar Rp
    }
    return num.toFixed(4);
}

export default DataTable;
