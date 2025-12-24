/**
 * Period Selector Component
 */
export function PeriodSelector({ value, onChange }) {
    const periods = [
        { value: 'today', label: 'Today' },
        { value: 'week', label: 'Past Week' },
        { value: 'month', label: 'Past Month' },
        { value: 'ytd', label: 'Year to Date' },
        { value: 'all', label: 'All Time' },
    ];

    return (
        <div className="control-group">
            <label className="control-label">Period:</label>
            <select value={value} onChange={(e) => onChange(e.target.value)}>
                {periods.map((p) => (
                    <option key={p.value} value={p.value}>
                        {p.label}
                    </option>
                ))}
            </select>
        </div>
    );
}

export default PeriodSelector;
