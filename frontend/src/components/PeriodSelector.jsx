/**
 * Period Selector Component
 */
export function PeriodSelector({ value, onChange }) {
    const periods = [
        { value: 'today', label: 'Today' },
        { value: '2d', label: '2 Days' },
        { value: '3d', label: '3 Days' },
        { value: '5d', label: '5 Days' },
        { value: '10d', label: '10 Days' },
        { value: '20d', label: '20 Days' },
        { value: '60d', label: '60 Days' },
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
