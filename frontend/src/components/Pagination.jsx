/**
 * Pagination Component
 */
export function Pagination({ page, pages, total, limit, onPageChange }) {
    const start = (page - 1) * limit + 1;
    const end = Math.min(page * limit, total);

    return (
        <div className="pagination">
            <div className="pagination-info">
                Showing {start} - {end} of {total} results
            </div>
            <div className="pagination-buttons">
                <button
                    className="pagination-button"
                    onClick={() => onPageChange(1)}
                    disabled={page === 1}
                >
                    ««
                </button>
                <button
                    className="pagination-button"
                    onClick={() => onPageChange(page - 1)}
                    disabled={page === 1}
                >
                    «
                </button>

                {getPageNumbers(page, pages).map((p, i) => (
                    typeof p === 'number' ? (
                        <button
                            key={i}
                            className={`pagination-button ${page === p ? 'active' : ''}`}
                            onClick={() => onPageChange(p)}
                        >
                            {p}
                        </button>
                    ) : (
                        <span key={i} style={{ padding: '0 4px', color: 'var(--color-text-muted)' }}>
                            ...
                        </span>
                    )
                ))}

                <button
                    className="pagination-button"
                    onClick={() => onPageChange(page + 1)}
                    disabled={page === pages}
                >
                    »
                </button>
                <button
                    className="pagination-button"
                    onClick={() => onPageChange(pages)}
                    disabled={page === pages}
                >
                    »»
                </button>
            </div>
        </div>
    );
}

/**
 * Get array of page numbers to display
 */
function getPageNumbers(current, total) {
    if (total <= 7) {
        return Array.from({ length: total }, (_, i) => i + 1);
    }

    const pages = [];

    // Always show first page
    pages.push(1);

    // Show ellipsis or pages around current
    if (current > 3) {
        pages.push('...');
    }

    // Pages around current
    const start = Math.max(2, current - 1);
    const end = Math.min(total - 1, current + 1);

    for (let i = start; i <= end; i++) {
        pages.push(i);
    }

    // Show ellipsis or pages before last
    if (current < total - 2) {
        pages.push('...');
    }

    // Always show last page
    if (total > 1) {
        pages.push(total);
    }

    return pages;
}

export default Pagination;
