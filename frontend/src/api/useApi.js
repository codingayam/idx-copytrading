import { useState, useEffect, useCallback } from 'react';

/**
 * Custom hook for API data fetching with loading and error states
 */
export function useApi(fetchFn, deps = []) {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    const refetch = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            const result = await fetchFn();
            setData(result);
        } catch (err) {
            setError(err.message || 'An error occurred');
        } finally {
            setLoading(false);
        }
    }, [fetchFn]);

    useEffect(() => {
        refetch();
    }, deps);

    return { data, loading, error, refetch };
}

export default useApi;
