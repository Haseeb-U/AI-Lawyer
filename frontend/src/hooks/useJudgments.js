import { useState, useCallback, useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import { toast } from 'sonner';
import { judgmentService } from '../services/judgment.service';

export const useJudgments = () => {
    const location = useLocation();
    const queryParams = new URLSearchParams(location.search);
    const initialLocationQuery = queryParams.get('q') || '';

    const [judgments, setJudgments] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    
    // Search & Pagination
    const [search, setSearch] = useState(initialLocationQuery);
    const [page, setPage] = useState(1);
    const [hasMore, setHasMore] = useState(true);
    const [isInitialLoad, setIsInitialLoad] = useState(true);

    // Debounce for Query
    const [debouncedSearch, setDebouncedSearch] = useState('');

    useEffect(() => {
        const handler = setTimeout(() => {
            setDebouncedSearch(search);
        }, 400);
        return () => clearTimeout(handler);
    }, [search]);

    // Summary States
    const [summary, setSummary] = useState(null);
    const [summaryLoading, setSummaryLoading] = useState(false);
    const [summaryError, setSummaryError] = useState(null);

    const fetchJudgments = useCallback(async (isNewSearch = false) => {
        const currentPage = isNewSearch ? 1 : page;
        
        if (isNewSearch) {
            setLoading(true);
            setJudgments([]); // Clear data immediately to trigger Skeleton loaders
        }

        setError(null);

        try {
            // Fetch more judgments (limit 50) to allow client-side filtering UX
            const response = await judgmentService.searchJudgments(search, currentPage, 50);
            const fetchedData = response.data || [];
            
            if (isNewSearch) {
                setJudgments(fetchedData);
            } else {
                setJudgments(prev => [...prev, ...fetchedData]);
            }
            
            // Check if there's more data
            const hasMoreData = fetchedData.length === 50; 
            setHasMore(hasMoreData);
            setPage(currentPage + 1);
        } catch (err) {
            console.error('Error fetching judgments', err);
            setError('Failed to fetch judgments. Please try again.');
            if (!isInitialLoad) {
                toast.error('Failed to fetch judgments. Please try again.');
            }
            setHasMore(false);
        } finally {
            setLoading(false);
            if (isInitialLoad) setIsInitialLoad(false);
        }
    }, [search, page, isInitialLoad]);

    const fetchSummary = async (title) => {
        setSummary(null);
        setSummaryLoading(true);
        setSummaryError(null);
        
        try {
            const data = await judgmentService.getSummary(title);
            if (data.success || data.summary) {
                setSummary(data.summary);
                toast.success('AI Synopsis generated successfully!');
            } else {
                setSummaryError(data.error || 'Failed to generate summary.');
                toast.error(data.error || 'Failed to generate summary.');
            }
        } catch (err) {
            console.error('Error fetching summary', err);
            setSummaryError('Cannot reach the AI service right now to generate the summary.');
            toast.error('Cannot reach the AI service right now to generate the summary.');
        } finally {
            setSummaryLoading(false);
        }
    };

    // Auto-search on Debounced Filter Change
    useEffect(() => {
        if (!isInitialLoad) {
            fetchJudgments(true);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [debouncedSearch]);

    // Initial search to load all judgments
    useEffect(() => {
        fetchJudgments(true);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []); 

    return {
        judgments,
        loading,
        error,
        search,
        setSearch,
        page,
        hasMore,
        summary,
        summaryLoading,
        summaryError,
        fetchJudgments,
        fetchSummary
    };
};