import { useState, useCallback, useEffect } from 'react';
import { toast } from 'sonner';
import { searchService } from '../services/search.service';

export const useSearch = () => {
    // Search Filters
    const [query, setQuery] = useState('');
    const [type, setType] = useState('all');
    const [year, setYear] = useState('');
    
    // Core States
    const [results, setResults] = useState([]);
    const [loading, setLoading] = useState(false);
    const [page, setPage] = useState(1);
    const [totalPages, setTotalPages] = useState(0);
    const [totalCount, setTotalCount] = useState(0);
    const [documentTypes, setDocumentTypes] = useState([]);
    const [isInitialLoad, setIsInitialLoad] = useState(true);

    // Debounce for Query
    const [debouncedQuery, setDebouncedQuery] = useState('');

    useEffect(() => {
        const handler = setTimeout(() => {
            setDebouncedQuery(query);
        }, 400);
        return () => clearTimeout(handler);
    }, [query]);

    // Auto-search on filter change (debounced for text query)
    useEffect(() => {
        if (!isInitialLoad) {
            performSearch(true);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [debouncedQuery, type, year]);

    // Fetch available document types from backend
    useEffect(() => {
        const fetchTypes = async () => {
            try {
                const data = await searchService.getTypes();
                if (data && data.types) {
                    setDocumentTypes(data.types); // Backend sends an array of strings
                }
            } catch (error) {
                console.error('Failed to fetch document types', error);
                toast.error('Failed to fetch document types from server.');
            }
        };
        fetchTypes();
    }, []);

    const performSearch = useCallback(async (isNewSearch = false, specificPage = null) => {
        const targetPage = isNewSearch ? 1 : (specificPage || page);
        
        if (isNewSearch) {
            setPage(1);
            setResults([]); // Clear results immediately to trigger Skeleton loaders
        } else if (specificPage) {
            setPage(specificPage);
        }

        setLoading(true);

        try {
            const params = {
                page: targetPage,
                limit: 10
            };

            // Only append valid filters to API call
            if (query && query.trim()) params.query = query.trim();
            if (type && type !== 'all') params.document_type = type;
            if (year && String(year).trim()) params.year = String(year).trim();

            const data = await searchService.performSearch(params);
            setResults(data.results || []);
            setTotalPages(data.totalPages || 0);
            setTotalCount(data.count || 0);

        } catch (error) {
            console.error('Search failed', error);
            if (!isInitialLoad) {
                toast.error('Search failed. Please verify your connection.');
            }
            setResults([]);
            setTotalPages(0);
            setTotalCount(0);
        } finally {
            setLoading(false);
            if (isInitialLoad) setIsInitialLoad(false);
        }
    }, [query, type, year, page, isInitialLoad]);

    // Initial search to load all documents
    useEffect(() => {
        performSearch(true);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const getPageRange = useCallback(() => {
        let startPage = Math.max(1, page - 2);
        let endPage = Math.min(totalPages, page + 2);
        if (endPage - startPage < 4) {
            if (startPage === 1) {
                endPage = Math.min(totalPages, startPage + 4);
            } else if (endPage === totalPages) {
                startPage = Math.max(1, endPage - 4);
            }
        }
        return { start: startPage, end: endPage };
    }, [page, totalPages]);

    return {
        query, setQuery,
        type, setType,
        year, setYear,
        results,
        loading,
        page,
        totalPages,
        totalCount,
        documentTypes,
        performSearch,
        getPageRange
    };
};