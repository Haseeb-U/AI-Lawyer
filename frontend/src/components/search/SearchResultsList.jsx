import React from 'react';
import { IconSearch } from '@tabler/icons-react';
import { SearchResultCard } from './SearchResultCard';
import { SearchPagination } from './SearchPagination';
import { Button } from '../ui/button';
import { Skeleton } from '../ui/skeleton';

export const SearchResultsList = ({
    loading,
    results,
    query,
    totalCount,
    type,
    year,
    setQuery,
    setType,
    setYear,
    performSearch,
    page,
    totalPages,
    getPageRange,
    handlePageChange
}) => {
    
    if (loading && results.length === 0) {
        return (
            <div className="flex flex-col gap-6">
                {[...Array(4)].map((_, i) => (
                    <div key={i} className="bg-[var(--card-bg)] border border-[var(--card-border)] rounded-3xl p-7 shadow-sm">
                        <Skeleton className="h-6 w-3/4 mb-4 rounded-lg" />
                        <Skeleton className="h-4 w-full mb-2 rounded-md" />
                        <Skeleton className="h-4 w-5/6 mb-6 rounded-md" />
                        <div className="flex gap-3">
                            <Skeleton className="h-8 w-24 rounded-lg" />
                            <Skeleton className="h-8 w-24 rounded-lg" />
                        </div>
                    </div>
                ))}
            </div>
        );
    }

    if (results.length > 0) {
        return (
            <div className="flex flex-col gap-6">
                <div className="text-[15px] font-bold text-[var(--text-soft)] mb-2 flex items-center gap-2">
                    <span>Showing results for <span className="text-[var(--foreground)]">{query ? `"${query}"` : "All Documents"}</span></span>
                    <span className="w-1.5 h-1.5 rounded-full bg-slate-300"></span>
                    <span className="text-[#d4af37]">{totalCount} match{totalCount !== 1 ? 'es' : ''} found</span>
                </div>
                
                {results.map((result, idx) => (
                    <SearchResultCard key={idx} result={result} />
                ))}
                
                <SearchPagination 
                    page={page}
                    totalPages={totalPages}
                    getPageRange={getPageRange}
                    handlePageChange={handlePageChange}
                />
            </div>
        );
    }

    if (!loading) {
        return (
            <div className="py-24 flex flex-col items-center justify-center text-center bg-[var(--card-bg)] border border-dashed border-[var(--card-border)] rounded-3xl h-full shadow-sm">
                <div className="h-20 w-20 bg-black/5 dark:bg-white/5 text-slate-300 rounded-full flex items-center justify-center mb-6">
                    <IconSearch size={36} stroke={1.5} />
                </div>
                <h3 className="text-xl font-bold text-[var(--foreground)] mb-2">No documents found</h3>
                <p className="text-[var(--text-soft)] max-w-sm mb-6 text-[15px]">We couldn't abstract any legal documents matching your current search parameters.</p>
                
                {(query || type !== 'all' || year) && (
                    <Button
                        variant="secondary"
                        onClick={() => {
                            setQuery('');
                            setType('all');
                            setYear('');
                        }}
                        className="text-[#b48512] font-semibold hover:text-[#d4af37] flex items-center gap-1.5 bg-[#d4af37]/10 hover:bg-[#d4af37]/20 px-5 py-5 rounded-xl transition-colors h-auto"
                    >
                        Reset all filters
                    </Button>
                )}
            </div>
        );
    }

    return null;
};