import React from 'react';

// Hooks (Logic & State Layer)
import { useSearch } from '../hooks/useSearch';

// Components (UI Layer)
import { SearchHeader } from '../components/search/SearchHeader';
import { SearchResultsList } from '../components/search/SearchResultsList';

const Search = () => {
    const {
        query, setQuery,
        results,
        loading,
        type, setType,
        year, setYear,
        page,
        totalPages,
        totalCount,
        documentTypes,
        performSearch,
        getPageRange
    } = useSearch();

    const handleKeyDown = (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            performSearch(true);
        }
    };

    const handlePageChange = (newPage) => {
        if (newPage >= 1 && newPage <= totalPages) {
            performSearch(false, newPage);
            window.scrollTo({ top: 0, behavior: 'smooth' });
        }
    };

    return (
        <div className="flex-1 flex flex-col bg-[var(--background)] overflow-hidden relative h-full">
            
            {/* Header & Floating Search Bar */}
            <SearchHeader 
                query={query} 
                setQuery={setQuery}
                type={type} 
                setType={setType}
                year={year} 
                setYear={setYear}
                documentTypes={documentTypes}
                loading={loading}
                performSearch={performSearch}
                handleKeyDown={handleKeyDown}
            />

            {/* Main Content Area */}
            <div className="flex-1 overflow-y-auto w-full custom-scrollbar pb-24 px-8 lg:px-12 pt-8">
                <div className="max-w-7xl mx-auto space-y-10">
                    <SearchResultsList 
                        loading={loading}
                        results={results}
                        query={query}
                        totalCount={totalCount}
                        type={type}
                        year={year}
                        setQuery={setQuery}
                        setType={setType}
                        setYear={setYear}
                        performSearch={performSearch}
                        page={page}
                        totalPages={totalPages}
                        getPageRange={getPageRange}
                        handlePageChange={handlePageChange}
                    />
                </div>
            </div>
        </div>
    );
};

export default Search;
