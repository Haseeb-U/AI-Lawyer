export const useSearchController = (performSearch, totalPages) => {
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

    return {
        handleKeyDown,
        handlePageChange
    };
};