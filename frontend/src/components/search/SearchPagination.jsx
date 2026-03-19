import React from 'react';
import { IconChevronLeft, IconChevronRight } from '@tabler/icons-react';
import { Button } from '../ui/button';

export const SearchPagination = ({
    page,
    totalPages,
    getPageRange,
    handlePageChange
}) => {
    if (totalPages <= 1) return null;

    const { start, end } = getPageRange();
    
    return (
        <div className="flex items-center justify-center gap-2 mt-10 p-4 bg-[var(--card-bg)] border border-[var(--card-border)] rounded-2xl shadow-sm w-fit mx-auto">
            <button 
                onClick={() => handlePageChange(page - 1)}
                disabled={page === 1}
                className="p-2.5 rounded-xl hover:bg-[var(--card-border)] disabled:opacity-30 disabled:hover:bg-transparent text-[var(--foreground)] transition-colors"
            >
                <IconChevronLeft size={20} stroke={2.5} />
            </button>
            
            <div className="flex gap-1.5 px-2">
                {Array.from({ length: end - start + 1 }).map((_, i) => {
                    const pageNum = start + i;
                    return (
                        <button
                            key={pageNum}
                            onClick={() => handlePageChange(pageNum)}
                            className={`h-10 w-10 flex items-center justify-center rounded-xl font-bold transition-all ${page === pageNum ? 'bg-[var(--navbar-bg)] text-white shadow-md border border-[#d4af37]/30' : 'bg-transparent text-[var(--text-soft)] hover:bg-[var(--card-border)]'}`}
                        >
                            {pageNum}
                        </button>
                    );
                })}
            </div>
            
            <button 
                onClick={() => handlePageChange(page + 1)}
                disabled={page === totalPages}
                className="p-2.5 rounded-xl hover:bg-[var(--card-border)] disabled:opacity-30 disabled:hover:bg-transparent text-[var(--foreground)] transition-colors"
            >
                <IconChevronRight size={20} stroke={2.5} />
            </button>
        </div>
    );
};