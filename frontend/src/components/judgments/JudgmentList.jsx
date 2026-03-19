import React from 'react';
import { IconLoader, IconGavel, IconScale, IconRefresh, IconFileText } from '@tabler/icons-react';
import { Button } from '../ui/button';
import { Skeleton } from '../ui/skeleton';

import { useJudgmentContext } from '../../context/JudgmentContext';

export const JudgmentList = () => {
    const {
        error,
        loading,
        judgments: { length: judgmentsLength } = {},
        filteredJudgments,
        selectedCourt,
        selectedYear,
        clearFilters,
        handleSummarize,
        hasMore,
        loadMore
    } = useJudgmentContext();
    if (error) {
        return (
            <div className="bg-rose-50 border border-rose-100 rounded-2xl p-6 text-rose-700 shadow-sm flex items-start gap-4">
                <div className="shrink-0 bg-[var(--card-bg)] p-2 border border-rose-100 rounded-xl">
                    <IconRefresh className="text-rose-500" stroke={2} />
                </div>
                <div>
                    <h3 className="font-bold text-[15px]">Failed to load rulings</h3>
                    <p className="text-[13px] font-medium opacity-80 mt-1">{error}</p>
                </div>
            </div>
        );
    }

    if (loading && judgmentsLength === 0) {
        return (
            <div className="space-y-4">
                {[1, 2, 3].map(i => (
                    <div key={i} className="bg-[var(--card-bg)] rounded-2xl border border-[var(--card-border)] p-6 flex flex-col gap-4">
                        <Skeleton className="h-6 w-3/4 rounded-lg" />
                        <Skeleton className="h-4 w-full rounded-md" />
                        <Skeleton className="h-4 w-5/6 rounded-md" />
                        <div className="flex gap-3 pt-2">
                            <Skeleton className="h-8 w-24 rounded-lg" />
                            <Skeleton className="h-8 w-24 rounded-lg" />
                        </div>
                    </div>
                ))}
            </div>
        );
    }

    if (judgmentsLength > 0 && filteredJudgments.length === 0) {
        return (
            <div className="bg-[var(--card-bg)] border text-center border-dashed border-[var(--card-border)] rounded-2xl p-16 flex flex-col items-center justify-center">
                <div className="h-16 w-16 bg-black/5 dark:bg-white/5 text-[var(--text-muted)] rounded-2xl flex items-center justify-center mb-4">
                    <IconGavel size={32} stroke={1.5} />
                </div>
                <h3 className="font-bold text-[var(--foreground)] text-[16px] mb-2 tracking-tight">No Exact Matches</h3>
                <p className="text-[var(--text-soft)] text-[13px] font-medium mb-6">Your current filters returned 0 results. Try broadening your criteria.</p>
                {(selectedCourt !== 'All' || selectedYear !== 'All') && (
                    <Button onClick={clearFilters} className="bg-[var(--navbar-bg)] hover:bg-[#051326]/90 text-[#d4af37] dark:text-[#d4af37] border border-[#d4af37]/30 rounded-xl shadow-sm px-6 h-auto py-2.5">
                        Reset Filters
                    </Button>
                )}
            </div>
        );
    }

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between mb-2">
                <p className="text-[13px] font-bold text-[var(--text-soft)] uppercase tracking-widest">
                    {filteredJudgments.length} document{filteredJudgments.length !== 1 && 's'}
                </p>
                {(selectedCourt !== 'All' || selectedYear !== 'All') && (
                    <button onClick={clearFilters} className="text-[12px] font-bold text-[var(--brand-500)] hover:text-[#b48512] transition-colors uppercase tracking-wider">
                        Clear Filters &times;
                    </button>
                )}
            </div>

            {filteredJudgments.map(judgment => (
                <div key={judgment.id} className="bg-[var(--card-bg)] rounded-2xl p-6 border border-[var(--card-border)] shadow-sm hover:shadow-md hover:border-[var(--card-border)] transition-all group flex flex-col relative overflow-hidden">
                    <div className="absolute top-0 left-0 w-1 h-full bg-[#d4af37] opacity-0 group-hover:opacity-100 transition-opacity"></div>
                    
                    <div className="flex items-center gap-2.5 mb-4">
                        <span className="px-2.5 py-1 bg-black/5 dark:bg-white/5 border border-[var(--card-border)] text-[var(--text-soft)] font-bold text-[10px] rounded-md uppercase tracking-widest shadow-[0_1px_2px_rgba(0,0,0,0.02)]">
                            {judgment.court}
                        </span>
                        <span className="px-2.5 py-1 bg-[var(--navbar-bg)] text-[var(--brand-500)] font-bold text-[10px] rounded-md shadow-sm">
                            {judgment.year}
                        </span>
                    </div>

                    <h3 className="text-[17px] font-bold text-[var(--foreground)] group-hover:text-[#d4af37] transition-colors leading-snug mb-3 tracking-tight">
                        {judgment.title}
                    </h3>
                    
                    <p className="text-[var(--text-soft)] font-medium text-[13px] line-clamp-3 leading-relaxed mb-6">
                        {judgment.snippet}
                    </p>

                    <div className="mt-auto border-t border-[var(--card-border)] pt-4 flex flex-wrap gap-3 items-center">
                        <Button 
                            onClick={() => handleSummarize(judgment)}
                            className="bg-[var(--navbar-bg)] text-[#d4af37] dark:text-[#d4af37] border border-[#d4af37]/30 hover:bg-[#051326]/90 text-[13px] h-auto py-2 px-5 font-semibold rounded-xl flex items-center gap-2 transition-all shadow-sm cursor-pointer"
                        >
                            <IconScale size={16} stroke={2} className="text-[#d4af37]" /> View Summary
                        </Button>
                        {judgment.source_url && (
                            <a 
                                href={judgment.source_url}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="flex items-center gap-2 text-[13px] font-bold text-[var(--text-soft)] hover:text-[#051326] dark:hover:text-[#051326] bg-[var(--card-bg)] hover:bg-[#d4af37] dark:hover:bg-[#d4af37] border border-[var(--card-border)] hover:border-[#d4af37] px-5 py-2 rounded-xl transition-all"
                            >
                               <IconFileText size={16} stroke={1.5} /> View Original
                            </a>
                        )}
                    </div>
                </div>
            ))}

            {hasMore && (
                <div className="flex justify-center pt-8 pb-4">
                    <Button 
                        onClick={loadMore} 
                        disabled={loading}
                        className="bg-[var(--card-bg)] border border-[var(--card-border)] text-[var(--foreground)] dark:text-[var(--foreground)] hover:bg-black/5 dark:hover:bg-white/5 hover:text-[var(--foreground)] font-semibold px-8 py-2.5 rounded-xl shadow-sm transition-all h-auto disabled:opacity-50 text-[13px]"
                    >
                        {loading ? <IconLoader className="animate-spin" size={18} /> : 'Load More Filings'}
                    </Button>
                </div>
            )}
        </div>
    );
};