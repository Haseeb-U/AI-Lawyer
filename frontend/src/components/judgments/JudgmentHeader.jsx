import React from 'react';
import { IconGavel, IconSearch, IconLoader } from '@tabler/icons-react';
import { Input } from '../ui/input';
import { Button } from '../ui/button';
import { useJudgmentContext } from '../../context/JudgmentContext';

export const JudgmentHeader = () => {
    const { search, setSearch, handleSearchSubmit, loading, fetchJudgments } = useJudgmentContext();
    return (
        <div className="w-full bg-transparent sticky top-0 z-30 pt-8 pb-4">
            <div className="max-w-7xl mx-auto px-6 lg:px-12">
                <div className="flex flex-col md:flex-row md:items-end justify-between gap-6 border-b border-[var(--card-border)] pb-6">
                    <div>
                        <h1 className="text-[24px] font-bold text-[var(--foreground)] tracking-tight flex items-center gap-3">
                            <div className="h-8 w-8 bg-[var(--navbar-bg)] text-[var(--brand-500)] rounded-lg flex items-center justify-center shadow-md">
                                <IconGavel size={18} stroke={2} />
                            </div>
                            Judgments Library
                        </h1>
                        <p className="text-[var(--text-soft)] font-medium mt-2 text-[14px]">Search and analyze historical case laws seamlessly.</p>
                    </div>

                    <form onSubmit={(e) => handleSearchSubmit(e, fetchJudgments)} className="w-full md:w-[450px] shrink-0">
                        <div className="relative flex items-center bg-[var(--card-bg)] shadow-sm rounded-xl border border-[var(--card-border)] pl-4 pr-1.5 py-1.5 focus-within:ring-2 focus-within:ring-[#d4af37]/10 focus-within:border-[#d4af37]/30 transition-all hover:border-[var(--card-border)]">
                            <IconSearch className="text-[var(--text-muted)] mr-2 shrink-0" size={18} />
                            <Input
                                type="text"
                                placeholder="Search sections, keywords..."
                                value={search}
                                onChange={(e) => setSearch(e.target.value)}
                                className="flex-1 border-0 shadow-none focus-visible:ring-0 focus-visible:ring-offset-0 bg-transparent outline-none text-[14px] text-[var(--foreground)] placeholder:text-[var(--text-muted)] font-medium py-1 px-0 h-auto"
                            />
                            <Button
                                type="submit"
                                disabled={loading}
                                className="ml-2 bg-[var(--navbar-bg)] text-[#d4af37] dark:text-[#d4af37] border border-[#d4af37]/30 px-5 py-2 rounded-lg font-bold hover:bg-[#051326]/90 transition-all h-auto text-[13px]"
                            >
                                {loading ? <IconLoader className="animate-spin" size={16} /> : 'Search Cases'}
                            </Button>
                        </div>
                    </form>
                </div>
            </div>
        </div>
    );
};