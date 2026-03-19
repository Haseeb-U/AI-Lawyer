import React from 'react';
import { IconSearch, IconBook, IconLoader } from '@tabler/icons-react';
import { Input } from '../ui/input';
import { Button } from '../ui/button';

export const SearchHeader = ({
    query,
    setQuery,
    type,
    setType,
    year,
    setYear,
    documentTypes,
    loading,
    performSearch,
    handleKeyDown
}) => {
    return (
        <div className="w-full bg-transparent sticky top-0 z-30 pt-8 pb-4">
            <div className="max-w-7xl mx-auto px-6 lg:px-12">
                <div className="flex flex-col mb-4">
                    <h1 className="text-[24px] font-bold text-[var(--foreground)] tracking-tight flex items-center gap-3">
                        <div className="h-8 w-8 bg-[var(--navbar-bg)] text-[var(--brand-500)] rounded-lg flex items-center justify-center shadow-md">
                            <IconBook size={18} stroke={2} />
                        </div>
                        Legal Knowledge Base
                    </h1>
                    <p className="text-[var(--text-soft)] font-medium mt-2 text-[14px]">Perform deep semantic searches across federal acts, provincial laws, and constitution.</p>
                </div>

                {/* Search Field & Filters */}
                <div className="flex flex-col md:flex-row gap-3 relative z-20 border-b border-[var(--card-border)] pb-6 mt-4">
                    <div className="flex-1 bg-[var(--card-bg)] shadow-sm rounded-xl border border-[var(--card-border)] pl-4 py-1.5 focus-within:ring-2 focus-within:ring-[#d4af37]/10 focus-within:border-[#d4af37]/30 transition-all hover:border-[var(--card-border)] flex items-center">
                        <IconSearch className="text-[var(--text-muted)] mr-2 shrink-0" size={18} />
                        <Input
                            type="text"
                            placeholder="E.g. Section 302 of Pakistan Penal Code or Article 25..."
                            className="flex-1 border-0 shadow-none focus-visible:ring-0 focus-visible:ring-offset-0 bg-transparent outline-none text-[14px] text-[var(--foreground)] placeholder:text-[var(--text-muted)] font-medium py-1 px-0 h-auto"
                            value={query}
                            onChange={(e) => setQuery(e.target.value)}
                            onKeyDown={handleKeyDown}
                        />
                    </div>
                    
                    <div className="flex gap-3">
                        <select
                            value={type}
                            onChange={(e) => setType(e.target.value)}
                            className="w-full md:w-40 px-3 bg-[var(--card-bg)] border border-[var(--card-border)] text-[var(--foreground)] font-medium text-[13px] rounded-xl focus:outline-none focus:ring-2 focus:ring-[#d4af37]/10 focus:border-[#d4af37]/30 shadow-sm transition-all hover:border-[var(--card-border)]"
                        >
                            <option className="bg-[var(--card-bg)] text-[var(--foreground)]" value="all">All Documents</option>
                            {documentTypes.map((dt, i) => (
                                <option className="bg-[var(--card-bg)] text-[var(--foreground)]" key={i} value={dt}>{dt}</option>
                            ))}
                        </select>

                        <Input 
                            type="number"
                            placeholder="Year"
                            value={year}
                            onChange={(e) => setYear(e.target.value)}
                            className="w-24 px-3 bg-[var(--card-bg)] border border-[var(--card-border)] font-medium text-[13px] rounded-xl focus:outline-none focus:ring-2 focus:ring-[#d4af37]/10 focus:border-[#d4af37]/30 shadow-sm transition-all hover:border-[var(--card-border)] placeholder:text-[var(--text-muted)] h-auto"
                        />
                        <Button
                            className="px-6 bg-[var(--navbar-bg)] text-[#d4af37] dark:text-[#d4af37] border border-[#d4af37]/30 rounded-xl hover:bg-[#051326]/90 transition-all font-bold text-[13px] flex items-center justify-center gap-2 h-auto shrink-0"
                            onClick={() => performSearch(true)}
                        >
                            {loading ? <IconLoader className="animate-spin" size={16} /> : 'Search'}
                        </Button>
                    </div>
                </div>
            </div>
        </div>
    );
};