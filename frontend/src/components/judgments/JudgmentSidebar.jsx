import React from 'react';
import { IconFilter, IconBuildingBank, IconCalendar } from '@tabler/icons-react';

import { useJudgmentContext } from '../../context/JudgmentContext';

export const JudgmentSidebar = () => {
    const { availableCourts, selectedCourt, setSelectedCourt, availableYears, selectedYear, setSelectedYear } = useJudgmentContext();
    return (
        <div className="bg-[var(--card-bg)] rounded-2xl border border-[var(--card-border)] p-6 shadow-sm sticky top-8">
            <div className="flex items-center gap-2 mb-6 pb-4 border-b border-[var(--card-border)]">
                <IconFilter size={20} className="text-[var(--text-muted)]" stroke={1.5} />
                <h3 className="font-bold text-[var(--foreground)] tracking-tight">Filters</h3>
            </div>

            <div className="mb-6">
                <label className="text-[11px] font-bold text-[var(--text-muted)] uppercase tracking-widest mb-3 flex items-center gap-1.5">
                    <IconBuildingBank size={14} /> Focus Court
                </label>
                <div className="flex flex-col gap-2.5 max-h-48 overflow-y-auto pr-2 custom-scrollbar">
                    {availableCourts.map(court => (
                        <label key={court} className="flex items-center gap-3 cursor-pointer group">
                            <input
                                type="radio"
                                name="court"
                                value={court}
                                checked={selectedCourt === court}
                                onChange={(e) => setSelectedCourt(e.target.value)}
                                className="w-4 h-4 text-[#d4af37] focus:ring-[#d4af37]/20 border-[var(--card-border)] bg-[var(--card-bg)]"
                            />
                            <span className={`text-[13px] transition-colors line-clamp-1 ${selectedCourt === court ? 'text-[var(--foreground)] font-bold' : 'text-[var(--text-soft)] font-medium group-hover:text-[var(--foreground)]'}`}>
                                {court}
                            </span>
                        </label>
                    ))}
                </div>
            </div>

            <div>
                <label className="text-[11px] font-bold text-[var(--text-muted)] uppercase tracking-widest mb-3 flex items-center gap-1.5">
                    <IconCalendar size={14} /> Target Year
                </label>
                <select
                    value={selectedYear}
                    onChange={(e) => setSelectedYear(e.target.value)}
                    className="w-full bg-[var(--card-bg)] hover:bg-[var(--card-border)] transition-colors border border-[var(--card-border)] text-[var(--foreground)] text-[13px] rounded-xl px-4 py-2.5 outline-none focus:ring-2 focus:ring-[#d4af37]/20 focus:border-[#d4af37]/50 font-medium"
                >
                    {availableYears.map(year => (
                        <option className="bg-[var(--card-bg)] text-[var(--foreground)]" key={year} value={year}>{year}</option>
                    ))}
                </select>
            </div>
        </div>
    );
};