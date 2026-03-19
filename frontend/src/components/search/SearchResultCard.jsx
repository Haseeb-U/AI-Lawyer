import React from 'react';
import { IconFileText, IconBuildingBank, IconCalendar, IconExternalLink } from '@tabler/icons-react';

export const SearchResultCard = ({ result }) => {
    return (
        <div className="bg-[var(--card-bg)] border border-[var(--card-border)] rounded-3xl p-7 shadow-sm hover:shadow-md hover:-translate-y-0.5 transition-all duration-300 group">
            <h3 className="text-xl font-bold text-[var(--foreground)] mb-3 group-hover:text-[#d4af37] transition-colors leading-snug">
                {result.title || 'Untitled Document'}
            </h3>
            
            <p className="text-[var(--text-soft)] mb-6 line-clamp-3 leading-relaxed text-[15.5px]">
                {result.text || 'No snippet available for this document.'}
            </p>
            
            <div className="flex flex-wrap gap-4 text-sm justify-between items-center w-full pt-5 border-t border-[var(--card-border)]">
                <div className="flex flex-wrap items-center gap-2.5">
                    {result.metadata?.document_type && (
                        <span className="flex items-center gap-1.5 px-3 py-1 bg-[var(--navbar-bg)] text-white font-bold rounded-lg uppercase tracking-wider text-[11px] shadow-sm">
                            <IconFileText size={14} stroke={2} className="text-[#d4af37]" />
                            {result.metadata.document_type}
                        </span>
                    )}
                    {result.metadata?.court && (
                        <span className="flex items-center gap-1.5 px-3 py-1 bg-black/5 dark:bg-white/5 border border-[var(--card-border)] text-[var(--text-soft)] font-semibold rounded-lg text-xs">
                            <IconBuildingBank size={14} className="text-[var(--text-muted)]" />
                            {result.metadata.court}
                        </span>
                    )}
                    {result.metadata?.year && (
                        <span className="flex items-center gap-1.5 px-3 py-1 bg-[#d4af37]/10 text-[#b48512] border border-[#d4af37]/20 font-bold rounded-lg text-xs">
                            <IconCalendar size={14} />
                            {result.metadata.year}
                        </span>
                    )}
                </div>

                {result.metadata?.source_url && (
                    <a
                        href={result.metadata.source_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex items-center gap-1.5 text-[#051326] dark:text-gray-200 hover:text-[#d4af37] font-semibold px-4 py-2 rounded-xl hover:bg-[#051326] transition-colors border border-transparent hover:border-[#051326] hover:shadow-md"
                    >
                        View Source <IconExternalLink size={16} stroke={2} />
                    </a>
                )}
            </div>
        </div>
    );
};