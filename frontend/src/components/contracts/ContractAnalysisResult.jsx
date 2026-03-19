import React from 'react';
import { IconCheck } from '@tabler/icons-react';
import { marked } from 'marked';

export const ContractAnalysisResult = ({ analysis }) => {
    if (!analysis) return null;

    // Parse the markdown
    const html = marked.parse(analysis || '');

    return (
        <div className="bg-[var(--card-bg)] border border-[var(--card-border)] rounded-[24px] p-8 mt-6 shadow-sm">
            <h3 className="text-[18px] font-bold flex items-center gap-2.5 mb-6 pb-4 border-b border-[var(--card-border)] tracking-tight text-[var(--foreground)]">
                <span className="h-6 w-6 rounded bg-[#d4af37]/10 text-[#b48512] flex items-center justify-center border border-[#d4af37]/20">
                    <IconCheck size={16} stroke={2.5} />
                </span>
                AI Legal Review
            </h3>

            <div
                className="prose prose-sm sm:prose-base max-w-none 
                    prose-headings:text-[var(--foreground)] prose-headings:font-bold tracking-tight
                    prose-p:text-[var(--text-soft)] prose-p:leading-relaxed
                    prose-li:text-[var(--text-soft)]
                    prose-strong:text-[var(--foreground)]
                    prose-a:text-[#d4af37] hover:prose-a:text-[#b48512]
                    prose-blockquote:border-l-[#d4af37] prose-blockquote:bg-[var(--card-border)] prose-blockquote:py-1 prose-blockquote:px-4 prose-blockquote:rounded-r-lg prose-blockquote:not-italic
                    dark:prose-invert"
                dangerouslySetInnerHTML={{ __html: html }}
            />
        </div>
    );
};