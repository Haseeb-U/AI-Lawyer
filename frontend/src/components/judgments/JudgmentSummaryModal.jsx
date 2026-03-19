import React from 'react';
import { IconScale, IconX } from '@tabler/icons-react';
import { Button } from '../ui/button';

import { useJudgmentContext } from '../../context/JudgmentContext';

export const JudgmentSummaryModal = () => {
    const {
        selectedJudgment,
        summaryLoading,
        summaryError,
        summary,
        setIsSummaryModalOpen
    } = useJudgmentContext();

    const closeModal = () => setIsSummaryModalOpen(false);
    return (
        <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-slate-900/60 backdrop-blur-md">
            <div className="bg-[var(--card-bg)] rounded-3xl w-full max-w-3xl max-h-[85vh] flex flex-col shadow-2xl relative overflow-hidden animate-in zoom-in-95 duration-200 ring-1 ring-slate-900/5">
                
                {/* Decorative background overlay */}
                <div className="absolute top-0 inset-x-0 h-40 bg-gradient-to-b from-rose-50/50 to-transparent pointer-events-none"></div>

                {/* Modal Header */}
                <div className="p-6 md:p-8 pb-4 flex justify-between items-start relative z-10 shrink-0">
                    <div>
                        <div className="flex items-center gap-2 text-[#d4af37] font-bold text-xs uppercase tracking-widest mb-3">
                            <IconScale size={16} stroke={2.5}/> AI Legal Analysis
                        </div>
                        <h2 className="text-2xl font-bold text-[var(--foreground)] leading-tight pr-8">
                            {selectedJudgment?.title}
                        </h2>
                    </div>
                    <button 
                        onClick={closeModal}
                        className="p-2 -mr-2 -mt-2 text-[var(--text-muted)] hover:text-[var(--foreground)] hover:bg-[var(--card-border)] rounded-xl transition-colors shrink-0"
                    >
                        <IconX size={24} stroke={2.5} />
                    </button>
                </div>

                {/* Modal Body */}
                <div className="px-6 md:px-8 pb-8 overflow-y-auto relative z-10 flex-1 custom-scrollbar">
                    {summaryLoading ? (
                        <div className="flex flex-col items-center justify-center py-20 text-[var(--text-soft)]">
                            <div className="relative mb-6">
                                <div className="absolute inset-0 bg-rose-200 rounded-full blur-xl opacity-50 animate-pulse"></div>
                                <div className="h-16 w-16 bg-[var(--card-bg)] border border-[var(--card-border)] shadow-xl rounded-2xl flex items-center justify-center relative z-10">
                                    <IconScale className="animate-pulse text-rose-600" size={32} stroke={1.5} />
                                </div>
                            </div>
                            <p className="font-semibold text-[var(--foreground)] text-lg">Analyzing Legal Document</p>
                            <p className="text-sm mt-1 text-[var(--text-soft)]">Extracting key facts and verdict summary...</p>
                        </div>
                    ) : summaryError ? (
                        <div className="p-5 bg-red-50 text-red-700 rounded-2xl border border-red-100/50 flex items-start gap-4 mt-4">
                            <IconX className="shrink-0 mt-0.5" size={20} /> 
                            <div>
                                <h4 className="font-bold mb-1">Analysis Failed</h4>
                                <p className="text-sm opacity-90">{summaryError}</p>
                            </div>
                        </div>
                    ) : (
                        <div className="mt-2 bg-black/5 dark:bg-white/5 border border-[var(--card-border)] rounded-2xl p-6 md:p-8 prose prose-slate prose-headings:text-[var(--foreground)] prose-p:text-[var(--text-soft)] prose-strong:text-[var(--foreground)] max-w-none prose-p:leading-relaxed text-[15.5px]">
                            {summary ? (
                                <div dangerouslySetInnerHTML={{ __html: summary.replace(/\n/g, '<br />') }} />
                            ) : (
                                <p className="italic text-[var(--text-muted)] text-center py-10">Summary generated successfully but contains no measurable text.</p>
                            )}
                        </div>
                    )}
                </div>
                
                {/* Modal Footer */}
                <div className="p-4 md:p-6 border-t border-[var(--card-border)] bg-[var(--card-bg)] flex justify-end gap-3 z-10 shrink-0">
                    <Button 
                        variant="secondary"
                        onClick={closeModal}
                        className="px-6 py-5 bg-[var(--card-border)] text-[var(--foreground)] hover:bg-[#d4af37] hover:text-[#051326] dark:hover:bg-[#d4af37] dark:hover:text-[#051326] rounded-xl font-bold transition-colors text-sm h-auto"
                    >
                        Dismiss
                    </Button>
                </div>
            </div>
        </div>
    );
};