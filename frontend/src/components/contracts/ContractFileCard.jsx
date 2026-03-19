import React from 'react';
import { IconFileText, IconX, IconLoader } from '@tabler/icons-react';
import { Button } from '../ui/button';

export const ContractFileCard = ({
    file,
    loading,
    progress,
    analysis,
    handleRemove,
    analyzeContract
}) => {
    return (
        <div className="bg-[var(--card-bg)] border border-[var(--card-border)] rounded-[24px] p-6 flex flex-col gap-6 shadow-sm">
            <div className="flex items-center justify-between p-4 bg-black/5 dark:bg-white/5 rounded-xl border border-[var(--card-border)] transition-all">
                <div className="flex items-center gap-4">
                    <div className="h-12 w-12 bg-[var(--navbar-bg)] text-white rounded-lg flex items-center justify-center shadow-sm">
                        <IconFileText size={24} stroke={1.5} className="text-[#d4af37]" />
                    </div>
                    <div>
                        <p className="font-semibold text-[14px] text-[var(--foreground)] truncate max-w-sm tracking-tight">{file.name}</p>
                        <p className="text-[12px] text-[var(--text-soft)] font-medium mt-0.5">{(file.size / 1024 / 1024).toFixed(2)} MB • {file.type.split('/')[1]?.toUpperCase() || 'TXT'}</p>
                    </div>
                </div>
                <button
                    onClick={handleRemove}
                    disabled={loading}
                    className="p-2 text-[var(--text-muted)] hover:text-rose-600 hover:bg-rose-50/50 dark:hover:bg-rose-900/20 rounded-lg transition-colors disabled:opacity-50"
                >
                    <IconX size={20} stroke={2} />
                </button>
            </div>

            {!analysis && (
                <div className="flex flex-col items-center pt-2">
                    {loading ? (
                        <div className="w-full space-y-4 px-2">
                            <div className="flex justify-between text-[13px] text-[var(--text-muted)] font-semibold tracking-tight">
                                <span className="flex items-center gap-2"><IconLoader size={16} className="animate-spin text-[#d4af37]" /> Analyzing clauses...</span>
                                <span className="text-[#d4af37]">{progress}%</span>
                            </div>
                            <div className="w-full bg-[var(--card-border)] rounded-full h-2 overflow-hidden">
                                <div
                                    className="bg-[#d4af37] h-full rounded-full transition-all duration-300 ease-out"
                                    style={{ width: `${progress}%` }}
                                ></div>
                            </div>
                        </div>
                    ) : (
                        <Button
                            onClick={analyzeContract}
                            className="px-8 w-full md:w-auto shadow-md bg-[var(--navbar-bg)] text-[#d4af37] hover:bg-slate-800 transition-colors border border-[#d4af37]/30"
                        >
                            Start AI Contract Analysis
                        </Button>
                    )}
                </div>
            )}
        </div>
    );
};
