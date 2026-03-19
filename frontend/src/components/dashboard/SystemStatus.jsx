import React from 'react';
import { IconCheck, IconServer, IconDatabase, IconBrain } from "@tabler/icons-react";

export const SystemStatus = ({ systemStatus }) => {
    return (
        <div className="bg-[var(--card-bg)] rounded-2xl p-6 md:p-8 shadow-sm border border-[var(--card-border)] flex flex-col h-full h-full min-h-[400px]">
            <div className="flex items-center justify-between mb-8 pb-4 border-b border-[var(--card-border)]">
                <div>
                    <h2 className="text-[18px] font-bold text-[var(--foreground)] tracking-tight flex items-center gap-2">
                        System Diagnostics
                    </h2>
                    <p className="text-[13px] text-[var(--text-soft)] mt-1 font-medium">Real-time model and database health.</p>
                </div>
            </div>

            <div className="space-y-4 flex-1 flex flex-col pt-2">
                {/* Core Server Node */}
                <div className="flex items-center justify-between p-4 rounded-xl border border-[var(--card-border)] bg-black/5 dark:bg-white/5 hover:bg-black/5 dark:bg-white/5 transition-colors">
                    <div className="flex items-center gap-3.5">
                        <div className="h-10 w-10 flex items-center justify-center bg-[var(--card-bg)] rounded-lg border border-[var(--card-border)] shadow-sm text-[var(--text-soft)]">
                            <IconServer size={20} stroke={1.5} />
                        </div>
                        <div>
                            <p className="text-[14px] font-bold text-[var(--foreground)] leading-tight">API Gateway</p>
                            <p className="text-[12px] text-[var(--text-soft)] mt-0.5">Python Backend</p>
                        </div>
                    </div>
                    {systemStatus.api ?
                        <div className="bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 h-8 w-8 rounded-lg flex items-center justify-center border border-emerald-500/20">
                            <IconCheck size={18} stroke={2} />
                        </div> :
                        <div className="h-2.5 w-2.5 bg-rose-500 rounded-full animate-pulse mr-3"></div>
                    }
                </div>

                {/* Database Node */}
                <div className="flex items-center justify-between p-4 rounded-xl border border-[var(--card-border)] bg-black/5 dark:bg-white/5 hover:bg-black/5 dark:bg-white/5 transition-colors">
                    <div className="flex items-center gap-3.5">
                        <div className="h-10 w-10 flex items-center justify-center bg-[var(--card-bg)] rounded-lg border border-[var(--card-border)] shadow-sm text-[var(--text-soft)]">
                            <IconDatabase size={20} stroke={1.5} />
                        </div>
                        <div>
                            <p className="text-[14px] font-bold text-[var(--foreground)] leading-tight">Vector Database</p>
                            <p className="text-[12px] text-[var(--text-soft)] mt-0.5">ChromaDB Connect</p>
                        </div>
                    </div>
                    {systemStatus.db ?
                        <div className="bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 h-8 w-8 rounded-lg flex items-center justify-center border border-emerald-500/20">
                            <IconCheck size={18} stroke={2} />
                        </div> :
                        <div className="h-2.5 w-2.5 bg-yellow-500 rounded-full animate-pulse mr-3"></div>
                    }
                </div>

                {/* LLM Engine Node */}
                <div className="flex items-center justify-between p-4 rounded-xl border border-[var(--card-border)] bg-black/5 dark:bg-white/5 hover:bg-black/5 dark:bg-white/5 transition-colors">
                    <div className="flex items-center gap-3.5">
                        <div className="h-10 w-10 flex items-center justify-center bg-[var(--navbar-bg)] rounded-lg border border-slate-800 shadow-sm text-[var(--brand-500)]">
                            <IconBrain size={20} stroke={1.5} />
                        </div>
                        <div>
                            <p className="text-[14px] font-bold text-[var(--foreground)] leading-tight">Neural Engine</p>
                            <p className="text-[12px] text-[var(--text-soft)] mt-0.5">Local Llama3/Mistral</p>
                        </div>
                    </div>
                    {systemStatus.ai ?
                        <div className="bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 h-8 w-8 rounded-lg flex items-center justify-center border border-emerald-500/20">
                            <IconCheck size={18} stroke={2} />
                        </div> :
                        <div className="h-2.5 w-2.5 bg-rose-500 rounded-full animate-pulse mr-3"></div>
                    }
                </div>
            </div>
            
            <div className="mt-auto pt-6 text-center">
               <span className="inline-flex items-center gap-2 text-[12px] font-semibold text-[var(--text-muted)] bg-black/5 dark:bg-white/5 px-3 py-1.5 rounded-lg border border-[var(--card-border)]">
                    <span className="relative flex h-2 w-2">
                      <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                      <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
                    </span>
                    All Primary Systems Operational
               </span>
            </div>
        </div>
    );
};