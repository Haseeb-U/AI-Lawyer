import React from 'react';
import {
    IconHistory,
    IconMessageDots,
    IconFileText,
    IconGavel,
    IconSearch,
    IconLoader
} from "@tabler/icons-react";
import { Skeleton } from '../ui/skeleton';

export const ActivityTimeline = ({ activities, isLoading }) => {
    
    // Map backend activity types to local component rendering styles
    const getIconInfo = (type) => {
        switch (type) {
            case 'chat': return { icon: <IconMessageDots size={18} stroke={1.5} />, bg: 'bg-[var(--navbar-bg)]', color: 'text-[var(--brand-500)]' };
            case 'contract': return { icon: <IconFileText size={18} stroke={1.5} />, bg: 'bg-[var(--navbar-bg)]', color: 'text-[var(--brand-500)]' };
            case 'judgment': return { icon: <IconGavel size={18} stroke={1.5} />, bg: 'bg-[var(--navbar-bg)]', color: 'text-[var(--brand-500)]' };
            case 'search': return { icon: <IconSearch size={18} stroke={1.5} />, bg: 'bg-[var(--navbar-bg)]', color: 'text-[var(--brand-500)]' };
            default: return { icon: <IconHistory size={18} stroke={1.5} />, bg: 'bg-[var(--navbar-bg)]', color: 'text-[var(--brand-500)]' };
        }
    };

    return (
        <div className="bg-[var(--card-bg)] rounded-2xl p-6 md:p-8 shadow-sm border border-[var(--card-border)] flex flex-col h-full min-h-[400px]">
            <div className="flex items-center justify-between mb-8 pb-4 border-b border-[var(--card-border)]">
                <div>
                    <h2 className="text-[18px] font-bold text-[var(--foreground)] tracking-tight flex items-center gap-2">
                        Recent Activity
                    </h2>
                    <p className="text-[13px] text-[var(--text-soft)] mt-1 font-medium">Your historical interactions across all modules.</p>
                </div>
            </div>

            <div className="flex-1 overflow-y-auto pr-2 custom-scrollbar relative">
                {isLoading ? (
                    <div className="space-y-6 relative before:absolute before:inset-0 before:ml-5 before:-translate-x-px md:before:mx-auto md:before:translate-x-0 before:h-full before:w-0.5 before:bg-gradient-to-b before:from-transparent before:via-[var(--card-border)] before:to-transparent">
                        {[1, 2, 3].map((i) => (
                            <div key={i} className="relative flex items-center justify-between md:justify-normal md:odd:flex-row-reverse group is-active">
                                <div className="flex items-center justify-center w-10 h-10 rounded-xl border border-[var(--card-bg)] shrink-0 md:order-1 md:group-odd:-translate-x-1/2 md:group-even:translate-x-1/2 shadow-sm z-10 box-content bg-[var(--card-border)] relative">
                                    <Skeleton className="w-8 h-8 rounded-lg" />
                                </div>
                                <div className="w-[calc(100%-4rem)] md:w-[calc(50%-2.5rem)] bg-[var(--card-bg)] p-4 rounded-2xl border border-[var(--card-border)] shadow-[0_2px_10px_-4px_rgba(0,0,0,0.03)]">
                                    <div className="flex items-center justify-between mb-1">
                                        <Skeleton className="h-4 w-3/4 rounded-md" />
                                        <Skeleton className="h-4 w-12 rounded-lg" />
                                    </div>
                                    <Skeleton className="h-3 w-full rounded-md mt-2" />
                                </div>
                            </div>
                        ))}
                    </div>
                ) : activities.length > 0 ? (
                    <div className="space-y-6 relative before:absolute before:inset-0 before:ml-5 before:-translate-x-px md:before:mx-auto md:before:translate-x-0 before:h-full before:w-0.5 before:bg-gradient-to-b before:from-transparent before:via-[var(--card-border)] before:to-transparent">
                        {activities.map((activity, index) => {
                            const iconInfo = getIconInfo(activity.type);
                            return (
                                <div key={index} className="relative flex items-center justify-between md:justify-normal md:odd:flex-row-reverse group is-active">
                                    <div className="flex items-center justify-center w-10 h-10 rounded-xl border border-[var(--card-bg)] shrink-0 md:order-1 md:group-odd:-translate-x-1/2 md:group-even:translate-x-1/2 shadow-sm z-10 box-content bg-[var(--card-border)] relative">
                                        <div className={`w-8 h-8 rounded-lg flex items-center justify-center ${iconInfo.bg} ${iconInfo.color} shadow-inner`}>
                                            {iconInfo.icon}
                                        </div>
                                    </div>
                                    <div className="w-[calc(100%-4rem)] md:w-[calc(50%-2.5rem)] bg-[var(--card-bg)] p-4 rounded-2xl border border-[var(--card-border)] shadow-[0_2px_10px_-4px_rgba(0,0,0,0.03)] hover:border-[var(--card-border)] transition-colors">
                                        <div className="flex items-center justify-between mb-1">
                                            <div className="font-bold text-[var(--foreground)] text-[14px] tracking-tight">{activity.title}</div>
                                            <div className="text-[11px] font-semibold text-[var(--text-muted)] bg-black/5 dark:bg-white/5 px-2 py-0.5 rounded-lg border border-[var(--card-border)]">{activity.date}</div>
                                        </div>
                                        <div className="text-[var(--text-soft)] text-[13px] font-medium line-clamp-1">{activity.desc}</div>
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                ) : (
                    <div className="flex flex-col items-center justify-center h-full text-[var(--text-muted)] gap-3 border-2 border-dashed border-[var(--card-border)] rounded-2xl bg-black/5 dark:bg-white/5 min-h-[250px]">
                        <IconHistory className="h-10 w-10 text-[var(--text-muted)] opacity-50" stroke={1.5} />
                        <span className="text-[13px] font-medium">No recent activity found.</span>
                    </div>
                )}
            </div>
        </div>
    );
};