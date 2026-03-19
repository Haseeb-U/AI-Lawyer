import React from 'react';
import { motion } from 'framer-motion';
import {
    IconRobot,
    IconFileText,
    IconGavel,
    IconClock
} from "@tabler/icons-react";

export const DashboardStats = ({ stats, itemVariants }) => {
    const statCards = [
        { label: "AI Consultations", value: stats.aiConsultations, trend: "Live updates", icon: <IconRobot size={18} stroke={1.5} className="text-[var(--text-soft)] group-hover:text-[var(--brand-500)] transition-colors" /> },
        { label: "Contracts Analyzed", value: stats.contractsAnalyzed, trend: "Secure logs", icon: <IconFileText size={18} stroke={1.5} className="text-[var(--text-soft)] group-hover:text-[var(--brand-500)] transition-colors" /> },
        { label: "Judgments Viewed", value: stats.judgmentsViewed, trend: "Auto-synced", icon: <IconGavel size={18} stroke={1.5} className="text-[var(--text-soft)] group-hover:text-[var(--brand-500)] transition-colors" /> },
        { label: "Hours Saved", value: stats.hoursSaved, trend: "Estimated total", icon: <IconClock size={18} stroke={1.5} className="text-[var(--text-soft)] group-hover:text-[var(--brand-500)] transition-colors" /> }
    ];

    return (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-5">
            {statCards.map((stat, i) => (
                <motion.div
                    key={i}
                    variants={itemVariants}
                    whileHover={{ y: -3, transition: { duration: 0.2 } }}
                    className="bg-[var(--card-bg)] rounded-2xl p-6 shadow-sm border border-[var(--card-border)] hover:border-[var(--card-border)] hover:shadow-md transition-all flex flex-col justify-between group cursor-pointer"
                >
                    <div className="flex flex-row items-center justify-between mb-4">
                        <span className="text-[12px] font-bold text-[var(--text-soft)] uppercase tracking-wider bg-black/5 dark:bg-white/5 px-2 py-0.5 rounded-lg border border-[var(--card-border)]">
                            {stat.label}
                        </span>
                        <div className="h-9 w-9 rounded-xl border border-[var(--card-border)] bg-black/5 dark:bg-white/5 group-hover:bg-[var(--navbar-bg)] flex items-center justify-center transition-all duration-300 shadow-sm">
                            {stat.icon}
                        </div>
                    </div>
                    <div className="mt-2 text-left">
                        <h3 className="text-[28px] font-bold text-[var(--foreground)] tracking-tight leading-none mb-2">{stat.value}</h3>
                        <p className="text-[12px] text-[var(--text-muted)] font-medium flex items-center gap-1.5 mt-2">
                            <span className="h-1.5 w-1.5 rounded-full bg-emerald-500 ring-4 ring-emerald-500/20"></span>
                            {stat.trend}
                        </p>
                    </div>
                </motion.div>
            ))}
        </div>
    );
};