import React from 'react';
import { useNavigate } from 'react-router-dom';
import { IconRobot, IconFileText, IconGavel, IconSearch, IconArrowUpRight } from '@tabler/icons-react';

const FeatureGrid = () => {
    const navigate = useNavigate();

    const features = [
        {
            title: 'AI Consultant',
            description: 'Direct conversations with custom trained legal logic models.',
            icon: <IconRobot size={20} stroke={2} />,
            href: '/chatbot',
            color: 'text-[var(--brand-500)]',
            bg: 'bg-[var(--navbar-bg)]'
        },
        {
            title: 'Contract Analyzer',
            description: 'Automated extraction of key clauses and legal obligations.',
            icon: <IconFileText size={20} stroke={2} />,
            href: '/contracts',
            color: 'text-[var(--brand-500)]',
            bg: 'bg-[var(--navbar-bg)]'
        },
        {
            title: 'Judgments Library',
            description: 'Intelligently summarized historical precedents.',
            icon: <IconGavel size={20} stroke={2} />,
            href: '/judgments',
            color: 'text-[var(--brand-500)]',
            bg: 'bg-[var(--navbar-bg)]'
        },
        {
            title: 'Semantic Legal Search',
            description: 'Deep context-aware queries across federal directives.',
            icon: <IconSearch size={20} stroke={2} />,
            href: '/search',
            color: 'text-[var(--brand-500)]',
            bg: 'bg-[var(--navbar-bg)]'
        }
    ];

    return (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-5">  
            {features.map((feature, idx) => (
                <div
                    key={idx}
                    onClick={() => navigate(feature.href)}
                    className="bg-[var(--card-bg)] rounded-2xl p-6 shadow-sm border border-[var(--card-border)] cursor-pointer group transition-all duration-300 hover:border-[var(--card-border)] hover:shadow-md flex flex-col"
                >
                    <div className="flex items-start justify-between mb-5">
                        <div className={`w-11 h-11 rounded-xl flex items-center justify-center shadow-sm ${feature.bg} ${feature.color}`}>
                            {feature.icon}
                        </div>
                        <div className="h-8 w-8 rounded-full border border-[var(--card-border)] flex items-center justify-center group-hover:bg-black/5 dark:bg-white/5 transition-colors">
                            <IconArrowUpRight size={16} stroke={2} className="text-[var(--text-muted)] group-hover:text-[var(--foreground)] transition-colors" />
                        </div>
                    </div>

                    <h3 className="text-[16px] font-bold text-[var(--foreground)] tracking-tight mb-2">{feature.title}</h3>
                    <p className="text-[var(--text-soft)] text-[13px] leading-relaxed mb-1">{feature.description}</p>
                </div>
            ))}
        </div>
    );
};

export default FeatureGrid;
