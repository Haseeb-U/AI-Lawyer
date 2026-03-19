import React from 'react';
import { motion } from 'framer-motion';

// Logic Hook
import { useDashboardData } from '../hooks/useDashboardData';

// Sub-components
import { DashboardHeader } from '../components/dashboard/DashboardHeader';
import { DashboardStats } from '../components/dashboard/DashboardStats';
import { ActivityTimeline } from '../components/dashboard/ActivityTimeline';
import FeatureGrid from '../components/dashboard/FeatureGrid';
import ErrorBoundary from '../components/ErrorBoundary';

const Dashboard = () => {
    const {
        userName,
        stats,
        activities,
        systemStatus,
        isLoading,
        getGreeting,
        currentDate
    } = useDashboardData();

    const containerVariants = {
        hidden: { opacity: 0 },
        visible: { 
            opacity: 1, 
            transition: { staggerChildren: 0.1 } 
        }
    };

    const itemVariants = {
        hidden: { y: 20, opacity: 0 },
        visible: { 
            y: 0, 
            opacity: 1, 
            transition: { type: "spring", stiffness: 300, damping: 24 } 
        }
    };

    return (
        <div className="flex-1 flex flex-col bg-[var(--background)] overflow-hidden relative h-full">
            {/* Modular Top Header */}
            <DashboardHeader 
                userName={userName}
                getGreeting={getGreeting}
                currentDate={currentDate}
            />

            {/* Dashboard Scrollable Body */}
            <div className="flex-1 overflow-y-auto w-full custom-scrollbar pb-24 px-8 lg:px-12 pt-8">
                <motion.div 
                    variants={containerVariants}
                    initial="hidden"
                    animate="visible"
                    className="max-w-7xl mx-auto space-y-10"
                >
                    {/* Premium Cards Row */}
                    <DashboardStats 
                        stats={stats} 
                        itemVariants={itemVariants} 
                    />

                    {/* Lower Layout Area: Workspace & Activity Area */}
                    <div className="flex flex-col lg:flex-row gap-8 pt-2">
                        {/* Left Column: Feature Grid Module Container (60%) */}
                        <motion.div variants={itemVariants} className="flex flex-col w-full lg:w-[60%]">
                            <div className="mb-6 flex items-center justify-between">
                                <div>
                                    <h2 className="text-[18px] font-semibold text-[var(--foreground)] flex items-center gap-2 tracking-tight">
                                        Workspace <span className="text-[10px] font-bold tracking-wider text-white bg-[var(--brand-500)] text-[#051326] px-[5px] py-[1px] rounded uppercase leading-relaxed inline-flex align-middle relative -top-0.5 shadow-sm">Pro</span>
                                    </h2>
                                    <p className="text-[13px] text-[var(--text-soft)] mt-1 font-medium">Access your primary legal instruments and modules.</p>
                                </div>
                            </div>
                            <div className="flex-1">
                                <ErrorBoundary variant="local">
                                    <FeatureGrid />
                                </ErrorBoundary>
                            </div>
                        </motion.div>

                        {/* Right Column: Activity Timeline (40%) */}
                        <motion.div variants={itemVariants} className="flex flex-col w-full lg:w-[40%] h-full min-h-[400px]">
                            <ErrorBoundary variant="local">
                                <ActivityTimeline 
                                    activities={activities}
                                    isLoading={isLoading}
                                />
                            </ErrorBoundary>
                        </motion.div>
                    </div>
                </motion.div>
            </div>
        </div>
    );
};

export default Dashboard;
