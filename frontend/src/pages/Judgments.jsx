import React from 'react';

// Providers and Hooks
import { JudgmentProvider, useJudgmentContext } from '../context/JudgmentContext';
import ErrorBoundary from '../components/ErrorBoundary';

// Components (UI Layer)
import { JudgmentHeader } from '../components/judgments/JudgmentHeader';
import { JudgmentSidebar } from '../components/judgments/JudgmentSidebar';
import { JudgmentList } from '../components/judgments/JudgmentList';
import { JudgmentSummaryModal } from '../components/judgments/JudgmentSummaryModal';

const JudgmentsContent = () => {
    // 1. Get all centralized logic
    const { isSummaryModalOpen, setIsSummaryModalOpen, selectedJudgment, summaryLoading, summaryError, summary } = useJudgmentContext();

    return (
        <div className="flex-1 flex flex-col bg-[var(--background)] overflow-hidden relative h-full">
            
            {/* Modular Header */}
            <ErrorBoundary variant="local">
                <JudgmentHeader />
            </ErrorBoundary>

            <div className="flex-1 overflow-y-auto w-full custom-scrollbar pb-24 px-8 lg:px-12 pt-8">
                <div className="max-w-7xl mx-auto flex flex-col lg:flex-row gap-8">
                    
                    {/* Modular Sidebar Filters */}
                    <div className="w-full lg:w-80 shrink-0">
                        <ErrorBoundary variant="local">
                            <JudgmentSidebar />
                        </ErrorBoundary>
                    </div>

                    {/* Modular Logic-View For Cards */}
                    <div className="flex-1 min-w-0">
                        <ErrorBoundary variant="local">
                            <JudgmentList />
                        </ErrorBoundary>
                    </div>
                </div>
            </div>

            {/* AI Summary Modal Overlay */}
            {isSummaryModalOpen && (
                <ErrorBoundary variant="local">
                    <JudgmentSummaryModal />
                </ErrorBoundary>
            )}
        </div>
    );
};

const Judgments = () => {
    return (
        <JudgmentProvider>
            <JudgmentsContent />
        </JudgmentProvider>
    );
};

export default Judgments;
