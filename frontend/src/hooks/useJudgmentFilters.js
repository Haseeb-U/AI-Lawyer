import { useState, useMemo } from 'react';

export const useJudgmentFilters = (judgments, fetchJudgments, fetchSummary) => {
    const [selectedCourt, setSelectedCourt] = useState('All');
    const [selectedYear, setSelectedYear] = useState('All');
    const [selectedJudgment, setSelectedJudgment] = useState(null);
    const [isSummaryModalOpen, setIsSummaryModalOpen] = useState(false);

    // Dynamic Client-side computations for filters based on fetched data
    const availableCourts = useMemo(() => {
        const courts = judgments.map(j => j.court).filter(Boolean);
        return ['All', ...new Set(courts)];
    }, [judgments]);

    const availableYears = useMemo(() => {
        const years = judgments.map(j => j.year).filter(Boolean);
        return ['All', ...new Set(years)].sort((a, b) => {
            if(a === 'All') return -1;
            if(b === 'All') return 1;
            return b - a;
        });
    }, [judgments]);

    // Apply Client-side filtering
    const filteredJudgments = useMemo(() => {
        return judgments.filter(j => {
            const matchCourt = selectedCourt === 'All' || j.court === selectedCourt;
            const matchYear = selectedYear === 'All' || j.year?.toString() === selectedYear?.toString();
            return matchCourt && matchYear;
        });
    }, [judgments, selectedCourt, selectedYear]);

    const handleSearchSubmit = (e, fetchJudgmentsCallback) => {
        e.preventDefault();
        // Reset client filters on new search to avoid confusion
        setSelectedCourt('All');
        setSelectedYear('All');
        fetchJudgmentsCallback(true);
    };

    const handleSummarize = (judgment) => {
        setSelectedJudgment(judgment);
        setIsSummaryModalOpen(true);
        fetchSummary(judgment.title); // Backend summarizeRoute takes :title 
    };

    const clearFilters = () => {
        setSelectedCourt('All');
        setSelectedYear('All');
    };

    return {
        selectedCourt,
        setSelectedCourt,
        selectedYear,
        setSelectedYear,
        selectedJudgment,
        isSummaryModalOpen,
        setIsSummaryModalOpen,
        availableCourts,
        availableYears,
        filteredJudgments,
        handleSearchSubmit,
        handleSummarize,
        clearFilters
    };
};