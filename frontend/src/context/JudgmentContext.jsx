import React, { createContext, useContext } from 'react';
import { useJudgments } from '../hooks/useJudgments';
import { useJudgmentFilters } from '../hooks/useJudgmentFilters';

const JudgmentContext = createContext();

export const JudgmentProvider = ({ children }) => {
    const judgmentsData = useJudgments();
    const filtersData = useJudgmentFilters(
        judgmentsData.judgments, 
        judgmentsData.fetchJudgments, 
        judgmentsData.fetchSummary
    );

    const loadMore = () => {
        if (!judgmentsData.loading && judgmentsData.hasMore) {
            judgmentsData.fetchJudgments(false);
        }
    };

    return (
        <JudgmentContext.Provider value={{...judgmentsData, ...filtersData, loadMore}}>
            {children}
        </JudgmentContext.Provider>
    );
};

export const useJudgmentContext = () => {
    const context = useContext(JudgmentContext);
    if (!context) {
        throw new Error("useJudgmentContext must be used within JudgmentProvider");
    }
    return context;
};
