import api from '../utils/api';

export const judgmentService = {
    searchJudgments: async (searchQuery, page = 1, limit = 10) => {
        const response = await api.get(`/judgments?search=${encodeURIComponent(searchQuery)}&page=${page}&limit=${limit}`);
        return response.data;
    },
    getSummary: async (title) => {
        const response = await api.get(`/judgments/${encodeURIComponent(title)}/summary`);
        return response.data;
    }
};