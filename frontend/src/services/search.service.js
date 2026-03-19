import api from '../utils/api';

export const searchService = {
    getTypes: async () => {
        const response = await api.get('/search/types');
        return response.data;
    },
    performSearch: async (params) => {
        const queryString = new URLSearchParams(params).toString();
        const response = await api.get(`/search?${queryString}`);
        return response.data;
    }
};