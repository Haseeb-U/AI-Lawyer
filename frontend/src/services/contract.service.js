import api from '../utils/api';

export const contractService = {
    analyzeContract: async (file) => {
        const formData = new FormData();
        formData.append('contract', file);
        const response = await api.post('/contracts/analyze', formData);
        return response.data;
    }
};