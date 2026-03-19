import apiClient from '../utils/api';

/**
 * AuthService handles all authentication-related API requests.
 * Uses the configured Axios instance (apiClient) for automatic header & error management.
 */
const AuthService = {
    login: async (email, password) => {
        const response = await apiClient.post('/auth/login', { email, password });
        return response.data;
    },

    register: async (username, email, password) => {
        const response = await apiClient.post('/auth/register', { username, email, password });
        return response.data;
    },

    verifyToken: async () => {
        const response = await apiClient.get('/auth/verify');
        return response.data;
    }
};

export default AuthService;
