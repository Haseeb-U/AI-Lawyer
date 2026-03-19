import axios from 'axios';
import { toast } from 'sonner';
import { ROUTES } from '../constants/routes.constants';

export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api';

export const getAuthToken = () => localStorage.getItem('token');

/**
 * Note: While we use localStorage for the token currently (MVP phase),
 * for a strict enterprise application, consider migrating to HttpOnly cookies
 * handled by the backend to prevent XSS attacks.
 */
export const setAuthToken = (token) => {
    if (token) {
        localStorage.setItem('token', token);
    } else {
        localStorage.removeItem('token');
    }
};

export const setUser = (user) => {
    if (user) {
        localStorage.setItem('user', JSON.stringify(user));
    } else {
        localStorage.removeItem('user');
    }
};

export const getUser = () => {
    const user = localStorage.getItem('user');
    return user ? JSON.parse(user) : null;
};

// Create a global Axios instance
const apiClient = axios.create({
    baseURL: API_BASE_URL,
    headers: {
        'Content-Type': 'application/json',
    },
});

// Request Interceptor: Attach token to every request
apiClient.interceptors.request.use(
    (config) => {
        const token = getAuthToken();
        if (token) {
            config.headers.Authorization = `Bearer ${token}`;
        }
        
        // Let Axios handle FormData headers automatically
        if (config.data instanceof FormData) {
            delete config.headers['Content-Type'];
        }
        
        return config;
    },
    (error) => Promise.reject(error)
);

// Response Interceptor: Handle global errors like 401 Unauthorized
apiClient.interceptors.response.use(
    (response) => {
        // Any status code that lies within the range of 2xx causes this function to trigger
        return response;
    },
    (error) => {
        // Any status codes that falls outside the range of 2xx cause this function to trigger
        if (error.response && error.response.status === 401) {
            // Token expired or invalid
            setAuthToken(null);
            setUser(null);

            // Redirect to login if we are not already there
            if (window.location.pathname !== ROUTES.AUTH) {
                toast.error('Session expired. Please log in again.');
                window.location.href = ROUTES.AUTH;
            }
        } else if (error.response && error.response.status >= 500) {
            toast.error('Server error. Please try again later.');
        } else if (!error.response && error.message === 'Network Error') {
            toast.error('Network error. Please check your connection.');
        }
        return Promise.reject(error);
    }
);

export default apiClient;
