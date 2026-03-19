import React, { createContext, useContext, useState, useEffect, useMemo, useCallback } from 'react';
import { getAuthToken, getUser, setAuthToken, setUser as setStorageUser } from '../utils/api';
import AuthService from '../services/auth.service';

const AuthContext = createContext(null);

export const AuthProvider = ({ children }) => {
    const [user, setUser] = useState(getUser());
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const token = getAuthToken();
        if (token && !user) {
            setUser(getUser());
        }
        setLoading(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const login = useCallback(async (email, password) => {
        try {
            const data = await AuthService.login(email, password);
            if (data.success) {
                setAuthToken(data.token);
                setStorageUser(data.user);
                setUser(data.user);
                return { success: true };
            }
            return { success: false, error: data.error || 'Login failed' };
        } catch {
            return { success: false, error: 'Network error or invalid server response' };
        }
    }, []);

    const register = useCallback(async (username, email, password) => {
        try {
            const data = await AuthService.register(username, email, password);
            if (data.success) {
                return { success: true };
            }
            return { success: false, error: data.error || 'Registration failed' };
        } catch {
            return { success: false, error: 'Network error or invalid server response' };
        }
    }, []);

    const logout = useCallback(() => {
        setAuthToken(null);
        setStorageUser(null);
        setUser(null);
    }, []);

    // Memoizing the context value to prevent unnecessary re-renders across the app
    const contextValue = useMemo(() => ({
        user,
        login,
        register,
        logout,
        loading
    }), [user, loading, login, register, logout]);

    return (
        <AuthContext.Provider value={contextValue}>
            {!loading && children}
        </AuthContext.Provider>
    );
};

// eslint-disable-next-line react-refresh/only-export-components
export const useAuth = () => useContext(AuthContext);
