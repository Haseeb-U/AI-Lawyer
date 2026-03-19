import React from 'react';
import { Navigate, Outlet } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { ROUTES } from '../constants/routes.constants';

const ProtectedRoute = () => {
    const { user, loading } = useAuth();

    if (loading) {
        return (
            <div className="min-h-screen flex items-center justify-center bg-[#E8EDF2]">
                <div className="w-10 h-10 border-4 border-[#051326] border-t-transparent rounded-full animate-spin"></div>
            </div>
        );
    }

    return user ? <Outlet /> : <Navigate to={ROUTES.AUTH} replace />;
};

export default ProtectedRoute;
