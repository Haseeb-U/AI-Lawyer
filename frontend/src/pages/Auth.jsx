import React from 'react';
import { useNavigate } from 'react-router-dom';
import { ROUTES } from '../constants/routes.constants';
import { useAuthForm } from '../hooks/useAuthForm';

// Components
import { LoginForm } from '../components/auth/LoginForm';
import { SignupForm } from '../components/auth/SignupForm';
import { AuthOverlay } from '../components/auth/AuthOverlay';

const Auth = () => {
    const navigate = useNavigate();
    
    // Abstracted Logic (Service + Hook + Context pattern)
    const {
        isLogin,
        formData,
        agreed,
        setAgreed,
        error,
        success,
        loading,
        handleInputChange,
        toggleMode,
        handleLogin,
        handleSignup
    } = useAuthForm();

    return (
        <div className="min-h-screen w-full bg-[radial-gradient(circle_at_15%_18%,_#f4f6fb_0%,_#e8edf5_38%,_#d8dee8_100%)] dark:bg-[radial-gradient(circle_at_20%_12%,_#1a2538_0%,_#0a1020_45%,_#04070f_100%)] flex items-center justify-center p-4 lg:p-8 font-sans transition-colors duration-300">
            <div className="relative w-full max-w-[900px] min-h-[550px] bg-[var(--card-bg)]/95 dark:bg-[#0b1324]/90 backdrop-blur-sm rounded-3xl shadow-[0_28px_70px_rgba(5,15,35,0.18)] dark:shadow-[0_30px_80px_rgba(0,0,0,0.55)] border border-white/60 dark:border-white/10 overflow-hidden flex flex-col md:flex-row">
                
                {/* ---------- BACK BUTTON ---------- */}
                <button 
                    onClick={() => navigate(ROUTES.HOME)} 
                    className={`absolute top-6 ${isLogin ? 'left-6' : 'right-6'} text-[var(--foreground)] z-50 hover:text-[var(--brand-500)] transition-colors flex items-center gap-1 font-medium text-sm drop-shadow-md`}
                >
                    &larr; Back
                </button>

                {/* ==================== LEFT PANEL (LOGIN FORM) ==================== */}
                <LoginForm 
                    isLogin={isLogin}
                    formData={formData}
                    handleInputChange={handleInputChange}
                    handleLogin={handleLogin}
                    loading={loading}
                    toggleMode={toggleMode}
                    success={success}
                    error={error}
                />

                {/* ==================== RIGHT PANEL (SIGNUP FORM) ==================== */}
                <SignupForm
                    isLogin={isLogin}
                    formData={formData}
                    agreed={agreed}
                    setAgreed={setAgreed}
                    handleInputChange={handleInputChange}
                    handleSignup={handleSignup}
                    loading={loading}
                    toggleMode={toggleMode}
                    success={success}
                    error={error}
                />

                {/* ==================== SLIDING OVERLAY (BLUE PANEL) ==================== */}
                <AuthOverlay isLogin={isLogin} />
            </div>
        </div>
    );
};

export default Auth;

