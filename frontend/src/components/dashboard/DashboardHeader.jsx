import React, { useState, useRef, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { IconSearch, IconRobot, IconGavel, IconArrowRight, IconLogout } from "@tabler/icons-react";
import { useAuth } from '../../context/AuthContext';

export const DashboardHeader = ({ userName, getGreeting, currentDate }) => {
    const [searchQuery, setSearchQuery] = useState('');
    const [showSuggestions, setShowSuggestions] = useState(false);
    const searchRef = useRef(null);
    const navigate = useNavigate();
    const { user, logout } = useAuth();
    const [showProfileMenu, setShowProfileMenu] = useState(false);
    const profileRef = useRef(null);

    // Handle click outside to close suggestions
    useEffect(() => {
        const handleClickOutside = (event) => {
            if (searchRef.current && !searchRef.current.contains(event.target)) {
                setShowSuggestions(false);
            }
            if (profileRef.current && !profileRef.current.contains(event.target)) {
                setShowProfileMenu(false);
            }
        };
        document.addEventListener("mousedown", handleClickOutside);
        return () => document.removeEventListener("mousedown", handleClickOutside);
    }, []);

    const handleAskAI = () => {
        setShowSuggestions(false);
        navigate('/chatbot', { state: { initialQuery: searchQuery } });
    };

    const handleSeeMoreJudgments = () => {
        setShowSuggestions(false);
        navigate(`/judgments?q=${encodeURIComponent(searchQuery)}`);
    };

    const handleLogout = () => {
        setShowProfileMenu(false);
        logout();
        navigate('/auth');
    };

    return (
        <header className="flex flex-col sm:flex-row justify-between items-start sm:items-center px-8 lg:px-12 py-5 border-b border-[var(--card-border)] bg-[var(--card-bg)]/70 backdrop-blur-2xl sticky top-0 z-30 shrink-0">
            <div className="flex flex-col">
                <h1 className="text-2xl font-bold text-[var(--foreground)] tracking-tight flex items-center gap-2">
                    {getGreeting()}, {userName} <span className="text-xl animate-wave">👋</span>
                </h1>
                <p className="text-[var(--text-soft)] text-[13px] font-medium mt-1">{currentDate}</p>
            </div>
            
            <div className="flex items-center gap-4 mt-4 sm:mt-0">
                {/* Search Bar */}
                <div className="relative hidden lg:block group z-50" ref={searchRef}>
                    <IconSearch className="absolute left-3 top-1/2 -translate-y-1/2 h-[15px] w-[15px] text-[var(--text-muted)] group-focus-within:text-[var(--foreground)] transition-colors z-10" />
                    <input 
                        type="text" 
                        value={searchQuery}
                        onChange={(e) => {
                            setSearchQuery(e.target.value);
                            setShowSuggestions(true);
                        }}
                        onFocus={() => setShowSuggestions(true)}
                        placeholder="E.g. Pakistan Penal Code 1860..."
                        className="pl-9 pr-4 py-2 bg-[var(--card-border)]/50 hover:bg-[var(--card-border)] border border-transparent rounded-full text-[13px] focus:outline-none focus:ring-2 focus:ring-[#d4af37]/20 focus:border-[#d4af37]/50 focus:bg-[var(--card-bg)] w-72 transition-all placeholder:text-[var(--text-soft)] text-[var(--foreground)] font-medium"
                    />

                    {/* Search Suggestions Dropdown */}
                    {showSuggestions && searchQuery.trim() && (
                        <div className="absolute top-12 right-0 w-[380px] bg-[var(--card-bg)] border border-[var(--card-border)] rounded-xl shadow-2xl overflow-hidden flex flex-col pt-2 pb-2">
                            {/* Option 1: Ask AI */}
                            <div 
                                onClick={handleAskAI}
                                className="px-4 py-3 flex items-center justify-between hover:bg-[var(--border-color)]/30 cursor-pointer transition-colors group/ai"
                            >
                                <div className="flex items-center gap-3 overflow-hidden pr-2">
                                    <div className="p-2 bg-[#d4af37]/10 text-[#d4af37] rounded-lg group-hover/ai:bg-[#d4af37]/20 transition-colors">
                                        <IconRobot size={18} />
                                    </div>
                                    <div className="flex flex-col truncate">
                                        <span className="text-[14px] font-medium text-[var(--foreground)] truncate">"{searchQuery}"</span>
                                        <span className="text-[12px] text-[var(--text-soft)] mt-0.5">Let the AI analyze this legislation</span>
                                    </div>
                                </div>
                                <span className="text-[10px] bg-[#d4af37]/20 text-[#d4af37] border border-[#d4af37]/30 px-2 py-1 rounded font-bold uppercase tracking-wide shrink-0 whitespace-nowrap">Ask AI</span>
                            </div>

                            <div className="h-[1px] bg-[var(--card-border)] w-full my-2"></div>

                            {/* Option 2: Judgments Section */}
                            <div className="px-4 py-1">
                                <div className="flex justify-between items-center mb-3 px-1">
                                    <span className="text-[11px] font-semibold text-[var(--text-muted)] uppercase tracking-wider">Judgments & Precedents</span>
                                    <button 
                                        onClick={handleSeeMoreJudgments} 
                                        className="text-[11px] font-semibold text-[#d4af37] hover:underline flex items-center gap-1 transition-colors"
                                    >
                                        See more <IconArrowRight size={12} stroke={2.5} />
                                    </button>
                                </div>
                                <div 
                                    onClick={handleSeeMoreJudgments}
                                    className="flex items-center gap-4 px-3 py-2.5 rounded-lg hover:bg-[var(--border-color)]/30 cursor-pointer transition-colors group/judg"
                                >
                                     <IconGavel size={18} className="text-[var(--text-muted)] group-hover/judg:text-[#d4af37] transition-colors" />
                                     <div className="flex flex-col overflow-hidden">
                                        <span className="text-[13px] font-medium text-[var(--foreground)] truncate">Search "{searchQuery}"</span>
                                        <span className="text-[12px] text-[var(--text-soft)] truncate">Find related case laws and historic rulings</span>
                                     </div>
                                </div>
                            </div>
                        </div>
                    )}
                </div>
                {/* User Profile */}
                <div className="relative" ref={profileRef}>
                    <button 
                        onClick={() => setShowProfileMenu(!showProfileMenu)}
                        className="h-10 w-10 bg-[var(--navbar-bg)] border border-[#d4af37]/30 rounded-full flex items-center justify-center text-[#d4af37] shadow-sm ring-2 ring-transparent hover:ring-[#d4af37]/50 transition-all cursor-pointer"
                    >
                        <span className="text-sm font-bold">{userName?.charAt(0)?.toUpperCase()}</span>
                    </button>

                    {/* Profile Dropdown Premium UI */}
                    {showProfileMenu && (
                        <div className="absolute top-14 right-0 w-72 bg-[var(--card-bg)] border border-[var(--card-border)] rounded-2xl shadow-[0_12px_40px_rgba(0,0,0,0.12)] overflow-hidden flex flex-col z-[100] animate-in fade-in slide-in-from-top-2 duration-200 origin-top-right">
                            {/* Header Section with Mini Avatar */}
                            <div className="p-4 border-b border-[var(--card-border)] flex items-center gap-3">
                                <div className="h-10 w-10 min-w-[40px] bg-[#d4af37]/10 rounded-full flex items-center justify-center text-[#d4af37] font-bold border border-[#d4af37]/20">
                                    {userName?.charAt(0)?.toUpperCase()}
                                </div>
                                <div className="flex flex-col overflow-hidden">
                                    <p className="text-[14px] font-bold text-[var(--foreground)] truncate">{userName}</p>
                                    <p className="text-[12px] text-[var(--text-soft)] truncate">{user?.email || 'user@example.com'}</p>
                                </div>
                            </div>
                            
                            {/* Action Section */}
                            <div className="p-2">
                                <button 
                                    onClick={handleLogout}
                                    className="w-full flex items-center gap-3 px-3 py-2.5 text-[13px] font-semibold text-red-500 hover:text-red-600 hover:bg-red-50 dark:hover:bg-red-500/10 rounded-xl transition-all cursor-pointer group"
                                >
                                    <div className="bg-red-500/10 p-1.5 rounded-lg group-hover:bg-red-500/20 transition-colors">
                                        <IconLogout size={16} stroke={2.5} />
                                    </div>
                                    Sign Out
                                </button>
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </header>
    );
};
