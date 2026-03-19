import React from 'react';
// eslint-disable-next-line no-unused-vars
import { motion } from 'framer-motion';
import { BrandMark } from './BrandMark';

// Cloud/Wavy edge SVG to mimic the reference image
const CloudEdge = ({ isRight }) => (
    <svg
        className={`absolute top-[-5%] ${isRight ? '-right-[60px]' : '-left-[60px] rotate-180'} w-[65px] h-[110%] text-[#051326] drop-shadow-2xl z-0 pointer-events-none`}
        viewBox="0 0 100 1000"
        fill="currentColor"
        preserveAspectRatio="none"
    >
        <path d="M0,0 Q100,50 30,150 Q120,250 40,350 Q100,450 20,550 Q130,650 30,750 Q100,850 0,1000 Z" />
    </svg>
);

export const AuthOverlay = ({ isLogin }) => {
    return (
        <motion.div
            initial={false}
            animate={{ x: isLogin ? '100%' : '0%' }}
            transition={{ type: 'spring', damping: 20, stiffness: 85 }}
            className="hidden md:flex absolute top-0 left-0 w-1/2 h-full bg-[#051326] flex-col items-center justify-center z-40 text-center shadow-2xl"
        >
            {/* The Cloud Edge SVG that toggles direction based on sliding state */}
            <div className="absolute inset-0 pointer-events-none overflow-visible">
                <CloudEdge isRight={!isLogin} />
            </div>

            {/* Gold accent glow */}
            <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-full h-full bg-[radial-gradient(ellipse_at_center,_var(--tw-gradient-stops))] from-[#d4af37]/10 via-transparent to-transparent pointer-events-none z-0"></div>

            <div className="relative z-10 flex flex-col items-center w-full px-12">
                <motion.h3 
                    key={`w-${isLogin}`}
                    initial={{ opacity: 0, y: -20 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.1 }}
                    className="text-xl text-white font-semibold mb-6"
                >
                    {isLogin ? 'Welcome Back to' : 'Welcome to'}
                </motion.h3>
                
                <motion.div 
                    key={`logo-${isLogin}`}
                    initial={{ scale: 0.5, opacity: 0 }} 
                    animate={{ scale: 1, opacity: 1 }} 
                    transition={{ type: 'spring', delay: 0.15 }}
                    className="relative mb-10 mt-4 group"
                >
                    {/* Glowing aura */}
                    <div className="absolute inset-0 bg-[#d4af37] blur-[30px] opacity-20 group-hover:opacity-40 transition-opacity duration-700"></div>

                    {/* Circular Border with metallic gradient */}
                    <div className="relative w-32 h-32 rounded-full p-[2px] shadow-2xl bg-gradient-to-b from-[#f9e8a2] via-[#d4af37] to-transparent">
                        
                        {/* Inner Circular Area */}
                        <div className="w-full h-full bg-gradient-to-tr from-[#051021] via-[#051326] to-[#0d2a4a] rounded-full flex items-center justify-center relative overflow-hidden">
                            
                            {/* Inner subtle glow */}
                            <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-24 h-24 bg-[#d4af37]/5 rounded-full blur-xl"></div>
                            <div className="absolute top-0 right-0 w-16 h-16 bg-white/10 rounded-full blur-xl -translate-y-1/2 translate-x-1/2"></div>
                            
                            {/* BrandMark Scale */}
                            <div className="relative z-10 flex items-center justify-center drop-shadow-[0_4px_12px_rgba(0,0,0,0.5)]">
                                <BrandMark size={70} stroke="#ffffff" accent="#ffffff" strokeWidth={2.4} mode="color" />
                            </div>
                            
                            {/* Outer pulsing ring inside circle */}
                            <div className="absolute inset-2 rounded-full border border-[#d4af37]/20 border-t-[#d4af37]/40 shadow-[0_0_15px_rgba(212,175,55,0.1)_inset]"></div>
                        </div>
                    </div>
                </motion.div>

                <motion.h1 
                    key={`title-${isLogin}`}
                    initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.2 }}
                    className="text-[32px] font-bold text-white tracking-wide mb-2"
                >
                    AI-Lawyer
                </motion.h1>

                <p className="text-[#d4af37] text-xs uppercase tracking-[0.3em] mb-4 font-semibold">Legal Intelligence Platform</p>

                <motion.p 
                    key={`desc-${isLogin}`}
                    initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.25 }}
                    className="text-[13px] text-gray-300 leading-relaxed max-w-[280px]"
                >
                    {isLogin 
                        ? "Log in to access your dashboard, review contracts, and explore legal insights with AI precision."
                        : "Join our platform to automate your legal processes and access premium AI consultation features."}
                </motion.p>
            </div>
        </motion.div>
    );
};
