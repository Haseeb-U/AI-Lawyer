import React from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';
import { ROUTES } from '../../constants/routes.constants';
import { Sidebar, SidebarBody, SidebarLink } from '../ui/sidebar';
import ThemeToggle from '../ThemeToggle';
import { 
    IconLayoutDashboard, 
    IconRobot,
    IconFileText,
    IconGavel,
    IconSearch,
    IconLogout,
    IconScale
} from "@tabler/icons-react";
import { motion } from "framer-motion";

const Logo = () => (
    <div className="font-normal flex space-x-3 items-center text-sm px-2 py-2 relative z-20">
        <div className="h-7 w-7 bg-[var(--brand-500)] rounded-lg flex-shrink-0 flex items-center justify-center shadow-lg shadow-black/20">
            <IconScale className="h-4 w-4 text-[var(--navbar-bg)]" stroke={2} />
        </div>
        <motion.span initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="font-bold text-white text-[16px] whitespace-pre tracking-wide">
            AI-Lawyer
        </motion.span>
    </div>
);

const LogoIcon = () => (
    <div className="font-normal flex space-x-2 items-center text-sm px-2 py-2 relative z-20 tooltip-trigger">
        <div className="h-7 w-7 bg-[var(--brand-500)] rounded-lg flex-shrink-0 flex items-center justify-center shadow-lg hover:brightness-110 transition-all">
            <IconScale className="h-4 w-4 text-[var(--navbar-bg)]" stroke={2} />
        </div>
    </div>
);

const AppSidebar = ({ open, setOpen }) => {
    const { user, logout } = useAuth();
    const navigate = useNavigate();
    const location = useLocation();

    const handleLogout = () => {
        logout();
        navigate(ROUTES.HOME);
    };

    const links = [
        { label: "Overview", href: ROUTES.DASHBOARD, icon: <IconLayoutDashboard className="h-5 w-5 flex-shrink-0 transition-colors" /> },
        { label: "AI Consultant", href: "/chatbot", icon: <IconRobot className="h-5 w-5 flex-shrink-0 transition-colors" /> },
        { label: "Contracts", href: "/contracts", icon: <IconFileText className="h-5 w-5 flex-shrink-0 transition-colors" /> },
        { label: "Judgments", href: "/judgments", icon: <IconGavel className="h-5 w-5 flex-shrink-0 transition-colors" /> },
        { label: "Legal Search", href: "/search", icon: <IconSearch className="h-5 w-5 flex-shrink-0 transition-colors" /> },
    ];

    const userName = user?.name || user?.username || 'Admin User';
    const userInitials = userName.charAt(0).toUpperCase();

    return (
        <div className="flex-shrink-0 z-20">
            <Sidebar open={open} setOpen={setOpen} animate={true}>
                <SidebarBody className="justify-between gap-10 h-full border-none bg-transparent">
                    <div className="flex flex-col flex-1 overflow-y-auto overflow-x-hidden pt-3 -mx-1">
                        {open ? <Logo /> : <LogoIcon />}

                        <div className="mt-8 flex flex-col gap-1.5 px-1.5">
                            {links.map((link, idx) => {
                                const isActive = location.pathname === link.href;
                                return (
                                    <SidebarLink
                                        key={idx}
                                        link={link}
                                        className={`rounded-xl transition-all duration-200 px-3 py-2.5 ${isActive ? 'bg-[var(--brand-500)]/15 text-[var(--brand-500)] font-semibold tracking-tight shadow-[inset_0_1px_1px_rgba(255,255,255,0.05)]' : 'hover:bg-[var(--card-bg)]/5 text-white/50 hover:text-white/90'}`}
                                    />
                                );
                            })}
                        </div>
                    </div>

                    <div className="pb-4 -mx-1 flex flex-col gap-2">
                        <div className="flex items-center justify-between px-3 mb-2">
                            <motion.span
                                animate={{ display: open ? "inline-block" : "none", opacity: open ? 1 : 0 }}
                                className="text-[11px] font-bold text-white/40 uppercase tracking-widest whitespace-pre"
                            >
                                Settings
                            </motion.span>
                            <ThemeToggle />
                        </div>
                        
                        <SidebarLink link={{
                            label: userName,
                            href: "#",
                            icon: (
                                <div className="h-8 w-8 ml-0.5 rounded-full bg-[#112240] border border-white/10 flex items-center justify-center text-white/90 text-[13px] font-bold shrink-0 shadow-sm">
                                    {userInitials}
                                </div>
                            ),
                        }} className="hover:bg-[var(--card-bg)]/5 rounded-xl transition-all px-2 py-2 text-white/70 hover:text-white font-medium" />

                        <div className="h-px bg-[var(--card-bg)]/10 my-1 mx-3"></div>

                        <button
                            onClick={handleLogout}
                            className="flex items-center justify-start gap-3.5 group/sidebar py-2.5 px-3 hover:bg-rose-500/10 rounded-xl transition-all w-full focus:outline-none"
                        >
                            <IconLogout className="text-white/40 h-5 w-5 flex-shrink-0 ml-0.5 group-hover/sidebar:text-rose-400 transition-colors" />
                            <motion.span
                                animate={{ display: open ? "inline-block" : "none", opacity: open ? 1 : 0 }}
                                className="text-white/50 group-hover/sidebar:text-rose-400 text-sm font-medium transition-colors whitespace-pre"
                            >
                                Sign out
                            </motion.span>
                        </button>
                    </div>
                </SidebarBody>
            </Sidebar>
        </div>
    );
};

export default AppSidebar;
