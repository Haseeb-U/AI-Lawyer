import React from "react";
import { useAuth } from '../context/AuthContext';
import { useTheme } from '../context/ThemeContext';
import { useSidebar } from '../context/SidebarContext';
import { useNavigate, useLocation } from 'react-router-dom';
import { Sun, Moon, LogOut, Settings, Menu, ArrowLeft, ChevronDown } from 'lucide-react';
import {
    DropdownMenu, DropdownMenuContent, DropdownMenuItem,
    DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger
} from '@/components/ui/dropdown-menu';

export default function Navbar() {
  const { logout, user } = useAuth();
  const { theme, toggleTheme } = useTheme();
  const { toggleMobileSidebar } = useSidebar();
  const navigate = useNavigate();
  const location = useLocation();

  const canGoBack = location.pathname !== '/dashboard';
  const handleBack = () => { window.history.length > 1 ? navigate(-1) : navigate('/dashboard'); };

  const initials = user?.username
        ? user.username.split(' ').map(n => n[0]).join('').toUpperCase().slice(0, 2)
        : 'U';

  return (
    <nav className="app-navbar" style={{position: 'sticky', top: 0, zIndex: 1000, background: 'var(--bg-color)', display: 'flex', justifyContent: 'space-between', padding: '1rem', borderBottom: '1px solid var(--border-color)'}}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <button className="mobile-nav-menu-btn" onClick={toggleMobileSidebar} aria-label="Menu" style={{background:'transparent', border:'none', cursor:'pointer', display: 'flex', alignItems:'center'}}>
                <Menu size={20} />
            </button>
            {canGoBack && (
                <button className="professional-back-btn" onClick={handleBack} style={{background:'transparent', border:'none', cursor:'pointer', display: 'flex', alignItems:'center', gap:'4px'}}>
                    <ArrowLeft size={16} />
                    <span>Back</span>
                </button>
            )}
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
            <button
                className="icon-btn theme-toggle-btn"
                onClick={toggleTheme}
                aria-label="Toggle theme"
                style={{ borderRadius: '10px', background:'transparent', border:'none', cursor:'pointer', display: 'flex', alignItems:'center' }}
            >
                {theme === 'dark' ? <Sun size={17} /> : <Moon size={17} />}
            </button>

            <DropdownMenu>
                <DropdownMenuTrigger asChild>
                    <button
                        className="user-info"
                        style={{ cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '0.6rem', background: 'var(--card-bg)', border: '1px solid var(--border-color)', borderRadius: '10px', padding: '0.45rem 0.9rem', color: 'var(--text-color)', fontSize: '0.9rem', fontWeight: 500 }}
                    >
                        <span style={{
                            width: '28px', height: '28px', borderRadius: '8px', fontSize: '0.75rem', fontWeight: 700,
                            background: 'linear-gradient(135deg, var(--accent-color), var(--accent-light))',
                            display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'white'
                        }}>
                            {initials}
                        </span>
                        <span style={{ maxWidth: '120px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {user?.username || 'User'}
                        </span>
                        <ChevronDown size={14} />
                    </button>
                </DropdownMenuTrigger>
                <DropdownMenuContent
                    align="end"
                    style={{ minWidth: '200px', background: 'var(--card-bg)', border: '1px solid var(--border-color)', borderRadius: '12px', padding: '0.5rem' }}
                >
                    <DropdownMenuLabel style={{ padding: '0.5rem 0.75rem' }}>
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '2px' }}>
                            <span style={{ fontSize: '0.9rem', fontWeight: 600 }}>{user?.username}</span>
                            <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>{user?.email}</span>
                        </div>
                    </DropdownMenuLabel>
                    <DropdownMenuSeparator style={{ background: 'var(--border-color)', margin: '0.375rem 0' }} />
                    <DropdownMenuItem onClick={() => navigate('/settings')} style={{ cursor: 'pointer', padding: '0.5rem', gap: '0.5rem', display: 'flex', alignItems: 'center' }}>
                        <Settings size={15} /> Settings
                    </DropdownMenuItem>
                    <DropdownMenuItem onClick={logout} style={{ cursor: 'pointer', padding: '0.5rem', gap: '0.5rem', display: 'flex', alignItems: 'center', color: 'red' }}>
                        <LogOut size={15} /> Log out
                    </DropdownMenuItem>
                </DropdownMenuContent>
            </DropdownMenu>
        </div>
    </nav>
  );
}
