import React from 'react';
import { IconMessageDots, IconScale } from '@tabler/icons-react';
import { useChat } from '../../context/ChatContext';

const ChatHeader = () => {
    const { isSidebarOpen, setIsSidebarOpen } = useChat();

    return (
        <div className="h-[4.5rem] border-b border-[var(--card-border)] flex items-center pr-4 md:pr-8 pl-4 md:pl-8 shrink-0 bg-[var(--card-bg)]/80 backdrop-blur-md sticky top-0 z-10 justify-between">
            <div className="flex items-center gap-3.5">
                <div className="h-10 w-10 bg-[var(--navbar-bg)] text-[#d4af37] shadow-md shadow-[#051326]/10 rounded-xl flex items-center justify-center transition-transform hover:scale-105 border border-[#d4af37]/20">
                    <IconScale size={22} stroke={1.5} />
                </div>
                <div>
                   <h2 className="font-bold text-[16px] text-[var(--foreground)] tracking-tight leading-tight">AI Legal Consultant</h2>
                   <div className="flex items-center gap-1.5 mt-0.5">
                       <span className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse shadow-[0_0_8px_rgba(16,185,129,0.5)]"></span>
                       <p className="text-[12.5px] text-[var(--text-soft)] font-medium leading-none">Powered by Legal AI</p>
                   </div>
                </div>
            </div>
            <button
                onClick={() => setIsSidebarOpen(!isSidebarOpen)}        
                className="p-2.5 bg-[var(--card-bg)] shadow-sm border border-[var(--card-border)] hover:bg-black/5 dark:bg-white/5 hover:border-[var(--card-border)] rounded-xl text-[var(--foreground)] transition-all hover:shadow-md active:scale-95 flex items-center gap-2"
                title="Toggle Sidebar"
            >
                <IconMessageDots size={20} stroke={2} />
                <span className="text-sm font-semibold pr-1 hidden md:block">History</span>
            </button>
        </div>
    );
};

export default ChatHeader;