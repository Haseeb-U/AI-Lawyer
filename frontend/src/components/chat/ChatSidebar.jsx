import React from 'react';
import { IconPlus, IconMessageDots, IconTrash } from '@tabler/icons-react';
import { useChat } from '../../context/ChatContext';

const ChatSidebar = () => {
    const { 
        chats, 
        currentChatId, 
        isSidebarOpen, 
        setIsSidebarOpen, 
        createNewChat, 
        loadChat, 
        deleteChat 
    } = useChat();

    return (
        <>
            <div className={`transition-all duration-300 border-[var(--card-border)] bg-black/[0.02] dark:bg-white/[0.02] flex flex-col overflow-hidden shrink-0 h-full absolute md:relative z-20 top-0 right-0 backdrop-blur-md shadow-[-4px_0_24px_-4px_rgba(0,0,0,0.05)] ${isSidebarOpen ? 'w-[280px] border-l' : 'w-0'}`}>
                <div className="p-4 border-b border-[var(--card-border)]/50">
                    <button
                        onClick={createNewChat}
                        className="w-full flex items-center justify-center gap-2 bg-[var(--navbar-bg)] text-[#d4af37] rounded-xl py-3 hover:bg-slate-800 hover:shadow-lg hover:-translate-y-0.5 transition-all text-sm font-semibold shadow-sm border border-[#d4af37]/30"
                    >
                        <IconPlus size={18} stroke={2.5} /> New Session
                    </button>
                </div>
                <div className="flex-1 overflow-y-auto p-3 space-y-1.5 custom-scrollbar">    
                    {chats.length === 0 ? (
                        <div className="text-center text-[var(--text-muted)] text-sm mt-8 px-4">
                            Your previous legal sessions will appear here.      
                        </div>
                    ) : (
                        chats.map(chat => (
                            <div
                                key={chat._id || chat.id}
                                onClick={() => {
                                    loadChat(chat._id || chat.id);
                                    if(window.innerWidth < 768) setIsSidebarOpen(false);
                                }}
                                className={`group flex items-center justify-between p-3.5 rounded-2xl cursor-pointer border transition-all ${currentChatId === (chat._id || chat.id) ? 'bg-[var(--card-bg)] shadow-sm border-[var(--card-border)] text-[var(--foreground)] font-medium' : 'border-transparent hover:bg-[var(--card-bg)]/60 text-[var(--text-soft)] hover:text-[var(--foreground)]'}`}
                                >
                                <div className="flex items-center gap-3 overflow-hidden">
                                    <IconMessageDots size={18} className={`shrink-0 ${currentChatId === (chat._id || chat.id) ? 'text-[#d4af37]' : 'text-[var(--text-muted)]'}`} />
                                    <span className="truncate text-sm">{chat.title || 'Legal Query'}</span>
                                </div>
                                <button
                                    onClick={(e) => deleteChat(e, chat._id || chat.id)}     
                                    className="opacity-0 group-hover:opacity-100 p-1.5 hover:text-red-500 hover:bg-red-50 rounded-lg transition-all text-[var(--text-muted)]"
                                >
                                    <IconTrash size={16} />
                                </button>
                            </div>
                        ))
                    )}
                </div>
            </div>

            {/* Mobile Sidebar Overlay */}
            {isSidebarOpen && (
                <div onClick={() => setIsSidebarOpen(false)} className="md:hidden fixed inset-0 bg-slate-900/20 z-10"></div>
            )}
        </>
    );
};

export default ChatSidebar;