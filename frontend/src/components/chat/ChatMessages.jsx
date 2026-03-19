import React from 'react';
import { IconRobot, IconLoader, IconScale } from '@tabler/icons-react';
import { marked } from 'marked';
import { useAuth } from '../../context/AuthContext';
import { useChat } from '../../context/ChatContext';

const ChatMessages = () => {
    const { user } = useAuth();
    const { messages, loading, messagesEndRef, setInput } = useChat();

    if (messages.length === 0) {
        return (
            <div className="flex-1 overflow-y-auto p-4 md:p-8 flex items-center justify-center">
                <div className="flex flex-col items-center text-center max-w-2xl w-full">
                    <div className="h-20 w-20 bg-[var(--navbar-bg)] text-[#d4af37] rounded-3xl flex items-center justify-center mb-6 shadow-xl ring-4 ring-[#d4af37]/10 relative border border-[#d4af37]/30">
                        <IconScale size={36} stroke={1.5} className="relative z-10" />
                    </div>
                    <h1 className="text-3xl font-bold text-[var(--foreground)] mb-3 tracking-tight">AI Legal Assistant</h1>
                    <p className="text-[var(--text-soft)] mb-8 leading-relaxed text-[15px] max-w-lg">
                        Specialized in Pakistani Civil Law. Ask about contracts, property disputes, family matters, or request case references.
                    </p>

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 w-full">
                        <button onClick={() => setInput("What is the procedure for filing a civil suit under the Code of Civil Procedure (CPC)?")} className="p-4 bg-[var(--card-bg)] border border-[var(--card-border)] rounded-2xl hover:border-[#d4af37]/50 hover:shadow-md hover:-translate-y-0.5 text-left transition-all text-[14px] text-[var(--foreground)] font-medium group">
                            <span className="text-[var(--text-muted)] block mb-1.5 text-[11px] font-bold uppercase tracking-wider group-hover:text-[#d4af37] transition-colors">Civil Law</span>
                            "What is the procedure for filing a civil suit under the Code of Civil Procedure (CPC)?"
                        </button>
                        <button onClick={() => setInput("How is property inheritance divided under Muslim Family Laws Ordinance?")} className="p-4 bg-[var(--card-bg)] border border-[var(--card-border)] rounded-2xl hover:border-[#d4af37]/50 hover:shadow-md hover:-translate-y-0.5 text-left transition-all text-[14px] text-[var(--foreground)] font-medium group">
                            <span className="text-[var(--text-muted)] block mb-1.5 text-[11px] font-bold uppercase tracking-wider group-hover:text-[#d4af37] transition-colors">Family Law</span>
                            "How is property inheritance divided under Muslim Family Laws Ordinance?"
                        </button>
                    </div>
                </div>
            </div>
        );
    }

    return (
        <div className="flex-1 overflow-y-auto p-4 md:p-8 shrink-0">
            <div className="max-w-4xl mx-auto flex flex-col gap-6 pb-20">
                {messages.map((msg, idx) => (
                    <div key={idx} className={`flex gap-4 ${msg.role === 'user' ? 'flex-row-reverse' : ''}`}>
                        <div className={`h-11 w-11 shrink-0 rounded-2xl flex items-center justify-center font-bold text-lg shadow-sm transition-transform hover:scale-105 ${msg.role === 'user' ? 'bg-[var(--navbar-bg)] text-[#d4af37] border border-[#d4af37]/30' : 'bg-[var(--card-bg)] border-2 border-[var(--card-border)] text-[var(--foreground)]'}`}>
                            {msg.role === 'user' ? (user?.name?.charAt(0).toUpperCase() || 'U') : <IconScale size={24} stroke={1.5} className="text-[#d4af37]" />}
                        </div>
                        <div className={`max-w-[85%] px-7 py-5 text-[15.5px] leading-relaxed shadow-sm ${msg.role === 'user' ? 'bg-[var(--navbar-bg)] text-white font-medium rounded-[1.75rem] rounded-tr-md border border-[#d4af37]/20' : 'bg-[var(--card-bg)] border border-[var(--card-border)] rounded-[1.75rem] rounded-tl-md'}`}>
                            {msg.role === 'user' ? (
                                <div className="whitespace-pre-wrap">{msg.content}</div>
                            ) : (
                                <div className="prose max-w-none prose-p:leading-relaxed prose-sm md:prose-base prose-headings:text-[var(--foreground)] prose-a:text-[#d4af37] font-medium text-[var(--foreground)] dark:prose-invert" dangerouslySetInnerHTML={{ __html: marked.parse(msg.content || '') }} />
                            )}

                            {/* Reference sources if AI generated them */}
                            {msg.role === 'assistant' && msg.sources && msg.sources.length > 0 && (
                                <div className="mt-5 pt-5 border-t border-[var(--card-border)]">
                                    <p className="text-[11px] font-bold text-[var(--text-muted)] mb-3 uppercase tracking-widest">References & Citations</p>
                                    <div className="flex flex-wrap gap-2">
                                        {msg.sources.map((src, sIdx) => (
                                            <span key={sIdx} className="inline-flex items-center gap-1.5 text-xs px-3 py-1.5 bg-black/5 dark:bg-white/5 border border-[var(--card-border)] rounded-lg text-[var(--text-soft)] font-medium shadow-sm hover:bg-[var(--card-border)] transition-colors cursor-default">
                                                <IconScale size={14} stroke={1.5} className="text-[#d4af37]" /> {src}
                                            </span>
                                        ))}
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>
                ))}
                {loading && (
                    <div className="flex gap-4">
                        <div className="h-11 w-11 shrink-0 rounded-2xl bg-[var(--navbar-bg)] animate-pulse text-[#d4af37] shadow-md flex items-center justify-center border border-[#d4af37]/30">
                            <IconScale size={24} stroke={1.5} />
                        </div>
                        <div className="bg-[var(--card-bg)] border border-[var(--card-border)] rounded-[1.75rem] rounded-tl-md px-7 py-5 shadow-sm flex items-center gap-3 text-[var(--text-soft)]">
                            <IconLoader className="animate-spin text-[#d4af37]" size={20} />
                            <span className="text-[15px] font-medium text-[var(--foreground)]">Analyzing legal texts and formatting response...</span>
                        </div>
                    </div>
                )}
                <div ref={messagesEndRef} className="h-4 w-full"></div>
            </div>
        </div>
    );
};

export default ChatMessages;