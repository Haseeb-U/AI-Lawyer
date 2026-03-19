import React from 'react';
import { IconArrowUp, IconSquareFilled } from '@tabler/icons-react';
import { useChat } from '../../context/ChatContext';

const ChatInput = () => {
    const { input, setInput, loading, handleSend, stopGeneration } = useChat();

    const handleKeyDown = (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            handleSend();
        }
    };

    return (
        <div className="p-4 md:p-6 bg-transparent shrink-0 pt-8">
            <div className="max-w-4xl mx-auto flex items-end gap-3 bg-[var(--card-bg)] border border-[var(--card-border)] p-2.5 pl-6 rounded-[2rem] focus-within:ring-4 focus-within:ring-[#d4af37]/10 focus-within:border-[#d4af37]/50 transition-all shadow-md hover:shadow-lg">
                <textarea
                    className="flex-1 bg-transparent border-none outline-none py-3 text-[var(--foreground)] max-h-32 resize-none placeholder:text-[var(--text-muted)] font-medium text-[15px] leading-relaxed"
                    placeholder="Type your legal query here..."
                    rows="1"
                    value={input}
                    onChange={(e) => {
                        setInput(e.target.value);
                        e.target.style.height = 'auto';
                        e.target.style.height = e.target.scrollHeight + 'px';
                    }}
                    onKeyDown={handleKeyDown}
                />
                <button
                    onClick={() => loading ? stopGeneration() : handleSend()}
                    disabled={!input.trim() && !loading}
                    className="h-10 w-10 mb-1 lg:h-11 lg:w-11 shrink-0 bg-black dark:bg-white text-white dark:text-black border border-transparent rounded-full flex items-center justify-center hover:opacity-80 transition-all shadow-md active:scale-95 disabled:opacity-30 disabled:hover:opacity-30"
                >
                    {loading ? (
                        <div className="h-4 w-4 bg-current rounded-[3px]" />
                    ) : (
                        <IconArrowUp size={22} stroke={2.5} />
                    )}
                </button>
            </div>
            <div className="text-center mt-4">
                <span className="text-xs text-[var(--text-muted)] font-medium tracking-wide">AI-Lawyer can make mistakes. Consider verifying important information.</span>
            </div>
        </div>
    );
};

export default ChatInput;