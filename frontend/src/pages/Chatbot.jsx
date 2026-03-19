import React from 'react';
import ChatSidebar from '../components/chat/ChatSidebar';
import ChatHeader from '../components/chat/ChatHeader';
import ChatMessages from '../components/chat/ChatMessages';
import ChatInput from '../components/chat/ChatInput';
import { ChatProvider } from '../context/ChatContext';
import ErrorBoundary from '../components/ErrorBoundary';

const ChatbotContent = () => {
    return (
        <div className="flex-1 flex bg-[var(--background)] overflow-hidden relative h-full font-sans selection:bg-slate-900 selection:text-white">  
            {/* Main Chat Area */}
            <div className="flex-1 flex flex-col h-full bg-[var(--background)] min-w-0">      
                <ErrorBoundary variant="local">
                    {/* Header */}
                    <ChatHeader />

                    {/* Messages Body */}
                    <ChatMessages />

                    {/* Input Area */}
                    <ChatInput />
                </ErrorBoundary>
            </div>

            {/* Chat History Sidebar */}
            <ErrorBoundary variant="local">
                <ChatSidebar />
            </ErrorBoundary>
        </div>
    );
};

const Chatbot = () => {
    return (
        <ChatProvider>
            <ChatbotContent />
        </ChatProvider>
    );
};

export default Chatbot;