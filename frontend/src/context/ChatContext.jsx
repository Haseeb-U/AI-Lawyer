/* eslint-disable react-refresh/only-export-components */
import React, { createContext, useContext, useState, useEffect, useRef, useCallback } from 'react';
import { useLocation } from 'react-router-dom';
import { chatService } from '../services/chat.service';
import { audioNotification } from '../services/audioNotification';
import { useAuth } from './AuthContext';

const ChatContext = createContext();

export const ChatProvider = ({ children }) => {
    const { user } = useAuth();
    const location = useLocation();
    const [chats, setChats] = useState([]);
    const [currentChatId, setCurrentChatId] = useState(null);
    const [messages, setMessages] = useState([]);
    const [input, setInput] = useState(location.state?.initialQuery || '');
    const [loading, setLoading] = useState(false);
    const [isSidebarOpen, setIsSidebarOpen] = useState(window.innerWidth > 768);

    const messagesEndRef = useRef(null);
    const abortControllerRef = useRef(null);

    const stopGeneration = () => {
        if (abortControllerRef.current) {
            abortControllerRef.current.abort();
            abortControllerRef.current = null;
        }
        setLoading(false);
    };

    const fetchChats = useCallback(async () => {
        try {
            const data = await chatService.getChatHistory();
            const chatsList = data.chats || data.history || [];
            setChats(chatsList);
            // Only auto-load if no chat is currently selected and there are chats available
            if (!currentChatId && chatsList.length > 0) {
                // If you want to auto-load the latest chat, uncomment this:
                // loadChat(chatsList[0]._id || chatsList[0].id);
            }
        } catch (error) {
            console.error("Failed to fetch chat history", error);
        }
    }, [currentChatId]);

    // Initial load
    useEffect(() => {
        fetchChats();
    }, [fetchChats]);

    const scrollToBottom = useCallback(() => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }, []);

    // Scroll to bottom when messages change or loading state changes
    useEffect(() => {
        scrollToBottom();
    }, [messages, loading, scrollToBottom]);

    const loadChat = async (chatId) => {
        setCurrentChatId(chatId);
        try {
            console.log("Loading chat with ID:", chatId);
            const data = await chatService.getChatDetails(chatId);
            console.log("Chat details data received:", data);
            let chatMessages = data.chat?.messages || data.messages || [];

            // Format previous complex source objects into simple strings just like how new messages are appended
            chatMessages = chatMessages.map(msg => ({
                ...msg,
                content: msg.content || '',
                sources: msg.sources ? msg.sources.map(s => {
                    if (typeof s === 'string') return s;
                    return s.title || s.document?.title || "Legal Document";
                }) : []
            }));

            setMessages(chatMessages);
        } catch (error) {
            console.error("Failed to load chat", error);
            if (error.response) console.error("Error response:", error.response.data);
        }
    };

    const createNewChat = () => {
        setCurrentChatId(null);
        setMessages([]);
        if (window.innerWidth < 768) {
            setIsSidebarOpen(false);
        }
    };

    const deleteChat = async (e, chatId) => {
        e.stopPropagation();
        try {
            await chatService.deleteChat(chatId);
            setChats(prev => prev.filter(c => c.id !== chatId));
            if (currentChatId === chatId) {
                setCurrentChatId(null);
                setMessages([]);
            }
            audioNotification.play('success');
        } catch (error) {
            console.error("Failed to delete chat", error);
            audioNotification.play('error');
        }
    };

    const handleSend = async () => {
        if (!input.trim() || loading) return;

        const userMessage = input.trim();
        
        if (userMessage.length < 3) {
            audioNotification.play('warning');
            setMessages(prev => [...prev, {
                role: 'assistant',
                content: "Please enter a slightly longer question (at least 3 characters) so I can help you better."
            }]);
            setInput('');
            return;
        }
        
        if (userMessage.length > 1000) {
            audioNotification.play('warning');
            setMessages(prev => [...prev, {
                role: 'assistant',
                content: "Your question is too long. Please summarize it to under 1000 characters."
            }]);
            return;
        }

        setInput('');
        setMessages(prev => [...prev, { role: 'user', content: userMessage }]); 
        setLoading(true);

        try {
            let targetChatId = currentChatId;

            // If it's a new chat, create it on backend first
            if (!targetChatId || targetChatId === 'new') {
                const newChatTitle = userMessage.length > 30 ? userMessage.substring(0, 30) + '...' : userMessage;
                try {
                    const newChatRes = await chatService.createNewChat(newChatTitle);
                    if (newChatRes.success && newChatRes.chat) {
                        targetChatId = newChatRes.chat._id || newChatRes.chat.id;
                        setCurrentChatId(targetChatId);
                        // Make sure the new chat immediately appears in the sidebar
                        fetchChats();
                    }
                } catch (e) {
                    console.error("Failed to crate chat session", e);
                }
            }

            const payload = {
                question: userMessage,
                user_id: user?.id || 'anonymous'
            };

            if (targetChatId && targetChatId !== 'new') {
                payload.chatId = targetChatId;
            }

            abortControllerRef.current = new AbortController();
            const data = await chatService.askQuestion(payload, abortControllerRef.current.signal);

            setMessages(prev => [...prev, {
                role: 'assistant',
                content: data.answer,
                sources: data.sources ? data.sources.map(s => s.title || s.document?.title || "Legal Document") : []
            }]);

            audioNotification.play('notification');

        } catch (error) {
            if (error.name === 'CanceledError' || error.message?.includes('canceled') || error.code === 'ERR_CANCELED') {
                console.log("Request canceled by user");
                return;
            }
            console.error("Chat error", error);
            
            const errorMessage = error.response?.data?.error 
                || "I'm sorry, I encountered an error while processing your request. Please try again or check your connection.";
                
            setMessages(prev => [...prev, {
                role: 'assistant',
                content: errorMessage
            }]);
            audioNotification.play('error');
        } finally {
            setLoading(false);
        }
    };

    const value = {
        chats,
        currentChatId,
        messages,
        input,
        setInput,
        loading,
        isSidebarOpen,
        setIsSidebarOpen,
        messagesEndRef,
        loadChat,
        createNewChat,
        deleteChat,
        handleSend,
        stopGeneration
    };

    return (
        <ChatContext.Provider value={value}>
            {children}
        </ChatContext.Provider>
    );
};

export const useChat = () => {
    const context = useContext(ChatContext);
    if (!context) {
        throw new Error("useChat must be used within a ChatProvider");
    }
    return context;
};