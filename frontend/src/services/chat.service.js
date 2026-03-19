import api from '../utils/api';

class ChatService {
    async getChatHistory() {
        const response = await api.get('/chat/history');
        return response.data;
    }

    async getChatDetails(chatId) {
        const response = await api.get(`/chat/${chatId}`);
        return response.data;
    }

    async createNewChat(title = 'New Chat') {
        const response = await api.post('/chat/new', { title });
        return response.data;
    }

    async askQuestion(payload, signal) {
        const response = await api.post('/chat/ask', payload, { signal });
        return response.data;
    }

    async deleteChat(chatId) {
        const response = await api.delete(`/chat/${chatId}`);
        return response.data;
    }
}

export const chatService = new ChatService();