import { useQuery } from '@tanstack/react-query';
import { toast } from 'sonner';
import { dashboardService } from '../services/dashboard.service';
import { chatService } from '../services/chat.service';
import { useAuth } from '../context/AuthContext';

export const useDashboardData = () => {
    const { user } = useAuth();
    const userName = user?.name || user?.username || 'Client';

    const getGreeting = () => {
        const hour = new Date().getHours();
        if (hour < 12) return 'Good morning';
        if (hour < 18) return 'Good afternoon';
        return 'Good evening';
    };

    const currentDate = new Date().toLocaleDateString('en-US', {
        weekday: 'long',
        year: 'numeric',
        month: 'long',
        day: 'numeric'
    });

    const fetchAllDashboardData = async () => {
        let statsData = {
            aiConsultations: 0,
            contractsAnalyzed: 0,
            judgmentsViewed: 0,
            hoursSaved: "0h"
        };
        let activitiesData = [];
        let systemData = {
            databaseIndexing: 100,
            usageQuotaPercentage: 10,
            statusMessage: "Your legal assistant is fully operational and ready to assist."
        };

        try {
            // 1. Fetch real chat history
            let realChatCount = 0;
            try {
                const chatHistory = await chatService.getChatHistory();
                realChatCount = chatHistory?.length || 0;
            } catch (e) {
                console.warn("Failed to fetch real chat history", e);
            }

            // 2. Fetch stats
            try {
                const fetchedStats = await dashboardService.getStats();
                if (fetchedStats) {
                    statsData = {
                        aiConsultations: fetchedStats.aiConsultations || realChatCount,
                        contractsAnalyzed: fetchedStats.contractsAnalyzed || 0,
                        judgmentsViewed: fetchedStats.judgmentsViewed || 0,
                        hoursSaved: fetchedStats.hoursSaved || "0h"
                    };
                }
            } catch (err) {
                // fallback
                statsData.aiConsultations = realChatCount;
            }

            // 3. Fetch Recent Activity
            try {
                const recentActs = await dashboardService.getRecentActivity();
                activitiesData = recentActs || [];
            } catch (err) {
                console.warn("Backend /dashboard/activity not ready yet.");
            }

            // 4. Fetch System Status
            try {
                const sysStat = await dashboardService.getSystemStatus();
                if (sysStat) systemData = sysStat;
            } catch (err) {
                console.warn("System status fallback.");
            }

            return { stats: statsData, activities: activitiesData, systemStatus: systemData };

        } catch (error) {
            console.error("Dashboard DB fetch error:", error);
            throw new Error("Failed to load dashboard data");
        }
    };

    const { data, isLoading, isError } = useQuery({
        queryKey: ['dashboardData', user?.id],
        queryFn: fetchAllDashboardData,
        staleTime: 5 * 60 * 1000, 
        onError: () => toast.error("Failed to load dashboard data.")
    });

    return {
        userName,
        getGreeting,
        currentDate,
        stats: data?.stats || { aiConsultations: 0, contractsAnalyzed: 0, judgmentsViewed: 0, hoursSaved: "0h" },
        activities: data?.activities || [],
        systemStatus: data?.systemStatus || { databaseIndexing: 100, usageQuotaPercentage: 0, statusMessage: "Establishing secure connection..." },
        isLoading
    };
};