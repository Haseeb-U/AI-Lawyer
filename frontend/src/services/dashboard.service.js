import api from '../utils/api';

export const dashboardService = {
    /**
     * Fetches the overall statistics for the user.
     * Expected Backend Route: GET /api/dashboard/stats
     * @returns {Promise<{
     *   aiConsultations: number,
     *   contractsAnalyzed: number,
     *   judgmentsViewed: number,
     *   hoursSaved: string
     * }>}
     */
    getStats: async () => {
        const response = await api.get('/dashboard/stats');
        return response.data;
    },

    /**
     * Fetches the recent activity timeline for the user.
     * Expected Backend Route: GET /api/dashboard/activity
     * @returns {Promise<Array<{
     *   id: string,
     *   title: string,
     *   time: string,
     *   module: string,
     *   color: string,
     *   iconType: string
     * }>>}
     */
    getRecentActivity: async () => {
        const response = await api.get('/dashboard/activity');
        return response.data;
    },

    /**
     * Fetches AI model status and system quota limits.
     * Expected Backend Route: GET /api/dashboard/system-status
     * @returns {Promise<{
     *   databaseIndexing: number,
     *   usageQuotaPercentage: number,
     *   statusMessage: string
     * }>}
     */
    getSystemStatus: async () => {
        const response = await api.get('/dashboard/system-status');
        return response.data;
    }
};