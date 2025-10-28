/**
 * Chat Routes
 * Defines API endpoints for chat functionality
 */

import express from 'express';
import { askQuestion, healthCheck, getDocuments } from '../controllers/chatController.js';

const router = express.Router();

/**
 * POST /api/chat/ask
 * Ask a legal question and get AI-generated response
 * 
 * Request body:
 * {
 *   "query": "What are the provisions for employee compensation?",
 *   "topK": 5  // Optional, default is 5
 * }
 * 
 * Response:
 * {
 *   "success": true,
 *   "data": {
 *     "answer": "...",
 *     "sources": [...],
 *     "query": "...",
 *     "metadata": {...}
 *   }
 * }
 */
router.post('/ask', askQuestion);

/**
 * GET /api/chat/health
 * Check health status of chat service
 */
router.get('/health', healthCheck);

/**
 * GET /api/chat/documents
 * Get list of all unique documents in the database
 * 
 * Response:
 * {
 *   "success": true,
 *   "count": 123,
 *   "documents": [
 *     {
 *       "title": "Document Title",
 *       "year": 2021,
 *       "source_page": "https://...",
 *       "document_type": "act",
 *       "source_website": "balochistan-code"
 *     },
 *     ...
 *   ]
 * }
 */
router.get('/documents', getDocuments);

export default router;
