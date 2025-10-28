/**
 * Chat Controller
 * Handles HTTP requests for chat endpoints
 */

import ragService from '../services/ragService.js';
import Embeddings from '../models/Embeddings.js';

/**
 * Handle POST /api/chat/ask
 * Process user query and return AI-generated response
 */
export const askQuestion = async (req, res) => {
  try {
    const { query, question, topK } = req.body;

    // Accept both 'query' and 'question' parameters
    const userQuery = query || question;

    // Validate request body
    if (!userQuery) {
      return res.status(400).json({
        success: false,
        error: 'Query or question is required in request body',
      });
    }

    // Validate query
    const validation = ragService.validateQuery(userQuery);
    if (!validation.valid) {
      return res.status(400).json({
        success: false,
        error: validation.error,
      });
    }

    // Process query using RAG pipeline
    const result = await ragService.processQuery(
      validation.query,
      topK || 5 // Default to 5 results if not specified
    );

    // Return successful response
    return res.status(200).json({
      success: true,
      answer: result.answer,
      sources: result.sources,
      query: validation.query,
      metadata: result.metadata,
      data: result,
    });
  } catch (error) {
    console.error('Error in askQuestion controller:', error);
    
    // Return error response
    return res.status(500).json({
      success: false,
      error: 'An error occurred while processing your question',
      message: error.message,
    });
  }
};

/**
 * Handle GET /api/chat/health
 * Check health status of the chat service
 */
export const healthCheck = async (req, res) => {
  try {
    // You can add more health checks here (e.g., Qdrant connection)
    return res.status(200).json({
      success: true,
      status: 'healthy',
      timestamp: new Date().toISOString(),
      services: {
        embedding: 'ready',
        vectorDB: 'ready',
        llm: 'ready',
      },
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      status: 'unhealthy',
      error: error.message,
    });
  }
};

/**
 * Handle GET /api/chat/documents
 * Get list of all unique documents from database
 */
export const getDocuments = async (req, res) => {
  try {
    // Aggregate unique documents with title, year, source_page, and document_type
    const documents = await Embeddings.aggregate([
      {
        $group: {
          _id: {
            title: '$title',
            year: '$year',
            source_page: '$source_page',
          },
          document_type: { $first: '$document_type' },
          source_website: { $first: '$source_website' },
          court: { $first: '$court' },
        }
      },
      {
        $project: {
          _id: 0,
          title: '$_id.title',
          year: '$_id.year',
          source_page: '$_id.source_page',
          document_type: 1,
          source_website: 1,
          court: 1,
        }
      },
      {
        $sort: { title: 1, year: -1 }
      }
    ]);

    return res.status(200).json({
      success: true,
      count: documents.length,
      documents: documents,
    });
  } catch (error) {
    console.error('Error fetching documents:', error);
    return res.status(500).json({
      success: false,
      error: 'Failed to fetch documents',
      message: error.message,
    });
  }
};
