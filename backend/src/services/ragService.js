/**
 * RAG Service
 * Orchestrates the RAG (Retrieval-Augmented Generation) pipeline
 * Combines embedding generation, vector search, and LLM generation
 */

import embeddingService from './embeddingService.js';
import qdrantService from './qdrantService.js';
import llmService from './llmService.js';

class RAGService {
  /**
   * Process a user query using RAG pipeline
   * @param {string} query - The user's question
   * @param {number} topK - Number of relevant chunks to retrieve (default: 5)
   * @returns {Promise<Object>} - Response with answer and sources
   */
  async processQuery(query, topK = 5) {
    try {
      console.log('🔍 Processing query:', query);

      // Step 1: Translate query to English if it's in Urdu (for embedding and search)
      console.log('🌐 Translating query if needed...');
      const translationResult = await llmService.translateQueryToEnglish(query);
      const queryForEmbedding = translationResult.translatedQuery;
      const originalQuery = translationResult.originalQuery;
      
      if (translationResult.isUrdu) {
        console.log('📝 Detected Urdu query, using translated version for search');
      }

      // Step 2: Generate embedding for the translated/English query
      console.log('📊 Generating query embedding...');
      const queryEmbedding = await embeddingService.generateEmbedding(queryForEmbedding);
      
      // Step 3: Search for similar chunks in Qdrant
      console.log('🔎 Searching for relevant legal documents...');
      const relevantChunks = await qdrantService.searchSimilar(queryEmbedding, topK);
      
      if (relevantChunks.length === 0) {
        return {
          answer: 'I apologize, but I could not find relevant legal documents to answer your question. Please try rephrasing your query or ask about a different legal topic.',
          sources: [],
          query: originalQuery,
        };
      }

      console.log(`✅ Found ${relevantChunks.length} relevant chunks`);

      // Step 4: Generate response using LLM with context - USE ORIGINAL QUERY
      console.log('🤖 Generating response with LLM using original query...');
      const llmResult = await llmService.generateResponse(originalQuery, relevantChunks);
      const answer = llmResult.answer;
      const usedSourceIndices = llmResult.usedSources;

      console.log(`📚 LLM used ${usedSourceIndices.length} out of ${relevantChunks.length} sources`);

      // Step 4: Prepare sources for citation - only include sources that were actually used
      const sources = relevantChunks
        .map((chunk, index) => ({ chunk, originalIndex: index + 1 }))
        .filter(({ originalIndex }) => usedSourceIndices.includes(originalIndex))
        .map(({ chunk, originalIndex }) => {
          const source = {
            // Reference Information - keep the original index so it matches citations in the answer
            index: originalIndex,
            relevance_score: parseFloat(chunk.score.toFixed(4)),
            
            // Document Details
            document: {
              title: chunk.title,
              type: chunk.document_type,
              year: chunk.year,
            },
            
            // chunk Information
            chunk: {
              title: chunk.chunk_title || 'N/A',
              type: chunk.chunk_type || 'N/A',
              text: chunk.chunk, // Full chunk text for modal
            },
            
            // Source Links
            links: {},
          };

          // Add optional fields only if they exist
          if (chunk.court) {
            source.document.court = chunk.court;
          }
          
          if (chunk.source_url) {
            source.links.document_url = chunk.source_url;
          }
          
          if (chunk.source_page) {
            source.links.source_page = chunk.source_page;
          }

          return source;
        });

      console.log('✅ Response generated successfully');

      return {
        answer,
        sources,
        query: originalQuery,
        metadata: {
          chunks_retrieved: relevantChunks.length,
          chunks_used: usedSourceIndices.length,
          model: 'google/gemini-2.0-flash-exp:free',
          embedding_model: 'sentence-transformers/all-mpnet-base-v2',
          query_translated: translationResult.isUrdu,
          query_for_search: translationResult.isUrdu ? queryForEmbedding : undefined,
        },
      };
    } catch (error) {
      console.error('❌ Error in RAG pipeline:', error);
      throw new Error(`RAG processing failed: ${error.message}`);
    }
  }

  /**
   * Validate query before processing
   * @param {string} query - The user's question
   * @returns {Object} - Validation result
   */
  validateQuery(query) {
    if (!query || typeof query !== 'string') {
      return {
        valid: false,
        error: 'Query must be a non-empty string',
      };
    }

    const trimmedQuery = query.trim();
    
    if (trimmedQuery.length === 0) {
      return {
        valid: false,
        error: 'Query cannot be empty',
      };
    }

    if (trimmedQuery.length < 3) {
      return {
        valid: false,
        error: 'Query is too short (minimum 3 characters)',
      };
    }

    if (trimmedQuery.length > 1000) {
      return {
        valid: false,
        error: 'Query is too long (maximum 1000 characters)',
      };
    }

    return {
      valid: true,
      query: trimmedQuery,
    };
  }
}

export default new RAGService();
