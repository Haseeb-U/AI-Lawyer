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
      // Retrieve a larger set first (50 chunks) to find all potentially relevant ones
      console.log('🔎 Searching for relevant legal documents...');
      const initialChunks = await qdrantService.searchSimilar(queryEmbedding, 50);
      
      // Apply dynamic filtering based on score threshold
      let relevantChunks = this.filterChunksByScore(initialChunks, topK);
      
      if (relevantChunks.length === 0) {
        return {
          answer: 'I apologize, but I could not find relevant legal documents to answer your question. Please try rephrasing your query or ask about a different legal topic.',
          sources: [],
          query: originalQuery,
        };
      }

      console.log(`✅ Found ${relevantChunks.length} relevant chunks`);

      // Log max and min relevance scores of chunks being sent to LLM
      const maxScore = relevantChunks[0].score; // First chunk has highest score
      const minScore = relevantChunks[relevantChunks.length - 1].score; // Last chunk has lowest score
      console.log(`📊 Relevance Score Range → Max: ${maxScore.toFixed(4)}, Min: ${minScore.toFixed(4)}`);

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

  /**
   * Filter chunks by multiple criteria (ANY match includes the chunk)
   * A chunk is included if it meets ANY of these criteria:
   * 1. Is in the top 10 chunks (minimum guarantee)
   * 2. Score > (highest_score - 0.1)
   * 3. Score > average score of all retrieved chunks
   * 4. Score > 0.5 (absolute threshold)
   * @param {Array} chunks - Array of chunks with scores
   * @param {number} minChunks - Minimum number of chunks to return (default: 10)
   * @returns {Array} - Filtered chunks
   */
  filterChunksByScore(chunks, minChunks = 10) {
    if (!chunks || chunks.length === 0) {
      return [];
    }

    // If we have fewer than minChunks, return all
    if (chunks.length <= minChunks) {
      console.log(`📊 Retrieved ${chunks.length} chunks (less than minimum ${minChunks})`);
      return chunks;
    }

    // Calculate thresholds
    const highestScore = chunks[0].score;
    const relativeThreshold = highestScore - 0.1;
    
    // Calculate average score
    const averageScore = chunks.reduce((sum, chunk) => sum + chunk.score, 0) / chunks.length;
    
    const absoluteThreshold = 0.5;

    console.log(`📊 Score Analysis:`);
    console.log(`   • Highest: ${highestScore.toFixed(4)}`);
    console.log(`   • Average: ${averageScore.toFixed(4)}`);
    console.log(`   • Relative Threshold (highest - 0.1): ${relativeThreshold.toFixed(4)}`);
    console.log(`   • Absolute Threshold: ${absoluteThreshold.toFixed(4)}`);

    // Filter chunks based on multiple criteria (OR logic)
    const filteredChunks = chunks.filter((chunk, index) => {
      const isTopN = index < minChunks; // Criterion 1: Top N
      const meetsRelativeThreshold = chunk.score > relativeThreshold; // Criterion 2
      const meetsAverageThreshold = chunk.score > averageScore; // Criterion 3
      const meetsAbsoluteThreshold = chunk.score > absoluteThreshold; // Criterion 4

      return isTopN || meetsRelativeThreshold || meetsAverageThreshold || meetsAbsoluteThreshold;
    });

    // Count how many chunks met each criterion
    const criteriaStats = {
      topN: chunks.slice(0, minChunks).length,
      relativeThreshold: chunks.filter(c => c.score > relativeThreshold).length,
      averageThreshold: chunks.filter(c => c.score > averageScore).length,
      absoluteThreshold: chunks.filter(c => c.score > absoluteThreshold).length,
    };

    console.log(`📊 Chunks meeting each criterion:`);
    console.log(`   • Top ${minChunks}: ${criteriaStats.topN}`);
    console.log(`   • Score > ${relativeThreshold.toFixed(4)}: ${criteriaStats.relativeThreshold}`);
    console.log(`   • Score > ${averageScore.toFixed(4)} (avg): ${criteriaStats.averageThreshold}`);
    console.log(`   • Score > ${absoluteThreshold}: ${criteriaStats.absoluteThreshold}`);
    console.log(`📊 Final result: ${chunks.length} chunks → ${filteredChunks.length} chunks selected`);

    return filteredChunks;
  }
}

export default new RAGService();
