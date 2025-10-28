/**
 * Embedding Service
 * Generates embeddings using all-mpnet-base-v2 model locally
 */

import { pipeline } from '@xenova/transformers';

class EmbeddingService {
  constructor() {
    this.pipe = null;
    this.modelName = 'Xenova/all-mpnet-base-v2';
    this.isLoading = false;
  }

  /**
   * Initialize the embedding model
   * @returns {Promise<void>}
   */
  async initialize() {
    if (this.pipe) return; // Already initialized
    
    if (this.isLoading) {
      // Wait for the model to finish loading
      while (this.isLoading) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }
      return;
    }

    try {
      this.isLoading = true;
      console.log('🔄 Loading embedding model:', this.modelName);
      
      // Use Xenova version which has ONNX models
      this.pipe = await pipeline('feature-extraction', this.modelName);
      
      console.log('✅ Embedding model loaded successfully');
      this.isLoading = false;
    } catch (error) {
      this.isLoading = false;
      console.error('❌ Failed to load embedding model:', error);
      throw error;
    }
  }

  /**
   * Generate embedding for a text
   * @param {string} text - The text to embed
   * @returns {Promise<Array<number>>} - The embedding vector (768 dimensions)
   */
  async generateEmbedding(text) {
    try {
      // Ensure model is initialized
      await this.initialize();

      if (!text || text.trim().length === 0) {
        throw new Error('Text cannot be empty');
      }

      // Generate embedding
      const output = await this.pipe(text, {
        pooling: 'mean',
        normalize: true,
      });

      // Convert to array and return
      // The output is a Tensor, we need to convert it properly
      const embedding = Array.from(output.data);
      
      return embedding;
    } catch (error) {
      console.error('Error generating embedding:', error);
      throw new Error(`Embedding generation failed: ${error.message}`);
    }
  }

  /**
   * Generate embeddings for multiple texts
   * @param {Array<string>} texts - Array of texts to embed
   * @returns {Promise<Array<Array<number>>>} - Array of embedding vectors
   */
  async generateEmbeddings(texts) {
    try {
      await this.initialize();
      
      const embeddings = await Promise.all(
        texts.map(text => this.generateEmbedding(text))
      );
      
      return embeddings;
    } catch (error) {
      console.error('Error generating embeddings:', error);
      throw new Error(`Batch embedding generation failed: ${error.message}`);
    }
  }
}

export default new EmbeddingService();
