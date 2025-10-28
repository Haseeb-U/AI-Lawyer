/**
 * Embedding Model
 * MongoDB schema for storing document metadata and embeddings information
 * Vector embeddings are stored in Qdrant, this stores the metadata
 */

import mongoose from 'mongoose';

const embeddingSchema = new mongoose.Schema(
  {
    id: {
      type: Number,
      required: true,
      unique: true,
      index: true,
      description: 'Unique identifier for the embedding (matches Qdrant point ID)'
    },
    chunk_index: {
      type: Number,
      required: true,
      description: 'Index of this chunk within the parent document'
    },
    chunk_title: {
      type: String,
      description: 'Title/heading of the chunk section (e.g., "Preamble and Enactment Clause")'
    },
    chunk_type: {
      type: String,
      description: 'Type of chunk (e.g., "preamble", "section", "article")'
    },
    cleaned_path: {
      type: String,
      description: 'File path to the cleaned text document'
    },
    court: {
      type: String,
      index: true,
      default: null,
      description: 'Court or jurisdiction (e.g., Supreme Court, High Court)'
    },
    document_type: {
      type: String,
      required: true,
      index: true,
      description: 'Type of legal document (e.g., "ordinance", "act", "judgment")'
    },
    download_date: {
      type: String,
      required: true,
      description: 'Date when the document was downloaded/processed'
    },
    source_page: {
      type: String,
      description: 'Source page URL where the document was found'
    },
    source_url: {
      type: String,
      description: 'Direct URL to the source document'
    },
    source_website: {
      type: String,
      index: true,
      description: 'Source website identifier (e.g., "balochistan-code", "punjab-code")'
    },
    title: {
      type: String,
      required: true,
      index: true,
      description: 'Title of the legal document'
    },
    year: {
      type: Number,
      required: true,
      index: true,
      description: 'Year of the document'
    },
    // Note: Vector embeddings are stored in Qdrant, not here
    // This collection stores metadata only
  },
  {
    timestamps: true, // Adds createdAt and updatedAt fields
    collection: 'Embeddings', // Explicitly set collection name
  }
);

// Compound indexes for common query patterns
embeddingSchema.index({ court: 1, year: 1 });
embeddingSchema.index({ document_type: 1, year: 1 });
embeddingSchema.index({ title: 1, chunk_index: 1 });
embeddingSchema.index({ source_website: 1, document_type: 1 });

// Virtual for formatted date
embeddingSchema.virtual('formattedDownloadDate').get(function() {
  if (this.download_date) {
    return new Date(this.download_date).toLocaleDateString();
  }
  return null;
});

// Instance method to get summary
embeddingSchema.methods.getSummary = function() {
  return {
    id: this.id,
    title: this.title,
    year: this.year,
    court: this.court,
    document_type: this.document_type,
    chunk_title: this.chunk_title,
    chunk_index: this.chunk_index,
    source_website: this.source_website
  };
};

// Static method to find by document metadata
embeddingSchema.statics.findByDocument = function(filters) {
  const query = {};
  
  if (filters.court) query.court = filters.court;
  if (filters.year) query.year = filters.year;
  if (filters.document_type) query.document_type = filters.document_type;
  if (filters.source_website) query.source_website = filters.source_website;
  if (filters.title) query.title = new RegExp(filters.title, 'i');
  
  return this.find(query);
};

// Static method to get unique values for filters
embeddingSchema.statics.getFilterOptions = async function() {
  const [courts, years, documentTypes, sourceWebsites] = await Promise.all([
    this.distinct('court'),
    this.distinct('year'),
    this.distinct('document_type'),
    this.distinct('source_website')
  ]);
  
  return {
    courts: courts.filter(Boolean).sort(),
    years: years.filter(Boolean).sort((a, b) => b - a),
    documentTypes: documentTypes.filter(Boolean).sort(),
    sourceWebsites: sourceWebsites.filter(Boolean).sort()
  };
};

// Static method to find all chunks of a document
embeddingSchema.statics.findDocumentChunks = function(title, year) {
  return this.find({ title, year }).sort({ chunk_index: 1 });
};

// Pre-save middleware to validate data
embeddingSchema.pre('save', function(next) {
  // Ensure required fields are present
  if (!this.title || this.title.trim().length === 0) {
    next(new Error('Title cannot be empty'));
  }
  if (this.chunk_index < 0) {
    next(new Error('Chunk index must be non-negative'));
  }
  next();
});

const Embeddings = mongoose.model('Embeddings', embeddingSchema);

export default Embeddings;
