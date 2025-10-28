/**
 * Main Express Application
 * Configures middleware and routes for the AI Lawyer backend
 */

import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import chatRoutes from './routes/chatRoutes.js';
import { connectDB } from './config/database.js';

// Load environment variables
dotenv.config();

// Get __dirname equivalent in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Initialize Express app
const app = express();
const PORT = process.env.PORT || 3000;

// =========================
// MIDDLEWARE
// =========================

// Enable CORS for all origins (adjust for production)
app.use(cors());

// Parse JSON request bodies
app.use(express.json());

// Parse URL-encoded bodies
app.use(express.urlencoded({ extended: true }));

// Serve static files from the public directory
app.use(express.static(path.join(__dirname, '..', 'public')));

// Request logging middleware
app.use((req, res, next) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
  next();
});

// =========================
// ROUTES
// =========================

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    success: true,
    message: 'AI Lawyer Backend is running',
    timestamp: new Date().toISOString(),
  });
});

// Chat routes
app.use('/api/chat', chatRoutes);

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    success: true,
    message: 'Welcome to AI Lawyer API',
    version: '1.0.0',
    endpoints: {
      health: '/health',
      chat: {
        ask: 'POST /api/chat/ask',
        health: 'GET /api/chat/health',
      },
    },
  });
});

// =========================
// ERROR HANDLING
// =========================

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: 'Endpoint not found',
    path: req.path,
  });
});

// Global error handler
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({
    success: false,
    error: 'Internal server error',
    message: err.message,
  });
});

// =========================
// START SERVER
// =========================

// Connect to MongoDB before starting the server
connectDB()
  .then(() => {
    app.listen(PORT, () => {
      console.log('\n🚀 AI Lawyer Backend Server Started');
      console.log(`📡 Server running on http://localhost:${PORT}`);
      console.log(`🔗 API Documentation: http://localhost:${PORT}/`);
      console.log(`💬 Chat endpoint: http://localhost:${PORT}/api/chat/ask`);
      console.log('\n⏳ Loading AI models (this may take a moment)...\n');
    });
  })
  .catch((error) => {
    console.error('Failed to start server:', error);
    process.exit(1);
  });

export default app;
