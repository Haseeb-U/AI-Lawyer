/**
 * MongoDB Database Configuration
 * Handles connection to MongoDB Atlas
 */

import mongoose from 'mongoose';
import dotenv from 'dotenv';

dotenv.config();

/**
 * Connect to MongoDB
 */
export const connectDB = async () => {
  try {
    const mongoURI = process.env.MONGO_URI;
    const dbName = process.env.MONGO_DB_NAME;

    if (!mongoURI) {
      throw new Error('MONGO_URI is not defined in environment variables');
    }

    const options = {
      dbName: dbName || 'AILawyerDB',
      maxPoolSize: 10,
      serverSelectionTimeoutMS: 5000,
      socketTimeoutMS: 45000,
    };

    await mongoose.connect(mongoURI, options);

    console.log('✅ MongoDB connected successfully');
    console.log(`📊 Database: ${mongoose.connection.db.databaseName}`);
    console.log(`🌐 Host: ${mongoose.connection.host}`);

    // Handle connection events
    mongoose.connection.on('error', (err) => {
      console.error('❌ MongoDB connection error:', err);
    });

    mongoose.connection.on('disconnected', () => {
      console.warn('⚠️  MongoDB disconnected');
    });

    mongoose.connection.on('reconnected', () => {
      console.log('♻️  MongoDB reconnected');
    });

  } catch (error) {
    console.error('❌ MongoDB connection failed:', error.message);
    process.exit(1);
  }
};

/**
 * Disconnect from MongoDB
 */
export const disconnectDB = async () => {
  try {
    await mongoose.disconnect();
    console.log('MongoDB disconnected');
  } catch (error) {
    console.error('Error disconnecting from MongoDB:', error);
    throw error;
  }
};

/**
 * Get MongoDB connection status
 */
export const getConnectionStatus = () => {
  const states = {
    0: 'disconnected',
    1: 'connected',
    2: 'connecting',
    3: 'disconnecting',
  };
  
  return {
    status: states[mongoose.connection.readyState],
    database: mongoose.connection.db?.databaseName,
    host: mongoose.connection.host,
  };
};

export default {
  connectDB,
  disconnectDB,
  getConnectionStatus,
};
