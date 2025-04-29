// server.js
const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');

// Create Express app
const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(express.json());
app.use(cors());

// PostgreSQL connection pool
const pool = new Pool({
  user: process.env.DATABASE_USER,
  host: process.env.DATABASE_HOST,
  database: process.env.DATABASE_NAME,
  password: process.env.DATABASE_PASSWORD,
  port: process.env.DATABASE_PORT,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Test database connection
pool.connect((err, client, release) => {
  if (err) {
    return console.error('Error acquiring client', err.stack);
  }
  console.log('Connected to PostgreSQL database');
  release();
});

// GET endpoint for paginated commits with date filter
app.get('/api/commits', async (req, res) => {
  try {
    // Extract query parameters with defaults
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const startDate = req.query.start_date;
    const endDate = req.query.end_date;
    
    // Calculate offset
    const offset = (page - 1) * limit;
    
    // Input validation
    if (page < 1 || limit < 1) {
      return res.status(400).json({ 
        success: false, 
        error: 'Page and limit must be positive integers' 
      });
    }
    
    if (limit > 100) {
      return res.status(400).json({ 
        success: false, 
        error: 'Maximum limit is 100 records per request' 
      });
    }
    
    // Build WHERE clause based on provided date range
    let whereClause = '';
    const queryParams = [];
    let paramCounter = 1;
    
    if (startDate && endDate) {
      whereClause = `WHERE created_at BETWEEN '${startDate}'::timestamptz AND '${endDate}'::timestamptz`;
    } else if (startDate) {
      whereClause = `WHERE created_at >= '${startDate}'::timestamptz`;
    } else if (endDate) {
      whereClause = `WHERE created_at <= '${endDate}'::timestamptz`;
    }
    
    // Add pagination parameters
    // queryParams.push(limit, offset);
    
    // SQL query for paginated results with optional date range
    const query = `
      SELECT * FROM commits 
      ${whereClause}
      ORDER BY created_at ASC
      LIMIT ${limit} OFFSET ${page*limit}
    `;
    
    // Count total records query for pagination metadata
    const countQuery = `
      SELECT COUNT(*) AS total FROM commits 
      ${whereClause}
    `;
    
    // Clone the query parameters array for the count query
    // but exclude the limit and offset which are the last two params
    const countQueryParams = whereClause ? queryParams.slice(0, queryParams.length - 2) : [];
    console.log(query, queryParams)
    console.log(countQuery, countQueryParams)
    
    // Execute both queries in parallel
    const [commitsResult, countResult] = await Promise.all([
      pool.query(query, queryParams),
      pool.query(countQuery, countQueryParams)
    ]);
    
    const commits = commitsResult.rows;
    const totalCount = parseInt(countResult.rows[0].total);
    const totalPages = Math.ceil(totalCount / limit);
    
    // Send response with pagination metadata and filter info
    res.status(200).json({
      success: true,
      data: commits,
      pagination: {
        total: totalCount,
        page,
        limit,
        totalPages,
        hasNextPage: page < totalPages,
        hasPrevPage: page > 1,
      },
      filters: {
        startDate: startDate || null,
        endDate: endDate || null
      }
    });
  } catch (error) {
    console.error('Error fetching commits:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: process.env.NODE_ENV === 'development' ? error.message : undefined
    });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'ok' });
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('Shutting down server...');
  pool.end(() => {
    console.log('Pool has ended');
    process.exit(0);
  });
});
