package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Service class for Snowflake operations
 */
public class SnowflakeService {
    private static final Logger logger = LoggerFactory.getLogger(SnowflakeService.class);
    
    private final SnowflakeConfig snowflakeConfig;
    private Connection connection;

    public SnowflakeService(SnowflakeConfig snowflakeConfig) {
        this.snowflakeConfig = snowflakeConfig;
    }

    /**
     * Establish connection to Snowflake
     * @throws SQLException if connection fails
     */
    public void connect() throws SQLException {
        logger.info("Connecting to Snowflake...");
        
        String jdbcUrl = String.format("jdbc:snowflake://%s?db=%s&schema=%s&warehouse=%s&role=%s",
                snowflakeConfig.getUrl().replace("jdbc:snowflake://", ""),
                snowflakeConfig.getDatabase(),
                snowflakeConfig.getSchema(),
                snowflakeConfig.getWarehouse(),
                snowflakeConfig.getRole());

        this.connection = DriverManager.getConnection(
                jdbcUrl,
                snowflakeConfig.getUser(),
                snowflakeConfig.getPassword()
        );
        
        logger.info("Successfully connected to Snowflake");
    }

    /**
     * Create a table for storing file metadata and content
     * @param tableName The name of the table to create
     * @throws SQLException if table creation fails
     */
    public void createFileTable(String tableName) throws SQLException {
        logger.info("Creating table: {}", tableName);
        
        String createTableSQL = String.format("""
            CREATE TABLE IF NOT EXISTS %s (
                file_name VARCHAR(255),
                file_size BIGINT,
                file_content BINARY,
                upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                s3_key VARCHAR(500)
            )
            """, tableName);

        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSQL);
            logger.info("Table {} created successfully", tableName);
        }
    }

    /**
     * Insert file data into Snowflake table
     * @param tableName The target table name
     * @param fileName The name of the file
     * @param fileSize The size of the file
     * @param fileContent The binary content of the file
     * @param s3Key The S3 key of the file
     * @throws SQLException if insert fails
     */
    public void insertFile(String tableName, String fileName, long fileSize, 
                          byte[] fileContent, String s3Key) throws SQLException {
        logger.info("Inserting file {} into table {}", fileName, tableName);
        
        String insertSQL = String.format("""
            INSERT INTO %s (file_name, file_size, file_content, s3_key)
            VALUES (?, ?, ?, ?)
            """, tableName);

        try (PreparedStatement statement = connection.prepareStatement(insertSQL)) {
            statement.setString(1, fileName);
            statement.setLong(2, fileSize);
            statement.setBinaryStream(3, new ByteArrayInputStream(fileContent), fileContent.length);
            statement.setString(4, s3Key);
            
            int rowsAffected = statement.executeUpdate();
            logger.info("Successfully inserted file {} ({} rows affected)", fileName, rowsAffected);
        }
    }

    /**
     * Insert file data using InputStream (for large files)
     * @param tableName The target table name
     * @param fileName The name of the file
     * @param fileSize The size of the file
     * @param fileInputStream The input stream containing the file content
     * @param s3Key The S3 key of the file
     * @throws SQLException if insert fails
     */
    public void insertFileStream(String tableName, String fileName, long fileSize, 
                                InputStream fileInputStream, String s3Key) throws SQLException {
        logger.info("Inserting file stream {} into table {}", fileName, tableName);
        
        String insertSQL = String.format("""
            INSERT INTO %s (file_name, file_size, file_content, s3_key)
            VALUES (?, ?, ?, ?)
            """, tableName);

        try (PreparedStatement statement = connection.prepareStatement(insertSQL)) {
            statement.setString(1, fileName);
            statement.setLong(2, fileSize);
            statement.setBinaryStream(3, fileInputStream, fileSize);
            statement.setString(4, s3Key);
            
            int rowsAffected = statement.executeUpdate();
            logger.info("Successfully inserted file stream {} ({} rows affected)", fileName, rowsAffected);
        }
    }

    /**
     * Get list of files already uploaded to Snowflake
     * @param tableName The table name to query
     * @return List of S3 keys already uploaded
     * @throws SQLException if query fails
     */
    public List<String> getUploadedFiles(String tableName) throws SQLException {
        logger.info("Getting list of uploaded files from table {}", tableName);
        
        String selectSQL = String.format("SELECT DISTINCT s3_key FROM %s", tableName);
        List<String> uploadedFiles = new ArrayList<>();
        
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(selectSQL)) {
            
            while (resultSet.next()) {
                uploadedFiles.add(resultSet.getString("s3_key"));
            }
        }
        
        logger.info("Found {} uploaded files", uploadedFiles.size());
        return uploadedFiles;
    }

    /**
     * Check if a file already exists in Snowflake
     * @param tableName The table name to check
     * @param s3Key The S3 key to check
     * @return true if file exists, false otherwise
     * @throws SQLException if query fails
     */
    public boolean fileExists(String tableName, String s3Key) throws SQLException {
        String selectSQL = String.format("SELECT COUNT(*) FROM %s WHERE s3_key = ?", tableName);
        
        try (PreparedStatement statement = connection.prepareStatement(selectSQL)) {
            statement.setString(1, s3Key);
            
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getInt(1) > 0;
                }
            }
        }
        
        return false;
    }

    /**
     * Execute a custom SQL query
     * @param sql The SQL query to execute
     * @throws SQLException if query execution fails
     */
    public void executeQuery(String sql) throws SQLException {
        logger.info("Executing query: {}", sql);
        
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            logger.info("Query executed successfully");
        }
    }

    /**
     * Close the Snowflake connection
     */
    public void close() {
        if (connection != null) {
            try {
                connection.close();
                logger.info("Snowflake connection closed");
            } catch (SQLException e) {
                logger.error("Error closing Snowflake connection", e);
            }
        }
    }

    /**
     * Test the Snowflake connection
     * @return true if connection is valid, false otherwise
     */
    public boolean testConnection() {
        try {
            if (connection == null || connection.isClosed()) {
                connect();
            }
            
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("SELECT CURRENT_TIMESTAMP()")) {
                
                if (resultSet.next()) {
                    logger.info("Snowflake connection test successful. Current time: {}", 
                              resultSet.getTimestamp(1));
                    return true;
                }
            }
        } catch (SQLException e) {
            logger.error("Snowflake connection test failed", e);
        }
        
        return false;
    }
}
