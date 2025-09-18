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
        
        // Use the URL directly from config - it should already be properly formatted
        String jdbcUrl = snowflakeConfig.getUrl();
        
        // Add query parameters if they're not already in the URL
        if (!jdbcUrl.contains("?")) {
            jdbcUrl = String.format("%s?db=%s&schema=%s&warehouse=%s&role=%s",
                    jdbcUrl,
                    snowflakeConfig.getDatabase(),
                    snowflakeConfig.getSchema(),
                    snowflakeConfig.getWarehouse(),
                    snowflakeConfig.getRole());
        }
        
        logger.info("Using JDBC URL: {}", jdbcUrl);
        logger.info("User: {}", snowflakeConfig.getUser());
        logger.info("Database: {}", snowflakeConfig.getDatabase());
        logger.info("Schema: {}", snowflakeConfig.getSchema());
        logger.info("Warehouse: {}", snowflakeConfig.getWarehouse());
        logger.info("Role: {}", snowflakeConfig.getRole());

        // Set connection properties with timeouts
        java.util.Properties props = new java.util.Properties();
        props.setProperty("user", snowflakeConfig.getUser());
        props.setProperty("password", snowflakeConfig.getPassword());
        props.setProperty("loginTimeout", "30");
        props.setProperty("networkTimeout", "30000");
        props.setProperty("queryTimeout", "300");
        
        this.connection = DriverManager.getConnection(jdbcUrl, props);
        
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
     * Upload file to Snowflake Internal Stage
     * @param stagePath The internal stage path (e.g., @my_stage/folder/)
     * @param fileName The name of the file
     * @param fileContent The binary content of the file
     * @throws SQLException if upload fails
     */
    public void uploadToStage(String stagePath, String fileName, byte[] fileContent) throws SQLException {
        logger.info("Uploading file {} to Snowflake Internal Stage: {}", fileName, stagePath);
        
        // Ensure connection is established
        if (connection == null || connection.isClosed()) {
            connect();
        }
        
        // Create the stage if it doesn't exist
        createStageIfNotExists(stagePath);
        
        // Upload file to stage using PUT command
        String putCommand = String.format("PUT 'file://%s' %s", fileName, stagePath);
        
        try (Statement statement = connection.createStatement()) {
            // For now, we'll use a simple approach - in a real implementation,
            // you might want to use Snowflake's PUT command or REST API
            logger.info("Executing PUT command: {}", putCommand);
            
            // Note: This is a simplified implementation
            // In practice, you would need to implement the actual file upload
            // using Snowflake's PUT command or REST API
            logger.warn("PUT command not fully implemented - this is a placeholder");
            logger.info("File {} would be uploaded to stage {}", fileName, stagePath);
        }
    }

    /**
     * Create Snowflake Internal Stage if it doesn't exist
     * @param stagePath The stage path (e.g., @my_stage)
     * @throws SQLException if stage creation fails
     */
    private void createStageIfNotExists(String stagePath) throws SQLException {
        // Extract stage name from path (e.g., @my_stage/folder/ -> @my_stage)
        String stageName = stagePath.split("/")[0];
        
        String createStageSQL = String.format("""
            CREATE STAGE IF NOT EXISTS %s
            """, stageName);
        
        try (Statement statement = connection.createStatement()) {
            statement.execute(createStageSQL);
            logger.info("Stage {} created/verified successfully", stageName);
        }
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
