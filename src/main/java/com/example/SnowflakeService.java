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
     * Upload file to Snowflake Internal Stage (for local files only)
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
        
        // For in-memory file content, we need to use a different approach
        // Since Snowflake PUT command requires a file path, we'll:
        // 1. Create a temporary file
        // 2. Use PUT command to upload it
        // 3. Clean up the temporary file
        
        java.io.File tempFile = null;
        try {
            // Create temporary file
            tempFile = java.io.File.createTempFile("snowflake_upload_", "_" + fileName);
            java.nio.file.Files.write(tempFile.toPath(), fileContent);
            
            // Use Snowflake PUT command
            String putCommand = String.format("PUT 'file://%s' %s AUTO_COMPRESS=FALSE OVERWRITE=TRUE", 
                    tempFile.getAbsolutePath(), stagePath);
            
            logger.info("Executing PUT command: {}", putCommand);
            
            try (Statement statement = connection.createStatement()) {
                statement.execute(putCommand);
                logger.info("✅ Successfully uploaded file {} to stage {}", fileName, stagePath);
            }
            
        } catch (java.io.IOException e) {
            logger.error("Failed to create temporary file for upload", e);
            throw new SQLException("Failed to create temporary file for upload", e);
        } finally {
            // Clean up temporary file
            if (tempFile != null && tempFile.exists()) {
                if (tempFile.delete()) {
                    logger.debug("Cleaned up temporary file: {}", tempFile.getAbsolutePath());
                } else {
                    logger.warn("Failed to delete temporary file: {}", tempFile.getAbsolutePath());
                }
            }
        }
    }

    /**
     * Upload file directly to Snowflake Internal Stage using file path
     * @param stagePath The internal stage path (e.g., @my_stage/folder/)
     * @param filePath The local file path to upload
     * @throws SQLException if upload fails
     */
    public void uploadFileToStage(String stagePath, String filePath) throws SQLException {
        logger.info("Uploading file {} to Snowflake Internal Stage: {}", filePath, stagePath);
        
        // Ensure connection is established
        if (connection == null || connection.isClosed()) {
            connect();
        }
        
        // Create the stage if it doesn't exist
        createStageIfNotExists(stagePath);
        
        // Use Snowflake PUT command with file path
        String putCommand = String.format("PUT 'file://%s' %s AUTO_COMPRESS=FALSE OVERWRITE=TRUE", 
                filePath, stagePath);
        
        logger.info("Executing PUT command: {}", putCommand);
        
        try (Statement statement = connection.createStatement()) {
            statement.execute(putCommand);
            logger.info("✅ Successfully uploaded file {} to stage {}", filePath, stagePath);
        }
    }

    /**
     * Copy file from S3 to Snowflake Internal Stage using existing CompatibleS3Service
     * @param stagePath The internal stage path (e.g., @my_stage/folder/)
     * @param s3Service The CompatibleS3Service instance
     * @param s3Key The S3 object key
     * @throws SQLException if copy fails
     */
    public void copyFileFromS3ToStage(String stagePath, CompatibleS3Service s3Service, String s3Key) throws SQLException {
        logger.info("Copying FILE from S3 to Snowflake Internal Stage: {} -> {}", s3Key, stagePath);
        
        // Ensure connection is established
        if (connection == null || connection.isClosed()) {
            connect();
        }
        
        // Create the stage if it doesn't exist
        createStageIfNotExists(stagePath);
        
        // For copying files (not table data), we need to:
        // 1. Download the file from S3 using existing service
        // 2. Use PUT command to upload to Snowflake stage
        
        java.io.File tempFile = null;
        try {
            // Download file from S3 using existing service
            byte[] fileContent = s3Service.downloadFile(s3Key);
            
            // Create temporary file with exact filename (no prefix)
            String fileName = s3Key.substring(s3Key.lastIndexOf("/") + 1);
            java.io.File tempDir = java.io.File.createTempFile("snowflake_upload_", "");
            tempDir.delete(); // Delete the file
            tempDir.mkdirs(); // Create as directory
            tempFile = new java.io.File(tempDir, fileName);
            
            // Write file content preserving original data
            java.nio.file.Files.write(tempFile.toPath(), fileContent);
            
            // Verify file integrity
            byte[] writtenContent = java.nio.file.Files.readAllBytes(tempFile.toPath());
            if (writtenContent.length != fileContent.length) {
                logger.error("File corruption detected! Original: {} bytes, Written: {} bytes", 
                           fileContent.length, writtenContent.length);
                throw new SQLException("File corruption during download/write process");
            }
            
            logger.info("Downloaded file: {} ({} bytes) to temp file: {} - Integrity verified", 
                       fileName, fileContent.length, tempFile.getAbsolutePath());
            
            // Use PUT command to upload file to Snowflake stage
            String putCommand = String.format("PUT 'file://%s' %s AUTO_COMPRESS=FALSE OVERWRITE=TRUE", 
                    tempFile.getAbsolutePath(), stagePath);
            
            logger.info("Executing PUT command: {}", putCommand);
            
            try (Statement statement = connection.createStatement()) {
                statement.execute(putCommand);
                logger.info("✅ Successfully copied file from S3 to Snowflake stage {}", stagePath);
            }
            
        } catch (Exception e) {
            logger.error("Failed to copy file from S3 to Snowflake stage", e);
            throw new SQLException("Failed to copy file from S3 to Snowflake stage", e);
        } finally {
            // Clean up temporary file and directory
            if (tempFile != null && tempFile.exists()) {
                if (tempFile.delete()) {
                    logger.debug("Cleaned up temporary file: {}", tempFile.getAbsolutePath());
                } else {
                    logger.warn("Failed to delete temporary file: {}", tempFile.getAbsolutePath());
                }
                
                // Clean up temp directory
                java.io.File tempDir = tempFile.getParentFile();
                if (tempDir != null && tempDir.exists()) {
                    if (tempDir.delete()) {
                        logger.debug("Cleaned up temporary directory: {}", tempDir.getAbsolutePath());
                    } else {
                        logger.warn("Failed to delete temporary directory: {}", tempDir.getAbsolutePath());
                    }
                }
            }
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
