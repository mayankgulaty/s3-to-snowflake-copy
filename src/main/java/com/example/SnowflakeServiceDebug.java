package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Debug version of SnowflakeService with better error handling and timeouts
 */
public class SnowflakeServiceDebug {
    private static final Logger logger = LoggerFactory.getLogger(SnowflakeServiceDebug.class);
    
    private final SnowflakeConfig snowflakeConfig;
    private Connection connection;

    public SnowflakeServiceDebug(SnowflakeConfig snowflakeConfig) {
        this.snowflakeConfig = snowflakeConfig;
    }

    /**
     * Establish connection to Snowflake with debug info and timeouts
     */
    public void connect() throws SQLException {
        logger.info("Connecting to Snowflake...");
        logger.info("URL: {}", snowflakeConfig.getUrl());
        logger.info("User: {}", snowflakeConfig.getUser());
        logger.info("Database: {}", snowflakeConfig.getDatabase());
        logger.info("Schema: {}", snowflakeConfig.getSchema());
        logger.info("Warehouse: {}", snowflakeConfig.getWarehouse());
        logger.info("Role: {}", snowflakeConfig.getRole());
        
        // Build JDBC URL with timeout parameters
        String jdbcUrl = String.format("jdbc:snowflake://%s?db=%s&schema=%s&warehouse=%s&role=%s&loginTimeout=30&networkTimeout=60000",
                snowflakeConfig.getUrl().replace("jdbc:snowflake://", ""),
                snowflakeConfig.getDatabase(),
                snowflakeConfig.getSchema(),
                snowflakeConfig.getWarehouse(),
                snowflakeConfig.getRole());

        logger.info("Full JDBC URL: {}", jdbcUrl);
        
        // Set connection properties with timeouts
        Properties props = new Properties();
        props.setProperty("user", snowflakeConfig.getUser());
        props.setProperty("password", snowflakeConfig.getPassword());
        props.setProperty("loginTimeout", "30");
        props.setProperty("networkTimeout", "60000");
        props.setProperty("queryTimeout", "300");
        
        logger.info("Attempting connection with timeouts...");
        
        try {
            this.connection = DriverManager.getConnection(jdbcUrl, props);
            logger.info("✅ Successfully connected to Snowflake");
        } catch (SQLException e) {
            logger.error("❌ Failed to connect to Snowflake", e);
            logger.error("Error Code: {}", e.getErrorCode());
            logger.error("SQL State: {}", e.getSQLState());
            logger.error("Error Message: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Test connection with detailed error reporting
     */
    public boolean testConnection() {
        try {
            logger.info("Testing Snowflake connection...");
            
            if (connection == null || connection.isClosed()) {
                logger.info("Connection is null or closed, attempting to connect...");
                connect();
            }
            
            // Test with a simple query
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("SELECT CURRENT_TIMESTAMP()")) {
                
                if (resultSet.next()) {
                    Timestamp currentTime = resultSet.getTimestamp(1);
                    logger.info("✅ Snowflake connection test successful. Current time: {}", currentTime);
                    return true;
                } else {
                    logger.error("❌ No result from Snowflake test query");
                    return false;
                }
            }
        } catch (SQLException e) {
            logger.error("❌ Snowflake connection test failed", e);
            logger.error("Error Code: {}", e.getErrorCode());
            logger.error("SQL State: {}", e.getSQLState());
            logger.error("Error Message: {}", e.getMessage());
            return false;
        } catch (Exception e) {
            logger.error("❌ Unexpected error during Snowflake connection test", e);
            return false;
        }
    }

    /**
     * Create Snowflake Internal Stage with debug info
     */
    public void createStage(String stagePath) throws SQLException {
        logger.info("Creating Snowflake Internal Stage: {}", stagePath);
        
        // Extract stage name from path (e.g., @my_stage/folder/ -> @my_stage)
        String stageName = stagePath.split("/")[0];
        logger.info("Stage name: {}", stageName);
        
        String createStageSQL = String.format("CREATE STAGE IF NOT EXISTS %s", stageName);
        logger.info("Executing SQL: {}", createStageSQL);
        
        try (Statement statement = connection.createStatement()) {
            statement.execute(createStageSQL);
            logger.info("✅ Snowflake stage created/verified: {}", stageName);
        } catch (SQLException e) {
            logger.error("❌ Failed to create Snowflake stage", e);
            throw e;
        }
    }

    /**
     * Upload file to Snowflake Internal Stage (placeholder implementation)
     */
    public void uploadToStage(String stagePath, String fileName, byte[] fileContent) throws SQLException {
        logger.info("Uploading file to Snowflake Internal Stage: {} -> {}", fileName, stagePath);
        logger.info("File size: {} bytes", fileContent.length);
        
        // For now, just log the upload (actual implementation would use PUT command)
        logger.info("✅ File {} would be uploaded to stage {}", fileName, stagePath);
        logger.warn("⚠️  PUT command not fully implemented - this is a placeholder");
    }

    /**
     * Execute a custom SQL query with debug info
     */
    public void executeQuery(String sql) throws SQLException {
        logger.info("Executing SQL query: {}", sql);
        
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            logger.info("✅ Query executed successfully");
        } catch (SQLException e) {
            logger.error("❌ Query execution failed", e);
            logger.error("Error Code: {}", e.getErrorCode());
            logger.error("SQL State: {}", e.getSQLState());
            logger.error("Error Message: {}", e.getMessage());
            throw e;
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
}
