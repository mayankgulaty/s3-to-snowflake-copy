package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Main application class for copying files from IBM S3 Sonic to Snowflake
 */
public class S3ToSnowflakeCopy {
    private static final Logger logger = LoggerFactory.getLogger(S3ToSnowflakeCopy.class);
    
    private final S3Service s3Service;
    private final SnowflakeService snowflakeService;
    private final String targetTableName;

    public S3ToSnowflakeCopy(S3Config s3Config, SnowflakeConfig snowflakeConfig, String targetTableName) {
        this.s3Service = new S3Service(s3Config);
        this.snowflakeService = new SnowflakeService(snowflakeConfig);
        this.targetTableName = targetTableName;
    }

    /**
     * Main method to copy all files from S3 to Snowflake
     */
    public void copyAllFiles() {
        try {
            // Test connections
            if (!testConnections()) {
                logger.error("Connection tests failed. Exiting.");
                return;
            }

            // Get list of files from S3 based on patterns
            List<S3Object> s3Objects = s3Service.listObjects();
            logger.info("Found {} files in S3 bucket", s3Objects.size());

            if (s3Objects.isEmpty()) {
                logger.info("No files found in S3 bucket. Exiting.");
                return;
            }

            // Group files by target table
            Map<String, List<S3Object>> filesByTable = groupFilesByTargetTable(s3Objects);
            
            // Process each table group
            AtomicInteger totalSuccessCount = new AtomicInteger(0);
            AtomicInteger totalSkipCount = new AtomicInteger(0);
            AtomicInteger totalErrorCount = new AtomicInteger(0);

            for (Map.Entry<String, List<S3Object>> entry : filesByTable.entrySet()) {
                String tableName = entry.getKey();
                List<S3Object> tableFiles = entry.getValue();
                
                logger.info("Processing {} files for table: {}", tableFiles.size(), tableName);
                
                // Create target table if it doesn't exist
                snowflakeService.createFileTable(tableName);
                
                // Get list of already uploaded files for this table
                List<String> uploadedFiles = snowflakeService.getUploadedFiles(tableName);
                logger.info("Found {} already uploaded files in table {}", uploadedFiles.size(), tableName);

                // Process files for this table
                processFilesForTable(tableName, tableFiles, uploadedFiles, 
                                   totalSuccessCount, totalSkipCount, totalErrorCount);
            }

            // Print summary
            logger.info("Copy operation completed:");
            logger.info("  - Successfully copied: {}", totalSuccessCount.get());
            logger.info("  - Skipped (already uploaded): {}", totalSkipCount.get());
            logger.info("  - Errors: {}", totalErrorCount.get());

        } catch (Exception e) {
            logger.error("Fatal error during copy operation", e);
        } finally {
            // Close connections
            s3Service.close();
            snowflakeService.close();
        }
    }

    /**
     * Group files by their target table based on FilePattern rules
     */
    private Map<String, List<S3Object>> groupFilesByTargetTable(List<S3Object> s3Objects) {
        Map<String, List<S3Object>> filesByTable = new HashMap<>();
        
        for (S3Object s3Object : s3Objects) {
            String tableName = s3Service.getTargetTableName(s3Object.key());
            filesByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(s3Object);
        }
        
        return filesByTable;
    }

    /**
     * Process files for a specific table
     */
    private void processFilesForTable(String tableName, List<S3Object> tableFiles, 
                                    List<String> uploadedFiles,
                                    AtomicInteger successCount, AtomicInteger skipCount, AtomicInteger errorCount) {
        for (S3Object s3Object : tableFiles) {
            String s3Key = s3Object.key();
            String fileName = extractFileName(s3Key);
            long fileSize = s3Object.size();

            try {
                // Check if file should be processed based on pattern rules
                if (!s3Service.shouldProcessFile(s3Key, fileSize)) {
                    logger.info("Skipping file {} (pattern rules)", fileName);
                    skipCount.incrementAndGet();
                    continue;
                }

                // Skip if file already uploaded
                if (uploadedFiles.contains(s3Key)) {
                    logger.info("Skipping file {} (already uploaded)", fileName);
                    skipCount.incrementAndGet();
                    continue;
                }

                // Copy file
                copySingleFile(s3Key, fileName, fileSize, tableName);
                successCount.incrementAndGet();
                logger.info("Successfully copied file: {} ({}) to table {}", 
                           fileName, formatFileSize(fileSize), tableName);

            } catch (Exception e) {
                logger.error("Error copying file: {}", fileName, e);
                errorCount.incrementAndGet();
            }
        }
    }

    /**
     * Copy a single file from S3 to Snowflake
     */
    private void copySingleFile(String s3Key, String fileName, long fileSize, String tableName) throws IOException, SQLException {
        logger.info("Copying file: {} ({} bytes) to table: {}", fileName, fileSize, tableName);

        // For large files (> 100MB), use stream processing
        if (fileSize > 100 * 1024 * 1024) {
            copyLargeFile(s3Key, fileName, fileSize, tableName);
        } else {
            copySmallFile(s3Key, fileName, fileSize, tableName);
        }
    }

    /**
     * Copy small files by loading into memory first
     */
    private void copySmallFile(String s3Key, String fileName, long fileSize, String tableName) throws IOException, SQLException {
        byte[] fileContent = s3Service.downloadFile(s3Key);
        snowflakeService.insertFile(tableName, fileName, fileSize, fileContent, s3Key);
    }

    /**
     * Copy large files using stream processing
     */
    private void copyLargeFile(String s3Key, String fileName, long fileSize, String tableName) throws IOException, SQLException {
        try (var inputStream = s3Service.downloadFileAsStream(s3Key)) {
            snowflakeService.insertFileStream(tableName, fileName, fileSize, inputStream, s3Key);
        }
    }

    /**
     * Test both S3 and Snowflake connections
     */
    private boolean testConnections() {
        logger.info("Testing connections...");
        
        // Test S3 connection
        try {
            List<S3Object> objects = s3Service.listObjects();
            logger.info("S3 connection test successful. Found {} objects", objects.size());
        } catch (Exception e) {
            logger.error("S3 connection test failed", e);
            return false;
        }

        // Test Snowflake connection
        if (!snowflakeService.testConnection()) {
            logger.error("Snowflake connection test failed");
            return false;
        }

        return true;
    }

    /**
     * Extract filename from S3 key
     */
    private String extractFileName(String s3Key) {
        int lastSlashIndex = s3Key.lastIndexOf('/');
        if (lastSlashIndex >= 0 && lastSlashIndex < s3Key.length() - 1) {
            return s3Key.substring(lastSlashIndex + 1);
        }
        return s3Key;
    }

    /**
     * Format file size in human readable format
     */
    private String formatFileSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1) + "";
        return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }

    /**
     * Main entry point
     */
    public static void main(String[] args) {
        logger.info("Starting S3 to Snowflake copy application");

        try {
            // Create S3 configuration using the builder
            S3Config s3Config = S3ConfigBuilder.createDefaultTTSConfig(
                "", // accessKey - you need to provide this
                ""  // secretKey - you need to provide this
            );

            SnowflakeConfig snowflakeConfig = new SnowflakeConfig(
                "jdbc:snowflake://a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com",
                "F_ICG_DEV_177688_ALERTS",
                "", // password - you need to provide this
                "D_ICG_DEV_177688_MASTER",
                "LANDING",
                "R_ICG_DEV_177688_APPADMIN",
                "W_ICG_DEV_177688_DEFAULT_XS"
            );

            // Create and run the copy application
            S3ToSnowflakeCopy app = new S3ToSnowflakeCopy(s3Config, snowflakeConfig, "S3_FILES");
            app.copyAllFiles();

        } catch (Exception e) {
            logger.error("Application failed", e);
            System.exit(1);
        }

        logger.info("Application completed");
    }

}
