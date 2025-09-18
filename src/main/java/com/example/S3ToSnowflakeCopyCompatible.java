package com.example;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * S3 to Snowflake copy using your existing S3FileCopy pattern
 * Compatible with your existing InputParams and S3 configuration
 */
public class S3ToSnowflakeCopyCompatible {
    private static final Logger logger = LoggerFactory.getLogger(S3ToSnowflakeCopyCompatible.class);

    public static void main(String[] args) {
        logger.info("Starting S3 to Snowflake copy (Compatible mode)...");
        
        try {
            // Hardcoded configuration - UPDATE THESE VALUES to match your existing setup
            String s3AccessKey = "your-base64-encoded-access-key";  // Base64 encoded like your existing code
            String s3SecretKey = "your-base64-encoded-secret-key";  // Base64 encoded like your existing code
            String s3Endpoint = "https://your-s3-endpoint.com";     // Your custom S3 endpoint
            String s3BucketName = "your-s3-bucket";
            String s3BucketPath = "/your/bucket/path";              // Your bucket path
            
            String snowflakeStagePath = "@my_stage/data/";          // Snowflake stage to copy to
            
            // Snowflake configuration
            SnowflakeConfig snowflakeConfig = new SnowflakeConfig(
                "jdbc:snowflake://a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com",
                "F_ICG_DEV_177688_ALERTS",
                "your-snowflake-password",  // UPDATE THIS
                "D_ICG_DEV_177688_MASTER",
                "LANDING",
                "R_ICG_DEV_177688_APPADMIN",
                "W_ICG_DEV_177688_DEFAULT_XS"
            );
            
            // Run the copy using your existing pattern
            copyS3ToSnowflakeCompatible(s3AccessKey, s3SecretKey, s3Endpoint, 
                                      s3BucketName, s3BucketPath, snowflakeStagePath, snowflakeConfig);
            
        } catch (Exception e) {
            logger.error("S3 to Snowflake copy failed", e);
            System.exit(1);
        }
        
        logger.info("S3 to Snowflake copy completed");
    }

    /**
     * Copy files from S3 to Snowflake using your existing pattern
     */
    public static void copyS3ToSnowflakeCompatible(String s3AccessKey, String s3SecretKey, 
                                                  String s3Endpoint, String s3BucketName, 
                                                  String s3BucketPath, String snowflakeStagePath, 
                                                  SnowflakeConfig snowflakeConfig) throws Exception {
        
        // Create compatible S3 service (matches your existing pattern)
        CompatibleS3Service s3Service = new CompatibleS3Service(
            s3AccessKey, s3SecretKey, s3Endpoint, s3BucketName, s3BucketPath
        );
        
        // Create Snowflake service
        SnowflakeService snowflakeService = new SnowflakeService(snowflakeConfig);
        
        try {
            // Test connections (matching your existing pattern)
            logger.info("Testing connections...");
            
            // Check S3 bucket access (matching your checkAccessToBucket method)
            if (!s3Service.checkAccessToBucket()) {
                throw new Exception("❌ Cannot access S3 bucket: " + s3BucketName);
            }
            logger.info("✅ S3 bucket access verified");
            
            // Test Snowflake connection
            if (!snowflakeService.testConnection()) {
                throw new Exception("❌ Snowflake connection failed");
            }
            logger.info("✅ Snowflake connection successful");
            
            // Create Snowflake stage if it doesn't exist
            createSnowflakeStage(snowflakeService, snowflakeStagePath);
            
            // List files from S3 (matching your existing pattern)
            List<S3ObjectSummary> s3Objects = s3Service.listObjectsWithPrefix(s3BucketPath.substring(1));
            logger.info("Found {} files in S3 bucket path: {}", s3Objects.size(), s3BucketPath);
            
            if (s3Objects.isEmpty()) {
                logger.warn("No files found in S3 bucket path: {}", s3BucketPath);
                return;
            }
            
            // Copy files (matching your existing processing pattern)
            int successCount = 0;
            int errorCount = 0;
            
            for (S3ObjectSummary s3Object : s3Objects) {
                String s3Key = s3Object.getKey();
                String fileName = extractFileName(s3Key);
                long fileSize = s3Object.getSize();
                
                try {
                    logger.info("Processing file: {} ({} bytes)", fileName, fileSize);
                    
                    // Download from S3 (matching your existing pattern)
                    byte[] fileContent = s3Service.downloadFile(s3Key);
                    
                    // Upload to Snowflake Internal Stage
                    snowflakeService.uploadToStage(snowflakeStagePath, fileName, fileContent);
                    
                    successCount++;
                    logger.info("✅ Successfully copied: {} to {}", fileName, snowflakeStagePath);
                    
                } catch (Exception e) {
                    errorCount++;
                    logger.error("❌ Failed to copy file: {}", fileName, e);
                }
            }
            
            // Print summary (matching your existing logging pattern)
            logger.info("Copy operation completed:");
            logger.info("  - Successfully copied: {}", successCount);
            logger.info("  - Failed: {}", errorCount);
            logger.info("  - Total processed: {}", successCount + errorCount);
            
        } finally {
            s3Service.close();
            snowflakeService.close();
        }
    }

    /**
     * Create Snowflake Internal Stage if it doesn't exist
     */
    private static void createSnowflakeStage(SnowflakeService snowflakeService, String stagePath) throws SQLException {
        // Extract stage name from path (e.g., @my_stage/folder/ -> @my_stage)
        String stageName = stagePath.split("/")[0];
        
        String createStageSQL = String.format("CREATE STAGE IF NOT EXISTS %s", stageName);
        snowflakeService.executeQuery(createStageSQL);
        logger.info("✅ Snowflake stage created/verified: {}", stageName);
    }

    /**
     * Extract filename from S3 key (matching your existing pattern)
     */
    private static String extractFileName(String s3Key) {
        int lastSlashIndex = s3Key.lastIndexOf('/');
        if (lastSlashIndex >= 0 && lastSlashIndex < s3Key.length() - 1) {
            return s3Key.substring(lastSlashIndex + 1);
        }
        return s3Key;
    }
}
