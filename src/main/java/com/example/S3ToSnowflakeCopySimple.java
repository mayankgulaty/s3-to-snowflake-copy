package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * Simple S3 to Snowflake copy application
 * Copies files FROM S3 TO Snowflake Internal Stage
 */
public class S3ToSnowflakeCopySimple {
    private static final Logger logger = LoggerFactory.getLogger(S3ToSnowflakeCopySimple.class);

    public static void main(String[] args) {
        logger.info("Starting S3 to Snowflake copy...");
        
        try {
            // Hardcoded configuration - UPDATE THESE VALUES
            String s3BucketName = "your-s3-bucket-name";
            String s3AccessKey = "your-s3-access-key";
            String s3SecretKey = "your-s3-secret-key";
            String s3Prefix = "data/reports/";  // S3 folder to copy from (optional)
            
            String snowflakeStagePath = "@my_stage/data/";  // Snowflake stage to copy to
            
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
            
            // Run the copy
            copyS3ToSnowflake(s3BucketName, s3AccessKey, s3SecretKey, s3Prefix, snowflakeStagePath, snowflakeConfig);
            
        } catch (Exception e) {
            logger.error("S3 to Snowflake copy failed", e);
            System.exit(1);
        }
        
        logger.info("S3 to Snowflake copy completed");
    }

    /**
     * Copy files from S3 to Snowflake Internal Stage
     */
    public static void copyS3ToSnowflake(String s3BucketName, String s3AccessKey, String s3SecretKey, 
                                        String s3Prefix, String snowflakeStagePath, 
                                        SnowflakeConfig snowflakeConfig) throws Exception {
        
        // Create S3 configuration
        S3Config s3Config = new S3Config(s3BucketName, s3AccessKey, s3SecretKey, null);
        
        // Create services
        S3Service s3Service = new S3Service(s3Config);
        SnowflakeService snowflakeService = new SnowflakeService(snowflakeConfig);
        
        try {
            // Test connections
            logger.info("Testing connections...");
            
            // Test S3 connection
            List<S3Object> s3Objects = s3Service.listObjects();
            logger.info("✅ S3 connection successful. Found {} objects", s3Objects.size());
            
            // Test Snowflake connection
            if (!snowflakeService.testConnection()) {
                throw new Exception("❌ Snowflake connection failed");
            }
            logger.info("✅ Snowflake connection successful");
            
            // Create Snowflake stage if it doesn't exist
            createSnowflakeStage(snowflakeService, snowflakeStagePath);
            
            // Copy files
            int successCount = 0;
            int errorCount = 0;
            
            for (S3Object s3Object : s3Objects) {
                String s3Key = s3Object.key();
                String fileName = extractFileName(s3Key);
                long fileSize = s3Object.size();
                
                // Skip if prefix doesn't match (if specified)
                if (s3Prefix != null && !s3Prefix.isEmpty() && !s3Key.startsWith(s3Prefix)) {
                    continue;
                }
                
                try {
                    logger.info("Copying file: {} ({} bytes)", fileName, fileSize);
                    
                    // Download from S3
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
            
            // Print summary
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
     * Extract filename from S3 key
     */
    private static String extractFileName(String s3Key) {
        int lastSlashIndex = s3Key.lastIndexOf('/');
        if (lastSlashIndex >= 0 && lastSlashIndex < s3Key.length() - 1) {
            return s3Key.substring(lastSlashIndex + 1);
        }
        return s3Key;
    }
}
