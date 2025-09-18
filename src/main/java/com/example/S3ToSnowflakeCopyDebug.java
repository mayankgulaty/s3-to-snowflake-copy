package com.example;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * Debug version of S3 to Snowflake copy with detailed logging and error handling
 */
public class S3ToSnowflakeCopyDebug {
    private static final Logger logger = LoggerFactory.getLogger(S3ToSnowflakeCopyDebug.class);

    public static void main(String[] args) {
        logger.info("=== DEBUG: Starting S3 to Snowflake copy ===");
        
        try {
            // Test Snowflake connection first
            testSnowflakeConnection();
            
            // If Snowflake connection works, proceed with full copy
            runFullCopy();
            
        } catch (Exception e) {
            logger.error("S3 to Snowflake copy failed", e);
            System.exit(1);
        }
        
        logger.info("=== DEBUG: S3 to Snowflake copy completed ===");
    }

    /**
     * Test only Snowflake connection
     */
    public static void testSnowflakeConnection() throws Exception {
        logger.info("=== DEBUG: Testing Snowflake connection only ===");
        
        // Snowflake configuration
        SnowflakeConfig snowflakeConfig = new SnowflakeConfig(
            "jdbc:snowflake://a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com",
            "F_ICG_DEV_177688_ALERTS",
            "YOUR_SNOWFLAKE_PASSWORD_HERE",  // UPDATE THIS
            "D_ICG_DEV_177688_MASTER",
            "LANDING",
            "R_ICG_DEV_177688_APPADMIN",
            "W_ICG_DEV_177688_DEFAULT_XS"
        );
        
        // Create debug Snowflake service
        SnowflakeServiceDebug snowflakeService = new SnowflakeServiceDebug(snowflakeConfig);
        
        try {
            // Test connection with detailed logging
            if (snowflakeService.testConnection()) {
                logger.info("✅ Snowflake connection test PASSED");
                
                // Test stage creation
                String testStagePath = "@test_stage/debug/";
                snowflakeService.createStage(testStagePath);
                logger.info("✅ Snowflake stage creation test PASSED");
                
            } else {
                logger.error("❌ Snowflake connection test FAILED");
                throw new Exception("Snowflake connection failed");
            }
            
        } finally {
            snowflakeService.close();
        }
    }

    /**
     * Run full S3 to Snowflake copy
     */
    public static void runFullCopy() throws Exception {
        logger.info("=== DEBUG: Running full S3 to Snowflake copy ===");
        
        // Hardcoded configuration - UPDATE THESE VALUES
        String s3AccessKey = "your-base64-encoded-access-key";
        String s3SecretKey = "your-base64-encoded-secret-key";
        String s3Endpoint = "https://your-s3-endpoint.com";
        String s3BucketName = "your-s3-bucket";
        String s3BucketPath = "/your/bucket/path";
        
        String snowflakeStagePath = "@my_stage/data/";
        
        // Snowflake configuration
        SnowflakeConfig snowflakeConfig = new SnowflakeConfig(
            "jdbc:snowflake://a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com",
            "F_ICG_DEV_177688_ALERTS",
            "YOUR_SNOWFLAKE_PASSWORD_HERE",  // UPDATE THIS
            "D_ICG_DEV_177688_MASTER",
            "LANDING",
            "R_ICG_DEV_177688_APPADMIN",
            "W_ICG_DEV_177688_DEFAULT_XS"
        );
        
        // Create services
        CompatibleS3Service s3Service = new CompatibleS3Service(
            s3AccessKey, s3SecretKey, s3Endpoint, s3BucketName, s3BucketPath
        );
        
        SnowflakeServiceDebug snowflakeService = new SnowflakeServiceDebug(snowflakeConfig);
        
        try {
            // Test S3 connection
            logger.info("=== DEBUG: Testing S3 connection ===");
            if (!s3Service.checkAccessToBucket()) {
                throw new Exception("❌ Cannot access S3 bucket: " + s3BucketName);
            }
            logger.info("✅ S3 connection test PASSED");
            
            // Test Snowflake connection
            logger.info("=== DEBUG: Testing Snowflake connection ===");
            if (!snowflakeService.testConnection()) {
                throw new Exception("❌ Snowflake connection failed");
            }
            logger.info("✅ Snowflake connection test PASSED");
            
            // Create Snowflake stage
            snowflakeService.createStage(snowflakeStagePath);
            
            // List S3 objects
            logger.info("=== DEBUG: Listing S3 objects ===");
            List<S3ObjectSummary> s3Objects = s3Service.listObjectsWithPrefix(s3BucketPath.substring(1));
            logger.info("Found {} objects in S3", s3Objects.size());
            
            if (s3Objects.isEmpty()) {
                logger.warn("No files found in S3 bucket path: {}", s3BucketPath);
                return;
            }
            
            // Process first file only (for debugging)
            logger.info("=== DEBUG: Processing first file only ===");
            S3ObjectSummary firstObject = s3Objects.get(0);
            String s3Key = firstObject.getKey();
            String fileName = extractFileName(s3Key);
            long fileSize = firstObject.getSize();
            
            logger.info("Processing file: {} ({} bytes)", fileName, fileSize);
            
            // Download from S3
            byte[] fileContent = s3Service.downloadFile(s3Key);
            logger.info("Downloaded {} bytes from S3", fileContent.length);
            
            // Upload to Snowflake
            snowflakeService.uploadToStage(snowflakeStagePath, fileName, fileContent);
            logger.info("✅ Successfully processed file: {}", fileName);
            
        } finally {
            s3Service.close();
            snowflakeService.close();
        }
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
