package com.example;

/**
 * Example showing how to copy files from S3 to Snowflake using the new COPY command
 */
public class S3ToSnowflakeCopyExample {
    
    public static void main(String[] args) {
        System.out.println("=== S3 to Snowflake Copy Example ===");
        
        // Snowflake configuration
        SnowflakeConfig snowflakeConfig = new SnowflakeConfig(
            "jdbc:snowflake://a_icg_dev.us-east-2.aws.snowflakecomputing.com?db=D_ICG_DEV_177688_MASTER&schema=LANDING&warehouse=W_ICG_DEV_177688_DEFAULT_XS&role=R_ICG_DEV_177688_APPADMIN",
            "F_ICG_DEV_177688_ALERTS",
            "YOUR_PASSWORD_HERE", // UPDATE THIS
            "D_ICG_DEV_177688_MASTER",
            "LANDING",
            "R_ICG_DEV_177688_APPADMIN",
            "W_ICG_DEV_177688_DEFAULT_XS"
        );
        
        // S3 configuration (using your existing pattern)
        String s3AccessKey = "your-base64-encoded-access-key";
        String s3SecretKey = "your-base64-encoded-secret-key";
        String s3Endpoint = "https://your-s3-endpoint.com";
        String s3Bucket = "your-s3-bucket";
        String s3BucketPath = "/your/bucket/path";
        String s3Key = "path/to/your/file.txt";
        
        // Snowflake stage path
        String stagePath = "@my_stage/data/";
        
        try {
            // Create S3 service using your existing pattern
            CompatibleS3Service s3Service = new CompatibleS3Service(
                s3AccessKey, s3SecretKey, s3Endpoint, s3Bucket, s3BucketPath
            );
            
            // Create Snowflake service
            SnowflakeService snowflakeService = new SnowflakeService(snowflakeConfig);
            
            // Method 1: Copy FILE from S3 to Snowflake using existing S3 service
            System.out.println("Copying FILE from S3 to Snowflake...");
            snowflakeService.copyFileFromS3ToStage(stagePath, s3Service, s3Key);
            
            System.out.println("✅ Successfully copied file from S3 to Snowflake!");
            
        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
