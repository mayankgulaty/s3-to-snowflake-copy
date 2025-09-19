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
        
        // S3 configuration
        String s3Bucket = "your-s3-bucket-name";
        String s3Key = "path/to/your/file.txt";
        String awsAccessKey = "your-aws-access-key";
        String awsSecretKey = "your-aws-secret-key";
        String awsRegion = "us-east-1";
        
        // Snowflake stage path
        String stagePath = "@my_stage/data/";
        
        try {
            SnowflakeService snowflakeService = new SnowflakeService(snowflakeConfig);
            
            // Method 1: Copy directly from S3 to Snowflake (RECOMMENDED)
            System.out.println("Copying from S3 to Snowflake...");
            snowflakeService.copyFromS3ToStage(
                stagePath, 
                s3Bucket, 
                s3Key, 
                awsAccessKey, 
                awsSecretKey, 
                awsRegion
            );
            
            System.out.println("✅ Successfully copied file from S3 to Snowflake!");
            
        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
