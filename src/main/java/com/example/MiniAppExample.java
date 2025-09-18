package com.example;

import java.util.Arrays;

/**
 * Example usage of the Mini File Copy Application
 * This demonstrates how to use the enhanced file copy functionality
 */
public class MiniAppExample {

    public static void main(String[] args) {
        System.out.println("=== Mini File Copy App Examples ===");
        
        // Example 1: Copy specific file to both S3 and Snowflake
        example1_CopySpecificFile();
        
        // Example 2: Copy all CSV files to S3 only
        example2_CopyCsvFilesToS3();
        
        // Example 3: Copy files with patterns to Snowflake only
        example3_CopyFilesToSnowflake();
    }

    /**
     * Example 1: Copy a specific file to both S3 and Snowflake
     */
    public static void example1_CopySpecificFile() {
        System.out.println("\n--- Example 1: Copy specific file to both S3 and Snowflake ---");
        
        FileCopyParams params = new FileCopyParams();
        params.setSourcePath("/path/to/your/data");  // Change this to your actual path
        params.setFileName("data.csv");              // Specific file to copy
        params.setBucketName("your-bucket-name");    // Your S3 bucket
        params.setAccessKey("your-access-key");      // Your S3 access key
        params.setSecretKey("your-secret-key");      // Your S3 secret key
        params.setTargetPath("uploads/data/");       // S3 target path (folders will be created automatically)
        params.setSnowflakeTableName("MY_DATA_FILES"); // Snowflake table name
        params.setSnowflakeSchema("LANDING");        // Snowflake schema
        
        // Both S3 and Snowflake copy are enabled by default
        params.setCopyToS3(true);
        params.setCopyToSnowflake(true);
        
        runExample(params, "example1");
    }

    /**
     * Example 2: Copy all CSV files to S3 only
     */
    public static void example2_CopyCsvFilesToS3() {
        System.out.println("\n--- Example 2: Copy all CSV files to S3 only ---");
        
        FileCopyParams params = new FileCopyParams();
        params.setSourcePath("/path/to/your/csv/files");  // Change this to your actual path
        params.setFilePatterns(Arrays.asList("*.csv"));   // All CSV files
        params.setBucketName("your-bucket-name");         // Your S3 bucket
        params.setAccessKey("your-access-key");           // Your S3 access key
        params.setSecretKey("your-secret-key");           // Your S3 secret key
        params.setTargetPath("csv-uploads/");             // S3 target path
        params.setCopyToS3(true);
        params.setCopyToSnowflake(false);                 // S3 only
        
        runExample(params, "example2");
    }

    /**
     * Example 3: Copy files with patterns to Snowflake only
     */
    public static void example3_CopyFilesToSnowflake() {
        System.out.println("\n--- Example 3: Copy files with patterns to Snowflake only ---");
        
        FileCopyParams params = new FileCopyParams();
        params.setSourcePath("/path/to/your/files");      // Change this to your actual path
        params.setFilePatterns(Arrays.asList("*.txt", "*.log", "*.json")); // Multiple patterns
        params.setSnowflakeTableName("LOG_FILES");        // Snowflake table name
        params.setSnowflakeSchema("LANDING");             // Snowflake schema
        params.setCopyToS3(false);                        // Snowflake only
        params.setCopyToSnowflake(true);
        
        runExample(params, "example3");
    }

    /**
     * Run an example with the given parameters
     */
    private static void runExample(FileCopyParams params, String exampleName) {
        try {
            System.out.println("Parameters: " + params);
            
            // Create Snowflake config (using your existing configuration)
            SnowflakeConfig snowflakeConfig = new SnowflakeConfig(
                "jdbc:snowflake://a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com",
                "F_ICG_DEV_177688_ALERTS",
                "YOUR_SNOWFLAKE_PASSWORD",  // You need to provide this
                "D_ICG_DEV_177688_MASTER",
                params.getSnowflakeSchema(),
                "R_ICG_DEV_177688_APPADMIN",
                "W_ICG_DEV_177688_DEFAULT_XS"
            );

            // Create enhanced file copy instance
            EnhancedFileCopy fileCopy = new EnhancedFileCopy(params, snowflakeConfig);

            try {
                // Test connections
                if (!fileCopy.testConnections()) {
                    System.out.println("❌ Connection tests failed for " + exampleName);
                    return;
                }

                // Run the copy operation
                String runId = exampleName + "_" + System.currentTimeMillis();
                fileCopy.copyFiles(runId);
                System.out.println("✅ " + exampleName + " completed successfully");

            } finally {
                fileCopy.close();
            }

        } catch (Exception e) {
            System.out.println("❌ " + exampleName + " failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Example of how to use the mini app programmatically
     */
    public static void copyFileProgrammatically() {
        System.out.println("\n--- Programmatic Usage Example ---");
        
        // Create parameters
        FileCopyParams params = new FileCopyParams();
        params.setSourcePath("/tmp/test-files");
        params.setFileName("test.csv");
        params.setBucketName("my-test-bucket");
        params.setAccessKey("AKIA...");
        params.setSecretKey("...");
        params.setTargetPath("test-uploads/");
        params.setSnowflakeTableName("TEST_FILES");
        
        // Create Snowflake config
        SnowflakeConfig snowflakeConfig = new SnowflakeConfig(
            "jdbc:snowflake://your-account.snowflakecomputing.com",
            "your-user",
            "your-password",
            "your-database",
            "LANDING",
            "your-role",
            "your-warehouse"
        );

        // Use the enhanced file copy
        EnhancedFileCopy fileCopy = new EnhancedFileCopy(params, snowflakeConfig);
        
        try {
            if (fileCopy.testConnections()) {
                fileCopy.copyFiles("programmatic_test");
                System.out.println("File copy completed successfully");
            }
        } catch (Exception e) {
            System.err.println("File copy failed: " + e.getMessage());
        } finally {
            fileCopy.close();
        }
    }
}
