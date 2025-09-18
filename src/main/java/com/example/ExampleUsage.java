package com.example;

import java.util.ArrayList;
import java.util.List;

/**
 * Example usage of the updated S3 to Snowflake copy application
 * with your existing S3 configuration structure
 */
public class ExampleUsage {

    public static void main(String[] args) {
        // Example 1: Using the builder with your existing structure
        exampleWithBuilder();
        
        // Example 2: Creating configuration manually
        exampleManualConfiguration();
        
        // Example 3: Custom file patterns
        exampleCustomPatterns();
    }

    /**
     * Example using the S3ConfigBuilder (recommended approach)
     */
    public static void exampleWithBuilder() {
        System.out.println("=== Example 1: Using S3ConfigBuilder ===");
        
        // Create S3 config using your existing variable names
        S3Config s3Config = new S3ConfigBuilder()
                .bucketName("tts-banzai-inystrsvcs-uat")
                .accessKey("your-access-key")
                .secretKey("your-secret-key")
                .endpoint("https://swdc-obj-wip4.nam.nsroot.net")
                .notificationEnable(true)
                .addCsvPattern("/167764/Investor Services/NAM/US/RAW_167764_TTS_INV_NAM_US_TEST", "TTS_CSV_FILES")
                .addJsonPattern("/167764/Investor Services/NAM/US/RAW_167764_TTS_INV_NAM_US_TEST", "TTS_JSON_FILES")
                .addTextPattern("/167764/Investor Services/NAM/US/RAW_167764_TTS_INV_NAM_US_TEST", "TTS_TEXT_FILES")
                .addCatchAllPattern("/167764/Investor Services/NAM/US/RAW_167764_TTS_INV_NAM_US_TEST", "S3_FILES")
                .build();
        
        System.out.println("S3 Config created: " + s3Config);
    }

    /**
     * Example creating configuration manually to match your existing structure
     */
    public static void exampleManualConfiguration() {
        System.out.println("\n=== Example 2: Manual Configuration ===");
        
        // Create file patterns list (your fileMetadata)
        List<FilePattern> fileMetadata = new ArrayList<>();
        
        // Add CSV pattern
        fileMetadata.add(new FilePattern(
            "/167764/Investor Services/NAM/US/RAW_167764_TTS_INV_NAM_US_TEST/*.csv",
            "CSV files from TTS INV NAM US TEST",
            true,
            "TTS_CSV_FILES",
            "csv",
            50 * 1024 * 1024, // 50MB max
            "stream"
        ));
        
        // Add JSON pattern
        fileMetadata.add(new FilePattern(
            "/167764/Investor Services/NAM/US/RAW_167764_TTS_INV_NAM_US_TEST/*.json",
            "JSON files from TTS INV NAM US TEST",
            true,
            "TTS_JSON_FILES",
            "json",
            100 * 1024 * 1024, // 100MB max
            "stream"
        ));
        
        // Create S3 config with your existing structure
        S3Config s3Config = new S3Config(
            "tts-banzai-inystrsvcs-uat",  // bucketName
            "your-access-key",            // accessKey
            "your-secret-key",            // secretKey
            fileMetadata,                 // fileMetadata (List<FilePattern>)
            "https://swdc-obj-wip4.nam.nsroot.net",  // endpoint
            true                          // notificationEnable
        );
        
        System.out.println("S3 Config created: " + s3Config);
    }

    /**
     * Example with custom file patterns for different file types
     */
    public static void exampleCustomPatterns() {
        System.out.println("\n=== Example 3: Custom File Patterns ===");
        
        S3Config s3Config = new S3ConfigBuilder()
                .bucketName("tts-banzai-inystrsvcs-uat")
                .accessKey("your-access-key")
                .secretKey("your-secret-key")
                .endpoint("https://swdc-obj-wip4.nam.nsroot.net")
                .notificationEnable(true)
                // Custom patterns for different file types
                .addFilePattern(
                    "/167764/Investor Services/NAM/US/RAW_167764_TTS_INV_NAM_US_TEST/data/*.parquet",
                    "Parquet data files",
                    true,
                    "TTS_PARQUET_DATA",
                    "parquet",
                    500 * 1024 * 1024, // 500MB max
                    "stream"
                )
                .addFilePattern(
                    "/167764/Investor Services/NAM/US/RAW_167764_TTS_INV_NAM_US_TEST/logs/*.log",
                    "Log files",
                    true,
                    "TTS_LOG_FILES",
                    "log",
                    20 * 1024 * 1024, // 20MB max
                    "memory"
                )
                .addFilePattern(
                    "/167764/Investor Services/NAM/US/RAW_167764_TTS_INV_NAM_US_TEST/archives/*.zip",
                    "Archive files",
                    true,
                    "TTS_ARCHIVE_FILES",
                    "zip",
                    1000 * 1024 * 1024, // 1GB max
                    "stream"
                )
                .build();
        
        System.out.println("S3 Config with custom patterns: " + s3Config);
    }

    /**
     * Example showing how to integrate with your existing code
     */
    public static void integrateWithExistingCode() {
        System.out.println("\n=== Integration Example ===");
        
        // Your existing variables
        String bucketName = "tts-banzai-inystrsvcs-uat";
        String accessKey = "your-access-key";
        String secretKey = "your-secret-key";
        List<FilePattern> fileMetadata = createYourFilePatterns();
        
        // Create S3Config using your existing structure
        S3Config s3Config = new S3Config(bucketName, accessKey, secretKey, fileMetadata);
        
        // Create Snowflake config
        SnowflakeConfig snowflakeConfig = new SnowflakeConfig(
            "jdbc:snowflake://a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com",
            "F_ICG_DEV_177688_ALERTS",
            "your-snowflake-password",
            "D_ICG_DEV_177688_MASTER",
            "LANDING",
            "R_ICG_DEV_177688_APPADMIN",
            "W_ICG_DEV_177688_DEFAULT_XS"
        );
        
        // Create and run the application
        S3ToSnowflakeCopy app = new S3ToSnowflakeCopy(s3Config, snowflakeConfig, "S3_FILES");
        // app.copyAllFiles(); // Uncomment to run
    }

    /**
     * Helper method to create your file patterns
     */
    private static List<FilePattern> createYourFilePatterns() {
        List<FilePattern> patterns = new ArrayList<>();
        
        // Add your specific file patterns here
        patterns.add(new FilePattern(
            "/167764/Investor Services/NAM/US/RAW_167764_TTS_INV_NAM_US_TEST/*.csv",
            "CSV files",
            true,
            "TTS_CSV_FILES",
            "csv",
            50 * 1024 * 1024,
            "stream"
        ));
        
        return patterns;
    }
}
