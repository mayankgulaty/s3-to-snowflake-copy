package com.example;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Example demonstrating how copyFileToSnowflake method works with hardcoded values
 */
public class SnowflakeCopyExample {

    public static void main(String[] args) {
        System.out.println("=== Snowflake Copy Method Example ===");
        
        try {
            // Example 1: Copy to Snowflake Internal Stage
            exampleCopyToInternalStage();
            
            // Example 2: Copy to Snowflake Table
            exampleCopyToTable();
            
        } catch (Exception e) {
            System.err.println("Example failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Example 1: Copy file to Snowflake Internal Stage
     */
    public static void exampleCopyToInternalStage() throws IOException, SQLException {
        System.out.println("\n--- Example 1: Copy to Snowflake Internal Stage ---");
        
        // Create hardcoded parameters for Internal Stage
        FileCopyParams params = new FileCopyParams();
        params.setSourcePath("/tmp/test-files");
        params.setFileName("data.csv");
        params.setSnowflakeStagePath("@my_data_stage/reports/2024/");
        params.setCopyToS3(false);
        params.setCopyToSnowflake(true);
        
        // Create Snowflake config (hardcoded)
        SnowflakeConfig snowflakeConfig = new SnowflakeConfig(
            "jdbc:snowflake://a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com",
            "F_ICG_DEV_177688_ALERTS",
            "your-password-here",  // You need to provide real password
            "D_ICG_DEV_177688_MASTER",
            "LANDING",
            "R_ICG_DEV_177688_APPADMIN",
            "W_ICG_DEV_177688_DEFAULT_XS"
        );
        
        // Create the enhanced file copy instance
        EnhancedFileCopy fileCopy = new EnhancedFileCopy(params, snowflakeConfig);
        
        try {
            // Test connection first
            if (fileCopy.testConnections()) {
                System.out.println("✅ Connection test passed");
                
                // This will call copyFileToSnowflake internally
                String runId = "example_stage_" + System.currentTimeMillis();
                fileCopy.copyFiles(runId);
                
                System.out.println("✅ File copied to Snowflake Internal Stage: " + params.getSnowflakeStagePath());
            } else {
                System.out.println("❌ Connection test failed");
            }
        } finally {
            fileCopy.close();
        }
    }

    /**
     * Example 2: Copy file to Snowflake Table
     */
    public static void exampleCopyToTable() throws IOException, SQLException {
        System.out.println("\n--- Example 2: Copy to Snowflake Table ---");
        
        // Create hardcoded parameters for Table
        FileCopyParams params = new FileCopyParams();
        params.setSourcePath("/tmp/test-files");
        params.setFileName("data.csv");
        params.setSnowflakeTableName("MY_FILES_TABLE");
        params.setSnowflakeSchema("LANDING");
        params.setCopyToS3(false);
        params.setCopyToSnowflake(true);
        
        // Create Snowflake config (hardcoded)
        SnowflakeConfig snowflakeConfig = new SnowflakeConfig(
            "jdbc:snowflake://a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com",
            "F_ICG_DEV_177688_ALERTS",
            "your-password-here",  // You need to provide real password
            "D_ICG_DEV_177688_MASTER",
            "LANDING",
            "R_ICG_DEV_177688_APPADMIN",
            "W_ICG_DEV_177688_DEFAULT_XS"
        );
        
        // Create the enhanced file copy instance
        EnhancedFileCopy fileCopy = new EnhancedFileCopy(params, snowflakeConfig);
        
        try {
            // Test connection first
            if (fileCopy.testConnections()) {
                System.out.println("✅ Connection test passed");
                
                // This will call copyFileToSnowflake internally
                String runId = "example_table_" + System.currentTimeMillis();
                fileCopy.copyFiles(runId);
                
                System.out.println("✅ File copied to Snowflake Table: " + params.getSnowflakeTableName());
            } else {
                System.out.println("❌ Connection test failed");
            }
        } finally {
            fileCopy.close();
        }
    }

    /**
     * Example 3: Direct method call demonstration
     * This shows exactly how copyFileToSnowflake method is called internally
     */
    public static void exampleDirectMethodCall() throws IOException, SQLException {
        System.out.println("\n--- Example 3: Direct Method Call ---");
        
        // Create a test file
        File testFile = new File("/tmp/test-data.csv");
        if (!testFile.exists()) {
            System.out.println("Creating test file: " + testFile.getAbsolutePath());
            // You would create the file here in a real scenario
        }
        
        // Hardcoded parameters
        FileCopyParams params = new FileCopyParams();
        params.setSnowflakeStagePath("@my_stage/data/");
        params.setCopyToSnowflake(true);
        
        // Hardcoded Snowflake config
        SnowflakeConfig snowflakeConfig = new SnowflakeConfig(
            "jdbc:snowflake://your-account.snowflakecomputing.com",
            "your-user",
            "your-password",
            "your-database",
            "LANDING",
            "your-role",
            "your-warehouse"
        );
        
        // Create EnhancedFileCopy instance
        EnhancedFileCopy fileCopy = new EnhancedFileCopy(params, snowflakeConfig);
        
        try {
            // This is exactly how copyFileToSnowflake is called internally:
            // 1. It checks if snowflakeStagePath is set
            // 2. If yes, calls copyFileToSnowflakeStage()
            // 3. If no, calls copyFileToSnowflakeTable()
            
            String s3Key = "data/test-data.csv"; // This would be the S3 key
            
            // The actual method call that happens internally:
            System.out.println("Internal method call: copyFileToSnowflake(file, s3Key)");
            System.out.println("  - file: " + testFile.getName());
            System.out.println("  - s3Key: " + s3Key);
            System.out.println("  - stagePath: " + params.getSnowflakeStagePath());
            
            // In the real implementation, this would be:
            // copyFileToSnowflake(testFile, s3Key);
            
        } finally {
            fileCopy.close();
        }
    }

    /**
     * Example 4: Show the exact method signature and parameters
     */
    public static void showMethodSignature() {
        System.out.println("\n--- Example 4: Method Signature ---");
        System.out.println("Method: private void copyFileToSnowflake(File file, String s3Key)");
        System.out.println();
        System.out.println("Parameters:");
        System.out.println("  - file: File object representing the source file");
        System.out.println("  - s3Key: String representing the S3 key (used for tracking)");
        System.out.println();
        System.out.println("Method Logic:");
        System.out.println("  1. Check if params.getSnowflakeStagePath() is set");
        System.out.println("  2. If YES: call copyFileToSnowflakeStage(file, s3Key)");
        System.out.println("  3. If NO: call copyFileToSnowflakeTable(file, s3Key)");
        System.out.println();
        System.out.println("Example calls:");
        System.out.println("  copyFileToSnowflake(new File(\"/data/report.csv\"), \"reports/report.csv\");");
        System.out.println("  copyFileToSnowflake(new File(\"/logs/app.log\"), \"logs/app.log\");");
    }
}
