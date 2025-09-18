package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * Mini application for flexible file copying
 * Provides command-line interface for copying files between local, S3, and Snowflake
 */
public class MiniFileCopyApp {
    private static final Logger logger = LoggerFactory.getLogger(MiniFileCopyApp.class);

    public static void main(String[] args) {
        logger.info("Starting Mini File Copy Application");
        
        try {
            // Parse command line arguments or use interactive mode
            if (args.length > 0) {
                runWithArgs(args);
            } else {
                runInteractiveMode();
            }
        } catch (Exception e) {
            logger.error("Application failed", e);
            System.exit(1);
        }
        
        logger.info("Application completed");
    }

    /**
     * Run with command line arguments
     */
    private static void runWithArgs(String[] args) throws Exception {
        FileCopyParams params = parseCommandLineArgs(args);
        runFileCopy(params);
    }

    /**
     * Run in interactive mode
     */
    private static void runInteractiveMode() throws Exception {
        Scanner scanner = new Scanner(System.in);
        
        System.out.println("=== Mini File Copy Application ===");
        System.out.println("This app can copy files from local filesystem to S3 and/or Snowflake");
        System.out.println();

        FileCopyParams params = new FileCopyParams();

        // Get source path
        System.out.print("Enter source directory path: ");
        params.setSourcePath(scanner.nextLine().trim());

        // Get file name (optional)
        System.out.print("Enter specific file name (or press Enter for all files): ");
        String fileName = scanner.nextLine().trim();
        if (!fileName.isEmpty()) {
            params.setFileName(fileName);
        }

        // Get file patterns (optional)
        System.out.print("Enter file patterns (comma-separated, e.g., *.csv,*.txt) or press Enter: ");
        String patternsInput = scanner.nextLine().trim();
        if (!patternsInput.isEmpty()) {
            List<String> patterns = Arrays.asList(patternsInput.split(","));
            params.setFilePatterns(patterns);
        }

        // Ask what to copy to
        System.out.print("Copy to S3? (y/n, default: y): ");
        String copyToS3 = scanner.nextLine().trim();
        params.setCopyToS3(copyToS3.isEmpty() || copyToS3.toLowerCase().startsWith("y"));

        if (params.isCopyToS3()) {
            System.out.print("Enter S3 bucket name: ");
            params.setBucketName(scanner.nextLine().trim());
            
            System.out.print("Enter S3 access key: ");
            params.setAccessKey(scanner.nextLine().trim());
            
            System.out.print("Enter S3 secret key: ");
            params.setSecretKey(scanner.nextLine().trim());
            
            System.out.print("Enter S3 target path (e.g., folder/subfolder/): ");
            params.setTargetPath(scanner.nextLine().trim());
        }

        System.out.print("Copy to Snowflake? (y/n, default: y): ");
        String copyToSnowflake = scanner.nextLine().trim();
        params.setCopyToSnowflake(copyToSnowflake.isEmpty() || copyToSnowflake.toLowerCase().startsWith("y"));

        if (params.isCopyToSnowflake()) {
            System.out.print("Copy to Snowflake Table or Internal Stage? (table/stage, default: stage): ");
            String copyType = scanner.nextLine().trim();
            
            if (copyType.isEmpty() || copyType.toLowerCase().startsWith("stage")) {
                System.out.print("Enter Snowflake Internal Stage path (e.g., @my_stage/folder/): ");
                params.setSnowflakeStagePath(scanner.nextLine().trim());
            } else {
                System.out.print("Enter Snowflake table name: ");
                params.setSnowflakeTableName(scanner.nextLine().trim());
                
                System.out.print("Enter Snowflake schema (default: LANDING): ");
                String schema = scanner.nextLine().trim();
                if (!schema.isEmpty()) {
                    params.setSnowflakeSchema(schema);
                }
            }
        }

        scanner.close();
        runFileCopy(params);
    }

    /**
     * Parse command line arguments
     */
    private static FileCopyParams parseCommandLineArgs(String[] args) {
        FileCopyParams params = new FileCopyParams();
        
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--source-path":
                    params.setSourcePath(args[++i]);
                    break;
                case "--file-name":
                    params.setFileName(args[++i]);
                    break;
                case "--file-patterns":
                    params.setFilePatterns(Arrays.asList(args[++i].split(",")));
                    break;
                case "--bucket":
                    params.setBucketName(args[++i]);
                    break;
                case "--access-key":
                    params.setAccessKey(args[++i]);
                    break;
                case "--secret-key":
                    params.setSecretKey(args[++i]);
                    break;
                case "--target-path":
                    params.setTargetPath(args[++i]);
                    break;
                case "--snowflake-stage":
                    params.setSnowflakeStagePath(args[++i]);
                    break;
                case "--snowflake-table":
                    params.setSnowflakeTableName(args[++i]);
                    break;
                case "--snowflake-schema":
                    params.setSnowflakeSchema(args[++i]);
                    break;
                case "--s3-only":
                    params.setCopyToS3(true);
                    params.setCopyToSnowflake(false);
                    break;
                case "--snowflake-only":
                    params.setCopyToS3(false);
                    params.setCopyToSnowflake(true);
                    break;
                case "--help":
                    printUsage();
                    System.exit(0);
                    break;
            }
        }
        
        return params;
    }

    /**
     * Run the file copy operation
     */
    private static void runFileCopy(FileCopyParams params) throws Exception {
        logger.info("Starting file copy with parameters: {}", params);

        // Create Snowflake config (using default values from your existing config)
        SnowflakeConfig snowflakeConfig = new SnowflakeConfig(
            "jdbc:snowflake://a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com",
            "F_ICG_DEV_177688_ALERTS",
            "", // You'll need to provide the password
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
                logger.error("Connection tests failed. Exiting.");
                return;
            }

            // Run the copy operation
            String runId = "run_" + System.currentTimeMillis();
            fileCopy.copyFiles(runId);

        } finally {
            fileCopy.close();
        }
    }

    /**
     * Print usage information
     */
    private static void printUsage() {
        System.out.println("Mini File Copy Application");
        System.out.println();
        System.out.println("Usage: java -cp <classpath> com.example.MiniFileCopyApp [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --source-path <path>        Source directory path");
        System.out.println("  --file-name <name>          Specific file name to copy");
        System.out.println("  --file-patterns <patterns>  File patterns (comma-separated)");
        System.out.println("  --bucket <name>             S3 bucket name");
        System.out.println("  --access-key <key>          S3 access key");
        System.out.println("  --secret-key <key>          S3 secret key");
        System.out.println("  --target-path <path>        S3 target path");
        System.out.println("  --snowflake-stage <path>    Snowflake Internal Stage path (e.g., @my_stage/folder/)");
        System.out.println("  --snowflake-table <name>    Snowflake table name (alternative to stage)");
        System.out.println("  --snowflake-schema <name>   Snowflake schema (default: LANDING)");
        System.out.println("  --s3-only                   Copy only to S3");
        System.out.println("  --snowflake-only            Copy only to Snowflake");
        System.out.println("  --help                      Show this help");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  # Copy all CSV files from /data to S3 and Snowflake Internal Stage");
        System.out.println("  java -cp . com.example.MiniFileCopyApp --source-path /data --file-patterns *.csv --bucket my-bucket --access-key AKIA... --secret-key ... --snowflake-stage @my_stage/data/");
        System.out.println();
        System.out.println("  # Copy specific file to S3 only");
        System.out.println("  java -cp . com.example.MiniFileCopyApp --source-path /data --file-name data.csv --bucket my-bucket --access-key AKIA... --secret-key ... --s3-only");
        System.out.println();
        System.out.println("  # Copy files to Snowflake Internal Stage only");
        System.out.println("  java -cp . com.example.MiniFileCopyApp --source-path /data --file-patterns *.txt,*.log --snowflake-stage @my_stage/logs/ --snowflake-only");
        System.out.println();
        System.out.println("  # Interactive mode");
        System.out.println("  java -cp . com.example.MiniFileCopyApp");
    }
}
