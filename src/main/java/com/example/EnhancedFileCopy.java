package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.*;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Enhanced file copy class that provides flexibility for copying files
 * between local filesystem, S3, and Snowflake
 */
public class EnhancedFileCopy {
    private static final Logger logger = LoggerFactory.getLogger(EnhancedFileCopy.class);
    
    private final S3Client s3Client;
    private final SnowflakeService snowflakeService;
    private final FileCopyParams params;

    public EnhancedFileCopy(FileCopyParams params, SnowflakeConfig snowflakeConfig) {
        this.params = params;
        this.s3Client = createS3Client();
        this.snowflakeService = new SnowflakeService(snowflakeConfig);
    }

    /**
     * Create S3 client with the provided credentials
     */
    private S3Client createS3Client() {
        AwsBasicCredentials credentials = AwsBasicCredentials.create(
            params.getAccessKey(),
            params.getSecretKey()
        );

        return S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .forcePathStyle(true)
                .build();
    }

    /**
     * Main method to copy files based on the parameters
     * This is similar to your copyFileToS3 method but enhanced
     */
    public void copyFiles(String runId) throws Exception {
        logger.info("Starting file copy operation with runId: {}", runId);
        logger.info("Parameters: {}", params);

        // Validate parameters
        validateParameters();

        // Get source files
        List<File> sourceFiles = getSourceFiles();
        logger.info("Found {} files to process", sourceFiles.size());

        if (sourceFiles.isEmpty()) {
            logger.warn("No files found matching the criteria");
            return;
        }

        // Process each file
        int successCount = 0;
        int errorCount = 0;

        for (File file : sourceFiles) {
            try {
                processFile(file, runId);
                successCount++;
                logger.info("Successfully processed file: {}", file.getName());
            } catch (Exception e) {
                errorCount++;
                logger.error("Error processing file: {}", file.getName(), e);
            }
        }

        logger.info("File copy operation completed. Success: {}, Errors: {}", successCount, errorCount);
    }

    /**
     * Process a single file - copy to S3 and/or Snowflake as configured
     */
    private void processFile(File file, String runId) throws Exception {
        String fileName = file.getName();
        long fileSize = file.length();
        String s3Key = buildS3Key(fileName);

        logger.info("Processing file: {} ({} bytes)", fileName, fileSize);

        // Copy to S3 if enabled
        if (params.isCopyToS3()) {
            copyFileToS3(file, s3Key);
            logger.info("File {} copied to S3 with key: {}", fileName, s3Key);
        }

        // Copy to Snowflake if enabled
        if (params.isCopyToSnowflake()) {
            copyFileToSnowflake(file, s3Key);
            logger.info("File {} copied to Snowflake table: {}", fileName, params.getSnowflakeTableName());
        }
    }

    /**
     * Copy file to S3 (similar to your existing method)
     */
    private void copyFileToS3(File file, String s3Key) throws IOException {
        logger.info("Copying file to S3: {} -> {}", file.getName(), s3Key);

        try (FileInputStream fileInputStream = new FileInputStream(file)) {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(params.getBucketName())
                    .key(s3Key)
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(fileInputStream, file.length()));
        }
    }

    /**
     * Copy file to Snowflake (either to Internal Stage or Table)
     */
    private void copyFileToSnowflake(File file, String s3Key) throws IOException, SQLException {
        if (params.getSnowflakeStagePath() != null && !params.getSnowflakeStagePath().isEmpty()) {
            // Copy to Snowflake Internal Stage
            copyFileToSnowflakeStage(file, s3Key);
        } else if (params.getSnowflakeTableName() != null && !params.getSnowflakeTableName().isEmpty()) {
            // Copy to Snowflake Table
            copyFileToSnowflakeTable(file, s3Key);
        } else {
            throw new IllegalArgumentException("Either Snowflake stage path or table name must be specified");
        }
    }

    /**
     * Copy file to Snowflake Internal Stage
     */
    private void copyFileToSnowflakeStage(File file, String s3Key) throws IOException, SQLException {
        logger.info("Copying file to Snowflake Internal Stage: {} -> {}", file.getName(), params.getSnowflakeStagePath());

        // Read file content
        byte[] fileContent = Files.readAllBytes(file.toPath());

        // Upload to Snowflake Internal Stage
        snowflakeService.uploadToStage(
            params.getSnowflakeStagePath(),
            file.getName(),
            fileContent
        );
    }

    /**
     * Copy file to Snowflake Table
     */
    private void copyFileToSnowflakeTable(File file, String s3Key) throws IOException, SQLException {
        logger.info("Copying file to Snowflake Table: {} -> table {}", file.getName(), params.getSnowflakeTableName());

        // Create table if it doesn't exist
        snowflakeService.createFileTable(params.getSnowflakeTableName());

        // Read file content
        byte[] fileContent = Files.readAllBytes(file.toPath());

        // Insert into Snowflake
        snowflakeService.insertFile(
            params.getSnowflakeTableName(),
            file.getName(),
            file.length(),
            fileContent,
            s3Key
        );
    }

    /**
     * Get source files based on parameters
     */
    private List<File> getSourceFiles() {
        File sourceDir = new File(params.getSourcePath());
        
        if (!sourceDir.exists() || !sourceDir.isDirectory()) {
            throw new IllegalArgumentException("Source path does not exist or is not a directory: " + params.getSourcePath());
        }

        // If specific file name is provided, look for that file
        if (params.getFileName() != null && !params.getFileName().isEmpty()) {
            File specificFile = new File(sourceDir, params.getFileName());
            if (specificFile.exists() && specificFile.isFile()) {
                return Arrays.asList(specificFile);
            } else {
                logger.warn("Specific file not found: {}", specificFile.getAbsolutePath());
                return Arrays.asList();
            }
        }

        // If file patterns are provided, filter by patterns
        if (params.getFilePatterns() != null && !params.getFilePatterns().isEmpty()) {
            return filterFilesByPatterns(sourceDir, params.getFilePatterns());
        }

        // Otherwise, return all files in the directory
        File[] allFiles = sourceDir.listFiles(File::isFile);
        return allFiles != null ? Arrays.asList(allFiles) : Arrays.asList();
    }

    /**
     * Filter files by patterns (similar to your existing logic)
     */
    private List<File> filterFilesByPatterns(File directory, List<String> patterns) {
        File[] files = directory.listFiles((dir, name) -> {
            return patterns.stream().anyMatch(pattern -> {
                if (pattern.endsWith("*")) {
                    String prefix = pattern.substring(0, pattern.length() - 1);
                    return name.startsWith(prefix);
                } else {
                    return name.equals(pattern);
                }
            });
        });
        return files != null ? Arrays.asList(files) : Arrays.asList();
    }

    /**
     * Build S3 key for the file
     */
    private String buildS3Key(String fileName) {
        if (params.getTargetPath() != null && !params.getTargetPath().isEmpty()) {
            String targetPath = params.getTargetPath().startsWith("/") ? 
                params.getTargetPath().substring(1) : params.getTargetPath();
            return targetPath.endsWith("/") ? targetPath + fileName : targetPath + "/" + fileName;
        }
        return fileName;
    }

    /**
     * Validate input parameters
     */
    private void validateParameters() {
        if (params.getSourcePath() == null || params.getSourcePath().isEmpty()) {
            throw new IllegalArgumentException("Source path is required");
        }

        if (params.isCopyToS3()) {
            if (params.getBucketName() == null || params.getBucketName().isEmpty()) {
                throw new IllegalArgumentException("Bucket name is required for S3 copy");
            }
            if (params.getAccessKey() == null || params.getAccessKey().isEmpty()) {
                throw new IllegalArgumentException("Access key is required for S3 copy");
            }
            if (params.getSecretKey() == null || params.getSecretKey().isEmpty()) {
                throw new IllegalArgumentException("Secret key is required for S3 copy");
            }
        }

        if (params.isCopyToSnowflake()) {
            if ((params.getSnowflakeStagePath() == null || params.getSnowflakeStagePath().isEmpty()) &&
                (params.getSnowflakeTableName() == null || params.getSnowflakeTableName().isEmpty())) {
                throw new IllegalArgumentException("Either Snowflake stage path or table name is required for Snowflake copy");
            }
        }
    }

    /**
     * Test connections
     */
    public boolean testConnections() {
        logger.info("Testing connections...");

        if (params.isCopyToS3()) {
            try {
                // Test S3 connection by listing objects
                s3Client.listObjectsV2(builder -> builder.bucket(params.getBucketName()).maxKeys(1));
                logger.info("S3 connection test successful");
            } catch (Exception e) {
                logger.error("S3 connection test failed", e);
                return false;
            }
        }

        if (params.isCopyToSnowflake()) {
            if (!snowflakeService.testConnection()) {
                logger.error("Snowflake connection test failed");
                return false;
            }
        }

        return true;
    }

    /**
     * Close resources
     */
    public void close() {
        if (s3Client != null) {
            s3Client.close();
        }
    }
}
