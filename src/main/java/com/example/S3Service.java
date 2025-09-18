package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Service class for S3 operations
 */
public class S3Service {
    private static final Logger logger = LoggerFactory.getLogger(S3Service.class);
    
    private final S3Client s3Client;
    private final S3Config s3Config;

    public S3Service(S3Config s3Config) {
        this.s3Config = s3Config;
        this.s3Client = createS3Client();
    }

    private S3Client createS3Client() {
        // Create credentials for IBM S3 Sonic
        AwsBasicCredentials credentials = AwsBasicCredentials.create(
            s3Config.getAccessKey(),
            s3Config.getSecretKey()
        );

        // Create S3 client with custom endpoint for IBM S3 Sonic
        return S3Client.builder()
                .region(Region.US_EAST_1) // Default region, can be adjusted
                .endpointOverride(java.net.URI.create(s3Config.getEndpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .forcePathStyle(true) // Required for IBM S3 Sonic
                .build();
    }

    /**
     * List all objects in the S3 bucket based on file patterns
     * @return List of S3Object
     */
    public List<S3Object> listObjects() {
        logger.info("Listing objects in bucket: {}", s3Config.getBucketName());
        
        List<S3Object> allObjects = new ArrayList<>();
        
        // If no file patterns specified, list all objects
        if (s3Config.getFileMetadata() == null || s3Config.getFileMetadata().isEmpty()) {
            allObjects = listAllObjects();
        } else {
            // Process each file pattern
            for (FilePattern pattern : s3Config.getFileMetadata()) {
                if (pattern.isEnabled()) {
                    logger.info("Processing pattern: {} - {}", pattern.getPattern(), pattern.getDescription());
                    List<S3Object> patternObjects = listObjectsByPattern(pattern);
                    allObjects.addAll(patternObjects);
                }
            }
        }
        
        logger.info("Found {} total objects in bucket", allObjects.size());
        return allObjects;
    }

    /**
     * List all objects in the bucket (no pattern filtering)
     * @return List of S3Object
     */
    private List<S3Object> listAllObjects() {
        List<S3Object> objects = new ArrayList<>();
        String continuationToken = null;
        
        do {
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(s3Config.getBucketName());
            
            if (continuationToken != null) {
                requestBuilder.continuationToken(continuationToken);
            }
            
            ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());
            objects.addAll(response.contents());
            continuationToken = response.nextContinuationToken();
            
        } while (continuationToken != null);
        
        return objects;
    }

    /**
     * List objects matching a specific file pattern
     * @param pattern The file pattern to match
     * @return List of S3Object matching the pattern
     */
    private List<S3Object> listObjectsByPattern(FilePattern pattern) {
        List<S3Object> objects = new ArrayList<>();
        String continuationToken = null;
        
        do {
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(s3Config.getBucketName())
                    .prefix(pattern.getPattern());
            
            if (continuationToken != null) {
                requestBuilder.continuationToken(continuationToken);
            }
            
            ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());
            
            // Filter objects by pattern if it contains wildcards
            for (S3Object object : response.contents()) {
                if (matchesPattern(object.key(), pattern.getPattern())) {
                    objects.add(object);
                }
            }
            
            continuationToken = response.nextContinuationToken();
            
        } while (continuationToken != null);
        
        logger.info("Found {} objects matching pattern: {}", objects.size(), pattern.getPattern());
        return objects;
    }

    /**
     * Check if a file key matches the given pattern
     * @param key The S3 object key
     * @param pattern The pattern to match (supports * wildcards)
     * @return true if matches, false otherwise
     */
    private boolean matchesPattern(String key, String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return true;
        }
        
        // Convert pattern to regex
        String regexPattern = pattern
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
        
        return key.matches(regexPattern);
    }

    /**
     * Download a file from S3 bucket
     * @param key The S3 object key
     * @return Byte array containing the file content
     * @throws IOException if there's an error reading the file
     */
    public byte[] downloadFile(String key) throws IOException {
        logger.info("Downloading file: {}", key);
        
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(s3Config.getBucket())
                .key(key)
                .build();

        try (ResponseInputStream<GetObjectResponse> response = s3Client.getObject(getObjectRequest);
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = response.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            
            byte[] fileContent = outputStream.toByteArray();
            logger.info("Successfully downloaded file: {} ({} bytes)", key, fileContent.length);
            return fileContent;
        }
    }

    /**
     * Download a file from S3 bucket and return as InputStream
     * @param key The S3 object key
     * @return ResponseInputStream containing the file content
     */
    public ResponseInputStream<GetObjectResponse> downloadFileAsStream(String key) {
        logger.info("Downloading file as stream: {}", key);
        
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(s3Config.getBucket())
                .key(key)
                .build();

        return s3Client.getObject(getObjectRequest);
    }

    /**
     * Check if a file exists in the S3 bucket
     * @param key The S3 object key
     * @return true if file exists, false otherwise
     */
    public boolean fileExists(String key) {
        try {
            s3Client.headObject(builder -> builder
                    .bucket(s3Config.getBucket())
                    .key(key)
            );
            return true;
        } catch (Exception e) {
            logger.debug("File does not exist: {}", key);
            return false;
        }
    }

    /**
     * Get file size
     * @param key The S3 object key
     * @return File size in bytes, or -1 if file doesn't exist
     */
    public long getFileSize(String key) {
        try {
            var response = s3Client.headObject(builder -> builder
                    .bucket(s3Config.getBucket())
                    .key(key)
            );
            return response.contentLength();
        } catch (Exception e) {
            logger.error("Error getting file size for key: {}", key, e);
            return -1;
        }
    }

    /**
     * Get the FilePattern that matches the given S3 object key
     * @param s3Key The S3 object key
     * @return Matching FilePattern or null if no match
     */
    public FilePattern getMatchingPattern(String s3Key) {
        if (s3Config.getFileMetadata() == null || s3Config.getFileMetadata().isEmpty()) {
            return null;
        }
        
        for (FilePattern pattern : s3Config.getFileMetadata()) {
            if (pattern.isEnabled() && matchesPattern(s3Key, pattern.getPattern())) {
                return pattern;
            }
        }
        
        return null;
    }

    /**
     * Get the target table name for a given S3 object
     * @param s3Key The S3 object key
     * @return Target table name or default if no pattern match
     */
    public String getTargetTableName(String s3Key) {
        FilePattern pattern = getMatchingPattern(s3Key);
        if (pattern != null && pattern.getTargetTable() != null && !pattern.getTargetTable().isEmpty()) {
            return pattern.getTargetTable();
        }
        return "S3_FILES"; // Default table name
    }

    /**
     * Check if file should be processed based on pattern rules
     * @param s3Key The S3 object key
     * @param fileSize The file size
     * @return true if file should be processed
     */
    public boolean shouldProcessFile(String s3Key, long fileSize) {
        FilePattern pattern = getMatchingPattern(s3Key);
        if (pattern == null) {
            return true; // Process if no pattern restrictions
        }
        
        // Check file size limit if specified
        if (pattern.getMaxFileSize() > 0 && fileSize > pattern.getMaxFileSize()) {
            logger.warn("File {} exceeds size limit for pattern {}: {} > {}", 
                       s3Key, pattern.getPattern(), fileSize, pattern.getMaxFileSize());
            return false;
        }
        
        return true;
    }

    /**
     * Close the S3 client
     */
    public void close() {
        if (s3Client != null) {
            s3Client.close();
        }
    }
}
