package com.example;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * S3Service compatible with your existing S3FileCopy pattern
 * Uses AWS SDK v1 and matches your existing configuration
 */
public class CompatibleS3Service {
    private static final Logger logger = LoggerFactory.getLogger(CompatibleS3Service.class);
    
    private final AmazonS3 s3Client;
    private final String bucketName;
    private final String bucketPath;

    public CompatibleS3Service(String accessKey, String secretKey, String endpoint, 
                              String bucketName, String bucketPath) {
        this.bucketName = bucketName;
        this.bucketPath = bucketPath;
        this.s3Client = createS3Client(accessKey, secretKey, endpoint);
    }

    /**
     * Create S3 client matching your existing pattern
     */
    private AmazonS3 createS3Client(String accessKey, String secretKey, String endpoint) {
        // Decode Base64 credentials (matching your pattern)
        String decodedAccessKey = new String(Base64.getDecoder().decode(accessKey));
        String decodedSecretKey = new String(Base64.getDecoder().decode(secretKey));
        
        // Create credentials
        BasicAWSCredentials credentials = new BasicAWSCredentials(decodedAccessKey, decodedSecretKey);
        
        // Build S3 client with custom endpoint (matching your pattern)
        return AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, ""))
                .build();
    }

    /**
     * Check access to bucket (matching your checkAccessToBucket method)
     */
    public boolean checkAccessToBucket() {
        try {
            s3Client.doesBucketExistV2(bucketName);
            logger.info("✅ Access to bucket {} verified", bucketName);
            return true;
        } catch (Exception e) {
            logger.error("❌ Cannot access bucket {}", bucketName, e);
            return false;
        }
    }

    /**
     * List all objects in the bucket
     */
    public List<S3ObjectSummary> listObjects() {
        logger.info("Listing objects in bucket: {}", bucketName);
        
        List<S3ObjectSummary> allObjects = new ArrayList<>();
        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(bucketName);
        
        ListObjectsV2Result result;
        do {
            result = s3Client.listObjectsV2(request);
            allObjects.addAll(result.getObjectSummaries());
            request.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());
        
        logger.info("Found {} objects in bucket", allObjects.size());
        return allObjects;
    }

    /**
     * List objects with prefix (matching your bucketPath pattern)
     */
    public List<S3ObjectSummary> listObjectsWithPrefix(String prefix) {
        logger.info("Listing objects in bucket: {} with prefix: {}", bucketName, prefix);
        
        List<S3ObjectSummary> allObjects = new ArrayList<>();
        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(prefix);
        
        ListObjectsV2Result result;
        do {
            result = s3Client.listObjectsV2(request);
            allObjects.addAll(result.getObjectSummaries());
            request.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());
        
        logger.info("Found {} objects with prefix {}", allObjects.size(), prefix);
        return allObjects;
    }

    /**
     * Download file from S3 (matching your pattern)
     */
    public byte[] downloadFile(String s3Key) throws IOException {
        logger.info("Downloading file: {}", s3Key);
        
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, s3Key);
        
        try (S3Object s3Object = s3Client.getObject(getObjectRequest);
             InputStream inputStream = s3Object.getObjectContent();
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            
            byte[] fileContent = outputStream.toByteArray();
            logger.info("Successfully downloaded file: {} ({} bytes)", s3Key, fileContent.length);
            return fileContent;
        }
    }

    /**
     * Upload file to S3 (matching your pattern)
     */
    public void uploadFile(String s3Key, byte[] fileContent) {
        logger.info("Uploading file to S3: {}", s3Key);
        
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(fileContent.length);
        
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, s3Key, 
                new java.io.ByteArrayInputStream(fileContent), metadata);
        
        s3Client.putObject(putObjectRequest);
        logger.info("Successfully uploaded file: {}", s3Key);
    }

    /**
     * Check if file exists in S3
     */
    public boolean fileExists(String s3Key) {
        try {
            s3Client.doesObjectExist(bucketName, s3Key);
            return true;
        } catch (Exception e) {
            logger.debug("File does not exist: {}", s3Key);
            return false;
        }
    }

    /**
     * Get file size
     */
    public long getFileSize(String s3Key) {
        try {
            ObjectMetadata metadata = s3Client.getObjectMetadata(bucketName, s3Key);
            return metadata.getContentLength();
        } catch (Exception e) {
            logger.error("Error getting file size for key: {}", s3Key, e);
            return -1;
        }
    }

    /**
     * Build S3 key with bucket path (matching your pattern)
     */
    public String buildS3Key(String fileName) {
        String tempBucketPath = bucketPath + "/" + fileName;
        return tempBucketPath.substring(1); // Remove leading slash
    }

    /**
     * Close the S3 client
     */
    public void close() {
        if (s3Client != null) {
            s3Client.shutdown();
        }
    }
}
