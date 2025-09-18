package com.example;

import java.util.List;

/**
 * Configuration class for IBM S3 Sonic bucket
 * Updated to match existing S3 configuration structure
 */
public class S3Config {
    private String bucketName;
    private String accessKey;
    private String secretKey;
    private List<FilePattern> fileMetadata;

    public S3Config() {}

    public S3Config(String bucketName, String accessKey, String secretKey, List<FilePattern> fileMetadata) {
        this.bucketName = bucketName;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.fileMetadata = fileMetadata;
    }


    // Getters and Setters
    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public List<FilePattern> getFileMetadata() {
        return fileMetadata;
    }

    public void setFileMetadata(List<FilePattern> fileMetadata) {
        this.fileMetadata = fileMetadata;
    }


    // Convenience methods for backward compatibility
    public String getBucket() {
        return bucketName;
    }

    public void setBucket(String bucket) {
        this.bucketName = bucket;
    }

    @Override
    public String toString() {
        return "S3Config{" +
                "bucketName='" + bucketName + '\'' +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='[HIDDEN]'" +
                ", fileMetadata=" + (fileMetadata != null ? fileMetadata.size() + " patterns" : "null") +
                '}';
    }
}
