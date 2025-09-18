package com.example;

/**
 * Configuration class for IBM S3 Sonic bucket
 */
public class S3Config {
    private String accessKey;
    private String secretKey;
    private String endpoint;
    private String bucket;
    private boolean notificationEnable;
    private String bucketPath;

    public S3Config() {}

    public S3Config(String accessKey, String secretKey, String endpoint, String bucket, 
                   boolean notificationEnable, String bucketPath) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpoint = endpoint;
        this.bucket = bucket;
        this.notificationEnable = notificationEnable;
        this.bucketPath = bucketPath;
    }

    // Getters and Setters
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

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public boolean isNotificationEnable() {
        return notificationEnable;
    }

    public void setNotificationEnable(boolean notificationEnable) {
        this.notificationEnable = notificationEnable;
    }

    public String getBucketPath() {
        return bucketPath;
    }

    public void setBucketPath(String bucketPath) {
        this.bucketPath = bucketPath;
    }

    @Override
    public String toString() {
        return "S3Config{" +
                "accessKey='" + accessKey + '\'' +
                ", secretKey='[HIDDEN]'" +
                ", endpoint='" + endpoint + '\'' +
                ", bucket='" + bucket + '\'' +
                ", notificationEnable=" + notificationEnable +
                ", bucketPath='" + bucketPath + '\'' +
                '}';
    }
}
