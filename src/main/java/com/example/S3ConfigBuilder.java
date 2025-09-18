package com.example;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder class to help create S3Config with FilePattern metadata
 */
public class S3ConfigBuilder {
    private String bucketName;
    private String accessKey;
    private String secretKey;
    private String endpoint;
    private boolean notificationEnable;
    private List<FilePattern> filePatterns;

    public S3ConfigBuilder() {
        this.filePatterns = new ArrayList<>();
    }

    public S3ConfigBuilder bucketName(String bucketName) {
        this.bucketName = bucketName;
        return this;
    }

    public S3ConfigBuilder accessKey(String accessKey) {
        this.accessKey = accessKey;
        return this;
    }

    public S3ConfigBuilder secretKey(String secretKey) {
        this.secretKey = secretKey;
        return this;
    }

    public S3ConfigBuilder endpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public S3ConfigBuilder notificationEnable(boolean notificationEnable) {
        this.notificationEnable = notificationEnable;
        return this;
    }

    public S3ConfigBuilder addFilePattern(String pattern, String description, boolean enabled) {
        this.filePatterns.add(new FilePattern(pattern, description, enabled));
        return this;
    }

    public S3ConfigBuilder addFilePattern(String pattern, String description, boolean enabled,
                                         String targetTable, String fileType, long maxFileSize, String processingMode) {
        this.filePatterns.add(new FilePattern(pattern, description, enabled, targetTable, fileType, maxFileSize, processingMode));
        return this;
    }

    public S3ConfigBuilder addFilePattern(FilePattern filePattern) {
        this.filePatterns.add(filePattern);
        return this;
    }

    public S3ConfigBuilder addCsvPattern(String basePath, String targetTable) {
        return addFilePattern(
            basePath + "/*.csv",
            "CSV files from " + basePath,
            true,
            targetTable,
            "csv",
            50 * 1024 * 1024, // 50MB max
            "stream"
        );
    }

    public S3ConfigBuilder addJsonPattern(String basePath, String targetTable) {
        return addFilePattern(
            basePath + "/*.json",
            "JSON files from " + basePath,
            true,
            targetTable,
            "json",
            100 * 1024 * 1024, // 100MB max
            "stream"
        );
    }

    public S3ConfigBuilder addTextPattern(String basePath, String targetTable) {
        return addFilePattern(
            basePath + "/*.txt",
            "Text files from " + basePath,
            true,
            targetTable,
            "txt",
            10 * 1024 * 1024, // 10MB max
            "memory"
        );
    }

    public S3ConfigBuilder addCatchAllPattern(String basePath, String targetTable) {
        return addFilePattern(
            basePath + "/*",
            "All other files from " + basePath,
            true,
            targetTable,
            "other",
            200 * 1024 * 1024, // 200MB max
            "stream"
        );
    }

    public S3Config build() {
        return new S3Config(bucketName, accessKey, secretKey, filePatterns, endpoint, notificationEnable);
    }

    /**
     * Create a default configuration for your TTS bucket
     */
    public static S3Config createDefaultTTSConfig(String accessKey, String secretKey) {
        return new S3ConfigBuilder()
                .bucketName("tts-banzai-inystrsvcs-uat")
                .accessKey(accessKey)
                .secretKey(secretKey)
                .endpoint("https://swdc-obj-wip4.nam.nsroot.net")
                .notificationEnable(true)
                .addCsvPattern("/167764/Investor Services/NAM/US/RAW_167764_TTS_INV_NAM_US_TEST", "TTS_CSV_FILES")
                .addJsonPattern("/167764/Investor Services/NAM/US/RAW_167764_TTS_INV_NAM_US_TEST", "TTS_JSON_FILES")
                .addTextPattern("/167764/Investor Services/NAM/US/RAW_167764_TTS_INV_NAM_US_TEST", "TTS_TEXT_FILES")
                .addCatchAllPattern("/167764/Investor Services/NAM/US/RAW_167764_TTS_INV_NAM_US_TEST", "S3_FILES")
                .build();
    }
}
