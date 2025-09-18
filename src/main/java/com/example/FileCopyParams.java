package com.example;

import java.util.List;

/**
 * Parameters class for file copy operations
 * Similar to InputParams but simplified for the mini app
 */
public class FileCopyParams {
    private String sourcePath;
    private String fileName;
    private String targetPath;
    private String bucketName;
    private String accessKey;
    private String secretKey;
    private List<String> filePatterns;
    private boolean copyToS3;
    private boolean copyToSnowflake;
    private String snowflakeTableName;
    private String snowflakeSchema;

    public FileCopyParams() {
        this.copyToS3 = true;
        this.copyToSnowflake = true;
        this.snowflakeSchema = "LANDING";
    }

    // Getters and Setters
    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getTargetPath() {
        return targetPath;
    }

    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

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

    public List<String> getFilePatterns() {
        return filePatterns;
    }

    public void setFilePatterns(List<String> filePatterns) {
        this.filePatterns = filePatterns;
    }

    public boolean isCopyToS3() {
        return copyToS3;
    }

    public void setCopyToS3(boolean copyToS3) {
        this.copyToS3 = copyToS3;
    }

    public boolean isCopyToSnowflake() {
        return copyToSnowflake;
    }

    public void setCopyToSnowflake(boolean copyToSnowflake) {
        this.copyToSnowflake = copyToSnowflake;
    }

    public String getSnowflakeTableName() {
        return snowflakeTableName;
    }

    public void setSnowflakeTableName(String snowflakeTableName) {
        this.snowflakeTableName = snowflakeTableName;
    }

    public String getSnowflakeSchema() {
        return snowflakeSchema;
    }

    public void setSnowflakeSchema(String snowflakeSchema) {
        this.snowflakeSchema = snowflakeSchema;
    }

    @Override
    public String toString() {
        return "FileCopyParams{" +
                "sourcePath='" + sourcePath + '\'' +
                ", fileName='" + fileName + '\'' +
                ", targetPath='" + targetPath + '\'' +
                ", bucketName='" + bucketName + '\'' +
                ", copyToS3=" + copyToS3 +
                ", copyToSnowflake=" + copyToSnowflake +
                ", snowflakeTableName='" + snowflakeTableName + '\'' +
                ", snowflakeSchema='" + snowflakeSchema + '\'' +
                '}';
    }
}
