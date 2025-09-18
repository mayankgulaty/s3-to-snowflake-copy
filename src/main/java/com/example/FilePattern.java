package com.example;

/**
 * Represents file pattern metadata for S3 operations
 */
public class FilePattern {
    private String pattern;
    private String description;
    private boolean enabled;
    private String targetTable;
    private String fileType;
    private long maxFileSize;
    private String processingMode;

    public FilePattern() {}

    public FilePattern(String pattern, String description, boolean enabled) {
        this.pattern = pattern;
        this.description = description;
        this.enabled = enabled;
    }

    public FilePattern(String pattern, String description, boolean enabled, 
                      String targetTable, String fileType, long maxFileSize, String processingMode) {
        this.pattern = pattern;
        this.description = description;
        this.enabled = enabled;
        this.targetTable = targetTable;
        this.fileType = fileType;
        this.maxFileSize = maxFileSize;
        this.processingMode = processingMode;
    }

    // Getters and Setters
    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    public void setMaxFileSize(long maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    public String getProcessingMode() {
        return processingMode;
    }

    public void setProcessingMode(String processingMode) {
        this.processingMode = processingMode;
    }

    @Override
    public String toString() {
        return "FilePattern{" +
                "pattern='" + pattern + '\'' +
                ", description='" + description + '\'' +
                ", enabled=" + enabled +
                ", targetTable='" + targetTable + '\'' +
                ", fileType='" + fileType + '\'' +
                ", maxFileSize=" + maxFileSize +
                ", processingMode='" + processingMode + '\'' +
                '}';
    }
}
