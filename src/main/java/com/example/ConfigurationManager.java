package com.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration manager for loading application settings
 */
public class ConfigurationManager {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationManager.class);
    private final Config config;

    public ConfigurationManager() {
        this.config = ConfigFactory.load();
    }

    public ConfigurationManager(String configFile) {
        this.config = ConfigFactory.load(configFile);
    }

    /**
     * Load S3 configuration from application.conf
     */
    public S3Config loadS3Config() {
        logger.info("Loading S3 configuration");
        
        return new S3Config(
            config.getString("s3.accessKey"),
            config.getString("s3.secretKey"),
            config.getString("s3.endpoint"),
            config.getString("s3.bucket"),
            config.getBoolean("s3.notificationEnable"),
            config.getString("s3.bucketPath")
        );
    }

    /**
     * Load Snowflake configuration from application.conf
     */
    public SnowflakeConfig loadSnowflakeConfig() {
        logger.info("Loading Snowflake configuration");
        
        return new SnowflakeConfig(
            config.getString("snowflake.url"),
            config.getString("snowflake.user"),
            config.getString("snowflake.password"),
            config.getString("snowflake.database"),
            config.getString("snowflake.schema"),
            config.getString("snowflake.role"),
            config.getString("snowflake.warehouse")
        );
    }

    /**
     * Get target table name
     */
    public String getTargetTableName() {
        return config.getString("app.targetTableName");
    }

    /**
     * Get large file threshold in MB
     */
    public int getLargeFileThresholdMB() {
        return config.getInt("app.largeFileThresholdMB");
    }

    /**
     * Get batch size for processing
     */
    public int getBatchSize() {
        return config.getInt("app.batchSize");
    }

    /**
     * Get maximum number of retries
     */
    public int getMaxRetries() {
        return config.getInt("app.maxRetries");
    }

    /**
     * Get retry delay in milliseconds
     */
    public long getRetryDelayMs() {
        return config.getLong("app.retryDelayMs");
    }

    /**
     * Validate configuration
     */
    public boolean validateConfig() {
        try {
            // Check required S3 settings
            if (config.getString("s3.accessKey").isEmpty()) {
                logger.error("S3 access key is not configured");
                return false;
            }
            if (config.getString("s3.secretKey").isEmpty()) {
                logger.error("S3 secret key is not configured");
                return false;
            }

            // Check required Snowflake settings
            if (config.getString("snowflake.password").isEmpty()) {
                logger.error("Snowflake password is not configured");
                return false;
            }

            logger.info("Configuration validation successful");
            return true;
        } catch (Exception e) {
            logger.error("Configuration validation failed", e);
            return false;
        }
    }
}
