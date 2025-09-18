# S3 to Snowflake Copy Application

This Java application copies files from IBM S3 Sonic bucket to Snowflake database.

## Features

- Downloads files from IBM S3 Sonic bucket
- Uploads files to Snowflake as binary data
- Handles large files efficiently using streaming
- Tracks already uploaded files to avoid duplicates
- Comprehensive logging and error handling
- Configuration management

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
- Valid IBM S3 Sonic credentials
- Valid Snowflake credentials

## Configuration

### 1. Update application.conf

Edit `src/main/resources/application.conf` and provide your credentials:

```hocon
s3 {
  accessKey = "your-s3-access-key"
  secretKey = "your-s3-secret-key"
  endpoint = "https://swdc-obj-wip4.nam.nsroot.net"
  bucket = "tts-banzai-inystrsvcs-uat"
  notificationEnable = true
  bucketPath = "/167764/Investor Services/NAM/US/RAW_167764_TTS_INV_NAM_US_TEST"
}

snowflake {
  url = "jdbc:snowflake://a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com"
  user = "F_ICG_DEV_177688_ALERTS"
  password = "your-snowflake-password"
  database = "D_ICG_DEV_177688_MASTER"
  schema = "LANDING"
  role = "R_ICG_DEV_177688_APPADMIN"
  warehouse = "W_ICG_DEV_177688_DEFAULT_XS"
}
```

### 2. Environment Variables (Alternative)

You can also set credentials as environment variables:

```bash
export S3_ACCESS_KEY="your-s3-access-key"
export S3_SECRET_KEY="your-s3-secret-key"
export SNOWFLAKE_PASSWORD="your-snowflake-password"
```

## Building and Running

### 1. Build the application

```bash
mvn clean package
```

### 2. Run the application

```bash
java -jar target/s3-to-snowflake-copy-1.0.0.jar
```

### 3. Run with Maven

```bash
mvn exec:java -Dexec.mainClass="com.example.S3ToSnowflakeCopy"
```

## Database Schema

The application creates a table with the following structure:

```sql
CREATE TABLE S3_FILES (
    file_name VARCHAR(255),
    file_size BIGINT,
    file_content BINARY,
    upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    s3_key VARCHAR(500)
);
```

## Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `app.targetTableName` | Target table name in Snowflake | `S3_FILES` |
| `app.largeFileThresholdMB` | Threshold for large file processing | `100` |
| `app.batchSize` | Batch size for processing | `100` |
| `app.maxRetries` | Maximum retry attempts | `3` |
| `app.retryDelayMs` | Delay between retries (ms) | `5000` |

## Logging

Logs are written to:
- Console output
- `logs/s3-to-snowflake.log` file

Log levels can be configured in `src/main/resources/logback.xml`.

## Error Handling

The application includes comprehensive error handling:
- Connection validation before processing
- Retry logic for failed operations
- Detailed error logging
- Graceful handling of large files

## Performance Considerations

- Files larger than 100MB are processed using streaming
- Duplicate files are automatically skipped
- Batch processing for better performance
- Connection pooling for database operations

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify credentials in `application.conf`
   - Check network connectivity to S3 and Snowflake
   - Ensure proper firewall rules

2. **Permission Errors**
   - Verify S3 bucket permissions
   - Check Snowflake role permissions
   - Ensure warehouse is running

3. **Memory Issues**
   - Large files are automatically streamed
   - Adjust JVM heap size if needed: `-Xmx2g`

### Logs

Check the log files for detailed error information:
```bash
tail -f logs/s3-to-snowflake.log
```

## Security Notes

- Never commit credentials to version control
- Use environment variables for sensitive data
- Rotate credentials regularly
- Monitor access logs

## License

This project is provided as-is for educational and development purposes.
