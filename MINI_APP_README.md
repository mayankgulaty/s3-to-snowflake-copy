# Mini File Copy Application

This is a flexible mini application that extends your existing S3 to Snowflake copy functionality. It allows you to copy files from your local filesystem to S3 and/or Snowflake with full control over source paths, file names, and target locations.

## Features

- **Flexible Source Selection**: Copy specific files, files matching patterns, or all files from a directory
- **Dual Destination Support**: Copy to S3, Snowflake Internal Stage, or both simultaneously
- **Automatic Folder Creation**: S3 target paths and Snowflake stages are created automatically
- **Pattern Matching**: Support for file patterns like `*.csv`, `*.txt`, etc.
- **Snowflake Internal Stage Support**: Copy files directly to Snowflake file storage (not just tables)
- **Interactive Mode**: Easy-to-use command-line interface
- **Programmatic API**: Use the classes directly in your code

## Quick Start

### 1. Interactive Mode (Recommended for first-time users)

```bash
./run-mini-app.sh
```

This will start an interactive session where you can:
- Enter your source directory path
- Specify file names or patterns
- Choose S3 and/or Snowflake destinations
- Provide credentials and target paths

### 2. Command Line Mode

```bash
# Copy all CSV files to both S3 and Snowflake Internal Stage
./run-mini-app.sh --source-path /data --file-patterns *.csv --bucket my-bucket --access-key AKIA... --secret-key ... --snowflake-stage @my_stage/data/

# Copy specific file to S3 only
./run-mini-app.sh --source-path /data --file-name data.csv --bucket my-bucket --access-key AKIA... --secret-key ... --s3-only

# Copy files to Snowflake Internal Stage only
./run-mini-app.sh --source-path /data --file-patterns *.txt,*.log --snowflake-stage @my_stage/logs/ --snowflake-only
```

## Command Line Options

| Option | Description | Required |
|--------|-------------|----------|
| `--source-path <path>` | Source directory path | Yes |
| `--file-name <name>` | Specific file name to copy | No |
| `--file-patterns <patterns>` | File patterns (comma-separated) | No |
| `--bucket <name>` | S3 bucket name | If copying to S3 |
| `--access-key <key>` | S3 access key | If copying to S3 |
| `--secret-key <key>` | S3 secret key | If copying to S3 |
| `--target-path <path>` | S3 target path (e.g., `folder/subfolder/`) | No |
| `--snowflake-stage <path>` | Snowflake Internal Stage path (e.g., @my_stage/folder/) | If copying to Snowflake |
| `--snowflake-table <name>` | Snowflake table name (alternative to stage) | If copying to Snowflake |
| `--snowflake-schema <name>` | Snowflake schema (default: LANDING) | No |
| `--s3-only` | Copy only to S3 | No |
| `--snowflake-only` | Copy only to Snowflake | No |
| `--help` | Show help message | No |

## Examples

### Example 1: Copy Specific File
```bash
./run-mini-app.sh \
  --source-path /home/user/data \
  --file-name report.csv \
  --bucket my-data-bucket \
  --access-key AKIA1234567890 \
  --secret-key abc123def456 \
  --target-path reports/2024/ \
  --snowflake-table REPORTS \
  --snowflake-schema DATA
```

### Example 2: Copy All CSV Files to S3 Only
```bash
./run-mini-app.sh \
  --source-path /var/logs \
  --file-patterns *.csv \
  --bucket log-bucket \
  --access-key AKIA1234567890 \
  --secret-key abc123def456 \
  --target-path logs/csv/ \
  --s3-only
```

### Example 3: Copy Multiple File Types to Snowflake
```bash
./run-mini-app.sh \
  --source-path /data/exports \
  --file-patterns *.json,*.xml,*.txt \
  --snowflake-table EXPORT_FILES \
  --snowflake-only
```

## Programmatic Usage

You can also use the classes directly in your Java code:

```java
// Create parameters
FileCopyParams params = new FileCopyParams();
params.setSourcePath("/path/to/files");
params.setFileName("data.csv");
params.setBucketName("my-bucket");
params.setAccessKey("your-access-key");
params.setSecretKey("your-secret-key");
params.setTargetPath("uploads/");
params.setSnowflakeTableName("MY_FILES");

// Create Snowflake config
SnowflakeConfig snowflakeConfig = new SnowflakeConfig(
    "jdbc:snowflake://your-account.snowflakecomputing.com",
    "your-user",
    "your-password",
    "your-database",
    "LANDING",
    "your-role",
    "your-warehouse"
);

// Use the enhanced file copy
EnhancedFileCopy fileCopy = new EnhancedFileCopy(params, snowflakeConfig);
try {
    if (fileCopy.testConnections()) {
        fileCopy.copyFiles("my_run_id");
    }
} finally {
    fileCopy.close();
}
```

## Configuration

The mini app uses your existing Snowflake configuration from the main application. Make sure your `application.conf` has the correct Snowflake settings:

```hocon
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

## Key Features

### Automatic Folder Creation
- S3 target paths are created automatically
- If you specify `--target-path "folder/subfolder/"`, the folders will be created in S3
- Snowflake tables are created automatically if they don't exist

### File Pattern Matching
- Supports wildcards: `*.csv`, `*.txt`, `data_*.json`
- Multiple patterns: `*.csv,*.txt,*.log`
- Exact matches: `specific-file.csv`

### Error Handling
- Connection tests before starting
- Detailed logging of success/failure
- Continues processing other files if one fails

### Flexibility
- Copy to S3 only, Snowflake only, or both
- Specify exact files or use patterns
- Control target locations precisely

## Troubleshooting

### Common Issues

1. **"Source path does not exist"**
   - Make sure the source directory path is correct and accessible

2. **"S3 connection test failed"**
   - Check your S3 credentials and bucket name
   - Ensure the bucket exists and you have access

3. **"Snowflake connection test failed"**
   - Verify your Snowflake credentials in the configuration
   - Check network connectivity to Snowflake

4. **"No files found matching the criteria"**
   - Check your file patterns or file name
   - Ensure files exist in the source directory

### Getting Help

Run with `--help` to see all available options:
```bash
./run-mini-app.sh --help
```

## Integration with Existing Code

This mini app is designed to work alongside your existing `S3FileCopy` class. You can:

1. Use it as a standalone tool for quick file transfers
2. Integrate the `EnhancedFileCopy` class into your existing applications
3. Use the `FileCopyParams` class as a replacement for `InputParams` in your existing code

The mini app provides the same core functionality as your existing `copyFileToS3` method but with enhanced flexibility and better error handling.
