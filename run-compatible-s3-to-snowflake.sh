#!/bin/bash

# Compatible S3 to Snowflake Copy Runner Script
# This script uses your existing S3FileCopy pattern

echo "=== Compatible S3 to Snowflake Copy Application ==="
echo "Using your existing S3FileCopy pattern with AWS SDK v1"
echo

# Check if Java is available
if ! command -v java &> /dev/null; then
    echo "❌ Java is not installed or not in PATH"
    exit 1
fi

# Check if Maven is available for building
if command -v mvn &> /dev/null; then
    echo "🔨 Building the application..."
    mvn clean compile -q
    if [ $? -ne 0 ]; then
        echo "❌ Build failed"
        exit 1
    fi
    echo "✅ Build successful"
    echo
fi

# Set classpath
CLASSPATH="target/classes"
if [ -d "target/lib" ]; then
    CLASSPATH="$CLASSPATH:target/lib/*"
fi

echo "🚀 Starting Compatible S3 to Snowflake copy..."
echo "   Make sure to update the hardcoded values in S3ToSnowflakeCopyCompatible.java"
echo "   This version uses your existing S3FileCopy pattern:"
echo "   - AWS SDK v1 (not v2)"
echo "   - Base64 decoded credentials"
echo "   - Custom endpoint configuration"
echo "   - Your existing bucket path pattern"
echo

java -cp "$CLASSPATH" com.example.S3ToSnowflakeCopyCompatible
