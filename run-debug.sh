#!/bin/bash

# Debug S3 to Snowflake Copy Runner Script
# This script helps debug Snowflake connection issues

echo "=== DEBUG: S3 to Snowflake Copy Application ==="
echo "This version has detailed logging and error handling"
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

echo "🚀 Starting DEBUG S3 to Snowflake copy..."
echo "   This will test Snowflake connection first"
echo "   Make sure to update the password in S3ToSnowflakeCopyDebug.java"
echo

java -cp "$CLASSPATH" com.example.S3ToSnowflakeCopyDebug
