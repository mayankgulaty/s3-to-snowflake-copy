#!/bin/bash

# Simple Snowflake Connection Test
# This helps isolate the connection issue

echo "=== Simple Snowflake Connection Test ==="
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

echo "🚀 Running simple Snowflake connection test..."
echo "   Make sure to update the password in SimpleSnowflakeTest.java"
echo

java -cp "$CLASSPATH" com.example.SimpleSnowflakeTest
