#!/bin/bash

# Java Debug Runner
# This runs the Java debug program step by step

echo "=== Java Debug: Snowflake Connection ==="
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

echo "🚀 Running Java debug program..."
echo "   This will test Snowflake connection step by step"
echo "   Make sure to update the password in JavaDebug.java first"
echo

java -cp "$CLASSPATH" com.example.JavaDebug
