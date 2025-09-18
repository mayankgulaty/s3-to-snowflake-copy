#!/bin/bash

# Mini File Copy App Runner Script
# This script makes it easy to run the mini file copy application

echo "=== Mini File Copy Application ==="
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

# Check if we have command line arguments
if [ $# -eq 0 ]; then
    echo "🚀 Starting interactive mode..."
    echo "   (Use --help to see command line options)"
    echo
    java -cp "$CLASSPATH" com.example.MiniFileCopyApp
else
    echo "🚀 Starting with command line arguments..."
    java -cp "$CLASSPATH" com.example.MiniFileCopyApp "$@"
fi
