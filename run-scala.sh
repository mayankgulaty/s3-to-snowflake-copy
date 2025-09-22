#!/bin/bash

# Scala Runner for S3 to Snowflake Copy
# This runs the Scala version of the application

echo "=== Scala S3 to Snowflake Copy ==="
echo

# Check if Scala is available
if ! command -v scala &> /dev/null; then
    echo "❌ Scala is not installed or not in PATH"
    echo "Please install Scala: https://www.scala-lang.org/download/"
    exit 1
fi

# Check if scalac is available
if ! command -v scalac &> /dev/null; then
    echo "❌ Scala compiler (scalac) is not installed or not in PATH"
    exit 1
fi

# Set classpath
CLASSPATH="lib/*"

echo "🔨 Compiling Scala sources..."
scalac -cp "$CLASSPATH" -d . src/main/scala/com/example/*.scala

if [ $? -ne 0 ]; then
    echo "❌ Compilation failed"
    exit 1
fi

echo "✅ Compilation successful"
echo

echo "🚀 Running Scala S3 to Snowflake Copy..."
echo "   Make sure to update the configuration in S3ToSnowflakeCopyCompatible.scala first"
echo

scala -cp ".:$CLASSPATH" com.example.S3ToSnowflakeCopyCompatible
