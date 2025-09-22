#!/bin/bash

# Test Snowflake JDBC Driver compatibility with Scala
echo "=== Snowflake JDBC Driver Test (Scala) ==="
echo

# Check if Scala is available
if ! command -v scala &> /dev/null; then
    echo "❌ Scala is not installed or not in PATH"
    exit 1
fi

# Set classpath
CLASSPATH="lib/*"

echo "🔨 Compiling Scala test..."
scalac -cp "$CLASSPATH" -d . src/main/scala/com/example/SnowflakeJdbcTest.scala

if [ $? -ne 0 ]; then
    echo "❌ Compilation failed"
    exit 1
fi

echo "✅ Compilation successful"
echo

echo "🚀 Testing Snowflake JDBC driver with Scala..."
echo "   This will test if the Snowflake JDBC driver works with Scala"
echo

if [ $# -eq 0 ]; then
    echo "❌ No password provided"
    echo "Usage: $0 <snowflake-password>"
    echo "Example: $0 your-password-here"
    exit 1
fi

scala -cp ".:$CLASSPATH" com.example.SnowflakeJdbcTest "$1"
