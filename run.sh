#!/bin/bash

# S3 to Snowflake Copy Application Runner
# This script helps you run the application with proper configuration

echo "S3 to Snowflake Copy Application"
echo "================================"

# Check if Java is installed
if ! command -v java &> /dev/null; then
    echo "Error: Java is not installed or not in PATH"
    exit 1
fi

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo "Error: Maven is not installed or not in PATH"
    exit 1
fi

# Build the application
echo "Building the application..."
mvn clean package -q

if [ $? -ne 0 ]; then
    echo "Error: Build failed"
    exit 1
fi

echo "Build successful!"

# Check if configuration file exists
if [ ! -f "src/main/resources/application.conf" ]; then
    echo "Error: Configuration file not found at src/main/resources/application.conf"
    echo "Please create the configuration file with your credentials"
    exit 1
fi

# Run the application
echo "Starting the application..."
echo "Make sure you have configured your credentials in src/main/resources/application.conf"
echo ""

java -jar target/s3-to-snowflake-copy-1.0.0.jar

echo ""
echo "Application completed. Check logs/s3-to-snowflake.log for detailed information."
