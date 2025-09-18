#!/bin/bash

# Network connectivity test for Snowflake
echo "=== Network Connectivity Test ==="
echo

# Test 1: Basic connectivity
echo "--- Test 1: Basic Connectivity ---"
echo "Testing if we can reach Snowflake host..."
ping -c 3 a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com

echo
echo "--- Test 2: Port Connectivity ---"
echo "Testing if port 443 is reachable..."
nc -z -v a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com 443

echo
echo "--- Test 3: DNS Resolution ---"
echo "Resolving Snowflake hostname..."
nslookup a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com

echo
echo "--- Test 4: SSL/TLS Test ---"
echo "Testing SSL connection..."
openssl s_client -connect a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com:443 -servername a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com < /dev/null 2>/dev/null | head -10

echo
echo "=== Network Test Complete ==="
