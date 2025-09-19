#!/bin/bash

echo "=== Testing Different Snowflake URL Formats ==="
echo

# Extract account name from the private link URL
ACCOUNT="a_icg_dev"
REGION="us-east-2"

echo "Account: $ACCOUNT"
echo "Region: $REGION"
echo

# Test different URL formats
echo "--- Testing URL Format 1: Standard Public URL ---"
URL1="https://${ACCOUNT}.${REGION}.aws.snowflakecomputing.com"
echo "Testing: $URL1"
curl -I --connect-timeout 10 "$URL1" 2>/dev/null && echo "✅ URL1 accessible" || echo "❌ URL1 not accessible"

echo
echo "--- Testing URL Format 2: With Account Parameter ---"
URL2="https://${ACCOUNT}.snowflakecomputing.com"
echo "Testing: $URL2"
curl -I --connect-timeout 10 "$URL2" 2>/dev/null && echo "✅ URL2 accessible" || echo "❌ URL2 not accessible"

echo
echo "--- Testing URL Format 3: Original Private Link ---"
URL3="https://a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com"
echo "Testing: $URL3"
curl -I --connect-timeout 10 "$URL3" 2>/dev/null && echo "✅ URL3 accessible" || echo "❌ URL3 not accessible"

echo
echo "--- Testing URL Format 4: Alternative Private Link ---"
URL4="https://${ACCOUNT}.${REGION}.privatelink.snowflakecomputing.com"
echo "Testing: $URL4"
curl -I --connect-timeout 10 "$URL4" 2>/dev/null && echo "✅ URL4 accessible" || echo "❌ URL4 not accessible"

echo
echo "=== URL Test Complete ==="
