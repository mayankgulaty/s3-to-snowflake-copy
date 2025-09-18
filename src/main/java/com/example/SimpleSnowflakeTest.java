package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Simple Snowflake connection test to isolate the issue
 */
public class SimpleSnowflakeTest {
    
    public static void main(String[] args) {
        System.out.println("=== Simple Snowflake Connection Test ===");
        
        // Test 1: Basic connection
        testBasicConnection();
        
        // Test 2: Connection with timeouts
        testConnectionWithTimeouts();
        
        // Test 3: Connection with different URL format
        testConnectionWithDifferentURL();
    }
    
    /**
     * Test 1: Basic connection
     */
    public static void testBasicConnection() {
        System.out.println("\n--- Test 1: Basic Connection ---");
        
        try {
            String url = "jdbc:snowflake://a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com?db=D_ICG_DEV_177688_MASTER&schema=LANDING&warehouse=W_ICG_DEV_177688_DEFAULT_XS&role=R_ICG_DEV_177688_APPADMIN";
            String user = "F_ICG_DEV_177688_ALERTS";
            String password = "YOUR_PASSWORD_HERE"; // UPDATE THIS
            
            System.out.println("URL: " + url);
            System.out.println("User: " + user);
            System.out.println("Attempting connection...");
            
            Connection conn = DriverManager.getConnection(url, user, password);
            System.out.println("✅ Basic connection successful!");
            
            // Test query
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT CURRENT_TIMESTAMP()");
            if (rs.next()) {
                System.out.println("✅ Query successful: " + rs.getTimestamp(1));
            }
            
            conn.close();
            
        } catch (Exception e) {
            System.out.println("❌ Basic connection failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Test 2: Connection with timeouts
     */
    public static void testConnectionWithTimeouts() {
        System.out.println("\n--- Test 2: Connection with Timeouts ---");
        
        try {
            String url = "jdbc:snowflake://a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com?db=D_ICG_DEV_177688_MASTER&schema=LANDING&warehouse=W_ICG_DEV_177688_DEFAULT_XS&role=R_ICG_DEV_177688_APPADMIN&loginTimeout=10&networkTimeout=30000";
            String user = "F_ICG_DEV_177688_ALERTS";
            String password = "YOUR_PASSWORD_HERE"; // UPDATE THIS
            
            System.out.println("URL with timeouts: " + url);
            System.out.println("Attempting connection with 10s timeout...");
            
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            props.setProperty("loginTimeout", "10");
            props.setProperty("networkTimeout", "30000");
            
            Connection conn = DriverManager.getConnection(url, props);
            System.out.println("✅ Connection with timeouts successful!");
            conn.close();
            
        } catch (Exception e) {
            System.out.println("❌ Connection with timeouts failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Test 3: Different URL format
     */
    public static void testConnectionWithDifferentURL() {
        System.out.println("\n--- Test 3: Different URL Format ---");
        
        try {
            // Try without the jdbc:snowflake:// prefix
            String url = "jdbc:snowflake://a_icg_dev.gfts.us-east-2.aws.privatelink.snowflakecomputing.com";
            String user = "F_ICG_DEV_177688_ALERTS";
            String password = "YOUR_PASSWORD_HERE"; // UPDATE THIS
            
            System.out.println("URL: " + url);
            System.out.println("Attempting connection...");
            
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            props.setProperty("db", "D_ICG_DEV_177688_MASTER");
            props.setProperty("schema", "LANDING");
            props.setProperty("warehouse", "W_ICG_DEV_177688_DEFAULT_XS");
            props.setProperty("role", "R_ICG_DEV_177688_APPADMIN");
            props.setProperty("loginTimeout", "10");
            
            Connection conn = DriverManager.getConnection(url, props);
            System.out.println("✅ Different URL format successful!");
            conn.close();
            
        } catch (Exception e) {
            System.out.println("❌ Different URL format failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
