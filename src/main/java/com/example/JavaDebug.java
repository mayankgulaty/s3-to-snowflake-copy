package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Java debug program to step through Snowflake connection issues
 */
public class JavaDebug {
    
    public static void main(String[] args) {
        System.out.println("=== Java Debug: Snowflake Connection ===");
        System.out.println("This will help us debug step by step");
        System.out.println();
        
        // Step 1: Check if Snowflake driver is loaded
        checkDriver();
        
        // Step 2: Test basic connection
        testBasicConnection();
        
        // Step 3: Test with timeouts
        testWithTimeouts();
        
        // Step 4: Test different URL formats
        testDifferentURLs();
        
        System.out.println("\n=== Debug Complete ===");
    }
    
    /**
     * Step 1: Check if Snowflake driver is loaded
     */
    public static void checkDriver() {
        System.out.println("--- Step 1: Checking Snowflake Driver ---");
        
        try {
            // Try to load the Snowflake driver
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
            System.out.println("✅ Snowflake driver loaded successfully");
        } catch (ClassNotFoundException e) {
            System.out.println("❌ Snowflake driver NOT found: " + e.getMessage());
            System.out.println("   Make sure snowflake-jdbc dependency is in classpath");
            return;
        }
        
        // Check driver version
        try {
            java.sql.Driver driver = DriverManager.getDriver("jdbc:snowflake://test");
            System.out.println("✅ Driver version: " + driver.getMajorVersion() + "." + driver.getMinorVersion());
        } catch (Exception e) {
            System.out.println("⚠️  Could not get driver version: " + e.getMessage());
        }
    }
    
    /**
     * Step 2: Test basic connection
     */
    public static void testBasicConnection() {
        System.out.println("\n--- Step 2: Basic Connection Test ---");
        
        String url = "jdbc:snowflake://a_icg_dev.us-east-2.aws.snowflakecomputing.com?db=D_ICG_DEV_177688_MASTER&schema=LANDING&warehouse=W_ICG_DEV_177688_DEFAULT_XS&role=R_ICG_DEV_177688_APPADMIN";
        String user = "F_ICG_DEV_177688_ALERTS";
        String password = "YOUR_PASSWORD_HERE"; // UPDATE THIS
        
        System.out.println("URL: " + url);
        System.out.println("User: " + user);
        System.out.println("Password: " + (password.equals("YOUR_PASSWORD_HERE") ? "NOT SET" : "SET"));
        
        if (password.equals("YOUR_PASSWORD_HERE")) {
            System.out.println("❌ Please update the password in JavaDebug.java first!");
            return;
        }
        
        System.out.println("Attempting connection...");
        
        try {
            Connection conn = DriverManager.getConnection(url, user, password);
            System.out.println("✅ Basic connection successful!");
            
            // Test a simple query
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT CURRENT_TIMESTAMP()");
            if (rs.next()) {
                System.out.println("✅ Query successful: " + rs.getTimestamp(1));
            }
            
            conn.close();
            System.out.println("✅ Connection closed successfully");
            
        } catch (Exception e) {
            System.out.println("❌ Basic connection failed: " + e.getMessage());
            System.out.println("Error type: " + e.getClass().getSimpleName());
            e.printStackTrace();
        }
    }
    
    /**
     * Step 3: Test with timeouts
     */
    public static void testWithTimeouts() {
        System.out.println("\n--- Step 3: Connection with Timeouts ---");
        
        String url = "jdbc:snowflake://a_icg_dev.us-east-2.aws.snowflakecomputing.com?db=D_ICG_DEV_177688_MASTER&schema=LANDING&warehouse=W_ICG_DEV_177688_DEFAULT_XS&role=R_ICG_DEV_177688_APPADMIN&loginTimeout=5&networkTimeout=10000";
        String user = "F_ICG_DEV_177688_ALERTS";
        String password = "YOUR_PASSWORD_HERE"; // UPDATE THIS
        
        if (password.equals("YOUR_PASSWORD_HERE")) {
            System.out.println("❌ Please update the password in JavaDebug.java first!");
            return;
        }
        
        System.out.println("Testing with 5 second timeout...");
        
        try {
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            props.setProperty("loginTimeout", "5");
            props.setProperty("networkTimeout", "10000");
            
            Connection conn = DriverManager.getConnection(url, props);
            System.out.println("✅ Connection with timeouts successful!");
            conn.close();
            
        } catch (Exception e) {
            System.out.println("❌ Connection with timeouts failed: " + e.getMessage());
            System.out.println("Error type: " + e.getClass().getSimpleName());
            e.printStackTrace();
        }
    }
    
    /**
     * Step 4: Test different URL formats
     */
    public static void testDifferentURLs() {
        System.out.println("\n--- Step 4: Different URL Formats ---");
        
        String user = "F_ICG_DEV_177688_ALERTS";
        String password = "YOUR_PASSWORD_HERE"; // UPDATE THIS
        
        if (password.equals("YOUR_PASSWORD_HERE")) {
            System.out.println("❌ Please update the password in JavaDebug.java first!");
            return;
        }
        
        // Test different URL formats
        String[] urls = {
            "jdbc:snowflake://a_icg_dev.us-east-2.aws.snowflakecomputing.com",
            "jdbc:snowflake://a_icg_dev.snowflakecomputing.com",
            "jdbc:snowflake://a_icg_dev.us-east-2.aws.snowflakecomputing.com?account=a_icg_dev"
        };
        
        for (int i = 0; i < urls.length; i++) {
            System.out.println("\nTesting URL format " + (i + 1) + ": " + urls[i]);
            
            try {
                Properties props = new Properties();
                props.setProperty("user", user);
                props.setProperty("password", password);
                props.setProperty("db", "D_ICG_DEV_177688_MASTER");
                props.setProperty("schema", "LANDING");
                props.setProperty("warehouse", "W_ICG_DEV_177688_DEFAULT_XS");
                props.setProperty("role", "R_ICG_DEV_177688_APPADMIN");
                props.setProperty("loginTimeout", "5");
                
                Connection conn = DriverManager.getConnection(urls[i], props);
                System.out.println("✅ URL format " + (i + 1) + " successful!");
                conn.close();
                break; // Stop on first success
                
            } catch (Exception e) {
                System.out.println("❌ URL format " + (i + 1) + " failed: " + e.getMessage());
            }
        }
    }
}
