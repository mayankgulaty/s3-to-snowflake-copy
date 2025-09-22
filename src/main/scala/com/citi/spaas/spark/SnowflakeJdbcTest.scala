package com.citi.spaas.spark

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties
import scala.util.{Failure, Success, Try}

/**
 * Test Snowflake JDBC driver compatibility with Scala
 */
object SnowflakeJdbcTest {
  
  def main(args: Array[String]): Unit = {
    println("=== Snowflake JDBC Driver Test (Scala) ===")
    
    // Test 1: Load driver
    testDriverLoading()
    
    // Test 2: Test connection (if password provided)
    if (args.length > 0) {
      testConnection(args(0))
    } else {
      println("❌ No password provided. Usage: scala SnowflakeJdbcTest <password>")
    }
  }
  
  /**
   * Test if Snowflake JDBC driver can be loaded
   */
  def testDriverLoading(): Unit = {
    println("\n--- Test 1: Driver Loading ---")
    
    Try {
      Class.forName("net.snowflake.client.jdbc.SnowflakeDriver")
      println("✅ Snowflake JDBC driver loaded successfully")
    } match {
      case Success(_) => 
        println("✅ Driver loading test passed")
      case Failure(e) => 
        println(s"❌ Driver loading failed: ${e.getMessage}")
    }
  }
  
  /**
   * Test Snowflake connection
   */
  def testConnection(password: String): Unit = {
    println("\n--- Test 2: Connection Test ---")
    
    val url = "jdbc:snowflake://a_icg_dev.us-east-2.aws.snowflakecomputing.com?db=D_ICG_DEV_177688_MASTER&schema=LANDING&warehouse=W_ICG_DEV_177688_DEFAULT_XS&role=R_ICG_DEV_177688_APPADMIN"
    val user = "F_ICG_DEV_177688_ALERTS"
    
    println(s"URL: $url")
    println(s"User: $user")
    println(s"Password: ${if (password.nonEmpty) "SET" else "NOT SET"}")
    
    val connectionResult = for {
      conn <- createConnection(url, user, password)
      result <- testQuery(conn)
      _ <- closeConnection(conn)
    } yield result
    
    connectionResult match {
      case Success(true) => 
        println("✅ Connection test successful!")
      case Success(false) => 
        println("❌ Connection test failed - no results returned")
      case Failure(e) => 
        println(s"❌ Connection test failed: ${e.getMessage}")
        e.printStackTrace()
    }
  }
  
  /**
   * Create Snowflake connection
   */
  def createConnection(url: String, user: String, password: String): Try[Connection] = {
    Try {
      val props = new Properties()
      props.setProperty("user", user)
      props.setProperty("password", password)
      props.setProperty("loginTimeout", "30")
      props.setProperty("networkTimeout", "30000")
      
      // Proxy settings (uncomment and configure as needed)
      // props.setProperty("useProxy", "true")
      // props.setProperty("proxyHost", "your-proxy-host")
      // props.setProperty("proxyPort", "8080")
      
      DriverManager.getConnection(url, props)
    } match {
      case Success(conn) => 
        println("✅ Connection created successfully")
        Success(conn)
      case Failure(e) => 
        println(s"❌ Connection creation failed: ${e.getMessage}")
        Failure(e)
    }
  }
  
  /**
   * Test a simple query
   */
  def testQuery(conn: Connection): Try[Boolean] = {
    Try {
      val statement = conn.createStatement()
      try {
        val resultSet = statement.executeQuery("SELECT CURRENT_TIMESTAMP()")
        try {
          if (resultSet.next()) {
            val timestamp = resultSet.getTimestamp(1)
            println(s"✅ Query successful: $timestamp")
            true
          } else {
            false
          }
        } finally {
          resultSet.close()
        }
      } finally {
        statement.close()
      }
    } match {
      case Success(result) => Success(result)
      case Failure(e) => 
        println(s"❌ Query execution failed: ${e.getMessage}")
        Failure(e)
    }
  }
  
  /**
   * Close connection
   */
  def closeConnection(conn: Connection): Try[Unit] = {
    Try {
      conn.close()
      println("✅ Connection closed successfully")
    } match {
      case Success(_) => Success(())
      case Failure(e) => 
        println(s"❌ Failed to close connection: ${e.getMessage}")
        Failure(e)
    }
  }
}
