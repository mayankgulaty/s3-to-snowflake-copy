package com.example

import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, FileInputStream}
import java.nio.file.Files
import java.sql.{Connection, DriverManager, SQLException, Statement}
import java.util.Properties
import scala.util.{Failure, Success, Try}

/**
 * Scala version of SnowflakeService with functional error handling
 */
class SnowflakeService(snowflakeConfig: SnowflakeConfig) {
  
  private val logger: Logger = LoggerFactory.getLogger(classOf[SnowflakeService])
  private var connection: Option[Connection] = None

  /**
   * Establish connection to Snowflake
   */
  def connect(): Try[Connection] = {
    logger.info("Connecting to Snowflake...")
    
    // Validate configuration first
    snowflakeConfig.validate match {
      case Left(error) => 
        logger.error("Invalid Snowflake configuration: {}", error)
        Failure(new SQLException(s"Invalid configuration: $error"))
      case Right(validConfig) =>
        Try {
          val jdbcUrl = validConfig.getJdbcUrl
          logger.info("Using JDBC URL: {}", jdbcUrl)
          logger.info("User: {}", validConfig.user)
          logger.info("Database: {}", validConfig.database)
          logger.info("Schema: {}", validConfig.schema)
          logger.info("Warehouse: {}", validConfig.warehouse)
          logger.info("Role: {}", validConfig.role)

          // Set connection properties with timeouts
          val props = new Properties()
          props.setProperty("user", validConfig.user)
          props.setProperty("password", validConfig.password)
          props.setProperty("loginTimeout", "30")
          props.setProperty("networkTimeout", "30000")
          props.setProperty("queryTimeout", "300")
          
          val conn = DriverManager.getConnection(jdbcUrl, props)
          connection = Some(conn)
          logger.info("Successfully connected to Snowflake")
          conn
        } match {
          case Success(conn) => Success(conn)
          case Failure(e) => 
            logger.error("Failed to connect to Snowflake", e)
            Failure(e)
        }
    }
  }

  /**
   * Upload file to Snowflake Internal Stage (for local files only)
   */
  def uploadToStage(stagePath: String, fileName: String, fileContent: Array[Byte]): Try[Unit] = {
    logger.info("Uploading file {} to Snowflake Internal Stage: {}", fileName, stagePath)
    
    for {
      _ <- ensureConnection()
      _ <- createStageIfNotExists(stagePath)
      tempFile <- createTempFile(fileName, fileContent)
      _ <- uploadFileToStage(stagePath, tempFile)
      _ <- cleanupTempFile(tempFile)
    } yield {
      logger.info("✅ Successfully uploaded file {} to stage {}", fileName, stagePath)
    }
  }

  /**
   * Upload file directly to Snowflake Internal Stage using file path
   */
  def uploadFileToStage(stagePath: String, filePath: String): Try[Unit] = {
    logger.info("Uploading file {} to Snowflake Internal Stage: {}", filePath, stagePath)
    
    for {
      _ <- ensureConnection()
      _ <- createStageIfNotExists(stagePath)
      _ <- uploadFileToStage(stagePath, new File(filePath))
    } yield {
      logger.info("✅ Successfully uploaded file {} to stage {}", filePath, stagePath)
    }
  }

  /**
   * Copy file from S3 to Snowflake Internal Stage using existing CompatibleS3Service
   */
  def copyFileFromS3ToStage(stagePath: String, s3Service: CompatibleS3Service, s3Key: String): Try[Unit] = {
    logger.info("Copying FILE from S3 to Snowflake Internal Stage: {} -> {}", s3Key, stagePath)
    
    for {
      _ <- ensureConnection()
      _ <- createStageIfNotExists(stagePath)
      fileContent <- s3Service.downloadFile(s3Key)
      tempFile <- createTempFileFromS3(s3Key, fileContent)
      _ <- uploadFileToStage(stagePath, tempFile)
      _ <- cleanupTempFile(tempFile)
    } yield {
      logger.info("✅ Successfully copied file from S3 to Snowflake stage {}", stagePath)
    }
  }

  /**
   * Test the Snowflake connection
   */
  def testConnection(): Try[Boolean] = {
    for {
      conn <- ensureConnection()
      result <- testConnectionWithStatement(conn)
    } yield result
  }

  /**
   * Close the Snowflake connection
   */
  def close(): Unit = {
    connection.foreach { conn =>
      Try {
        conn.close()
        logger.info("Snowflake connection closed")
      }.recover {
        case e: SQLException => 
          logger.error("Error closing Snowflake connection", e)
      }
    }
    connection = None
  }

  // Private helper methods

  private def ensureConnection(): Try[Connection] = {
    connection match {
      case Some(conn) if !conn.isClosed => Success(conn)
      case _ => connect()
    }
  }

  private def createStageIfNotExists(stagePath: String): Try[Unit] = {
    val stageName = stagePath.split("/")(0)
    val createStageSQL = s"CREATE STAGE IF NOT EXISTS $stageName"
    
    logger.info("Creating stage: {}", createStageSQL)
    
    for {
      conn <- ensureConnection()
      _ <- executeStatement(conn, createStageSQL)
    } yield {
      logger.info("Stage {} created/verified successfully", stageName)
    }
  }

  private def createTempFile(fileName: String, fileContent: Array[Byte]): Try[File] = {
    Try {
      val tempDir = File.createTempFile("snowflake_upload_", "")
      tempDir.delete() // Delete the file
      tempDir.mkdirs() // Create as directory
      val tempFile = new File(tempDir, fileName)
      
      Files.write(tempFile.toPath, fileContent)
      
      // Verify file integrity
      val writtenContent = Files.readAllBytes(tempFile.toPath)
      if (writtenContent.length != fileContent.length) {
        throw new SQLException(s"File corruption detected! Original: ${fileContent.length} bytes, Written: ${writtenContent.length} bytes")
      }
      
      logger.info("Downloaded file: {} ({} bytes) to temp file: {} - Integrity verified", 
                 fileName, fileContent.length, tempFile.getAbsolutePath)
      tempFile
    }
  }

  private def createTempFileFromS3(s3Key: String, fileContent: Array[Byte]): Try[File] = {
    val fileName = s3Key.substring(s3Key.lastIndexOf("/") + 1)
    createTempFile(fileName, fileContent)
  }

  private def uploadFileToStage(stagePath: String, tempFile: File): Try[Unit] = {
    val putCommand = s"PUT 'file://${tempFile.getAbsolutePath}' $stagePath AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    logger.info("Executing PUT command: {}", putCommand)
    
    for {
      conn <- ensureConnection()
      _ <- executeStatement(conn, putCommand)
    } yield {
      logger.info("PUT command executed successfully")
    }
  }

  private def cleanupTempFile(tempFile: File): Try[Unit] = {
    Try {
      if (tempFile.exists()) {
        if (tempFile.delete()) {
          logger.debug("Cleaned up temporary file: {}", tempFile.getAbsolutePath)
        } else {
          logger.warn("Failed to delete temporary file: {}", tempFile.getAbsolutePath)
        }
        
        // Clean up temp directory
        val tempDir = tempFile.getParentFile
        if (tempDir != null && tempDir.exists()) {
          if (tempDir.delete()) {
            logger.debug("Cleaned up temporary directory: {}", tempDir.getAbsolutePath)
          } else {
            logger.warn("Failed to delete temporary directory: {}", tempDir.getAbsolutePath)
          }
        }
      }
    }
  }

  private def testConnectionWithStatement(conn: Connection): Try[Boolean] = {
    Try {
      val statement = conn.createStatement()
      try {
        val resultSet = statement.executeQuery("SELECT CURRENT_TIMESTAMP()")
        try {
          if (resultSet.next()) {
            logger.info("Snowflake connection test successful. Current time: {}", resultSet.getTimestamp(1))
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
        logger.error("Snowflake connection test failed", e)
        Failure(e)
    }
  }

  private def executeStatement(conn: Connection, sql: String): Try[Unit] = {
    Try {
      val statement = conn.createStatement()
      try {
        statement.execute(sql)
      } finally {
        statement.close()
      }
    } match {
      case Success(_) => Success(())
      case Failure(e) => 
        logger.error("Failed to execute SQL: {}", sql, e)
        Failure(e)
    }
  }
}
