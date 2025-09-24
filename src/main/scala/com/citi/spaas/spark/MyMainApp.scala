package com.citi.spaas.spark

import org.slf4j.{Logger, LoggerFactory}
import scala.util.{Failure, Success, Try}

/**
 * Example main class that demonstrates how to call S3ToSnowflakeCopyApp functionality
 */
object MyMainApp {
  
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    logger.info("=== My Custom Main Application ===")
    
    // Example 1: Run the default copy operation
    logger.info("Running default S3 to Snowflake copy...")
    S3ToSnowflakeCopyApp.runDefaultCopy() match {
      case Success(_) => logger.info("✅ Default copy completed successfully!")
      case Failure(e) => logger.error("❌ Default copy failed", e)
    }
    
    // Example 2: Run copy with custom parameters
    logger.info("Running custom S3 to Snowflake copy...")
    val customResult = S3ToSnowflakeCopyApp.runCopyWithParams(
      s3AccessKey = "your-custom-access-key",
      s3SecretKey = "your-custom-secret-key", 
      s3Endpoint = "https://your-custom-endpoint.com",
      s3Bucket = "your-custom-bucket",
      s3BucketPath = "/your/custom/path",
      s3Key = "custom/file.txt",
      snowflakePassword = "your-custom-password",
      snowflakeStagePath = "@custom_stage/data/"
    )
    
    customResult match {
      case Success(_) => logger.info("✅ Custom copy completed successfully!")
      case Failure(e) => logger.error("❌ Custom copy failed", e)
    }
    
    // Example 3: Use individual components
    logger.info("Using individual components...")
    useIndividualComponents()
  }
  
  /**
   * Example of using individual components from S3ToSnowflakeCopyApp
   */
  private def useIndividualComponents(): Unit = {
    // Create your own configurations
    val s3Config = S3ToSnowflakeCopyApp.createS3Config(
      "access-key", "secret-key", "endpoint", "bucket", "/path"
    )
    val snowflakeConfig = S3ToSnowflakeCopyApp.createSnowflakeConfig("password")
    
    // Use the services individually
    for {
      s3Service <- S3ToSnowflakeCopyApp.createS3Service(s3Config)
      snowflakeService <- S3ToSnowflakeCopyApp.createSnowflakeService(snowflakeConfig)
      _ <- S3ToSnowflakeCopyApp.testConnections(s3Service, snowflakeService)
      fileList <- S3ToSnowflakeCopyApp.listS3Files(s3Service)
    } yield {
      logger.info(s"Found ${fileList.length} files in S3 bucket")
      // Do something with the file list
    }
  }
}
