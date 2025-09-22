package com.example

import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/**
 * Scala version of S3ToSnowflakeCopyCompatible
 * Main application for copying files from S3 to Snowflake using existing S3 service
 */
object S3ToSnowflakeCopyCompatible {
  
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    logger.info("=== S3 to Snowflake Copy Compatible (Scala) ===")
    
    // Configuration - UPDATE THESE VALUES
    val s3AccessKey = "your-base64-encoded-access-key"
    val s3SecretKey = "your-base64-encoded-secret-key"
    val s3Endpoint = "https://your-s3-endpoint.com"
    val s3Bucket = "your-s3-bucket"
    val s3BucketPath = "/your/bucket/path"
    val s3Key = "path/to/your/file.txt" // File to copy
    
    val snowflakePassword = "your-snowflake-password" // UPDATE THIS
    val snowflakeStagePath = "@my_stage/data/" // Snowflake stage to copy to
    
    // Create configurations
    val s3Config = createS3Config(s3AccessKey, s3SecretKey, s3Endpoint, s3Bucket, s3BucketPath)
    val snowflakeConfig = createSnowflakeConfig(snowflakePassword)
    
    // Run the copy process
    val result = for {
      s3Service <- createS3Service(s3Config)
      snowflakeService <- createSnowflakeService(snowflakeConfig)
      _ <- testConnections(s3Service, snowflakeService)
      _ <- copyFileFromS3ToSnowflake(s3Service, snowflakeService, s3Key, snowflakeStagePath)
    } yield ()
    
    result match {
      case Success(_) => 
        logger.info("✅ S3 to Snowflake copy completed successfully!")
        System.exit(0)
      case Failure(e) => 
        logger.error("❌ S3 to Snowflake copy failed", e)
        System.exit(1)
    }
  }

  /**
   * S3 configuration case class
   */
  case class S3Config(
    accessKey: String,
    secretKey: String,
    endpoint: String,
    bucket: String,
    bucketPath: String
  )

  /**
   * Create S3 configuration
   */
  private def createS3Config(accessKey: String, secretKey: String, endpoint: String, 
                            bucket: String, bucketPath: String): S3Config = {
    S3Config(accessKey, secretKey, endpoint, bucket, bucketPath)
  }

  /**
   * Create Snowflake configuration
   */
  private def createSnowflakeConfig(password: String): SnowflakeConfig = {
    SnowflakeConfig(
      url = "jdbc:snowflake://a_icg_dev.us-east-2.aws.snowflakecomputing.com",
      user = "F_ICG_DEV_177688_ALERTS",
      password = password,
      database = "D_ICG_DEV_177688_MASTER",
      schema = "LANDING",
      role = "R_ICG_DEV_177688_APPADMIN",
      warehouse = "W_ICG_DEV_177688_DEFAULT_XS"
    )
  }

  /**
   * Create S3 service
   */
  private def createS3Service(config: S3Config): Try[CompatibleS3Service] = {
    Try {
      logger.info("Creating S3 service with endpoint: {}", config.endpoint)
      new CompatibleS3Service(
        config.accessKey,
        config.secretKey,
        config.endpoint,
        config.bucket,
        config.bucketPath
      )
    } match {
      case Success(service) => 
        logger.info("✅ S3 service created successfully")
        Success(service)
      case Failure(e) => 
        logger.error("❌ Failed to create S3 service", e)
        Failure(e)
    }
  }

  /**
   * Create Snowflake service
   */
  private def createSnowflakeService(config: SnowflakeConfig): Try[SnowflakeService] = {
    Try {
      logger.info("Creating Snowflake service")
      new SnowflakeService(config)
    } match {
      case Success(service) => 
        logger.info("✅ Snowflake service created successfully")
        Success(service)
      case Failure(e) => 
        logger.error("❌ Failed to create Snowflake service", e)
        Failure(e)
    }
  }

  /**
   * Test both S3 and Snowflake connections
   */
  private def testConnections(s3Service: CompatibleS3Service, snowflakeService: SnowflakeService): Try[Unit] = {
    logger.info("Testing connections...")
    
    for {
      _ <- testS3Connection(s3Service)
      _ <- testSnowflakeConnection(snowflakeService)
    } yield {
      logger.info("✅ All connections tested successfully")
    }
  }

  /**
   * Test S3 connection
   */
  private def testS3Connection(s3Service: CompatibleS3Service): Try[Unit] = {
    logger.info("Testing S3 connection...")
    
    s3Service.listObjects() match {
      case Success(objects) => 
        logger.info("✅ S3 connection successful. Found {} objects", objects.length)
        Success(())
      case Failure(e) => 
        logger.error("❌ S3 connection failed", e)
        Failure(e)
    }
  }

  /**
   * Test Snowflake connection
   */
  private def testSnowflakeConnection(snowflakeService: SnowflakeService): Try[Unit] = {
    logger.info("Testing Snowflake connection...")
    
    snowflakeService.testConnection() match {
      case Success(true) => 
        logger.info("✅ Snowflake connection successful")
        Success(())
      case Success(false) => 
        logger.error("❌ Snowflake connection test failed")
        Failure(new RuntimeException("Snowflake connection test failed"))
      case Failure(e) => 
        logger.error("❌ Snowflake connection failed", e)
        Failure(e)
    }
  }

  /**
   * Copy file from S3 to Snowflake
   */
  private def copyFileFromS3ToSnowflake(s3Service: CompatibleS3Service, 
                                       snowflakeService: SnowflakeService,
                                       s3Key: String, 
                                       stagePath: String): Try[Unit] = {
    logger.info("Starting S3 to Snowflake copy process...")
    logger.info("S3 Key: {}", s3Key)
    logger.info("Snowflake Stage: {}", stagePath)
    
    // Check if file exists in S3
    if (!s3Service.objectExists(s3Key)) {
      val error = s"File not found in S3: $s3Key"
      logger.error("❌ {}", error)
      return Failure(new RuntimeException(error))
    }
    
    // Copy file from S3 to Snowflake
    snowflakeService.copyFileFromS3ToStage(stagePath, s3Service, s3Key) match {
      case Success(_) => 
        logger.info("✅ Successfully copied file from S3 to Snowflake")
        Success(())
      case Failure(e) => 
        logger.error("❌ Failed to copy file from S3 to Snowflake", e)
        Failure(e)
    }
  }

  /**
   * List files in S3 bucket
   */
  def listS3Files(s3Service: CompatibleS3Service): Try[List[String]] = {
    s3Service.listObjects() match {
      case Success(objects) => 
        val fileKeys = objects.map(_.getKey)
        logger.info("Found {} files in S3 bucket", fileKeys.length)
        Success(fileKeys)
      case Failure(e) => 
        logger.error("Failed to list S3 files", e)
        Failure(e)
    }
  }

  /**
   * Copy multiple files from S3 to Snowflake
   */
  def copyMultipleFiles(s3Service: CompatibleS3Service, 
                       snowflakeService: SnowflakeService,
                       s3Keys: List[String], 
                       stagePath: String): Try[List[Unit]] = {
    logger.info("Copying {} files from S3 to Snowflake", s3Keys.length)
    
    val results = s3Keys.map { s3Key =>
      logger.info("Copying file: {}", s3Key)
      copyFileFromS3ToSnowflake(s3Service, snowflakeService, s3Key, stagePath)
    }
    
    val failures = results.collect { case Failure(e) => e }
    if (failures.nonEmpty) {
      logger.error("Failed to copy {} files", failures.length)
      Failure(failures.head)
    } else {
      logger.info("✅ Successfully copied all {} files", s3Keys.length)
      Success(results.collect { case Success(unit) => unit })
    }
  }
}
