package com.example

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model._
import org.slf4j.{Logger, LoggerFactory}

import java.io.{ByteArrayOutputStream, IOException}
import java.util.Base64
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * Scala S3FileService - Handles file operations with S3
 * Uses AWS SDK v1 and matches your existing configuration
 */
class S3FileService(accessKey: String, secretKey: String, endpoint: String, 
                   bucketName: String, bucketPath: String) {
  
  private val logger: Logger = LoggerFactory.getLogger(classOf[S3FileService])
  private val s3Client: AmazonS3 = createS3Client(accessKey, secretKey, endpoint)

  /**
   * Create S3 client matching your existing pattern
   */
  private def createS3Client(accessKey: String, secretKey: String, endpoint: String): AmazonS3 = {
    // Decode Base64 credentials (matching your pattern)
    val decodedAccessKey = new String(Base64.getDecoder.decode(accessKey))
    val decodedSecretKey = new String(Base64.getDecoder.decode(secretKey))
    
    // Create credentials
    val credentials = new BasicAWSCredentials(decodedAccessKey, decodedSecretKey)
    
    // Build S3 client with custom endpoint (matching your pattern)
    AmazonS3ClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(credentials))
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, ""))
      .build()
  }

  /**
   * List all objects in the bucket
   */
  def listObjects(): List[S3ObjectSummary] = {
    logger.info("Listing all objects in bucket: {}", bucketName)
    
    val allObjects = scala.collection.mutable.ListBuffer[S3ObjectSummary]()
    val request = new ListObjectsV2Request()
      .withBucketName(bucketName)
      .withPrefix(bucketPath.stripPrefix("/"))
    
    var result: ListObjectsV2Result = null
    do {
      result = s3Client.listObjectsV2(request)
      allObjects ++= result.getObjectSummaries.asScala
      request.setContinuationToken(result.getNextContinuationToken)
    } while (result.isTruncated)
    
    logger.info("Found {} objects", allObjects.length)
    allObjects.toList
  }

  /**
   * List objects with specific prefix
   */
  def listObjectsWithPrefix(prefix: String): List[S3ObjectSummary] = {
    logger.info("Listing objects with prefix: {}", prefix)
    
    val allObjects = scala.collection.mutable.ListBuffer[S3ObjectSummary]()
    val request = new ListObjectsV2Request()
      .withBucketName(bucketName)
      .withPrefix(prefix)
    
    var result: ListObjectsV2Result = null
    do {
      result = s3Client.listObjectsV2(request)
      allObjects ++= result.getObjectSummaries.asScala
      request.setContinuationToken(result.getNextContinuationToken)
    } while (result.isTruncated)
    
    logger.info("Found {} objects with prefix {}", allObjects.length, prefix)
    allObjects.toList
  }

  /**
   * Download file from S3 (matching your pattern)
   */
  def downloadFile(s3Key: String): Try[Array[Byte]] = {
    logger.info("Downloading file: {}", s3Key)
    
    Try {
      val getObjectRequest = new GetObjectRequest(bucketName, s3Key)
      
      val s3Object = s3Client.getObject(getObjectRequest)
      try {
        val inputStream = s3Object.getObjectContent
        try {
          val outputStream = new ByteArrayOutputStream()
          try {
            val buffer = new Array[Byte](8192)
            var bytesRead = inputStream.read(buffer)
            while (bytesRead != -1) {
              outputStream.write(buffer, 0, bytesRead)
              bytesRead = inputStream.read(buffer)
            }
            
            val fileContent = outputStream.toByteArray
            logger.info("Successfully downloaded file: {} ({} bytes)", s3Key, fileContent.length)
            fileContent
          } finally {
            outputStream.close()
          }
        } finally {
          inputStream.close()
        }
      } finally {
        s3Object.close()
      }
    } match {
      case Success(content) => Success(content)
      case Failure(e) => 
        logger.error("Failed to download file: {}", s3Key, e)
        Failure(e)
    }
  }

  /**
   * Upload file to S3 (matching your pattern)
   */
  def uploadFile(s3Key: String, fileContent: Array[Byte]): Try[Unit] = {
    logger.info("Uploading file to S3: {}", s3Key)
    
    Try {
      val metadata = new ObjectMetadata()
      metadata.setContentLength(fileContent.length)
      
      val putObjectRequest = new PutObjectRequest(
        bucketName, 
        s3Key, 
        new java.io.ByteArrayInputStream(fileContent), 
        metadata
      )
      
      s3Client.putObject(putObjectRequest)
      logger.info("Successfully uploaded file: {}", s3Key)
    } match {
      case Success(_) => Success(())
      case Failure(e) => 
        logger.error("Failed to upload file: {}", s3Key, e)
        Failure(e)
    }
  }

  /**
   * Check if object exists in S3
   */
  def objectExists(s3Key: String): Boolean = {
    Try {
      s3Client.doesObjectExist(bucketName, s3Key)
    }.getOrElse(false)
  }

  /**
   * Get object metadata
   */
  def getObjectMetadata(s3Key: String): Try[ObjectMetadata] = {
    Try {
      s3Client.getObjectMetadata(bucketName, s3Key)
    } match {
      case Success(metadata) => Success(metadata)
      case Failure(e) => 
        logger.error("Failed to get metadata for: {}", s3Key, e)
        Failure(e)
    }
  }

  /**
   * Delete object from S3
   */
  def deleteObject(s3Key: String): Try[Unit] = {
    logger.info("Deleting object: {}", s3Key)
    
    Try {
      s3Client.deleteObject(bucketName, s3Key)
      logger.info("Successfully deleted object: {}", s3Key)
    } match {
      case Success(_) => Success(())
      case Failure(e) => 
        logger.error("Failed to delete object: {}", s3Key, e)
        Failure(e)
    }
  }
}
