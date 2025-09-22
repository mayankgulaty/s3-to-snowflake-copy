package com.example

/**
 * Scala case class for Snowflake connection configuration
 * Immutable data structure with default values
 */
case class SnowflakeConfig(
  url: String,
  user: String,
  password: String,
  database: String,
  schema: String,
  role: String,
  warehouse: String
) {
  
  /**
   * Create a copy with updated password (for security)
   */
  def withPassword(newPassword: String): SnowflakeConfig = 
    this.copy(password = newPassword)
  
  /**
   * Create a copy with hidden password for logging
   */
  def forLogging: SnowflakeConfig = 
    this.copy(password = "[HIDDEN]")
  
  /**
   * Validate configuration
   */
  def validate: Either[String, SnowflakeConfig] = {
    val errors = scala.collection.mutable.ListBuffer[String]()
    
    if (url.isEmpty) errors += "URL cannot be empty"
    if (user.isEmpty) errors += "User cannot be empty"
    if (password.isEmpty) errors += "Password cannot be empty"
    if (database.isEmpty) errors += "Database cannot be empty"
    if (schema.isEmpty) errors += "Schema cannot be empty"
    if (role.isEmpty) errors += "Role cannot be empty"
    if (warehouse.isEmpty) errors += "Warehouse cannot be empty"
    
    if (errors.nonEmpty) {
      Left(errors.mkString(", "))
    } else {
      Right(this)
    }
  }
  
  /**
   * Get JDBC URL with parameters
   */
  def getJdbcUrl: String = {
    if (url.contains("?")) {
      url
    } else {
      s"$url?db=$database&schema=$schema&warehouse=$warehouse&role=$role"
    }
  }
  
  override def toString: String = {
    s"SnowflakeConfig(url='$url', user='$user', password='[HIDDEN]', " +
    s"database='$database', schema='$schema', role='$role', warehouse='$warehouse')"
  }
}

object SnowflakeConfig {
  
  /**
   * Create default configuration
   */
  def default: SnowflakeConfig = SnowflakeConfig(
    url = "jdbc:snowflake://a_icg_dev.us-east-2.aws.snowflakecomputing.com",
    user = "F_ICG_DEV_177688_ALERTS",
    password = "YOUR_PASSWORD_HERE",
    database = "D_ICG_DEV_177688_MASTER",
    schema = "LANDING",
    role = "R_ICG_DEV_177688_APPADMIN",
    warehouse = "W_ICG_DEV_177688_DEFAULT_XS"
  )
  
  /**
   * Create configuration from individual parameters
   */
  def apply(url: String, user: String, password: String, database: String, 
           schema: String, role: String, warehouse: String): SnowflakeConfig = {
    new SnowflakeConfig(url, user, password, database, schema, role, warehouse)
  }
}
