package com.example;

/**
 * Configuration class for Snowflake connection
 */
public class SnowflakeConfig {
    private String url;
    private String user;
    private String password;
    private String database;
    private String schema;
    private String role;
    private String warehouse;

    public SnowflakeConfig() {}

    public SnowflakeConfig(String url, String user, String password, String database, 
                          String schema, String role, String warehouse) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.database = database;
        this.schema = schema;
        this.role = role;
        this.warehouse = warehouse;
    }

    // Getters and Setters
    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public void setWarehouse(String warehouse) {
        this.warehouse = warehouse;
    }

    @Override
    public String toString() {
        return "SnowflakeConfig{" +
                "url='" + url + '\'' +
                ", user='" + user + '\'' +
                ", password='[HIDDEN]'" +
                ", database='" + database + '\'' +
                ", schema='" + schema + '\'' +
                ", role='" + role + '\'' +
                ", warehouse='" + warehouse + '\'' +
                '}';
    }
}
