package com.db.hierarchicaltorelationaldm;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DatabaseConnector {
    private static final String URL_PREFIX = "jdbc:mysql://localhost:3306/";
    private static final String USER = "root";
    private static final String PASSWORD = "root";
    private static String databaseName;
    private static HikariDataSource dataSource;

    /**
     * Initialize connection pool for the current database
     */
    private static synchronized void initializePool() {
        if (dataSource == null && databaseName != null) {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(URL_PREFIX + databaseName);
            config.setUsername(USER);
            config.setPassword(PASSWORD);

            // Connection pool settings
            config.setMaximumPoolSize(10);
            config.setMinimumIdle(2);
            config.setIdleTimeout(30000);
            config.setConnectionTimeout(10000);
            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

            // Add these lines for connection validation
            config.setConnectionTestQuery("SELECT 1");
            config.setValidationTimeout(5000);

            dataSource = new HikariDataSource(config);

            System.out.println("Connection pool initialized for database: " + databaseName);
        }
    }

    /**
     * Close connection pool when application is shutting down
     */
    public static void closePool() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            System.out.println("Connection pool closed");
        }
    }

    /**
     * Checks if a database already exists
     * @param dbName The name of the database to check
     * @return true if the database exists, false otherwise
     */
    public static boolean checkDatabaseExists(String dbName) {
        try (Connection conn = DriverManager.getConnection(URL_PREFIX, USER, PASSWORD);
             Statement stmt = conn.createStatement()) {

            ResultSet resultSet = conn.getMetaData().getCatalogs();
            while (resultSet.next()) {
                String existingDb = resultSet.getString(1);
                if (existingDb.equalsIgnoreCase(dbName)) {
                    return true;
                }
            }
            return false;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Creates a database or uses existing one if it already exists
     * @param dbName The name of the database to create
     * @return true if database was created or already exists, false on failure
     */
    public static boolean createDatabase(String dbName) {
        boolean exists = checkDatabaseExists(dbName);
        if (exists) {
            System.out.println("Database '" + dbName + "' already exists. Using existing database.");
            setDatabaseName(dbName);
            return true;
        }

        try (Connection conn = DriverManager.getConnection(URL_PREFIX, USER, PASSWORD);
             Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("CREATE DATABASE " + dbName);
            System.out.println("Database created successfully: " + dbName);
            setDatabaseName(dbName);
            return true;
        } catch (SQLException e) {
            System.out.println("Database creation failed: " + e.getMessage());
            return false;
        }
    }

    /**
     * Sets the current database name and initializes the connection pool
     * @param dbName The database name
     */
    public static void setDatabaseName(String dbName) {
        // If changing database name, close existing pool
        if (dataSource != null && databaseName != null && !databaseName.equals(dbName)) {
            closePool();
            dataSource = null;
        }

        databaseName = dbName;
        initializePool();
    }

    /**
     * Gets the current database name
     * @return The database name
     */
    public static String getDatabaseName() {
        return databaseName;
    }

    /**
     * Gets a connection from the pool
     * @return A database connection
     * @throws SQLException If connection fails
     */
    public static Connection getConnection() throws SQLException {
        if (dataSource == null) {
            initializePool();
        }

        if (dataSource != null) {
            return dataSource.getConnection();
        } else {
            // Fallback to direct connection if pool isn't available
            return DriverManager.getConnection(URL_PREFIX + databaseName, USER, PASSWORD);
        }
    }

    /**
     * Executes a list of SQL statements, checking for duplicates on entity_hierarchy inserts
     * @param sqlStatements List of SQL statements to execute
     */
    public static void executeSQL(List<String> sqlStatements) {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            for (String sql : sqlStatements) {
                // Special handling for entity_hierarchy inserts to avoid duplicates
                if (sql.toUpperCase().startsWith("INSERT INTO ENTITY_HIERARCHY")) {
                    handleEntityHierarchyInsert(conn, sql);
                    continue;
                }

                // Extract table name from CREATE TABLE statement
                String tableName = null;
                if (sql.toUpperCase().startsWith("CREATE TABLE")) {
                    tableName = extractTableName(sql);
                }

                // Execute other SQL statements
                try {
                    stmt.executeUpdate(sql);
                    System.out.println("SQL statement executed successfully: " +
                            sql.substring(0, Math.min(sql.length(), 50)) + (sql.length() > 50 ? "..." : ""));
                } catch (SQLException e) {
                    // Handle table already exists error
                    if (e.getErrorCode() == 1050 && tableName != null) {
                        System.out.println("Table '" + tableName + "' already exists. Skipping creation.");
                    } else {
                        System.out.println("SQL execution failed: " + e.getMessage());
                    }
                    // Continue with the next statement even if this one failed
                }
            }
        } catch (SQLException e) {
            System.out.println("Database operation failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Extract table name from CREATE TABLE statement
     * @param sql SQL statement
     * @return table name or empty string if not found
     */
    private static String extractTableName(String sql) {
        Pattern pattern = Pattern.compile("CREATE\\s+TABLE\\s+(?:`([^`]+)`|([^\\s(]+))", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            // Return the first non-null group (either backtick quoted or unquoted table name)
            return matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
        }
        return "";
    }

    /**
     * Handles inserts into entity_hierarchy with duplicate prevention
     * @param conn Database connection
     * @param sql SQL insert statement
     */
    private static void handleEntityHierarchyInsert(Connection conn, String sql) throws SQLException {
        // Extract values using regex
        Pattern pattern = Pattern.compile("VALUES\\s*\\(\\s*'([^']*)'\\s*,\\s*'([^']*)'\\s*,\\s*'([^']*)'\\s*\\)",
                Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);

        if (matcher.find() && matcher.groupCount() >= 3) {
            String parentEntity = matcher.group(1);
            String childEntity = matcher.group(2);
            String relationName = matcher.group(3);

            // Check if this hierarchy relationship already exists
            if (!hierarchyRelationExists(conn, parentEntity, childEntity)) {
                // Insert the new relationship
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT INTO entity_hierarchy (parent_entity, child_entity, relation_name) VALUES (?, ?, ?)")) {
                    pstmt.setString(1, parentEntity);
                    pstmt.setString(2, childEntity);
                    pstmt.setString(3, relationName);
                    pstmt.executeUpdate();
                    System.out.println("Added hierarchy relation: " + parentEntity + " -> " + childEntity);
                }
            } else {
                System.out.println("Hierarchy relation already exists: " + parentEntity + " -> " + childEntity + ". Skipping.");
            }
        } else {
            // If regex failed, fall back to direct execution
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(sql);
                System.out.println("SQL statement executed successfully: " +
                        sql.substring(0, Math.min(sql.length(), 50)) + (sql.length() > 50 ? "..." : ""));
            }
        }
    }

    /**
     * Checks if a hierarchy relationship already exists
     * @param conn Database connection
     * @param parentEntity Parent entity name
     * @param childEntity Child entity name
     * @return true if relationship exists, false otherwise
     */
    private static boolean hierarchyRelationExists(Connection conn, String parentEntity, String childEntity)
            throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT COUNT(*) FROM entity_hierarchy WHERE parent_entity = ? AND child_entity = ?")) {
            stmt.setString(1, parentEntity);
            stmt.setString(2, childEntity);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
    }

    /**
     * Executes a single SQL statement
     * @param sql The SQL statement to execute
     * @return true if executed successfully, false otherwise
     */
    public static boolean executeSingleSQL(String sql) {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
            return true;
        } catch (SQLException e) {
            System.out.println("SQL execution failed: " + e.getMessage());
            return false;
        }
    }
}