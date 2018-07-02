package com.pronoia.junit.h2;

import static java.nio.file.Files.exists;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.isRegularFile;
import static java.nio.file.Files.notExists;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class H2Resource extends ExternalResource {
  protected Logger log = LoggerFactory.getLogger(this.getClass());
  Path sqlPath = Paths.get("src/test/H2/sql");
  Path csvPath = Paths.get("src/test/H2/csv");
  List<String> csvList = new LinkedList<>();
  JdbcDataSource dataSource;
  Connection connection;

  boolean logLoadedData = false;

  public Path getSqlPath() {
    return sqlPath;
  }

  public void setSqlPath(Path sqlPath) {
    this.sqlPath = sqlPath;
  }

  public Path getCsvPath() {
    return csvPath;
  }

  public void setCsvPath(Path csvPath) {
    this.csvPath = csvPath;
  }

  @Override
  protected void before() throws Throwable {
    this.dataSource = initializeDataSource();

    connection = dataSource.getConnection();

    try (Statement statement = connection.createStatement()) {
      this.doInitSchema(statement);
    }

    try (Statement statement = connection.createStatement()) {
      this.doBeforeLoad(statement);
    }

    try (Statement statement = connection.createStatement()) {
      this.doLoad(statement);
    }

    try (Statement statement = connection.createStatement()) {
      this.doAfterLoad(statement);
    }
  }

  @Override
  protected void after() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  public boolean isLogLoadedData() {
    return logLoadedData;
  }

  public void setLogLoadedData(boolean logLoadedData) {
    this.logLoadedData = logLoadedData;
  }

  public String getUrl() {
    return this.getUrl(true);
  }

  public String getUrl(boolean noCreate) {
    return (noCreate) ? dataSource.getURL() + ";IFEXISTS=TRUE" : dataSource.getUrl();
  }

  public String getUser() {
    return dataSource.getUser();
  }

  public String getPassword() {
    return dataSource.getPassword();
  }

  public ResultSet executeQuery(String query) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      return statement.executeQuery(query);
    }
  }

  /**
   * Override this method to customize the DB configuration
   *
   * @return
   */
  protected JdbcDataSource initializeDataSource() {
    JdbcDataSource dataSource = new JdbcDataSource();

    dataSource.setURL("jdbc:h2:mem:embedded-h2");
    dataSource.setUser("H2");
    dataSource.setPassword("H2");

    return dataSource;
  }

  /**
   * Override this method to customize the method for initializing the database schema.  By default, the list SQL scripts files are run
   */
  protected void doInitSchema(Statement statement) throws Exception {
    if (this.sqlPath != null && exists(sqlPath) && isDirectory(sqlPath)) {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(sqlPath)) {
        for (Path sqlScript : stream) {
          this.executeSqlScript(statement, sqlScript);
        }
      }
    }
  }

  /**
   * Override this method to perform any actions required before the CSV files are loaded.  The default does nothing.
   *
   * @param statement a SQL statement object
   * @throws Exception thrown on any error
   */
  protected void doBeforeLoad(Statement statement) throws Exception {

  }

  /**
   * Override this method to customize the method for loading the database.  By default, the list CSV files are loaded
   *
   * @param statement a SQL statement object
   * @throws Exception thrown on any error
   */
  protected void doLoad(Statement statement) throws Exception {
    if (this.csvPath != null && exists(csvPath) && isDirectory(csvPath)) {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(csvPath)) {
        for (Path csvFile : stream) {
          this.loadCsvFile(statement, csvFile);
        }
      }
    }
  }

  /**
   * Override this method to perform any actions required after the CSV files are loaded.  By default, if the logLoaded data attribute
   * the content of the is true tables loaded from the CSV files is logged at the INFO logging level.
   *
   * @param statement a SQL statement object
   * @throws Exception thrown on any error
   */
  protected void doAfterLoad(Statement statement) throws Exception {
    if (logLoadedData) {
      if (this.csvPath != null && exists(csvPath) && isDirectory(csvPath)) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(csvPath)) {
          for (Path csvFile : stream) {
            String fileName = csvFile.getFileName().toString();
            int dotIndex = fileName.lastIndexOf('.');

            String tableName = (-1 == dotIndex) ? fileName : fileName.substring(0, dotIndex);
            this.logTableContents(statement, tableName);
          }
        }
      }
    }
  }

  /**
   * Load a CSV file, using the filename as the table name.
   * <p>
   * NOTE:  Files not ending in .csv (case insensitive) are ignored
   *
   * @param statement a SQL statement object
   * @param csvFile   the Path to for the CSV data file
   * @throws Exception thrown on any error
   */
  protected void loadCsvFile(Statement statement, Path csvFile) throws Exception {
    if (csvFile == null) {
      throw new IllegalArgumentException("CSV File cannot be null");
    } else if (notExists(csvFile)) {
      throw new IllegalArgumentException(String.format("CSV File {} does not exist", csvFile.toString()));
    } else if (!isRegularFile(csvFile)) {
      throw new IllegalArgumentException(String.format("CSV File {} is not a regular file", csvFile.toString()));
    } else {
      String fileName = csvFile.getFileName().toString();
      int dotIndex = fileName.lastIndexOf('.');

      String tableName = (-1 == dotIndex) ? fileName : fileName.substring(0, dotIndex);

      log.info("Loading {}", csvFile);
      statement.execute(String.format("insert into %s (select * from csvread('%s'))", tableName, csvFile.toString()));
    }
  }

  /**
   * Execute a SQL Script.
   * <p>
   * NOTE:  Files not ending in .sql (case insensitive) are ignored
   *
   * @param statement a SQL statement object
   * @param sqlScript the Path to for the script
   * @throws Exception thrown on any error
   */
  protected void executeSqlScript(Statement statement, Path sqlScript) throws Exception {
    String tmpFileName = sqlScript.getFileName().toString();
    if (tmpFileName.endsWith(".")) {
      log.warn("Script file name ends with '.' - skipping {}", sqlScript);
      return;
    }

    int dotIndex = tmpFileName.lastIndexOf('.');
    if (dotIndex == -1) {
      log.warn("Script file name does not contain '.' - skipping {}", sqlScript);
      return;
    }

    String extension = tmpFileName.substring(dotIndex + 1);
    if (!extension.equalsIgnoreCase("sql")) {
      log.warn("Script file name name does not end with 'sql' - skipping {}", sqlScript);
      return;
    }

    if (!isRegularFile(sqlScript)) {
      log.warn("Script is not a regular file - skipping {}", sqlPath);
      return;
    }

    log.info("Executing {}", sqlScript);
    statement.execute(String.format("runscript from '%s'", sqlScript));
  }

  protected void logTableContents(Statement statement, String tableName) throws Exception {
    try (ResultSet resultSet = statement.executeQuery("select * from " + tableName)) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int columnCount = resultSetMetaData.getColumnCount();
      log.info("Table {}:", tableName);
      StringBuilder rowData = new StringBuilder();
      int rowNumber = 1;
      while (resultSet.next()) {
        for (int columnNumber = 1; columnNumber <= columnCount; ++columnNumber) {
          rowData
              .append(resultSetMetaData.getColumnName(columnNumber))
              .append(" = ")
              .append(resultSet.getString(columnNumber));
          if (columnNumber < columnCount) {
            rowData.append(", ");
          }
        }
        log.info("\t Row #{}: {}", rowNumber++, rowData);
        rowData.setLength(0);
      }
    }
  }

}
