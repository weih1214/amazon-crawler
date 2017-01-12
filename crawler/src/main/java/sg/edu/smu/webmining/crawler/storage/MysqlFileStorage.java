package sg.edu.smu.webmining.crawler.storage;

import com.mysql.jdbc.Statement;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import sg.edu.smu.webmining.crawler.storage.ex.StorageException;

import java.io.*;
import java.sql.*;


/**
 * Created by hwei on 6/1/2017.
 */
public class MysqlFileStorage implements FileStorage, AutoCloseable {

  /**
   * Created by hwei on 6/1/2017.
   */
  public static class StorageRecord implements Record {

    private final File recordFile;
    private final String url;
    private final String id;
    private final String md5;
    private final long timestamp = -1;

    private String lazyContent;

    private StorageRecord(String id, String url, File recordFile, String md5) {
      this.id = id;
      this.recordFile = recordFile;
      this.url = url;
      this.md5 = md5;
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return new BufferedInputStream(new FileInputStream(recordFile));
    }

    @Override
    public String getContent() throws IOException {
      if (lazyContent == null) {
        lazyContent = FileUtils.readFileToString(recordFile);
      }
      return lazyContent;
    }

    @Override
    public String getURL() {
      return url;
    }

    @Override
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public String getMD5() {
      return md5;
    }

    @Override
    public String getId() {
      return id;
    }

  }

  private final Connection connection;

  private final PreparedStatement insertStatement;
  private final PreparedStatement fetchStatement;

  private final File storageDir;

  public MysqlFileStorage(String dbLocation, String username, String password, File storageDir) throws SQLException {
    connection = DriverManager.getConnection(dbLocation, username, password);
    connection.setAutoCommit(false);
    this.insertStatement = connection.prepareStatement("INSERT INTO test (url, md5, location) VALUES(?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
    this.fetchStatement = connection.prepareStatement("SELECT * FROM test WHERE id = ?");
    this.storageDir = storageDir;
  }

  public MysqlFileStorage(String dbLocation, String username, String password, String storageDir) throws SQLException {
    this(dbLocation, username, password, new File(storageDir));
  }

  private void save(InputStream in, File recordDir, String recordName) throws IOException {
    if (!recordDir.exists() && !recordDir.mkdirs()) {
      throw new IOException("Cannot create the record dir: " + recordDir);
    }
    if (recordDir.exists() && !recordDir.isDirectory()) {
      throw new IOException("The record path is not a dir: " + recordDir);
    }
    final File recordFile = new File(recordDir, recordName);
    try (OutputStream out = new BufferedOutputStream(new FileOutputStream(recordFile))) {
      IOUtils.copy(in, out);
    }
  }

  @Override
  public Record put(Record record) throws StorageException {
    try {
      final String url = record.getURL();
      final String md5 = record.getMD5();
      final String subDirName = md5.substring(0, 2);
      insertStatement.setString(1, url);
      insertStatement.setString(2, md5);
      insertStatement.setString(3, subDirName);
      if (insertStatement.executeUpdate() == 1) {
        final ResultSet rs = insertStatement.getGeneratedKeys();
        if (rs.next()) {
          int source = rs.getInt(1);
          if (record instanceof BasicRecord) {
            ((BasicRecord) record).setId(String.valueOf(source));
          }
          save(record.getInputStream(), new File(storageDir, subDirName), String.valueOf(source));
        }
      }
      connection.commit();
      return record;
    } catch (SQLException | IOException e) {
      try {
        connection.rollback();
      } catch (SQLException rollbackException) {
        throw new StorageException(rollbackException);
      }
      throw new StorageException(e);
    }
  }

  @Override
  public Record get(String id) throws StorageException {
    try {
      fetchStatement.setInt(1, Integer.valueOf(id));
      final ResultSet rs = fetchStatement.executeQuery();
      if (rs.next()) {
        final String url = rs.getString("url");
        final String location = rs.getString("location");
        final String md5 = rs.getString("md5");
        return new StorageRecord(id, url, new File(new File(storageDir, location), id), md5);
      }
      return null;
    } catch (SQLException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }

}
