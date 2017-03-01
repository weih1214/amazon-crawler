package sg.edu.smu.webmining.crawler.storage;

import com.mysql.jdbc.Statement;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import sg.edu.smu.webmining.crawler.storage.ex.StorageException;

import java.io.*;
import java.sql.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


/**
 * Created by hwei on 6/1/2017.
 */
public class MysqlFileManager implements FileManager, AutoCloseable {

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
      this.url = url;
      this.recordFile = recordFile;
      this.md5 = md5;
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return new BufferedInputStream(new GZIPInputStream(new FileInputStream(recordFile)));
    }

    @Override
    public String getContent() throws IOException {
      if (lazyContent == null) {
        lazyContent = IOUtils.toString(getInputStream(), "UTF-8");
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

  public MysqlFileManager(String dbLocation, String username, String password, File storageDir) throws SQLException {
    connection = DriverManager.getConnection(dbLocation, username, password);
    connection.setAutoCommit(false);
    this.insertStatement = connection.prepareStatement("INSERT INTO record (url, md5, location) VALUES(?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
    this.fetchStatement = connection.prepareStatement("SELECT * FROM record WHERE id = ?");
    this.storageDir = storageDir;
  }

  public MysqlFileManager(String dbLocation, String username, String password, String storageDir) throws SQLException {
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
    try (final BufferedOutputStream out = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(recordFile)))) {
      IOUtils.copy(in, out);
    }
  }

  public String put(String url, String content, InputStream imageStream) throws StorageException {
    try {
      return put(url, new ByteArrayInputStream(content.getBytes("UTF-8")), imageStream);
    } catch (UnsupportedEncodingException e) {
      throw new StorageException(e);
    }
  }

  public String put(String url, InputStream content, InputStream imageStream) throws StorageException {
    try {
      if (!content.markSupported()) {
        final ByteArrayOutputStream copyOutputStream = new ByteArrayOutputStream();
        IOUtils.copy(content, copyOutputStream);
        content = new ByteArrayInputStream(copyOutputStream.toByteArray());
      }

      final String md5 = DigestUtils.md5Hex(content);
      content.reset();
      final String subDirName = md5.substring(0, 2);
      insertStatement.setString(1, url);
      insertStatement.setString(2, md5);
      insertStatement.setString(3, subDirName);
      if (insertStatement.executeUpdate() == 1) {
        final ResultSet rs = insertStatement.getGeneratedKeys();
        if (rs.next()) {
          int id = rs.getInt(1);
          final String sId = String.valueOf(id);
          save(content, new File(storageDir, subDirName), sId);
          connection.commit();
          return sId;
        }
      }

      connection.rollback();
      throw new StorageException("Cannot store the record");
    } catch (SQLException | IOException e) {
      try {
        connection.rollback();
      } catch (SQLException rbe) {
        throw new StorageException("Cannot store the record", rbe);
      }
      throw new StorageException("Cannot store the record", e);
    }
  }

  @Override
  public String put(String url, String content) throws StorageException {
    try {
      return put(url, new ByteArrayInputStream(content.getBytes("UTF-8")));
    } catch (UnsupportedEncodingException e) {
      throw new StorageException(e);
    }
  }

  @Override
  public String put(String url, InputStream content) throws StorageException {
    try {
      if (!content.markSupported()) {
        final ByteArrayOutputStream copyOutputStream = new ByteArrayOutputStream();
        IOUtils.copy(content, copyOutputStream);
        content = new ByteArrayInputStream(copyOutputStream.toByteArray());
      }

      final String md5 = DigestUtils.md5Hex(content);
      content.reset();
      final String subDirName = md5.substring(0, 2);
      insertStatement.setString(1, url);
      insertStatement.setString(2, md5);
      insertStatement.setString(3, subDirName);
      if (insertStatement.executeUpdate() == 1) {
        final ResultSet rs = insertStatement.getGeneratedKeys();
        if (rs.next()) {
          int id = rs.getInt(1);
          final String sId = String.valueOf(id);
          save(content, new File(storageDir, subDirName), sId);
          connection.commit();
          return sId;
        }
      }

      connection.rollback();
      throw new StorageException("Cannot store the record");
    } catch (SQLException | IOException e) {
      try {
        connection.rollback();
      } catch (SQLException rbe) {
        throw new StorageException("Cannot store the record", rbe);
      }
      throw new StorageException("Cannot store the record", e);
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
