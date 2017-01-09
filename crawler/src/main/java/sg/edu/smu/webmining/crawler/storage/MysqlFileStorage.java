package sg.edu.smu.webmining.crawler.storage;

import com.mysql.jdbc.Statement;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;

/**
 * Created by hwei on 6/1/2017.
 */
public class MysqlFileStorage implements FileStorage, AutoCloseable {

  private final Connection connection;
  private final String insertString = "INSERT INTO test (url, md5, location) VALUES(?, ?, ?)";
  private final PreparedStatement insertStatement;
  private final PreparedStatement fetchStatement;

  private final String globalDir;

  public MysqlFileStorage(String location, String user, String password, String globalDir) throws SQLException {
    this.connection = DriverManager.getConnection(location, user, password);
    this.insertStatement = connection.prepareStatement(insertString, Statement.RETURN_GENERATED_KEYS);
    this.fetchStatement = connection.prepareStatement("SELECT * FROM test WHERE id = ?");
    this.globalDir = globalDir;
  }
  //
  public void save(InputStream in, String dirPath, Integer source) throws IOException {
    new File(dirPath).mkdirs();
    final String fullPath = dirPath + source;
    final Path destination = Paths.get(fullPath);
    Files.copy(in, destination);
  }

  @Override
  public Record put(Record record) throws IOException {
    final String url = record.getURL();
//    final String md5 = record.getFingerprint();
    final String md5 = RandomStringUtils.random(10, true, true);
    final String specificPath = "\\" + md5.substring(1, 3) + "\\";
    final String dirPath = globalDir + specificPath;
    Integer source = 0;
    try {
      insertStatement.setString(1, url);
      insertStatement.setString(2, md5);
      insertStatement.setString(3, dirPath);
      if (insertStatement.executeUpdate() == 1) {
        ResultSet rs = insertStatement.getGeneratedKeys();
        if (rs.next()) {
          source = rs.getInt(1);
          record.setId(source);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    save(record.getInputStream(), dirPath, source);
    return record;
  }

  @Override
  public Record get(Integer id) throws FileNotFoundException {
    try {
      fetchStatement.setInt(1, id);
      final ResultSet rs = fetchStatement.executeQuery();
      String url = null;
      String location = null;
      String fileLocation = null;
      String md5 = null;
      InputStream in = null;
      if (rs.next()) {
        url = rs.getString("url");
        location = rs.getString("location");
        md5 = rs.getString("md5");
        fileLocation = location + id;
      }
      in = new FileInputStream(fileLocation);
      final String content = IOUtils.toString(in, "UTF-8");
      return new StorageRecord(url, content, id, fileLocation, md5, in);
    } catch (SQLException | IOException e) {
      e.printStackTrace();
    }
//    InMemoryRecord record =
    return null;
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }
}
