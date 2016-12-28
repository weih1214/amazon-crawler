package sg.edu.smu.webmining.crawler.databasemanager;

import java.sql.*;

/**
 * Created by hwei on 27/12/2016.
 */
public class MysqlRecordManager implements AutoCloseable, MysqlManager{

  private final Connection connection;

  private final PreparedStatement insertStatement;
  private final PreparedStatement idFetchStatement;

  public MysqlRecordManager(String location, String user, String password) throws SQLException {

    this.connection = DriverManager.getConnection(location, user, password);
    this.insertStatement = connection.prepareStatement("INSERT INTO table1 (CONTENT,MD5,URL) VALUES (?,?,?)");
    this.idFetchStatement = connection.prepareStatement("SELECT ID FROM table1 WHERE MD5 = ?");
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }

  @Override
  public boolean update(String md5, String url, String content) throws Exception {
    insertStatement.setString(1, content);
    insertStatement.setString(2, md5);
    insertStatement.setString(3, url);
    if (insertStatement.executeUpdate() == 1) {
      return true;
    }
    return false;
  }

  @Override
  public String fetchId(String md5) throws SQLException {
    idFetchStatement.setString(1, md5);
    ResultSet resultSet = idFetchStatement.executeQuery();
    while (resultSet.next()) {
      return resultSet.getString("ID");
    }
    return null;
  }
}
