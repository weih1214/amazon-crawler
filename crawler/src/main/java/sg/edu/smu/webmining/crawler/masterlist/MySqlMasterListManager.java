package sg.edu.smu.webmining.crawler.masterlist;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mtkachenko.2015 on 16/11/2016.
 */
public class MySqlMasterListManager implements AutoCloseable, MasterListManager {

  private final Connection connection;

  private final PreparedStatement insertStatement;
  private final PreparedStatement checkStatement;
  private final PreparedStatement updateStatement;

  public MySqlMasterListManager(String location, String user, String password) throws SQLException {
    connection = DriverManager.getConnection(location, user, password);

    // TODO: combine all these statements into one SQL statement
    insertStatement = connection.prepareStatement("INSERT INTO masterlist (product_id,first_update,recent_update,url, source) VALUES (?,now(),now(),?,?)");
    checkStatement = connection.prepareStatement("SELECT url FROM masterlist WHERE product_id = ?");
    updateStatement = connection.prepareStatement("UPDATE masterlist SET recent_update = now() WHERE product_id = ?");
  }

  @Override
  public void update(String productId, String url, String source) throws SQLException {
    checkStatement.setString(1, productId);
    ResultSet rs = checkStatement.executeQuery();
    if (rs.next()) {
      updateStatement.setString(1, productId);
      updateStatement.executeUpdate();
    } else {
      insertStatement.setString(1, productId);
      insertStatement.setString(2, url);
      insertStatement.setString(3, source);
      insertStatement.executeUpdate();
    }
  }

  @Override
  public List<String> getAllUrls() throws SQLException {
    final ResultSet rs = connection.prepareStatement("SELECT product_id FROM masterlist").executeQuery();
    final List<String> result = new ArrayList<>();
    while (rs.next()) {
      result.add(rs.getString("product_id"));
    }
    return result;
  }


  @Override
  public void close() throws SQLException {
    connection.close();
  }
}
