package sg.edu.smu.webmining.crawler.seedpagefetcher;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hwei on 16/1/2017.
 */
public class DBSeedpageManager implements SeedpageManager {

  private final Connection connection;
  private final PreparedStatement fetchStatement;

  public DBSeedpageManager(String location, String username, String password) throws SQLException {
    connection = DriverManager.getConnection(location, username, password);
    fetchStatement = connection.prepareStatement("SELECT url FROM masterlist");
  }

  @Override
  public String[] get() throws SQLException {
    final List<String> seedpageList = new ArrayList<>();
    ResultSet rs = fetchStatement.executeQuery();
    while (rs.next()) {
      seedpageList.add(rs.getString("url"));
    }
    String[] seedpage = new String[seedpageList.size()];
    seedpage = seedpageList.toArray(seedpage);
    return seedpage;
  }
}
