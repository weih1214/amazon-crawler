package sg.edu.smu.webmining.crawler.playground;

import java.sql.*;

/**
 * Created by hwei on 23/12/2016.
 */
public class MysqlPlayground {
  public static void main(String[] args) {
//    final String content = "HAHAHAHA";
//    final String md5 = "sdsdaxc12abnj222";
//    final String url = "www.amazon.com";
//    try {
//      Connection connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/play", "root", "nrff201607");
//      final PreparedStatement insert = connection.prepareStatement("INSERT INTO table1 (CONTENT,MD5,URL) VALUES (?,?,?)");
//      final PreparedStatement idfetcher = connection.prepareStatement("SELECT ID FROM table1 WHERE MD5 = ?");
//      insert.setString(1, content);
//      insert.setString(2, md5);
//      insert.setString(3, url);
//      if (insert.executeUpdate() == 1) {
//        idfetcher.setString(1, md5);
//        ResultSet resultSet = idfetcher.executeQuery();
//        while (resultSet.next()) {
//          System.out.println(resultSet.getString("ID"));
//        }
//      }
//
//    } catch (SQLException e) {
//      e.printStackTrace();
//    }
    try {
      Connection connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/play", "root", "nrff201607");
      final PreparedStatement checkStatement = connection.prepareStatement("SELECT * FROM test WHERE id = ?");
      final PreparedStatement updateStatement = connection.prepareStatement("UPDATE test SET info = ? WHERE id = 1");
      checkStatement.setInt(1,2);
      ResultSet rs = checkStatement.executeQuery();
      if (rs.next()) {
        updateStatement.setString(1, "XYZ");
        updateStatement.executeUpdate();
      }
      } catch (SQLException e1) {
      e1.printStackTrace();
    }


  }
}
