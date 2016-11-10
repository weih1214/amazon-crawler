package sg.edu.smu.webmining.crawler.pipeline;

import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.sql.*;
import java.util.List;

public class DatabasePipeline implements Pipeline {
  public void process(ResultItems resultItems, Task task) {

    // Convert hash map(links) into a String list
    // System.out.println(resultItems.get("product_id"));
    List<String> product_list = resultItems.get("product_ids");
    List<String> urls_list = resultItems.get("urls");

    // Set driver name and login information
    String driver = "com.mysql.jdbc.Driver";
    String url = "jdbc:mysql://127.0.0.1:3306/amazon";
    String user = "root";
    String password = "nrff201607";
    PreparedStatement insert = null;
    PreparedStatement check = null;
    PreparedStatement update = null;
    // Set connection and insert into database
    try {
      Class.forName(driver);
      Connection conn = DriverManager.getConnection(url, user, password);
      String insert_str = "INSERT INTO MASTERLIST(PRODUCT_ID,RECENT_UPDATE,URLS,first_update) VALUES(?,current_timestamp,?,current_timestamp)";
      String check_str = "select id from masterlist where product_id = ?";
      String update_str = "UPDATE masterlist set recent_update = ? where product_id = ?";
      insert = conn.prepareStatement(insert_str);
      check = conn.prepareStatement(check_str);
      update = conn.prepareStatement(update_str);

      for (int i = 0; i < product_list.size(); i = i + 1) {
        check.setString(1, product_list.get(i));
        ResultSet rs = check.executeQuery();
        if (!rs.next()) {
          insert.setString(1, product_list.get(i));
          insert.setString(2, urls_list.get(i));
          // insert.setTimestamp(3,
          // java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()));
          insert.executeUpdate();
        } else {
          update.setTimestamp(1, Timestamp.valueOf(java.time.LocalDateTime.now()));
          update.setString(2, product_list.get(i));
          update.executeUpdate();
        }
      }
      if (!conn.isClosed())
        System.out.println("Succeeded connecting to the Database!");
      conn.close();
    } catch (ClassNotFoundException e) {
      System.out.println("Sorry,can`t find the Driver!");
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
