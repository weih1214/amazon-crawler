package sg.edu.smu.webmining.crawler.masterlist;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by mtkachenko.2015 on 16/11/2016.
 */
public class DummyMasterListManager implements MasterListManager {


  @Override
  public void update(String productId, String url) throws SQLException {
    System.out.println("ID: " + productId);
    System.out.println("URL: " + url);
  }

  @Override
  public List<String> getAllUrls() throws SQLException {
    return Arrays.asList(
        "https://www.amazon.com/dp/B01E3SNO1G/",
        "https://www.amazon.com/dp/B003EM8008/",
        "https://www.amazon.com/dp/B01E3SNO3E/"
    );
  }

}
