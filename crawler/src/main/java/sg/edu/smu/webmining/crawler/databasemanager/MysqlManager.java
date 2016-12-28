package sg.edu.smu.webmining.crawler.databasemanager;

import java.sql.SQLException;

/**
 * Created by hwei on 27/12/2016.
 */
public interface MysqlManager {
  public boolean update(String md5, String url, String content) throws Exception;
  public String fetchId(String md5) throws SQLException;
}
