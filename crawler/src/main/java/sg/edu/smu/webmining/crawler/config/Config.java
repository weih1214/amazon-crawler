package sg.edu.smu.webmining.crawler.config;

import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * Created by hwei on 24/1/2017.
 */
public class Config {

  private String mongoHostname;
  private int mongoPort;

  private String mysqlHostname;
  private String mysqlUsername;
  private String mysqlPassword;

  private String storageDir;

  public Config(String filename) throws FileNotFoundException {
    final JSONObject obj = new JSONObject(new JSONTokener(new FileInputStream(filename)));
    mongoHostname = obj.getString("MongoHostname");
    mongoPort = obj.getInt("MongoPort");
    mysqlHostname = obj.getString("MysqlHostname");
    mysqlUsername = obj.getString("MysqlUsername");
    mysqlPassword = obj.getString("MysqlPassword");
    storageDir = obj.getString("StorageDir");
  }

  public String getMongoHostname() {
    return mongoHostname;
  }

  public Integer getMongoPort() {
    return mongoPort;
  }

  public String getMysqlHostname() {
    return mysqlHostname;
  }

  public String getMysqlUsername() {
    return mysqlUsername;
  }

  public String getMysqlPassword() {
    return mysqlPassword;
  }

  public String getStorageDir() {
    return storageDir;
  }
}
