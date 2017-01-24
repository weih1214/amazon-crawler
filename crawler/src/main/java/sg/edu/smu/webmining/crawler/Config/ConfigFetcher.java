package sg.edu.smu.webmining.crawler.Config;

import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * Created by hwei on 24/1/2017.
 */
public class ConfigFetcher {
  private final FileInputStream file;
  private final JSONObject obj;

  public ConfigFetcher(String fileName) throws FileNotFoundException {
    this.file = new FileInputStream(fileName);
    this.obj = new JSONObject(new JSONTokener(file));
  }

  public String getMongoHostname() {
    return obj.getString("MongoHostname");
  }

  public Integer getMongoPort() {
    return obj.getInt("MongoPort");
  }

  public String getMysqlHostname() {
    return obj.getString("MysqlHostname");
  }

  public String getMysqlUsername() {
    return obj.getString("MysqlUsername");
  }

  public String getMysqlPassword() {
    return obj.getString("MysqlPassword");
  }

  public String getStoragedir() {
    return obj.getString("Storagedir");
  }
}
