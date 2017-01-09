package sg.edu.smu.webmining.crawler.storage;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by hwei on 6/1/2017.
 */
public class StorageRecord implements Record {

  private final String url;
  private final String content;
  private final Integer id;
  private final String fileLocation;
  private final String md5;
  private final InputStream in;
  private long timestamp = -1;

  public StorageRecord(String url, String content, Integer id, String fileLocation, String md5, InputStream in) {
    this.url = url;
    this.content = content;
    this.id = id;
    this.fileLocation = fileLocation;
    this.md5 = md5;
    this.in = in;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return in;
  }

  @Override
  public String getContent() throws IOException {
    return content;
  }

  @Override
  public String getURL() {
    return url;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String getFingerprint() throws IOException {
    return md5;
  }

  @Override
  public Integer getId() {
    return id;
  }

  @Override
  public String getLocation() {
    return fileLocation;
  }

  @Override
  public void setId(Integer id) {

  }

  @Override
  public void setLocation(String fullPath) {

  }

}
