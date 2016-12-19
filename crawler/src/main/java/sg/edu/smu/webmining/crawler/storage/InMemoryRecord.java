package sg.edu.smu.webmining.crawler.storage;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by mtkachenko.2015 on 19/12/2016.
 */
public class InMemoryRecord implements Record {

  private final String url;
  private final String content;

  private String id = null;

  private long timestamp = -1;

  private String md5 = null;

  public InMemoryRecord(String url, String content) {
    this.url = url;
    this.content = content;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return new ByteArrayInputStream(content.getBytes("UTF-8"));
  }

  @Override
  public String getContent() {
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
    if (md5 == null) {
      md5 = DigestUtils.md5Hex(getInputStream());
    }
    return md5;
  }

  @Override
  public String getId() {
    return id;
  }

  void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  void setId(String id) {
    this.id = id;
  }

}
