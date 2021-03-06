package sg.edu.smu.webmining.crawler.storage;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by mtkachenko.2015 on 28/11/2016.
 */
public interface Record {

  public InputStream getInputStream() throws IOException;

  public byte[] getRawContent() throws IOException;

  /**
   * @return URL of the stored content
   */
  public String getURL();

  /**
   * @return valid timestamp if the record is stored, -1 otherwise
   */
  public long getTimestamp();

  /**
   * @return md5 hash of the input stream
   */
  public String getMD5() throws IOException;

  /**
   * @return valid id if the record is stored, null otherwise
   */
  public String getId();

}
