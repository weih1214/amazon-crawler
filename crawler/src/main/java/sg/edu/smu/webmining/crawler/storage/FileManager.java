package sg.edu.smu.webmining.crawler.storage;

import sg.edu.smu.webmining.crawler.storage.ex.StorageException;

import java.io.InputStream;

/**
 * Created by mtkachenko.2015 on 28/11/2016.
 */
public interface FileManager {

  public String put(String url, byte[] rawContent) throws StorageException;

  public String put(String url, InputStream content) throws StorageException;

  /**
   * Gets record by the internal record id.
   *
   * @param id record id
   * @return stored record
   */
  public Record get(String id) throws StorageException;

}
