package sg.edu.smu.webmining.crawler.storage;

import sg.edu.smu.webmining.crawler.storage.ex.StorageException;

/**
 * Created by mtkachenko.2015 on 28/11/2016.
 */
public interface FileStorage {

  /**
   * Puts a record into the storage. Updates the record (id and timestamp)
   *
   * @param record a record to be stored
   * @return updated record
   */
  public Record put(Record record) throws StorageException;

  /**
   * Gets record by the internal record id.
   *
   * @param id record id
   * @return stored record
   */
  public Record get(String id) throws StorageException;

}
