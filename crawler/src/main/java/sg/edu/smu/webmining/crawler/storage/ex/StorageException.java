package sg.edu.smu.webmining.crawler.storage.ex;

/**
 * Created by mtkachenko.2015 on 11/1/2017.
 */
public class StorageException extends Exception {

  public StorageException(String message) {
    super(message);
  }

  public StorageException(String message, Throwable cause) {
    super(message, cause);
  }

  public StorageException(Throwable cause) {
    super(cause);
  }

  public StorageException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
