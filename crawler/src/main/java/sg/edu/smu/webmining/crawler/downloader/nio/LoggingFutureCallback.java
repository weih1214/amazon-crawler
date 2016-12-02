package sg.edu.smu.webmining.crawler.downloader.nio;

import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mtkachenko.2015 on 1/12/2016.
 */
public class LoggingFutureCallback<T> implements FutureCallback<T> {

  public static <T> LoggingFutureCallback<T> create() {
    return new LoggingFutureCallback<>(false);
  }

  public static <T> LoggingFutureCallback<T> create(boolean debug) {
    return new LoggingFutureCallback<>(debug);
  }

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final boolean debug;

  public LoggingFutureCallback(boolean debug) {
    this.debug = debug;
  }


  @Override
  public void completed(T result) {
    if (debug) {
      logger.info("request completed successfully");
    }
  }

  @Override
  public void failed(Exception ex) {
    if (debug) {
      logger.error("request failed with", ex);
    }
  }

  @Override
  public void cancelled() {
    if (debug) {
      logger.debug("request is cancelled");
    }
  }

}
