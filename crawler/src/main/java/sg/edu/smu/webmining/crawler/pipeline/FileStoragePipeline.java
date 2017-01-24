package sg.edu.smu.webmining.crawler.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.edu.smu.webmining.crawler.storage.FileManager;
import sg.edu.smu.webmining.crawler.storage.ex.StorageException;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

/**
 * Created by hwei on 6/1/2017.
 */
public class FileStoragePipeline implements Pipeline {

  public static final String PAGE_URL = "Page url";
  public static final String PAGE_CONTENT = "Page content";

  public static final String SOURCE_FIELD = "source";

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final FileManager storage;

  public FileStoragePipeline(FileManager storage) {
    this.storage = storage;
  }

  @Override
  public void process(ResultItems resultItems, Task task) {
    try {
      final String source = storage.put(resultItems.get(PAGE_URL), (String) resultItems.get(PAGE_CONTENT));
      logger.info("raw file successfully stored: {}", source);
      resultItems.getAll().remove(PAGE_CONTENT);
      resultItems.getAll().remove(PAGE_URL);
      resultItems.put(SOURCE_FIELD, source);
    } catch (StorageException e) {
      logger.error("storage failure", e);
    }
  }

}
