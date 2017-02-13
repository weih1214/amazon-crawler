package sg.edu.smu.webmining.crawler.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.edu.smu.webmining.crawler.storage.FileManager;
import sg.edu.smu.webmining.crawler.storage.ex.StorageException;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

/**
 * Created by hwei on 6/1/2017.
 */
public class FileStoragePipeline implements Pipeline {

  public static final String SOURCE_FIELD = "$Source";

  private static final String PAGE_URL = "$PageUrl";
  private static final String PAGE_CONTENT = "$PageContent";

  public static void putStorageFields(Page page, String url, String content) {
    page.putField(PAGE_URL, url);
    page.putField(PAGE_CONTENT, content);
  }

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final FileManager storage;

  public FileStoragePipeline(FileManager storage) {
    this.storage = storage;
  }

  @Override
  public void process(ResultItems resultItems, Task task) {
    final String url = resultItems.get(PAGE_URL);
    final String content = resultItems.get(PAGE_CONTENT);
    resultItems.getAll().remove(PAGE_CONTENT);
    resultItems.getAll().remove(PAGE_URL);
    if (url == null || content == null) {
      logger.info("url or content of the page is not set");
      return;
    }
    try {
      final String source = storage.put(url, content);
      resultItems.put(SOURCE_FIELD, source);
      logger.info("raw file successfully stored: {}", source);
    } catch (StorageException e) {
      logger.error("storage failure", e);
    }
  }

}
