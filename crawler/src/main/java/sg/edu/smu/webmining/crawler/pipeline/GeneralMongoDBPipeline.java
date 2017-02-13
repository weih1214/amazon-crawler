package sg.edu.smu.webmining.crawler.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.edu.smu.webmining.crawler.db.MongoDBManager;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

/**
 * Created by hwei on 13/12/2016.
 */
public class GeneralMongoDBPipeline implements Pipeline {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final MongoDBManager manager;

  public GeneralMongoDBPipeline(MongoDBManager manager) {
    this.manager = manager;
  }

  @Override
  public void process(ResultItems resultItems, Task task) {
    final String source = resultItems.get(FileStoragePipeline.SOURCE_FIELD);
    resultItems.getAll().remove(FileStoragePipeline.SOURCE_FIELD);

    for (String key : resultItems.getAll().keySet()) {
      try {
        manager.addRecord(resultItems.get(key), source);
      } catch (Exception e) {
        logger.error("exception happened, when updating db", e);
      }
    }
  }

}
