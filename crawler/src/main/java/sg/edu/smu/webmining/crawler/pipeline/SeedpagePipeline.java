package sg.edu.smu.webmining.crawler.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.edu.smu.webmining.crawler.databasemanager.MongoDBManager;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

/**
 * Created by hwei on 24/1/2017.
 */
public class SeedpagePipeline implements Pipeline {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final MongoDBManager manager;

  public SeedpagePipeline(MongoDBManager manager) {
    this.manager = manager;
  }

  @Override
  public void process(ResultItems resultItems, Task task) {
    final String seedpage = resultItems.get("seedpage");
    final Integer totalProducts = resultItems.get("total products");
    if (seedpage == null || totalProducts == null) {
      return;
    }
    resultItems.getAll().remove("seedpage");
    resultItems.getAll().remove("total products");
    try {
      manager.update(seedpage, totalProducts);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
