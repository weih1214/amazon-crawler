package sg.edu.smu.webmining.crawler.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.edu.smu.webmining.crawler.db.MongoDBManager;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

/**
 * Created by hwei on 24/1/2017.
 */
public class SeedPagePipeline implements Pipeline {

  private static final String NUM_PRODUCTS = "$NumProducts";
  private static final String SEED_PAGE = "$SeedPage";

  public static void putSeedPageFields(Page page, String url, Integer numProducts) {
    page.putField(SEED_PAGE, url);
    page.putField(NUM_PRODUCTS, numProducts);
  }

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final MongoDBManager manager;

  public SeedPagePipeline(MongoDBManager manager) {
    this.manager = manager;
  }

  @Override
  public void process(ResultItems resultItems, Task task) {
    final String url = resultItems.get(SEED_PAGE);
    final Integer totalProducts = resultItems.get(NUM_PRODUCTS);
    resultItems.getAll().remove(SEED_PAGE);
    resultItems.getAll().remove(NUM_PRODUCTS);
    if (url == null || totalProducts == null) {
      logger.info("url or numProducts is not set");
      return;
    }
    try {
      manager.addSeedRecord(url, totalProducts);
    } catch (Exception e) {
      logger.error("exception happened, when updating db", e);
    }
  }

}
