package sg.edu.smu.webmining.crawler.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.edu.smu.webmining.crawler.db.MongoDBManager;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.util.Iterator;
import java.util.List;

/**
 * Created by hwei on 17/1/2017.
 */
public class MasterListMongoDBPipeline implements Pipeline {

  private static final String PRODUCT_IDS = "$ProductIDs";
  private static final String URLS = "$URLs";

  public static void putMasterListFields(Page page, List<String> urls, List<String> products) {
    page.putField(URLS, urls);
    page.putField(PRODUCT_IDS, products);
  }

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final MongoDBManager manager;

  public MasterListMongoDBPipeline(MongoDBManager manager) {
    this.manager = manager;
  }

  @Override
  public void process(ResultItems resultItems, Task task) {
    final String source = resultItems.get(FileStoragePipeline.SOURCE_FIELD);
    resultItems.getAll().remove(FileStoragePipeline.SOURCE_FIELD);

    final List<String> productList = resultItems.get(PRODUCT_IDS);
    final List<String> urlList = resultItems.get(URLS);
    if (productList == null || urlList == null) {
      return;
    }
    final Iterator<String> productIdIter = productList.iterator();
    final Iterator<String> urlIter = urlList.iterator();

    while (productIdIter.hasNext()) {
      final String productId = productIdIter.next();
      final String url = urlIter.next();

      try {
        manager.addUrlRecord(productId, url, source);
      } catch (Exception e) {
        logger.error("exception happened, when updating db", e);
      }
    }
  }
}
