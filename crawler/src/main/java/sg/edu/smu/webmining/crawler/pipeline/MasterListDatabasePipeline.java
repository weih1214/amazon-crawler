package sg.edu.smu.webmining.crawler.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.edu.smu.webmining.crawler.masterlist.MasterListManager;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.util.Iterator;
import java.util.List;

/**
 * Created by mtkachenko.2015 on 16/11/2016.
 */
public class MasterListDatabasePipeline implements Pipeline {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final MasterListManager manager;

  public MasterListDatabasePipeline(MasterListManager manager) {
    this.manager = manager;
  }

  @Override
  public void process(ResultItems resultItems, Task task) {
    final List<String> productList = resultItems.get("product_ids");
    final List<String> urlList = resultItems.get("urls");
    final String source = resultItems.get("source");
    System.out.println(source);

    final Iterator<String> productIdIter = productList.iterator();
    final Iterator<String> urlIter = urlList.iterator();

    while (productIdIter.hasNext()) {
      final String productId = productIdIter.next();
      final String url = urlIter.next();

      try {
        manager.update(productId, url, source);
      } catch (Exception e) {
        logger.error("exception happened, when updating db", e);
      }
    }

  }
}
