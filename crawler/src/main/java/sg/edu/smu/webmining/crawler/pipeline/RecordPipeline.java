package sg.edu.smu.webmining.crawler.pipeline;

import org.apache.commons.codec.digest.DigestUtils;
import sg.edu.smu.webmining.crawler.databasemanager.MysqlManager;
import sg.edu.smu.webmining.crawler.storage.InMemoryRecord;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

/**
 * Created by hwei on 21/12/2016.
 */
public class RecordPipeline implements Pipeline {

  private final MysqlManager manager;

  private final Pipeline inner;
  private String source = null;

  public RecordPipeline(Pipeline inner, MysqlManager manager) {

    this.inner = inner;
    this.manager = manager;
  }

  @Override
  public void process(ResultItems resultItems, Task task) {
    final InMemoryRecord record = new InMemoryRecord(resultItems.get("Page Url"), resultItems.get("Page Content"));
    final String content = record.getContent();
    final String md5 = DigestUtils.md5Hex(content);
    final String url = record.getURL();
    try {
      if (manager.update(md5, url, content)) {
        source = manager.fetchId(md5);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } resultItems.getAll().remove("Page Content");
    resultItems.getAll().remove("Page Url");
    resultItems.put("source", source);
    inner.process(resultItems, task);

  }
}
