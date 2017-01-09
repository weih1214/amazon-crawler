package sg.edu.smu.webmining.crawler.pipeline;

import sg.edu.smu.webmining.crawler.storage.FileStorage;
import sg.edu.smu.webmining.crawler.storage.InMemoryRecord;
import sg.edu.smu.webmining.crawler.storage.Record;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.io.IOException;

/**
 * Created by hwei on 6/1/2017.
 */
public class NewRecordPipeline implements Pipeline{

  private final FileStorage filestorage;
  private final Pipeline inner;

  public NewRecordPipeline(FileStorage filestorage, Pipeline inner) {

    this.filestorage = filestorage;
    this.inner = inner;
  }

  @Override
  public void process(ResultItems resultItems, Task task) {
    final InMemoryRecord record = new InMemoryRecord(resultItems.get("Page Url"), resultItems.get("Page Content"));
    Integer source = null;
    try {
      final Record updatedRecord = filestorage.put(record);
      source = updatedRecord.getId();
    } catch (IOException e) {
      e.printStackTrace();
    }
    resultItems.getAll().remove("Page Content");
    resultItems.getAll().remove("Page Url");
    resultItems.put("source", source);
    inner.process(resultItems, task);
  }
}
