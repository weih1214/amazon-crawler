package sg.edu.smu.webmining.crawler.pipeline;

import sg.edu.smu.webmining.crawler.storage.BasicRecord;
import sg.edu.smu.webmining.crawler.storage.FileStorage;
import sg.edu.smu.webmining.crawler.storage.Record;
import sg.edu.smu.webmining.crawler.storage.ex.StorageException;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

/**
 * Created by hwei on 6/1/2017.
 */
public class NewRecordPipeline implements Pipeline {

  private final FileStorage filestorage;

  public NewRecordPipeline(FileStorage filestorage) {
    this.filestorage = filestorage;
  }

  @Override
  public void process(ResultItems resultItems, Task task) {
    try {
      final Record record = filestorage.put(new BasicRecord(resultItems.get("Page Url"), resultItems.get("Page Content")));
      final String source = record.getId();
      resultItems.getAll().remove("Page Content");
      resultItems.getAll().remove("Page Url");
      resultItems.put("source", source);
    } catch (StorageException e) {
      e.printStackTrace();
    }
  }

}
