package sg.edu.smu.webmining.crawler.pipeline;

import sg.edu.smu.webmining.crawler.storage.FileManager;
import sg.edu.smu.webmining.crawler.storage.ex.StorageException;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

/**
 * Created by hwei on 6/1/2017.
 */
public class NewRecordPipeline implements Pipeline {

  private final FileManager filestorage;

  public NewRecordPipeline(FileManager filestorage) {
    this.filestorage = filestorage;
  }

  @Override
  public void process(ResultItems resultItems, Task task) {
    try {
      final String source = filestorage.put(resultItems.get("Page Url"), (String) resultItems.get("Page Content"));
      resultItems.getAll().remove("Page Content");
      resultItems.getAll().remove("Page Url");
      resultItems.put("source", source);
    } catch (StorageException e) {
      e.printStackTrace();
    }
  }

}
