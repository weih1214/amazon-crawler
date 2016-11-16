package sg.edu.smu.webmining.crawler.pipeline;

import org.apache.commons.io.FileUtils;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.io.File;
import java.io.IOException;

public class HtmlPipeline implements Pipeline {

  private final File dir;

  public HtmlPipeline(String dirPath) {
    this(new File(dirPath));
  }

  public HtmlPipeline(File dir) {
    this.dir = dir;
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new RuntimeException("Cannot create dir: " + dir);
      }
    }
  }

  public void process(ResultItems resultItems, Task task) {
    try {
      final String filename = resultItems.get("filename");
      final String html = resultItems.get("raw_html");
      FileUtils.writeStringToFile(new File(dir, filename + ".html"), html, "UTF-8");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}

