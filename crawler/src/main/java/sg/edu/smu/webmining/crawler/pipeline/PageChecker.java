package sg.edu.smu.webmining.crawler.pipeline;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

public class PageChecker implements Pipeline {

  @Override
  public void process(ResultItems resultItems, Task task) {
    try {
      String filename = resultItems.get("UrlName");
      PrintWriter printWriter = new PrintWriter(new File("D:/Data/Htmltext/" + filename + ".html"), "UTF-8");
      printWriter.print((String) resultItems.get("html"));
      printWriter.flush();
      printWriter.close();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

}
