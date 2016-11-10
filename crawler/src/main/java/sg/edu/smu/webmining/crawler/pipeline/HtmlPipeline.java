package sg.edu.smu.webmining.crawler.pipeline;

import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class HtmlPipeline implements Pipeline {

  public void process(ResultItems resultItems, Task task) {

    try {
      String filename = resultItems.get("Filename");
      PrintWriter printWriter = new PrintWriter(new File("E:/Htmltext/" + filename + ".html"), "UTF-8");
      printWriter.print((String)resultItems.get("html"));
      printWriter.flush();
      printWriter.close();
    } catch (FileNotFoundException | UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }
}

