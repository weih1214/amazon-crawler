package sg.edu.smu.webmining.crawler.pipeline;

import org.apache.commons.collections.MapUtils;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.util.Map;

/**
 * Created by mtkachenko.2015 on 30/11/2016.
 */
public class ReviewDebugPipeline implements Pipeline {

  @Override
  public void process(ResultItems resultItems, Task task) {
    resultItems.getAll().forEach((s, o) -> {
      if (o instanceof Map) {
        MapUtils.debugPrint(System.out, s, (Map) o);
      }
    });
  }

}
