package sg.edu.smu.webmining.crawler.processor;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.processor.PageProcessor;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by mtkachenko.2015 on 16/11/2016.
 */
public class DumpingPageProcessor implements PageProcessor {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final PageProcessor inner;
  private final File dir;

  private final AtomicInteger counter;

  public DumpingPageProcessor(PageProcessor inner, File dir) {
    this.inner = inner;
    this.dir = dir;
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new RuntimeException("Cannot create dir: " + dir);
      }
    }
    this.counter = new AtomicInteger(0);
  }

  @Override
  public void process(Page page) {
    if (page != null && page.getRawText() != null) {
      try {
        FileUtils.writeStringToFile(new File(dir, String.format("%08d", counter.getAndIncrement()) + ".html"), page.getRawText(), "UTF-8");
      } catch (Exception e) {
        logger.error("cannot write the html", e);
      }
    }

    inner.process(page);
  }

  @Override
  public Site getSite() {
    return inner.getSite();
  }

}
