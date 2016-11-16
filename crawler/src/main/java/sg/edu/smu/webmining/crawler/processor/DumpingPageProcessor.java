package sg.edu.smu.webmining.crawler.processor;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.processor.PageProcessor;

import java.io.File;

/**
 * Created by mtkachenko.2015 on 16/11/2016.
 */
public class DumpingPageProcessor implements PageProcessor {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final PageProcessor inner;
  private final File dir;

  public DumpingPageProcessor(PageProcessor inner, File dir) {
    this.inner = inner;
    this.dir = dir;
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new RuntimeException("Cannot create dir: " + dir);
      }
    }
  }

  @Override
  public void process(Page page) {
    if (page != null && page.getRawText() != null) {
      try {
        FileUtils.writeStringToFile(new File(dir, RandomStringUtils.random(8, true, true) + ".html"), page.getRawText(), "UTF-8");
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
