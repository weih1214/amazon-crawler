package sg.edu.smu.webmining.crawler.processor;

import org.apache.commons.lang.ArrayUtils;
import sg.edu.smu.webmining.crawler.config.Config;
import sg.edu.smu.webmining.crawler.db.MongoDBManager;
import sg.edu.smu.webmining.crawler.downloader.nio.ProxyNHttpClientDownloader;
import sg.edu.smu.webmining.crawler.downloader.nio.RawPage;
import sg.edu.smu.webmining.crawler.pipeline.FileStoragePipeline;
import sg.edu.smu.webmining.crawler.pipeline.GeneralMongoDBPipeline;
import sg.edu.smu.webmining.crawler.seedpagefetcher.DBSeedpageManager;
import sg.edu.smu.webmining.crawler.storage.MysqlFileManager;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hwei on 23/2/2017.
 */
public class ImagePageProcessor implements PageProcessor {

  private final static Pattern PATTERN_IMAGE_ID = Pattern.compile("/I/(.*)\\.jpg");

  private final Site site;

  public ImagePageProcessor(int cycleRetryTimes, int sleepTime, int retryTimes, String charset) {
    site = Site.me().setCycleRetryTimes(cycleRetryTimes).setSleepTime(sleepTime).setRetryTimes(retryTimes).setCharset(charset);
  }

  @Override
  public void process(Page page) {

    if (page instanceof RawPage) {
      byte[] rawContent = ((RawPage) page).getRawContent();
      final String url = page.getUrl().toString();
      String imageID = url;
      final Matcher m = PATTERN_IMAGE_ID.matcher(url);
      if (m.find()) {
        imageID = m.group(1);
      }
      Map<String, Object> map = new LinkedHashMap<>();
      map.put("Image ID", imageID);
      map.put("Reference ID", page.getRequest().getExtra("ID"));
      page.putField("Image", map);
      FileStoragePipeline.putStorageFields(page, url, rawContent);
    }
  }

  @Override
  public Site getSite() {
    return site;
  }

  public static void main(String[] args) throws SQLException, FileNotFoundException {

    final Config cf = new Config("D:\\config1.json");
    final Request[] seed1 = new DBSeedpageManager(cf.getMongoHostname(), cf.getMongoPort(), "ReviewPage", "content", Arrays.asList("Image List", "Review ID")).getRequestList().clone();
    final Request[] seed2 = new DBSeedpageManager(cf.getMongoHostname(), cf.getMongoPort(), "ProductPage", "content", Arrays.asList("Image ID", "Product ID")).getRequestList().clone();
    final Request[] seedpageRequest = (Request[])ArrayUtils.addAll(seed1, seed2);

    try {
      try (final MongoDBManager mongoManager = new MongoDBManager(cf.getMongoHostname(), cf.getMongoPort(), "ImagePage", "content")) {
        try (final MysqlFileManager mysqlFileStorage = new MysqlFileManager(cf.getMysqlHostname(), cf.getMysqlUsername(), cf.getMysqlPassword(), cf.getStorageDir())) {
          try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader()) {
            Spider spider = Spider.create(new ImagePageProcessor(cf.getCycleRetryTimes(), cf.getSleepTime(), cf.getRetryTimes(), cf.getCharset())).setDownloader(downloader).addPipeline(new FileStoragePipeline(mysqlFileStorage)).addPipeline(new GeneralMongoDBPipeline(mongoManager)).addRequest(seedpageRequest).thread(cf.getThreads());
            long time = System.currentTimeMillis();
            spider.run();
            System.out.println("Finished in " + ((System.currentTimeMillis() - time) / 60000) + "m");
          }
        }
      }
    } catch (Throwable ex) {
      System.err.println("Uncaught exception - " + ex.getMessage());
      ex.printStackTrace(System.err);
    }
  }
}
