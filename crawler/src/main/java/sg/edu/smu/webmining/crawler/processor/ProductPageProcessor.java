package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;
import sg.edu.smu.webmining.crawler.config.Config;
import sg.edu.smu.webmining.crawler.databasemanager.GeneralMongoDBManager;
import sg.edu.smu.webmining.crawler.downloader.nio.ProxyNHttpClientDownloader;
import sg.edu.smu.webmining.crawler.parse.ProductPage;
import sg.edu.smu.webmining.crawler.pipeline.FileStoragePipeline;
import sg.edu.smu.webmining.crawler.pipeline.GeneralMongoDBPipeline;
import sg.edu.smu.webmining.crawler.seedpagefetcher.DBSeedpageManager;
import sg.edu.smu.webmining.crawler.storage.MysqlFileManager;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ProductPageProcessor implements PageProcessor {

  private static final Pattern PRODUCT_ID_PATTERN = Pattern.compile("/dp/([A-Z0-9]{10})/");

  private final Site site;

  public ProductPageProcessor(int cycleRetryTimes, int sleepTime, int retryTimes, String charset) {
    site = Site.me().setCycleRetryTimes(cycleRetryTimes).setSleepTime(sleepTime).setRetryTimes(retryTimes).setCharset(charset);
  }


  @Override
  public void process(Page page) {
    final org.jsoup.nodes.Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com");
    final Matcher m = PRODUCT_ID_PATTERN.matcher(page.getUrl().toString());
    if (m.find()) {
      final String productId = m.group(1);
      ProductPage content = new ProductPage(doc, productId);
      page.putField(productId, content.asMap());
      page.putField("Page content", page.getRawText());
      page.putField("Page url", page.getUrl().toString());
    } else {
      page.setSkip(true);
    }

  }

  @Override
  public Site getSite() {
    return site;
  }

  public static void main(String[] args) throws SQLException, FileNotFoundException {

    final Config cf = new Config(args[0]);
    final String[] seedpageList = new DBSeedpageManager(cf.getMongoHostname(), cf.getMongoPort(), "Masterlist", "content", "Url").get();

    try {
      try (final GeneralMongoDBManager mongoManager = new GeneralMongoDBManager(cf.getMongoHostname(), cf.getMongoPort(), "ProductPage", "content")) {
        try (final MysqlFileManager mysqlFileStorage = new MysqlFileManager(cf.getMysqlHostname(), cf.getMysqlUsername(), cf.getMysqlPassword(), cf.getStorageDir())) {
          try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader()) {

            Spider spider = Spider.create(new ProductPageProcessor(cf.getCycleRetryTimes(), cf.getSleepTime(), cf.getRetryTimes(), cf.getCharset()))
                .setDownloader(downloader)
                .addPipeline(new FileStoragePipeline(mysqlFileStorage))
                .addPipeline(new GeneralMongoDBPipeline(mongoManager))
                .addUrl(seedpageList)
                .thread(cf.getThreads());

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
