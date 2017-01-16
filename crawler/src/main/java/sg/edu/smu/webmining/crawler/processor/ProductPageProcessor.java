package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;
import sg.edu.smu.webmining.crawler.databasemanager.GeneralMongoDBManager;
import sg.edu.smu.webmining.crawler.downloader.nio.ProxyNHttpClientDownloader;
import sg.edu.smu.webmining.crawler.offlinework.ProductPage;
import sg.edu.smu.webmining.crawler.pipeline.GeneralMongoDBPipeline;
import sg.edu.smu.webmining.crawler.pipeline.NewRecordPipeline;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProvider;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProviderTimerWrap;
import sg.edu.smu.webmining.crawler.proxy.source.FPLNetSource;
import sg.edu.smu.webmining.crawler.proxy.source.SSLProxiesOrgSource;
import sg.edu.smu.webmining.crawler.seedpagefetcher.DBSeedpageManager;
import sg.edu.smu.webmining.crawler.storage.MysqlFileManager;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ProductPageProcessor implements PageProcessor {

  private static final Pattern PRODUCT_ID_PATTERN = Pattern.compile("/dp/([A-Z0-9]{10})/");

  private Site site = Site.me()
      .setRetryTimes(5)
      .setSleepTime(5000)
      .setCharset("UTF-8")
      .setCycleRetryTimes(50);

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
      System.out.println(page.getUrl().toString());
    }else {
      page.setSkip(true);
    }

  }

  @Override
  public Site getSite() {
    return site;
  }

  public static void main(String[] args) throws SQLException {

    final String[] seedpageList = new DBSeedpageManager("jdbc:mysql://127.0.0.1:3306/amazon", "root", "nrff201607").get();

    DynamicProxyProviderTimerWrap provider = new DynamicProxyProviderTimerWrap(
        new DynamicProxyProvider()
            .addSource(new FPLNetSource())
            .addSource(new SSLProxiesOrgSource())
    );

    try {
      provider.startAutoRefresh();

      try (final GeneralMongoDBManager mongoManager = new GeneralMongoDBManager("localhost", 27017, "ProductPage", "content")) {
        try (final MysqlFileManager mysqlFileStorage = new MysqlFileManager("jdbc:mysql://127.0.0.1:3306/product", "root", "nrff201607", "D:\\product")) {
          try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader(provider)) {

            Spider spider = Spider.create(new ProductPageProcessor())
                .setDownloader(downloader)
                .addPipeline(new NewRecordPipeline(mysqlFileStorage))
                .addPipeline(new GeneralMongoDBPipeline(mongoManager))
                .addUrl(seedpageList)
                .thread(5);

            long time = System.currentTimeMillis();
            spider.run();
            System.out.println("Finished in " + ((System.currentTimeMillis() - time) / 60000) + "m");
          }
        }
      }

    } catch (Throwable ex) {
      System.err.println("Uncaught exception - " + ex.getMessage());
      ex.printStackTrace(System.err);
    } finally {
      provider.stopAutoRefresh();
    }
  }
}
