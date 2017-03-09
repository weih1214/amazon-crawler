package sg.edu.smu.webmining.crawler.fixer;

/**
 * Created by hwei on 9/3/2017.
 */

import org.apache.commons.lang3.RandomStringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import sg.edu.smu.webmining.crawler.config.Config;
import sg.edu.smu.webmining.crawler.db.MongoDBManager;
import sg.edu.smu.webmining.crawler.downloader.nio.ProxyNHttpClientDownloader;
import sg.edu.smu.webmining.crawler.downloader.nio.RawPage;
import sg.edu.smu.webmining.crawler.parse.Offer;
import sg.edu.smu.webmining.crawler.pipeline.FileStoragePipeline;
import sg.edu.smu.webmining.crawler.pipeline.GeneralMongoDBPipeline;
import sg.edu.smu.webmining.crawler.seedpagefetcher.DBSeedpageManager;
import sg.edu.smu.webmining.crawler.storage.MysqlFileManager;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.io.IOException;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hwei on 16/12/2016.
 */
public class OfferPageFixer implements PageProcessor {

  private static final Pattern PRODUCT_ID_PATTERN = Pattern.compile("(?:/offer-listing)?/([0-9A-Z]{10})/");

  private final Site site;

  public OfferPageFixer(int cycleRetryTimes, int sleepTime, int retryTimes, String charset) {
    site = Site.me().setCycleRetryTimes(cycleRetryTimes).setSleepTime(sleepTime).setRetryTimes(retryTimes).setCharset(charset);
  }

  public String extractProductId(String urlString) {
    final Matcher m = PRODUCT_ID_PATTERN.matcher(urlString);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  public String getNextLink(Document doc) {
    final Elements offerElements = doc.select("ul.a-pagination li");
    if (!offerElements.isEmpty()) {
      final String s1 = offerElements.last().className();
      if (s1.contains("a-disabled")) {
        return null;
      } else {
        return doc.baseUri() + offerElements.last().select("a").attr("href");
      }
    }
    return null;
  }

  private boolean isFullList(Element fullListElement) {
    final String urlInfo = fullListElement.text();
    return !urlInfo.contains("Show all offers");
  }

  @Override
  public void process(Page page) {
    final Document offerDoc = Jsoup.parse(page.getRawText(), "https://www.amazon.com");
    // Check whether the page shows the full offer list or not
    final Element fullListElement = offerDoc.select("div#olpOfferList p.a-text-center a").last();
    if (!isFullList(fullListElement)) {
      page.addTargetRequest(fullListElement.attr("href"));
      page.setSkip(true);
      return;
    }
    // Pagination Part

    final String nextLink = getNextLink(offerDoc);
    if (nextLink != null) {
      page.addTargetRequest(nextLink);
    }
    final Elements offerElements = offerDoc.select("div#olpOfferList div.olpOffer");
    final String productId = extractProductId(page.getUrl().toString());
    for (Element element : offerElements) {
      page.putField(RandomStringUtils.random(10, true, true), new Offer(element, productId).asMap());
    }
    if (page instanceof RawPage) {
      byte[] rawContent = ((RawPage) page).getRawContent();
      FileStoragePipeline.putStorageFields(page, page.getUrl().toString(), rawContent);
    }
  }

  @Override
  public Site getSite() {
    return site;
  }

  public static void main(String[] args) throws SQLException, IOException {

    final Config cf = new Config("D:\\config1.json");
    final String[] seedpageList = new DBSeedpageManager().getFixerSeedpage(cf.getOfferFilePath());

    try {
      try (final MongoDBManager mongoManager = new MongoDBManager(cf.getMongoHostname(), cf.getMongoPort(), "OfferPage", "content")) {
        try (final MysqlFileManager mysqlFileStorage = new MysqlFileManager(cf.getMysqlHostname(), cf.getMysqlUsername(), cf.getMysqlPassword(), cf.getStorageDir())) {
          try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader()) {

            Spider spider = Spider.create(new OfferPageFixer(cf.getCycleRetryTimes(), cf.getSleepTime(), cf.getRetryTimes(), cf.getCharset()))
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

