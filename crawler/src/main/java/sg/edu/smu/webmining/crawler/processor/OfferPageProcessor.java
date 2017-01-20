package sg.edu.smu.webmining.crawler.processor;

import org.apache.commons.lang3.RandomStringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import sg.edu.smu.webmining.crawler.databasemanager.GeneralMongoDBManager;
import sg.edu.smu.webmining.crawler.datatype.Offer;
import sg.edu.smu.webmining.crawler.downloader.nio.ProxyNHttpClientDownloader;
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

/**
 * Created by hwei on 16/12/2016.
 */
public class OfferPageProcessor implements PageProcessor {

  private static final Pattern PRODUCT_ID_PATTERN = Pattern.compile("(?:/offer-listing)?/([0-9A-Z]{10})/");

  private final Site site = Site.me()
      .setCycleRetryTimes(Integer.MAX_VALUE)
      .setSleepTime(1000)
      .setRetryTimes(3)
      .setCharset("UTF-8");

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
        StringBuilder sb = new StringBuilder(doc.baseUri());
        sb.append(offerElements.last().select("a").attr("href"));
        return sb.toString();
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
      final Offer offer = new Offer(productId, element);
      page.putField(RandomStringUtils.random(10, true, true), offer.asMap());
    }
    page.putField("Page content", page.getRawText());
    page.putField("Page url", page.getUrl().toString());
  }

  @Override
  public Site getSite() {
    return site;
  }

  public static void main(String[] args) throws SQLException {

    final String[] seedpageList = new DBSeedpageManager("localhost", 27017, "ProductPage", "content", "Offer Link").get();

    DynamicProxyProviderTimerWrap provider = new DynamicProxyProviderTimerWrap(
        new DynamicProxyProvider()
            .addSource(new FPLNetSource())
            .addSource(new SSLProxiesOrgSource())
    );

    try {
      provider.startAutoRefresh();

      try (final GeneralMongoDBManager mongoManager = new GeneralMongoDBManager("localhost", 27017, "OfferPage", "content")) {
        try (final MysqlFileManager mysqlFileStorage = new MysqlFileManager("jdbc:mysql://127.0.0.1:3306/record", "root", "nrff201607", "D:\\Offer")) {
          try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader(provider)) {

            Spider spider = Spider.create(new OfferPageProcessor())
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
