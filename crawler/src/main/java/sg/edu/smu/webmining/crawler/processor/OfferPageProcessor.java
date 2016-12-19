package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import sg.edu.smu.webmining.crawler.datatype.Offer;
import sg.edu.smu.webmining.crawler.downloader.nio.ProxyNHttpClientDownloader;
import sg.edu.smu.webmining.crawler.mongodb.GeneralMongoDBManager;
import sg.edu.smu.webmining.crawler.pipeline.GeneralMongoDBPipeline;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProvider;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProviderTimerWrap;
import sg.edu.smu.webmining.crawler.proxy.source.FPLNetSource;
import sg.edu.smu.webmining.crawler.proxy.source.SSLProxiesOrgSource;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hwei on 16/12/2016.
 */
public class OfferPageProcessor implements PageProcessor{

  private static final Pattern PRODUCT_ID_PATTERN = Pattern.compile("(?:/offer-listing)?/([0-9A-Z]{10})/");

  private final Site site = Site.me()
      .setCycleRetryTimes(Integer.MAX_VALUE)
      .setSleepTime(1000)
      .setRetryTimes(3)
      .setCharset("UTF-8");

  public String extractProductId(String urlString) {
    final Matcher m = PRODUCT_ID_PATTERN.matcher(urlString);
    if(m.find()) {
      return m.group(1);
    }
    return null;
  }

  public String getNextLink(Element paginationElement) {
    final String s1 = paginationElement.className();
    if(s1.contains("a-disabled")) {
      return null;
    } else {
      return paginationElement.select("a").attr("href");
    }
  }

  @Override
  public void process(Page page) {
    final Document offerDoc = Jsoup.parse(page.getRawText(), "https://www.amazon.com");
    // Pagination Part
    final String nextLink = getNextLink(offerDoc.select("ul.a-pagination li").last());
    if (nextLink != null) {
      page.addTargetRequest(nextLink);
    }
    final Elements offerElements = offerDoc.select("div#olpOfferList div.olpOffer");
    final String productId = extractProductId(page.getUrl().toString());
    for (Element element: offerElements) {
      final Offer offer = new Offer(productId, element);
      page.putField(offer.getSellerId(), offer.asMap());
    }


  }

  @Override
  public Site getSite() {
    return site;
  }

  public static void main(String[] args) {

    final String testUrl = "https://www.amazon.com/gp/offer-listing/B003EM8008/ref=olp_page_1";

    DynamicProxyProviderTimerWrap provider = new DynamicProxyProviderTimerWrap(
        new DynamicProxyProvider()
            .addSource(new FPLNetSource())
            .addSource(new SSLProxiesOrgSource())
    );

    try {
      provider.startAutoRefresh();

      try (final GeneralMongoDBManager manager = new GeneralMongoDBManager("localhost", 27017, "OfferPage", "content")) {
        try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader(provider)) {

          Spider spider = Spider.create(new OfferPageProcessor())
              .setDownloader(downloader)
              .addPipeline(new GeneralMongoDBPipeline(manager))
              .addUrl(testUrl)
              .thread(5);

          long time = System.currentTimeMillis();
          spider.run();
          System.out.println("Finished in " + ((System.currentTimeMillis() - time) / 60000) + "m");
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
