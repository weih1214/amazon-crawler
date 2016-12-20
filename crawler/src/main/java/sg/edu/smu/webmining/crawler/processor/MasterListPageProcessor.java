package sg.edu.smu.webmining.crawler.processor;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.edu.smu.webmining.crawler.downloader.nio.ProxyNHttpClientDownloader;
import sg.edu.smu.webmining.crawler.masterlist.MySqlMasterListManager;
import sg.edu.smu.webmining.crawler.pipeline.MasterListDatabasePipeline;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProvider;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProviderTimerWrap;
import sg.edu.smu.webmining.crawler.proxy.source.FPLNetSource;
import sg.edu.smu.webmining.crawler.proxy.source.InCloakSource;
import sg.edu.smu.webmining.crawler.proxy.source.SSLProxiesOrgSource;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class MasterListPageProcessor implements PageProcessor {

  private static final Pattern HIGH_PRICE_P = Pattern.compile("&high-price=([\\d.]+)");
  private static final Pattern LOW_PRICE_P = Pattern.compile("&low-price=([\\d.]+)");

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final Site site = Site.me()
      .setCycleRetryTimes(Integer.MAX_VALUE)
      .setSleepTime(1000)
      .setRetryTimes(3)
      .setCharset("UTF-8");

  private boolean isSearchStarted = false;
  private boolean isSeedPageVisited = false;
  private String seedPage = null;

  private Double maxPrice = null;
  private Double minPrice = null;

  private int maxNumItems = 9600;


  /*
   * This method is to check whether the page has items lee than 9600. If yes,
   * parse it; else just skip and further divide the range.
   */
  private int getNumItems(Page page) {
    int items = 0;
    final String no_item_string = page.getHtml()
        .xpath("//*[@class='a-size-base a-spacing-small a-spacing-top-small a-text-normal']/text()").toString();
    if (no_item_string != null) {
      final Pattern pattern1 = Pattern.compile("of (\\d+(?:,\\d+)*)");
      final Pattern pattern2 = Pattern.compile("(\\d+)");
      Matcher m;

      m = pattern1.matcher(no_item_string);
      if (m.find()) {
        items = Integer.parseInt(m.group(1).replaceAll("[,]", ""));
      } else {
        m = pattern2.matcher(no_item_string);
        if (m.find()) {
          items = Integer.parseInt(m.group());
        }
      }
    }

    logger.info("# of items = {}", items);

    return items;
  }

  private synchronized void visitSeedPage(Page page) {
    if (!isSeedPageVisited) {
      seedPage = page.getUrl().toString();
      page.addTargetRequest(seedPage + "&sort=price-asc-rank");
      page.addTargetRequest(seedPage + "&sort=price-desc-rank");
      page.setSkip(true);
      isSeedPageVisited = true;
      logger.info("seed page is visited");
    }
  }

  private List<Double> getAllPrices(Page page) {
    final List<String> strPrices = page.getHtml()
        .xpath("//*[@class='a-size-base a-color-price a-text-bold'or@class='a-size-base a-color-price s-price a-text-bold'or@class='a-size-base a-color-price'or@class='sx-price-whole']/text()")
        .all();
    final List<Double> result = new ArrayList<>(strPrices.size());

    for (String s : strPrices) {
      result.add(Double.parseDouble(s.replaceAll("[$,]", "")));
    }

    return result;
  }

  private synchronized void extractPriceRange(Page page) {
    try {
      final String url = page.getUrl().toString();
      if (url.contains("&sort=price-desc-rank")) {
        maxPrice = Collections.max(getAllPrices(page));
        logger.info("MaxPrice is set to {}", maxPrice);
      }

      if (url.contains("&sort=price-asc-rank")) {
        minPrice = Collections.min(getAllPrices(page));
        logger.info("MinPrice is set to {}", minPrice);
      }
    } catch (Exception e) {
      e.printStackTrace();
      page.addTargetRequest(page.getUrl().toString());
    }

    page.setSkip(true);
  }

  private Pair<Double, Double> getPagePriceRange(Page page) {
    final String url = page.getUrl().toString();

    Double highPrice = null;
    Double lowPrice = null;

    Matcher m;
    m = HIGH_PRICE_P.matcher(url);
    if (m.find()) {
      highPrice = Double.parseDouble(m.group(1));
    }
    m = LOW_PRICE_P.matcher(url);
    if (m.find()) {
      lowPrice = Double.parseDouble(m.group(1));
    }

    if (highPrice != null && lowPrice != null) {
      return Pair.of(lowPrice, highPrice);
    }

    logger.error("cannot extract price range, throwing exception");
    throw new RuntimeException("Cannot extract the page price range");
  }

  private boolean checkBoundary(Pair<Double, Double> priceRange) {
    final Double lowerBoundary = 100 * priceRange.getLeft();
    final Double upperBoundary = 100 * priceRange.getRight();
    if (lowerBoundary.intValue() == upperBoundary.intValue()) {
      return false;
    }
    return true;
  }

  private void contentProcess(Page page) {
    page.putField("product_ids", page.getHtml().css(".a-link-normal.s-access-detail-page.a-text-normal")
        .links().regex("/dp/(.*?)/").all());
    page.putField("urls", page.getHtml().css(".a-link-normal.s-access-detail-page.a-text-normal").links().all());

    final String nextPageUrl = page.getHtml().css("#pagnNextLink").links().toString();
    if (nextPageUrl != null) {
      page.addTargetRequest(nextPageUrl);
      logger.info("next page scheduled {}", nextPageUrl);
    }
  }

  @Override
  public void process(Page page) {
    logger.info("processing page {}", page.getUrl().toString());
    if (!isSearchStarted) {

      // store seedpage & addtarget(descending & ascending)
      if (!isSeedPageVisited) {
        visitSeedPage(page);
      }

      if (minPrice == null || maxPrice == null) {
        extractPriceRange(page);
      }

      if (minPrice != null && maxPrice != null) {
        final String url = seedPage + "&low-price=" + minPrice + "&high-price=" + maxPrice;
        page.addTargetRequest(url);
        isSearchStarted = true;
        logger.info("search started");
      }

    } else {
      final int numItems = getNumItems(page);
      if (numItems == 0) {
        logger.warn("skipping page (it has no items) {}", page.getUrl().toString());
        page.setSkip(true);
      } else if (numItems > maxNumItems) {
        final Pair<Double, Double> priceRange = getPagePriceRange(page);
        // Return true if range is splitable; return false,then start process
        if (checkBoundary(priceRange)) {
          final double mid = (priceRange.getLeft() + priceRange.getRight()) / 2;
          logger.info("splitting price range [{}-{}], [{}-{}]", priceRange.getLeft(), mid, mid,
              priceRange.getRight());

          final String leftPartUrl = seedPage + "&low-price=" + priceRange.getLeft() + "&high-price=" + mid;
          page.addTargetRequest(leftPartUrl);
          logger.info("url scheduled {}", leftPartUrl);

          final String rightPartUrl = seedPage + "&low-price=" + mid + "&high-price=" + priceRange.getRight();
          page.addTargetRequest(rightPartUrl);
          logger.info("url scheduled {}", rightPartUrl);

          page.setSkip(true);
        } else {
          contentProcess(page);
        }
      } else {
        contentProcess(page);
      }
    }
  }

  @Override
  public Site getSite() {
    return site;
  }

  public static void main(String[] args) {
    DynamicProxyProviderTimerWrap provider = new DynamicProxyProviderTimerWrap(
        new DynamicProxyProvider()
            .addSource(new FPLNetSource())
            .addSource(new SSLProxiesOrgSource())
            .addSource(new InCloakSource())
    );

    try {
      provider.startAutoRefresh();

      try (final MySqlMasterListManager manager = new MySqlMasterListManager("jdbc:mysql://127.0.0.1:3306/amazon", "root", "nrff201607")) {
        try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader(provider)) {

          Spider spider = Spider.create(new MasterListPageProcessor())
              .setDownloader(downloader)
              .addPipeline(new MasterListDatabasePipeline(manager))
              .addUrl("https://www.amazon.com/b/ref=lp_172541_ln_0?node=12097478011&ie=UTF8&qid=1476152128")
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
