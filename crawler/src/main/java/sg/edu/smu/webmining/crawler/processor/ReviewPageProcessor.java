package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import sg.edu.smu.webmining.crawler.Config.ConfigFetcher;
import sg.edu.smu.webmining.crawler.databasemanager.GeneralMongoDBManager;
import sg.edu.smu.webmining.crawler.datatype.Review;
import sg.edu.smu.webmining.crawler.datatype.ReviewOnPage;
import sg.edu.smu.webmining.crawler.datatype.ReviewPage;
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

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReviewPageProcessor implements PageProcessor {

  private static final Pattern URL_PRODUCT_ID_PATTERN1 = Pattern.compile("/product-reviews/(.*?)/");
  private static final Pattern URL_REVIEW_ID_PATTERN = Pattern.compile("/customer-reviews/(.*?)/");
  private static final Pattern URL_PRODUCT_ID_PATTERN2 = Pattern.compile("ASIN=(.{10})");

  private final Site site = Site.me()
      .setCycleRetryTimes(Integer.MAX_VALUE)
      .setSleepTime(1000)
      .setRetryTimes(3)
      .setCharset("UTF-8");

/*  public static String[] getReviewLinks(String folderPath) {
    List<Document> docList = ManagerTesting.load(ManagerTesting.listFilesForFolder(folderPath));
    List<String> reviewLinks = new ArrayList<>();
    for (Document doc : docList) {

      String s1 = doc.select("#revSum .a-link-emphasis.a-nowrap").attr("href");
      if (!s1.isEmpty()) {
        String s2 = s1.replaceAll("&reviewerType=.*?&", "&reviewerType=all_reviews&").replaceAll("&sortBy=.*", "&sortBy=recent");
        reviewLinks.add(s2);
      }
    }
    System.out.println(reviewLinks.size());
    System.out.println(reviewLinks);
    return reviewLinks.toArray(new String[0]);
  }*/

  private static String parseWithRegexp(Page page, Pattern pattern) {
    final String url = page.getUrl().toString();
    final Matcher m = pattern.matcher(url);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  private static String parseReviewId(Page page) {
    return parseWithRegexp(page, URL_REVIEW_ID_PATTERN);
  }

  private static String parseProductIdFromListPage(Page page) {
    return parseWithRegexp(page, URL_PRODUCT_ID_PATTERN1);
  }

  private static String parseProductIdFromCustomerPage(Page page) {
    return parseWithRegexp(page, URL_PRODUCT_ID_PATTERN2);
  }

  private Review parseReviewOnListPage(Page page, Element e) {
    return new ReviewOnPage(parseProductIdFromListPage(page), e);
  }

  private Review parseCustomerReview(Page page) {
    final Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com/");
    final String productId = parseProductIdFromCustomerPage(page);
    final String reviewId = parseReviewId(page);
    return new ReviewPage(reviewId, productId, doc, page.getUrl().toString());
  }

  private void parseProductReviews(Page page) {
    final Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com/");
    for (Element e : doc.select("#cm_cr-review_list div.a-section.review")) {
      final String commentNo = e.select("span.review-comment-total.aok-hidden").text().trim();
      final String helpfulVotes = e.select("span.review-votes").text().trim();
      if ((Integer.parseInt(commentNo) != 0) || helpfulVotes.contains("found this helpful")) {
        final String reviewUrl = e.select("a.a-size-base.a-link-normal.review-title.a-color-base.a-text-bold").attr("href");
        if (!reviewUrl.isEmpty()) {
          // TODO: logger
          //System.out.println(reviewUrl);
          page.addTargetRequest(reviewUrl);
        }
      } else {
        final Review review = parseReviewOnListPage(page, e);
        page.putField(review.getId(), review.asMap());
      }
    }
  }

  public void process(Page page) {
    final String url = page.getUrl().toString();
    if (url.contains("product-reviews")) {
      parseProductReviews(page);
      final String nextLink = page.getHtml().css("#cm_cr-pagination_bar .a-pagination li.a-last a").links().toString();
      if (!nextLink.isEmpty()) {
        page.addTargetRequest(nextLink);
      }
    } else if (url.contains("customer-reviews")) {
      final Review review = parseCustomerReview(page);
      page.putField(review.getId(), review.asMap());
    }
    page.putField("Page content", page.getRawText());
    page.putField("Page url", page.getUrl().toString());
  }

  @Override
  public Site getSite() {
    return site;
  }

  public static void main(String[] args) throws SQLException, FileNotFoundException {

    final ConfigFetcher cf = new ConfigFetcher("D:\\config.json");
    final String[] seedpageList = new DBSeedpageManager(cf.getMongoHostname(), cf.getMongoPort(), "ProductPage", "content", "Review Link").get();

    DynamicProxyProviderTimerWrap provider = new DynamicProxyProviderTimerWrap(
        new DynamicProxyProvider()
            .addSource(new FPLNetSource())
            .addSource(new SSLProxiesOrgSource())
    );

    try {
      provider.startAutoRefresh();

      try (final GeneralMongoDBManager mongoManager = new GeneralMongoDBManager(cf.getMongoHostname(), cf.getMongoPort(), "ReviewPage", "content")) {
        try (final MysqlFileManager mysqlFileStorage = new MysqlFileManager(cf.getMysqlHostname(), cf.getMysqlUsername(), cf.getMysqlPassword(), cf.getStoragedir())) {
          try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader(provider)) {

            Spider spider = Spider.create(new ReviewPageProcessor())
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
