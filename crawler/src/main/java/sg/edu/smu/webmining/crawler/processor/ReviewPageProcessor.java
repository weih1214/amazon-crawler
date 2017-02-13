package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.edu.smu.webmining.crawler.config.Config;
import sg.edu.smu.webmining.crawler.db.MongoDBManager;
import sg.edu.smu.webmining.crawler.parse.Review;
import sg.edu.smu.webmining.crawler.parse.ReviewOnPage;
import sg.edu.smu.webmining.crawler.parse.ReviewPage;
import sg.edu.smu.webmining.crawler.downloader.nio.ProxyNHttpClientDownloader;
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

public class ReviewPageProcessor implements PageProcessor {

  private static final Pattern URL_PRODUCT_ID_PATTERN1 = Pattern.compile("/product-reviews/(.*?)/");
  private static final Pattern URL_REVIEW_ID_PATTERN = Pattern.compile("/customer-reviews/(.*?)/");
  private static final Pattern URL_PRODUCT_ID_PATTERN2 = Pattern.compile("ASIN=(.{10})");
  private static final Pattern COMMENT_NUMBER_PATTERN = Pattern.compile("(\\d+) posts");

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final Site site;

  public ReviewPageProcessor(int cycleRetryTimes, int sleepTime, int retryTimes, String charset) {
    site = Site.me().setCycleRetryTimes(cycleRetryTimes).setSleepTime(sleepTime).setRetryTimes(retryTimes).setCharset(charset);
  }

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

  private Integer getTotalComments(Page page) {
    final Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com/");
    final String totalComments = doc.select("div.fosmall div.cdPageInfo").text();
    if (totalComments == null || totalComments.isEmpty()) {
      // Log failure to parse
      return null;
    }
    final Matcher m = COMMENT_NUMBER_PATTERN.matcher(totalComments);
    if (m.find()) {
      return Integer.parseInt(m.group(1));
    }
    // Log failure to regex
    return null;
  }

  private Review parseCustomerReview(Page page) {
    final Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com/");
    final String productId = parseProductIdFromCustomerPage(page);
    final String reviewId = parseReviewId(page);
    final Integer totalComments = getTotalComments(page);
    return new ReviewPage(reviewId, productId, doc, page.getUrl().toString(), totalComments);
  }

  private void parseProductReviews(Page page) {
    final Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com/");
    for (Element e : doc.select("#cm_cr-review_list div.a-section.review")) {
      String commentNo = e.select("span.review-comment-total.aok-hidden").text();
      if (commentNo == null || commentNo.isEmpty()) commentNo = "0";
      String helpfulVotes = e.select("span.review-votes").text();
      if (helpfulVotes == null || helpfulVotes.isEmpty()) helpfulVotes = "No";
      if ((Integer.parseInt(commentNo) != 0) || helpfulVotes.contains("found this helpful")) {
        final String reviewUrl = e.select("a.a-size-base.a-link-normal.review-title.a-color-base.a-text-bold").attr("href");
        if (!reviewUrl.isEmpty()) {
          logger.debug("reviewUrl is not found");
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
      if (nextLink != null && !nextLink.isEmpty()) {
        page.addTargetRequest(nextLink);
      }
    } else if (url.contains("customer-reviews")) {
      final Review review = parseCustomerReview(page);
      page.putField(review.getId(), review.asMap());
    }
    FileStoragePipeline.putStorageFields(page, page.getUrl().toString(), page.getRawText());
  }

  @Override
  public Site getSite() {
    return site;
  }

  public static void main(String[] args) throws SQLException, FileNotFoundException {

    final Config cf = new Config(args[0]);
    final String[] seedpageList = new DBSeedpageManager(cf.getMongoHostname(), cf.getMongoPort(), "ProductPage", "content", "Review Link").get();

    try {
      try (final MongoDBManager mongoManager = new MongoDBManager(cf.getMongoHostname(), cf.getMongoPort(), "ReviewPage", "content")) {
        try (final MysqlFileManager mysqlFileStorage = new MysqlFileManager(cf.getMysqlHostname(), cf.getMysqlUsername(), cf.getMysqlPassword(), cf.getStorageDir())) {
          try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader()) {

            Spider spider = Spider.create(new ReviewPageProcessor(cf.getCycleRetryTimes(), cf.getSleepTime(), cf.getRetryTimes(), cf.getCharset()))
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
