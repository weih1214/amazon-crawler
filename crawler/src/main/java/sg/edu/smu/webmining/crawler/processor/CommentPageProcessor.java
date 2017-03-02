package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import sg.edu.smu.webmining.crawler.config.Config;
import sg.edu.smu.webmining.crawler.db.MongoDBManager;
import sg.edu.smu.webmining.crawler.downloader.nio.RawPage;
import sg.edu.smu.webmining.crawler.parse.Comment;
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

/**
 * Created by hwei on 30/11/2016.
 */
public class CommentPageProcessor implements PageProcessor {

  private static final Pattern REVIEW_ID_PATTERN = Pattern.compile("(?:/review|/customer-reviews)/([a-zA-Z0-9]+)/");
  private static final Pattern COMMENT_NUMBER_PATTERN = Pattern.compile("(\\d+) posts");

  private final Site site;

  public CommentPageProcessor(int cycleRetryTimes, int sleepTime, int retryTimes, String charset) {
    site = Site.me().setCycleRetryTimes(cycleRetryTimes).setSleepTime(sleepTime).setRetryTimes(retryTimes).setCharset(charset);
  }

  private String parseReviewIdFromURL(String url) {
    final Matcher m = REVIEW_ID_PATTERN.matcher(url);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  private String getNextLink(Document doc) {
    final Elements paginationElements = doc.select("div.cdPageSelectorPagination a");
    if (!paginationElements.isEmpty()) {
      final Element pagination = paginationElements.last();
      if (pagination.text().contains("Next")) {
        return pagination.attr("href");
      }
    }
    return null;
  }

  public String getTotalComments(Document doc) {
    final String totalComments = doc.select("div.fosmall div.cdPageInfo").text();
    if (totalComments == null || totalComments.isEmpty()) {
      // Log failure to parse
      return null;
    }
    final Matcher m = COMMENT_NUMBER_PATTERN.matcher(totalComments);
    if (m.find()) {
      return m.group(1);
    }
    // Log failure to regex
    return null;
  }

  @Override
  public void process(Page page) {
    final Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com");
    final String url = page.getUrl().toString();
    final String nextURL = getNextLink(doc);
    if (nextURL != null) {
      page.addTargetRequest(nextURL);
    }
    for (Element e : doc.select("div.postBody")) {
      final String reviewId = parseReviewIdFromURL(url);
      final Comment comment = new Comment(reviewId, e);
      page.putField(comment.getCommentId(), comment.asMap());
    }
    if (page instanceof RawPage) {
      byte[] rawContent = ((RawPage) page).getRawContent();
      FileStoragePipeline.putStorageFields(page, url, rawContent);
    }
  }

  @Override
  public Site getSite() {
    return site;
  }

  public static void main(String[] args) throws SQLException, FileNotFoundException {

    final Config cf = new Config(args[0]);
    final String[] seedpageList = new DBSeedpageManager(cf.getMongoHostname(), cf.getMongoPort(), "ReviewPage", "content", "Comment Link").get();

    try {
      try (final MongoDBManager mongoManager = new MongoDBManager(cf.getMongoHostname(), cf.getMongoPort(), "CommentPage", "content")) {
        try (final MysqlFileManager mysqlFileStorage = new MysqlFileManager(cf.getMysqlHostname(), cf.getMysqlUsername(), cf.getMysqlPassword(), cf.getStorageDir())) {
          try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader()) {

            Spider spider = Spider.create(new CommentPageProcessor(cf.getCycleRetryTimes(), cf.getSleepTime(), cf.getRetryTimes(), cf.getCharset()))
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
