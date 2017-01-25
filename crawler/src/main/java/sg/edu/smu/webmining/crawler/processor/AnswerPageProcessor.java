package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import sg.edu.smu.webmining.crawler.config.Config;
import sg.edu.smu.webmining.crawler.databasemanager.GeneralMongoDBManager;
import sg.edu.smu.webmining.crawler.parse.Answer;
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
 * Created by hwei on 13/12/2016.
 */
public class AnswerPageProcessor implements PageProcessor {

  private static final Pattern QUESTION_ID_PATTERN_1 = Pattern.compile("/-/([a-zA-Z0-9]+)/");
  private static final Pattern QUESTION_ID_PATTERN_2 = Pattern.compile("/forum/([a-zA-Z0-9]+)/");
  private static final Pattern PRODUCT_ID_PATTERN = Pattern.compile("asin=([a-zA-Z0-9]{10})");

  private final Site site = Site.me()
      .setCycleRetryTimes(Integer.MAX_VALUE)
      .setSleepTime(1000)
      .setRetryTimes(3)
      .setCharset("UTF-8");

  private String getQuestionId(String pageUrl) {
    final Matcher m1 = QUESTION_ID_PATTERN_1.matcher(pageUrl);
    if (m1.find()) {
      return m1.group(1);
    } else {
      final Matcher m2 = QUESTION_ID_PATTERN_2.matcher(pageUrl);
      if (m2.find()) {
        return m2.group(1);
      }
    }
    return null;
  }

  private String getProductId(String pageUrl) {
    final Matcher m = PRODUCT_ID_PATTERN.matcher(pageUrl);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  private String getNextLink(Document doc) {

    final Elements paginationElements = doc.select("div.cdPageSelectorPagination");
    if (paginationElements.isEmpty()) {
      return null;
    } else {
      return paginationElements.first().children().last().attr("href");
    }
  }

  @Override
  public void process(Page page) {
    // Answer Page Processing
    final Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com");
    final String pageUrl = page.getUrl().toString();
    final String questionId = getQuestionId(pageUrl);
    final String productId = getProductId(pageUrl);
    final Elements answerElements = doc.select("div.cmQAList div.cmQAListItemAnswer");
    for (Element e : answerElements) {
      final Answer answerContent = new Answer(questionId, productId, e);
      page.putField(answerContent.getAnswerId(), answerContent.asMap());
    }
    page.putField("Page content", page.getRawText());
    page.putField("Page url", page.getUrl().toString());
    // Pagination
    final String nextLink = getNextLink(doc);
    if (nextLink != null && !nextLink.isEmpty()) {
      page.addTargetRequest(nextLink);
    }

  }

  @Override
  public Site getSite() {
    return site;
  }

  public static void main(String[] args) throws SQLException, FileNotFoundException {

    final Config cf = new Config("D:\\config.json");
    final String[] seedpageList = new DBSeedpageManager(cf.getMongoHostname(), cf.getMongoPort(), "QuestionPage", "content", "Answer Link").get();

    try {
      try (final GeneralMongoDBManager mongoManager = new GeneralMongoDBManager(cf.getMongoHostname(), cf.getMongoPort(), "AnswerPage", "content")) {
        try (final MysqlFileManager mysqlFileStorage = new MysqlFileManager(cf.getMysqlHostname(), cf.getMysqlUsername(), cf.getMysqlPassword(), cf.getStorageDir())) {
          try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader()) {

            Spider spider = Spider.create(new AnswerPageProcessor())
                .setDownloader(downloader)
                .addPipeline(new FileStoragePipeline(mysqlFileStorage))
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
    }
  }

}
