package sg.edu.smu.webmining.crawler.fixer;

/**
 * Created by hwei on 10/3/2017.
 */

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import sg.edu.smu.webmining.crawler.config.Config;
import sg.edu.smu.webmining.crawler.db.MongoDBManager;
import sg.edu.smu.webmining.crawler.downloader.nio.ProxyNHttpClientDownloader;
import sg.edu.smu.webmining.crawler.downloader.nio.RawPage;
import sg.edu.smu.webmining.crawler.parse.AnswerComment;
import sg.edu.smu.webmining.crawler.pipeline.FileStoragePipeline;
import sg.edu.smu.webmining.crawler.pipeline.GeneralMongoDBPipeline;
import sg.edu.smu.webmining.crawler.seedpagefetcher.DBSeedpageManager;
import sg.edu.smu.webmining.crawler.storage.MysqlFileManager;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.io.IOException;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hwei on 15/12/2016.
 */
public class AnswerCommentPageFixer implements PageProcessor {

  private static final Pattern QUESTION_ID_PATTERN = Pattern.compile("/(T[a-zA-Z0-9]+)/");
  private static final Pattern ANSWER_ID_PATTERN = Pattern.compile("Anchor=(Mx[0-9A-Z]{13,14})");

  private final Site site;

  public AnswerCommentPageFixer(int cycleRetryTimes, int sleepTime, int retryTimes, String charset) {
    site = Site.me().setCycleRetryTimes(cycleRetryTimes).setSleepTime(sleepTime).setRetryTimes(retryTimes).setCharset(charset);
  }

  public String getQuestionId(String questionIdLink) {
    final Matcher m = QUESTION_ID_PATTERN.matcher(questionIdLink);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  public String getNextLink(Elements paginationElements) {
    if (!paginationElements.isEmpty()) {
      if (paginationElements.last().text().equals("Next ›")) {
        return paginationElements.last().attr("href");
      }
    }
    return null;
  }

  @Override
  public void process(Page page) {
    final Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com");
    final String nextLink = getNextLink(doc.select("div.cdPageSelectorPagination a"));
    final String answerId = page.getRequest().getExtra("Answer ID").toString();
    if (nextLink != null) {
      Request request = new Request(nextLink).putExtra("Answer ID", answerId);
      page.addTargetRequest(request);
    }
    final Elements answerCommentElements = doc.select("div.cdCommentList div.cdComment");
    final String answerText = doc.select("span#long_cdAnswerDisplay").text().trim();
    final String questionId = getQuestionId(doc.select("span.cdQuestionLink a").attr("href"));
    for (Element element : answerCommentElements) {
      final AnswerComment ansComment = new AnswerComment(element, questionId, answerId, answerText);
      page.putField(ansComment.getAnswerCommentId(), ansComment.asMap());
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
    final Request[] seedpageList = new DBSeedpageManager().getRequestList(cf.getAnsCommentFilePath());

    try {
      try (final MongoDBManager mongoManager = new MongoDBManager(cf.getMongoHostname(), cf.getMongoPort(), "AnswerCommentPage", "content")) {
        try (final MysqlFileManager mysqlFileStorage = new MysqlFileManager(cf.getMysqlHostname(), cf.getMysqlUsername(), cf.getMysqlPassword(), cf.getStorageDir())) {
          try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader()) {

            Spider spider = Spider.create(new AnswerCommentPageFixer(cf.getCycleRetryTimes(), cf.getSleepTime(), cf.getRetryTimes(), cf.getCharset()))
                .setDownloader(downloader)
                .addPipeline(new FileStoragePipeline(mysqlFileStorage))
                .addPipeline(new GeneralMongoDBPipeline(mongoManager))
                .addRequest(seedpageList)
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

