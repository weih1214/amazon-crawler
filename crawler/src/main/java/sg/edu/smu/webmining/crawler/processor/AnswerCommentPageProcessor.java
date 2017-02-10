package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import sg.edu.smu.webmining.crawler.config.Config;
import sg.edu.smu.webmining.crawler.databasemanager.GeneralMongoDBManager;
import sg.edu.smu.webmining.crawler.parse.AnswerComment;
import sg.edu.smu.webmining.crawler.downloader.nio.ProxyNHttpClientDownloader;
import sg.edu.smu.webmining.crawler.pipeline.FileStoragePipeline;
import sg.edu.smu.webmining.crawler.pipeline.GeneralMongoDBPipeline;
import sg.edu.smu.webmining.crawler.seedpagefetcher.DBSeedpageManager;
import sg.edu.smu.webmining.crawler.storage.MysqlFileManager;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hwei on 15/12/2016.
 */
public class AnswerCommentPageProcessor implements PageProcessor {

  private static final Pattern QUESTION_ID_PATTERN = Pattern.compile("/(T[a-zA-Z0-9]+)/");
  private static final Pattern ANSWER_ID_PATTERN = Pattern.compile("Anchor=(Mx[0-9A-Z]{13,14})");

  private final Site site;

  public AnswerCommentPageProcessor(int cycleRetryTimes, int sleepTime, int retryTimes, String charset) {
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
    page.putField("Page content", page.getRawText());
    page.putField("Page url", page.getUrl().toString());
  }

  @Override
  public Site getSite() {
    return site;
  }

  public static String getAnswerId(String testUrl) {
    final Matcher m = ANSWER_ID_PATTERN.matcher(testUrl);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  public static Request[] getRequestArray(Config cf) throws SQLException {
    final String[] seedpageList = new DBSeedpageManager(cf.getMongoHostname(), cf.getMongoPort(), "AnswerPage", "content", "Answer Comment Link").get();
    List<Request> requestList = new ArrayList<>();
    for (String s1 : seedpageList) {
      requestList.add(new Request(s1).putExtra("Answer ID", getAnswerId(s1)));
    }
    Request[] requestArray = new Request[requestList.size()];
    requestArray = requestList.toArray(requestArray);
    return requestArray;
  }

  public static void main(String[] args) throws SQLException, FileNotFoundException {

//    final String testUrl = "https://www.amazon.com/gp/forum/cd/discussion.html/ref=cm_cd_al_tlc_cl?ie=UTF8&asin=B003EM8008&cdAnchor=Mx2C45GWDUJ4RS2";
//    Request request = new Request(testUrl).putExtra("Answer ID", getAnswerId(testUrl));
    final Config cf = new Config(args[0]);
    final Request[] requestArray = getRequestArray(cf);

    try {
      try (final GeneralMongoDBManager mongoManager = new GeneralMongoDBManager(cf.getMongoHostname(), cf.getMongoPort(), "AnswerCommentPage", "content")) {
        try (final MysqlFileManager mysqlFileStorage = new MysqlFileManager(cf.getMysqlHostname(), cf.getMysqlUsername(), cf.getMysqlPassword(), cf.getStorageDir())) {
          try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader()) {

            Spider spider = Spider.create(new AnswerCommentPageProcessor(cf.getCycleRetryTimes(), cf.getSleepTime(), cf.getRetryTimes(), cf.getCharset()))
                .setDownloader(downloader)
                .addPipeline(new FileStoragePipeline(mysqlFileStorage))
                .addPipeline(new GeneralMongoDBPipeline(mongoManager))
                .addRequest(requestArray)
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
