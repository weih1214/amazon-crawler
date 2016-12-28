package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import sg.edu.smu.webmining.crawler.databasemanager.MysqlRecordManager;
import sg.edu.smu.webmining.crawler.datatype.AnswerComment;
import sg.edu.smu.webmining.crawler.downloader.nio.ProxyNHttpClientDownloader;
import sg.edu.smu.webmining.crawler.databasemanager.GeneralMongoDBManager;
import sg.edu.smu.webmining.crawler.pipeline.GeneralMongoDBPipeline;
import sg.edu.smu.webmining.crawler.pipeline.RecordPipeline;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProvider;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProviderTimerWrap;
import sg.edu.smu.webmining.crawler.proxy.source.FPLNetSource;
import sg.edu.smu.webmining.crawler.proxy.source.SSLProxiesOrgSource;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hwei on 15/12/2016.
 */
public class AnswerCommentPageProcessor implements PageProcessor{

  private static final Pattern QUESTION_ID_PATTERN = Pattern.compile("/(T[a-zA-Z0-9]+)/");
  private static final Pattern ANSWER_ID_PATTERN = Pattern.compile("Anchor=(Mx[0-9A-Z]{13,14})");

  private final Site site = Site.me()
      .setCycleRetryTimes(Integer.MAX_VALUE)
      .setSleepTime(1000)
      .setRetryTimes(3)
      .setCharset("UTF-8");

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
    if(nextLink != null) {
      Request request = new Request(nextLink).putExtra("Answer ID", answerId);
      page.addTargetRequest(request);
    }
    final Elements answerCommentElements = doc.select("div.cdCommentList div.cdComment");
    final String answerText = doc.select("span#long_cdAnswerDisplay").text().trim();
    final String questionId = getQuestionId(doc.select("span.cdQuestionLink a").attr("href"));
    for (Element element: answerCommentElements) {
      final AnswerComment ansComment = new AnswerComment(element, questionId, answerId, answerText);
      page.putField(ansComment.getAnswerCommentId(), ansComment.asMap());
    }
    page.putField("Page Content", page.getRawText());
    page.putField("Page Url", page.getUrl().toString());
  }

  @Override
  public Site getSite() {
    return site;
  }

  public static String getAnswerId(String testUrl) {
    final Matcher m = ANSWER_ID_PATTERN.matcher(testUrl);
    if (m.find()){
      return m.group(1);
    }
    return null;
  }

  public static void main(String[] args) {

    final String testUrl = "https://www.amazon.com/gp/forum/cd/discussion.html/ref=cm_cd_al_tlc_cl?ie=UTF8&asin=B003EM8008&cdAnchor=Mx2C45GWDUJ4RS2";
    Request request = new Request(testUrl).putExtra("Answer ID", getAnswerId(testUrl));

    DynamicProxyProviderTimerWrap provider = new DynamicProxyProviderTimerWrap(
        new DynamicProxyProvider()
            .addSource(new FPLNetSource())
            .addSource(new SSLProxiesOrgSource())
    );

    try {
      provider.startAutoRefresh();

      try (final GeneralMongoDBManager mongoManager = new GeneralMongoDBManager("localhost", 27017, "AnswerCommentPage", "content")) {
        try (final MysqlRecordManager mysqlManager = new MysqlRecordManager("jdbc:mysql://127.0.0.1:3306/play", "root", "nrff201607")) {
          try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader(provider)) {

            Spider spider = Spider.create(new AnswerCommentPageProcessor())
                .setDownloader(downloader)
                .addPipeline(new RecordPipeline(new GeneralMongoDBPipeline(mongoManager), mysqlManager))
                .addRequest(request)
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
