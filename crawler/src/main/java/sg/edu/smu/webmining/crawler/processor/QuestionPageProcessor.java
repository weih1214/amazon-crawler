package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hwei on 7/12/2016.
 */
public class QuestionPageProcessor implements PageProcessor {

  private static final Pattern QUESTION_ID_PATTERN = Pattern.compile("/-/([a-zA-Z0-9]+)/");
  private static final Pattern CATEGORY_URL_PATTERN = Pattern.compile("/ask/questions/");
  private static final Pattern QUESTION_URL_PATTERN = Pattern.compile("/forum/");

  private final Site site = Site.me()
      .setCycleRetryTimes(Integer.MAX_VALUE)
      .setSleepTime(1000)
      .setRetryTimes(3)
      .setCharset("UTF-8");

  private List<String> extractAndAddUrls(Document doc) {
    List<String> urlList = new ArrayList<>();
    Elements result = doc.select("div.a-section.askTeaserQuestions a.a-link-normal");
    for (Element e : result) {
      final String urlString = e.attr("href");
      if (!urlString.isEmpty()) {
        urlList.add(urlString);
      }
    }
    return urlList;
  }

  private String extractNextUrl(Document doc) {
    return doc.select("ul.a-pagination li.a-last a").attr("href");
  }

  private Boolean isCategoryPage(String pageURL) {
    return CATEGORY_URL_PATTERN.matcher(pageURL).find();
  }

  private Boolean isQuestionPage(String pageURL) {
    return QUESTION_URL_PATTERN.matcher(pageURL).find();
  }


  @Override
  public void process(Page page) {
    final String pageURL = page.getUrl().toString();
    final Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com");
    if (isCategoryPage(pageURL)) {
      page.addTargetRequests(extractAndAddUrls(doc));
      page.addTargetRequest(extractNextUrl(doc));
      page.setSkip(true);
    } else if (isQuestionPage(pageURL)) {
      final Element questionElement = doc.select("div.cdAnswerListHeader").first();
      final Matcher m = QUESTION_ID_PATTERN.matcher(pageURL);
      String questionID = null;
      if (m.find()) {
        questionID = m.group(1);
      }
      final Question question = new Question(questionID, questionElement);
      page.putField(questionID, question.asMap());
      System.out.println(question.asMap());
    }
  }

  @Override
  public Site getSite() {
    return site;
  }

  public static void main(String[] args) {
    final String testUrl = "https://www.amazon.com/ask/questions/asin/B003EM8008/1/ref=ask_ql_psf_ql_hza?sort=HELPFUL&isAnswered=true";

    DynamicProxyProviderTimerWrap provider = new DynamicProxyProviderTimerWrap(
        new DynamicProxyProvider()
            .addSource(new FPLNetSource())
            .addSource(new SSLProxiesOrgSource())
    );

    try {
      provider.startAutoRefresh();

      try (final GeneralMongoDBManager manager = new GeneralMongoDBManager("localhost", 27017, "QuestionPage", "content")) {
        try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader(provider)) {

          Spider spider = Spider.create(new QuestionPageProcessor())
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
