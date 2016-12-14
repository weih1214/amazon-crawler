package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import sg.edu.smu.webmining.crawler.datatype.Comment;
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
 * Created by hwei on 30/11/2016.
 */
public class CommentPageProcessor implements PageProcessor {

  private static final Pattern REVIEW_ID_PATTERN = Pattern.compile("/review/([a-zA-Z0-9]+)/");

  private final Site site = Site.me()
      .setCycleRetryTimes(Integer.MAX_VALUE)
      .setSleepTime(1000)
      .setRetryTimes(3)
      .setCharset("UTF-8");

  private String parseReviewIdFromURL(String url) {
    final Matcher m = REVIEW_ID_PATTERN.matcher(url);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }


  @Override
  public void process(Page page) {
    final Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com");
    final String url = page.getUrl().toString();
    final String nextURL = doc.select("div.cdPageSelectorPagination a").last().attr("href");
    if (!nextURL.isEmpty()) {
      page.addTargetRequest(nextURL);
    }
    for (Element e : doc.select("div.postBody")) {
      final String reviewId = parseReviewIdFromURL(url);
      final Comment comment = new Comment(reviewId, e);
      page.putField(comment.getCommentId(), comment.asMap());
    }
  }

  @Override
  public Site getSite() {
    return site;
  }

  public static void main(String[] args) {

    final String testUrl = "https://www.amazon.com/review/R31D05IE78MJSI/ref=cm_cd_pg_pg2?ie=UTF8&asin=B003EM6AOG&cdForum=Fx270A461Y43MBR&cdPage=2&cdThread=Tx1RKMUDSJPVEC4&store=aht#wasThisHelpful";

    DynamicProxyProviderTimerWrap provider = new DynamicProxyProviderTimerWrap(
        new DynamicProxyProvider()
            .addSource(new FPLNetSource())
            .addSource(new SSLProxiesOrgSource())
    );

    try {
      provider.startAutoRefresh();

      try (final GeneralMongoDBManager manager = new GeneralMongoDBManager("localhost", 27017, "CommentPage", "content")) {
        try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader(provider)) {

          Spider spider = Spider.create(new CommentPageProcessor())
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
