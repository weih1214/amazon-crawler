package sg.edu.smu.webmining.crawler.processor;

import org.apache.commons.io.FileUtils;
import sg.edu.smu.webmining.crawler.downloader.ProxyHttpClientDownloader;
import sg.edu.smu.webmining.crawler.masterlist.MySqlMasterListManager;
import sg.edu.smu.webmining.crawler.pipeline.HtmlPipeline;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProvider;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProviderTimerWrap;
import sg.edu.smu.webmining.crawler.proxy.source.FPLNetSource;
import sg.edu.smu.webmining.crawler.proxy.source.SSLProxiesOrgSource;
import sg.edu.smu.webmining.crawler.robotstxt.HostDirectives;
import sg.edu.smu.webmining.crawler.robotstxt.RobotsTxtParser;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Scanner;


public class ProductPageProcessor implements PageProcessor {

  private Site site = Site.me()
      .setRetryTimes(5)
      .setSleepTime(5000)
      .setCharset("UTF-8")
      .setCycleRetryTimes(50);

  @Override
  public void process(Page page) {
    page.putField("html", page.getHtml());
    page.putField("raw_html", page.getRawText());
    page.putField("filename", page.getUrl().regex("/dp/([a-zA-Z0-9]+)", 1).toString());
  }

  @Override
  public Site getSite() {
    return site;
  }

  private static HostDirectives getAmazonRobotsTxt() { // TODO: rewrite to use in memory processing, no specific paths, e.g., "D:/bla-bla/"
    URL url = null;
    try {
      url = new URL("https://amazon.com/robots.txt");
    } catch (MalformedURLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    try {
      FileUtils.copyURLToFile(url, new File("D://robots.txt"));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    String content = null;
    try {
      content = new Scanner(new File("D://robots.txt")).useDelimiter("\\Z").next();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return RobotsTxtParser.parse(content);
  }

  public static void main(String[] args) {
    DynamicProxyProviderTimerWrap provider = new DynamicProxyProviderTimerWrap(new DynamicProxyProvider()
        .addSource(new FPLNetSource())
        .addSource(new SSLProxiesOrgSource())
    );

    provider.startAutoRefresh();

    final Spider spider = new Spider(new ProductPageProcessor())
        .setDownloader(new ProxyHttpClientDownloader(provider))
        .addPipeline(new HtmlPipeline("D:/Tmp/")) // TODO: config to specify where to put files, which database to use
        .thread(1);

    {
      final HostDirectives amazonHD = getAmazonRobotsTxt();
      try (final MySqlMasterListManager manager = new MySqlMasterListManager("jdbc:mysql://127.0.0.1:3306/amazon", "root", "nrff201607")) {
        for (String url : manager.getAllUrls()) {
          if (amazonHD.isAllowed(url)) {
            spider.addUrl(url);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    spider.run();

    provider.stopAutoRefresh();
  }
}
