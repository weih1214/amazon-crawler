package sg.edu.smu.webmining.crawler.livejournal;

import sg.edu.smu.webmining.crawler.downloader.nio.ProxyNHttpClientDownloader;
import sg.edu.smu.webmining.crawler.pipeline.HtmlPipeline;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProvider;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProviderTimerWrap;
import sg.edu.smu.webmining.crawler.proxy.source.FPLNetSource;
import sg.edu.smu.webmining.crawler.proxy.source.SSLProxiesOrgSource;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hwei on 20/12/2016.
 */
public class LiveJournalPageProcessor implements PageProcessor {

  public static Integer TOTALNUMBER = 1;
  private static Pattern FILE_NAME_PATTERN = Pattern.compile("http://(.*)\\.livejournal\\.com");

  private final Site site = Site.me()
      .setCycleRetryTimes(Integer.MAX_VALUE)
      .setSleepTime(5000)
      .setRetryTimes(3)
      .setCharset("UTF-8");


  @Override
  public void process(Page page) {
    final Matcher m = FILE_NAME_PATTERN.matcher(page.getRequest().toString());
    String fileName = null;
    if (m.find()) {
      fileName = m.group(1);
    }
    final String html = page.getRawText();
    if ( fileName != null && !html.isEmpty()) {
      page.putField("filename", fileName);
      page.putField("raw_html", html);
      System.out.println(TOTALNUMBER + " files have been stored!");
      TOTALNUMBER += 1;
    }
  }

  @Override
  public Site getSite() {
    return site;
  }

  public static Request[] getRequestArray() {
    final File file = new File("D://livejournal//livejournal.txt");
    Scanner sc = null;
    try {
      sc = new Scanner(file);
      List<String> stringList = new ArrayList<String>();
      List<Request> requestList = new ArrayList<>() ;
      while (sc.hasNextLine()) {
        stringList.add(sc.nextLine());
      }
      Iterator<String> iterator = stringList.iterator();
      while (iterator.hasNext()) {
        requestList.add(new Request((String) iterator.next()));
      }
      System.out.println(requestList.size());
      return requestList.toArray(new Request[requestList.size()]);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static void main(String[] args) {

    final Request[] requestArray = getRequestArray();


    DynamicProxyProviderTimerWrap provider = new DynamicProxyProviderTimerWrap(
        new DynamicProxyProvider()
            .addSource(new FPLNetSource())
            .addSource(new SSLProxiesOrgSource())
    );

    try {
      provider.startAutoRefresh();

      try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader(provider)) {

        Spider spider = Spider.create(new LiveJournalPageProcessor())
            .setDownloader(downloader)
            .addPipeline(new HtmlPipeline("D://liveresult"))
            .addRequest(requestArray)
            .thread(10);

        long time = System.currentTimeMillis();
        spider.run();
        System.out.println("Finished in " + ((System.currentTimeMillis() - time) / 60000) + "m");
      }


    } catch (Throwable ex) {
      System.err.println("Uncaught exception - " + ex.getMessage());
      ex.printStackTrace(System.err);
    } finally {
      provider.stopAutoRefresh();
    }
  }
}
