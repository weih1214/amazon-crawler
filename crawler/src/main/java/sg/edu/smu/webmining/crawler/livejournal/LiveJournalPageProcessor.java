package sg.edu.smu.webmining.crawler.livejournal;

import com.google.common.collect.Sets;
import sg.edu.smu.webmining.crawler.downloader.nio.ProxyNHttpClientDownloader;
import sg.edu.smu.webmining.crawler.pipeline.HtmlPipeline;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProviderFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hwei on 20/12/2016.
 */
public class LiveJournalPageProcessor implements PageProcessor {

  private static Pattern FILENAME_PATTERN = Pattern.compile("http://(.*)\\.livejournal\\.com");

  private final AtomicInteger total = new AtomicInteger(45287);

  private final Site site = Site.me()
      .setCycleRetryTimes(Integer.MAX_VALUE)
      .setSleepTime(5000)
      .setRetryTimes(3)
      .setCharset("UTF-8");


  @Override
  public void process(Page page) {
    if (page == null) {
      return;
    }

    String filename = null;
    final Matcher m = FILENAME_PATTERN.matcher(page.getRequest().toString());
    if (m.find()) {
      filename = m.group(1);
    }
    final String html = page.getRawText();
    if (filename != null && !html.isEmpty()) {
      page.putField("filename", filename);
      page.putField("raw_html", html);
      System.out.println(total.getAndIncrement() + " files have been stored!");
    }
  }

  @Override
  public Site getSite() {
    return site;
  }

  private static List<String> readURLs(String filename) throws IOException {
    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename)))) {
      final List<String> urls = new ArrayList<>();

      String line;
      while (null != (line = reader.readLine())) {
        urls.add(line.trim());
      }

      return urls;
    }
  }

  public static void main(String[] args) throws IOException {
    final String urlListFilename = "D://urls_to_process.txt";
    final String outDir = "D://liveresult";

    final List<String> urls = readURLs(urlListFilename);
    try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader(DynamicProxyProviderFactory.getDefault(), Sets.newHashSet(404, 410))) {

      Spider spider = Spider.create(new LiveJournalPageProcessor())
          .setDownloader(downloader)
          .addPipeline(new HtmlPipeline(outDir))
          .thread(10);

      for (final String url : urls) {
        spider.addUrl(url);
      }

      long time = System.currentTimeMillis();
      spider.run();
      System.out.println("Finished in " + ((System.currentTimeMillis() - time) / 60000) + "m");
    }
  }
}
