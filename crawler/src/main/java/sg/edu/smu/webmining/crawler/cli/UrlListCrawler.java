package sg.edu.smu.webmining.crawler.cli;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.edu.smu.webmining.crawler.downloader.nio.ProxyNHttpClientDownloader;
import sg.edu.smu.webmining.crawler.pipeline.HtmlPipeline;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProvider;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProviderTimerWrap;
import sg.edu.smu.webmining.crawler.proxy.source.FPLNetSource;
import sg.edu.smu.webmining.crawler.proxy.source.InCloakSource;
import sg.edu.smu.webmining.crawler.proxy.source.SSLProxiesOrgSource;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by mtkachenko on 21/12/2016.
 */
public class UrlListCrawler implements PageProcessor {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final AtomicInteger total = new AtomicInteger(1);

  private final Site site = Site.me()
      .setCycleRetryTimes(Integer.MAX_VALUE)
      .setSleepTime(5000)
      .setRetryTimes(3)
      .setCharset("UTF-8");

  private final Pattern filenamePattern;

  private UrlListCrawler(Pattern filenamePattern) {
    this.filenamePattern = filenamePattern;
  }

  @Override
  public void process(Page page) {
    if (page == null) {
      logger.debug("page is null");
      return;
    }
    String filename = null;
    final Matcher m = filenamePattern.matcher(page.getRequest().toString());
    if (m.find()) {
      filename = m.group(1);
    }
    final String html = page.getRawText();
    if (filename != null && !html.isEmpty()) {
      page.putField("filename", filename);
      page.putField("raw_html", html);

      System.out.print('.');
      total.incrementAndGet();
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
    if (args.length != 3) {
      System.out.println("USAGE: <url-list> <filename-pattern> <output-dir>");
      System.exit(1);
    }

    final String urlListFilename = args[0];
    final String filenameRegexp = args[1];
    final String outputDirName = args[2];

    final File outDir = new File(outputDirName);
    if (outDir.exists() && !outDir.isDirectory()) {
      System.err.println("The <output-dir> is not a directory!");
      System.exit(1);
    } else if (!outDir.exists() && !outDir.mkdirs()) {
      System.err.println("Cannot create <output-dir>!");
      System.exit(1);
    }

    final List<String> urls = readURLs(urlListFilename);
    final Pattern filenamePattern = Pattern.compile(filenameRegexp);

    final DynamicProxyProviderTimerWrap provider = new DynamicProxyProviderTimerWrap(
        new DynamicProxyProvider()
            .addSource(new FPLNetSource())
            .addSource(new SSLProxiesOrgSource())
            .addSource(new InCloakSource())
    );

    try {
      provider.startAutoRefresh();

      try (final ProxyNHttpClientDownloader downloader = new ProxyNHttpClientDownloader(provider, Sets.newHashSet(404, 410))) {

        Spider spider = Spider.create(new UrlListCrawler(filenamePattern))
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

    } finally {
      provider.stopAutoRefresh();
    }
  }
}
