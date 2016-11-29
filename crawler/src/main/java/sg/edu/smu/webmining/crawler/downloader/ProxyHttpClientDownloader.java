package sg.edu.smu.webmining.crawler.downloader;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.edu.smu.webmining.crawler.proxy.ProxyProvider;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.downloader.HttpClientDownloader;

import java.util.Map;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by weih1214 on 10/11/2016.
 */
public class ProxyHttpClientDownloader extends HttpClientDownloader {

  private static final int TIMEOUT = 20; // 5min

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final ProxyProvider proxyProvider;

  public ProxyHttpClientDownloader(ProxyProvider proxyProvider) {
    this.proxyProvider = proxyProvider;
  }

  // Common UserAgent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36
  // Source: https://techblog.willshouse.com/2012/01/03/most-common-user-agents/

  @Override
  protected HttpUriRequest getHttpUriRequest(Request request, Site site, Map<String, String> headers) {
    RequestBuilder requestBuilder = selectRequestMethod(request).setUri(request.getUrl());

    if (headers != null) {
      for (Map.Entry<String, String> headerEntry : headers.entrySet()) {
        requestBuilder.addHeader(headerEntry.getKey(), headerEntry.getValue());
      }
    }

    final HttpHost proxy = proxyProvider.next();

    RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
        .setConnectionRequestTimeout(site.getTimeOut())
        .setSocketTimeout(60 * 1000)
        .setConnectTimeout(site.getTimeOut())
        .setCookieSpec(CookieSpecs.BEST_MATCH)
        .setProxy(proxy);

    request.putExtra(Request.PROXY, proxy);

    final HttpUriRequest uriRequest = requestBuilder.setConfig(requestConfigBuilder.build()).build();
    uriRequest.setHeader("User-Agent", RandomStringUtils.random(32, true, true));
    return uriRequest;
  }

  private boolean isPageBlocked(Page page) {
    final String pageHtml = page.getRawText();
    if (pageHtml == null || pageHtml.isEmpty()) {
      logger.debug("Page {} is empty, assuming we are blocked", page.getUrl());
      return true;
    }
    final Pattern p1 = Pattern.compile("Robot Check");
    final Pattern p2 = Pattern.compile("Sorry, we just need to make sure you're not a robot");
    final Matcher m1 = p1.matcher(pageHtml);
    final Matcher m2 = p2.matcher(pageHtml);
    if (m1.find() && m2.find()) {
      logger.debug("Page {} is blocked", page.getUrl().toString());
      return true;
    }
    return false;
  }

  @Override
  public Page download(Request request, Task task) {
    try {
      final Future<Page> response = Executors.newSingleThreadExecutor().submit(() -> super.download(request, task));

      long time = System.currentTimeMillis();
      logger.debug("downloading...");
      Page page = response.get(TIMEOUT, TimeUnit.SECONDS);
      logger.debug("downloading took: {}s", (System.currentTimeMillis() - time) / 1000);
      if (page == null) {
        final Object statusCode = request.getExtra(Request.STATUS_CODE);
        if (statusCode != null && (Integer) statusCode != 200) {
          logger.debug("status code is not OK, request rescheduled");
          return addToCycleRetry(request, task.getSite());
        }
      } else {
        if (!page.isNeedCycleRetry() && isPageBlocked(page)) {
          return addToCycleRetry(request, task.getSite());
        }
      }

      return page;
    } catch (TimeoutException e) {
      logger.error("downloading times out, trying download again", e);
      return addToCycleRetry(request, task.getSite());
    } catch (ExecutionException e) {
      logger.error("exception happen when downloading, trying download again", e);
      return addToCycleRetry(request, task.getSite());
    } catch (InterruptedException e) {
      logger.error("downloader thread interrupted, terminating", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Page addToCycleRetry(Request request, Site site) {
    proxyProvider.remove((HttpHost) request.getExtra(Request.PROXY));
    return super.addToCycleRetry(request, site);
  }

  @Override
  protected void onError(Request request) {
    logger.warn("error happened, but not processed");
  }


}
