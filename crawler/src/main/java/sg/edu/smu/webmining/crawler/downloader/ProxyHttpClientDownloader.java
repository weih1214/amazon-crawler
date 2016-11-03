package sg.edu.smu.webmining.crawler.downloader;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpUriRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.edu.smu.webmining.crawler.proxy.ProxyProvider;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.downloader.HttpClientDownloader;

import java.util.Map;

/**
 * Created by mtkachenko.2015 on 2/11/2016.
 */
public class ProxyHttpClientDownloader extends HttpClientDownloader {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final ProxyProvider proxyProvider;

  public ProxyHttpClientDownloader(ProxyProvider proxyProvider) {
    this.proxyProvider = proxyProvider;
  }

  @Override
  protected HttpUriRequest getHttpUriRequest(Request request, Site site, Map<String, String> headers) {
    final HttpUriRequest uriRequest = super.getHttpUriRequest(request, site, headers);

    // Common UserAgent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36
    // Source: https://techblog.willshouse.com/2012/01/03/most-common-user-agents/

    uriRequest.setHeader("User-Agent", RandomStringUtils.random(32, true, true)); // use random user agent
    request.putExtra(Request.PROXY, proxyProvider.next()); // use random proxy

    return uriRequest;
  }

  @Override
  public Page download(Request request, Task task) {
    Page page = super.download(request, task);
    if (page == null) {
      final Object statusCode = request.getExtra(Request.STATUS_CODE);
      if (statusCode != null && (Integer) statusCode == 400) { // HTTP Error: Bad request
        logger.debug("bad request, request rescheduled");
        return addToCycleRetry(request, task.getSite());
      }
    }

    return page;
  }

  @Override
  protected Page addToCycleRetry(Request request, Site site) {
    proxyProvider.remove((HttpHost) request.getExtra(Request.PROXY));
    request.putExtra(Request.PROXY, proxyProvider.next());
    return super.addToCycleRetry(request, site);
  }

  @Override
  protected void onError(Request request) {
    logger.warn("Error happened, but not processed");
  }

}
