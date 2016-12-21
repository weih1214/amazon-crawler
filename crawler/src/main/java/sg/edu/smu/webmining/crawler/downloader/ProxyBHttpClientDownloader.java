package sg.edu.smu.webmining.crawler.downloader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.edu.smu.webmining.crawler.proxy.ProxyProvider;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.downloader.AbstractDownloader;
import us.codecraft.webmagic.downloader.HttpClientGenerator;
import us.codecraft.webmagic.selector.PlainText;
import us.codecraft.webmagic.utils.HttpConstant;
import us.codecraft.webmagic.utils.UrlUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProxyBHttpClientDownloader extends AbstractDownloader {

  private static class PageResponse {

    private final int statusCode;
    private final Page page;

    private PageResponse(int statusCode, Page page) {
      this.statusCode = statusCode;
      this.page = page;
    }

  }

  private static final int TIMEOUT = 20;

  private Logger logger = LoggerFactory.getLogger(getClass());

  private final Map<String, CloseableHttpClient> httpClients = new HashMap<>();

  private final HttpClientGenerator httpClientGenerator = new HttpClientGenerator();

  private final ProxyProvider proxyProvider;

  public ProxyBHttpClientDownloader(ProxyProvider proxyProvider) {
    this.proxyProvider = proxyProvider;
  }

  private CloseableHttpClient getHttpClient(Site site) {
    if (site == null) {
      return httpClientGenerator.getClient(null);
    }
    String domain = site.getDomain();
    CloseableHttpClient httpClient = httpClients.get(domain);
    if (httpClient == null) {
      synchronized (this) {
        httpClient = httpClients.computeIfAbsent(domain, k -> httpClientGenerator.getClient(site));
      }
    }
    return httpClient;
  }

  @Override
  public Page download(Request request, Task task) {
    Site site = null;
    if (task != null) {
      site = task.getSite();
    }
    final String charset = site != null ? site.getCharset() : null;
    Map<String, String> headers = null;
    if (site != null) {
      headers = site.getHeaders();
    }

    logger.info("downloading page {}", request.getUrl());
    final ExecutorService threadExecutor = Executors.newSingleThreadExecutor(r -> {
      final Thread thread = new Thread(r, "connection-thread");
      System.out.println("Created: " + thread.getName() + "-" + thread.getId());
      return thread;
    });
    Future<PageResponse> futureResponse = null;
    int statusCode = 0;
    try {
      final HttpUriRequest httpUriRequest = getHttpUriRequest(request, site, headers);
      final CloseableHttpClient httpClient = getHttpClient(site);
      logger.debug("downloading via proxy ({})...", request.getExtra(Request.PROXY));
      long time = System.currentTimeMillis();

      futureResponse = threadExecutor.submit(() -> {
        Thread thread = Thread.currentThread();
        final String threadId = thread.getName() + "-" + thread.getId();
        boolean outputConsumption = false;
        CloseableHttpResponse httpResponse = null;
        try {
          httpResponse = httpClient.execute(httpUriRequest);
          if (Thread.interrupted()) {
            System.out.println("Magic 1!, " + threadId);
            outputConsumption = true;
            return null;
          }
          final int innerStatureCode = httpResponse.getStatusLine().getStatusCode();
          if (innerStatureCode == 200) {
            Page page = handleResponse(request, charset, httpResponse, task);
            if (Thread.interrupted()) {
              System.out.println("Magic 2!, " + threadId);
              outputConsumption = true;
              return null;
            }
            return new PageResponse(innerStatureCode, page);
          }
          return new PageResponse(innerStatureCode, null);
        } finally {
          try {
            if (httpResponse != null) {
              EntityUtils.consume(httpResponse.getEntity());
              if (outputConsumption) {
                System.out.println("Consume Entity, " + threadId);
              }
            }
          } catch (IOException e) {
            logger.warn("close response fail", e);
          }
        }
      });

      final PageResponse pageResponse = futureResponse.get(TIMEOUT, TimeUnit.SECONDS);
      logger.debug("downloading took: {}s", (System.currentTimeMillis() - time) / 1000);

      statusCode = pageResponse.statusCode;

      if (statusCode == 200) {
        if (!pageResponse.page.isNeedCycleRetry() && isPageBlocked(pageResponse.page)) {
          logger.debug("page is blocked, rescheduling: {}", request.getUrl());
          return addToCycleRetry(request, site);
        }
        onSuccess(request);
        return pageResponse.page;
      } else {
        logger.debug("status code {}, rescheduling: {}", statusCode, request.getUrl());
        return addToCycleRetry(request, site);
      }

    } catch (ExecutionException e) {
      logger.warn("download page " + request.getUrl() + " error, rescheduling", e);
      return addToCycleRetry(request, site);
    } catch (InterruptedException e) {
      logger.error("downloader thread interrupted, terminating", e);
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      if (!futureResponse.isDone()) {
        futureResponse.cancel(true);
      }
      logger.error("downloading timeout, rescheduling", e);
      return addToCycleRetry(request, site);
    } finally {
      request.putExtra(Request.STATUS_CODE, statusCode);
      if (!threadExecutor.isShutdown()) {
        threadExecutor.shutdownNow();
      }
    }
  }

  @Override
  public void setThread(int thread) {
    httpClientGenerator.setPoolSize(thread);
  }

  private HttpUriRequest getHttpUriRequest(Request request, Site site, Map<String, String> headers) {
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
        //.setCookieSpec(CookieSpecs.BEST_MATCH)
        .setProxy(proxy);

    request.putExtra(Request.PROXY, proxy);

    final HttpUriRequest uriRequest = requestBuilder.setConfig(requestConfigBuilder.build()).build();
    uriRequest.setHeader("User-Agent", RandomStringUtils.random(32, true, true));
    return uriRequest;
  }

  private RequestBuilder selectRequestMethod(Request request) {
    String method = request.getMethod();
    if (method == null || method.equalsIgnoreCase(HttpConstant.Method.GET)) {
      //default get
      return RequestBuilder.get();
    } else if (method.equalsIgnoreCase(HttpConstant.Method.POST)) {
      RequestBuilder requestBuilder = RequestBuilder.post();
      NameValuePair[] nameValuePair = (NameValuePair[]) request.getExtra("nameValuePair");
      if (nameValuePair != null && nameValuePair.length > 0) {
        requestBuilder.addParameters(nameValuePair);
      }
      return requestBuilder;
    } else if (method.equalsIgnoreCase(HttpConstant.Method.HEAD)) {
      return RequestBuilder.head();
    } else if (method.equalsIgnoreCase(HttpConstant.Method.PUT)) {
      return RequestBuilder.put();
    } else if (method.equalsIgnoreCase(HttpConstant.Method.DELETE)) {
      return RequestBuilder.delete();
    } else if (method.equalsIgnoreCase(HttpConstant.Method.TRACE)) {
      return RequestBuilder.trace();
    }
    throw new IllegalArgumentException("Illegal HTTP Method " + method);
  }

  private Page handleResponse(Request request, String charset, HttpResponse httpResponse, Task task) throws IOException {
    String content = getContent(charset, httpResponse);
    Page page = new Page();
    page.setRawText(content);
    page.setUrl(new PlainText(request.getUrl()));
    page.setRequest(request);
    page.setStatusCode(httpResponse.getStatusLine().getStatusCode());
    return page;
  }

  private String getContent(String charset, HttpResponse httpResponse) throws IOException {
    if (charset == null) {
      byte[] contentBytes = IOUtils.toByteArray(httpResponse.getEntity().getContent());
      String htmlCharset = getHtmlCharset(httpResponse, contentBytes);
      if (htmlCharset != null) {
        return new String(contentBytes, htmlCharset);
      } else {
        logger.warn("Charset autodetect failed, use {} as charset. Please specify charset in Site.setCharset()", Charset.defaultCharset());
        return new String(contentBytes);
      }
    } else {
      return IOUtils.toString(httpResponse.getEntity().getContent(), charset);
    }
  }

  private String getHtmlCharset(HttpResponse httpResponse, byte[] contentBytes) throws IOException {
    String charset;
    // charset
    // 1、encoding in http header Content-Type
    String value = httpResponse.getEntity().getContentType().getValue();
    charset = UrlUtils.getCharset(value);
    if (StringUtils.isNotBlank(charset)) {
      logger.debug("Auto get charset: {}", charset);
      return charset;
    }
    // use default charset to decode first time
    Charset defaultCharset = Charset.defaultCharset();
    String content = new String(contentBytes, defaultCharset.name());
    // 2、charset in meta
    if (StringUtils.isNotEmpty(content)) {
      Document document = Jsoup.parse(content);
      Elements links = document.select("meta");
      for (Element link : links) {
        // 2.1、html4.01 <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
        String metaContent = link.attr("content");
        String metaCharset = link.attr("charset");
        int index = metaContent.indexOf("charset");
        if (index != -1) {
          metaContent = metaContent.substring(index, metaContent.length());
          charset = metaContent.split("=")[1];
          break;
        }
        // 2.2、html5 <meta charset="UTF-8" />
        else if (StringUtils.isNotEmpty(metaCharset)) {
          charset = metaCharset;
          break;
        }
      }
    }
    logger.debug("Auto get charset: {}", charset);
    // 3、todo use tools as cpdetector for content decode
    return charset;
  }

  private boolean isPageBlocked(Page page) {
    final String pageHtml = page.getRawText();
    if (pageHtml == null || pageHtml.isEmpty()) {
      logger.debug("page {} is empty, assuming we are blocked", page.getUrl());
      return true;
    }
    final Pattern p1 = Pattern.compile("Robot Check");
    final Pattern p2 = Pattern.compile("Sorry, we just need to make sure you're not a robot");
    final Matcher m1 = p1.matcher(pageHtml);
    final Matcher m2 = p2.matcher(pageHtml);
    if (m1.find() && m2.find()) {
      logger.debug("page {} is blocked", page.getUrl().toString());
      return true;
    }
    return false;
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
