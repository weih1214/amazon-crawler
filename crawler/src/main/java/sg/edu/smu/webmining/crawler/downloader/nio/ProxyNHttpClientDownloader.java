package sg.edu.smu.webmining.crawler.downloader.nio;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProviderFactory;
import sg.edu.smu.webmining.crawler.proxy.ProxyProvider;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.downloader.AbstractDownloader;
import us.codecraft.webmagic.selector.PlainText;
import us.codecraft.webmagic.utils.HttpConstant;
import us.codecraft.webmagic.utils.UrlUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProxyNHttpClientDownloader extends AbstractDownloader implements AutoCloseable {

  private static final int TIMEOUT = 20;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final Map<String, CloseableHttpAsyncClient> httpClients = new HashMap<>();

  private final NHttpClientGenerator httpClientGenerator = new NHttpClientGenerator();

  private final ProxyProvider proxyProvider;

  private final Set<Integer> stopStatusCodes;

  public ProxyNHttpClientDownloader() {
    this(DynamicProxyProviderFactory.getDefault());
  }

  public ProxyNHttpClientDownloader(ProxyProvider proxyProvider) {
    this(proxyProvider, Collections.emptySet());
  }

  public ProxyNHttpClientDownloader(ProxyProvider proxyProvider, Set<Integer> stopStatusCodes) {
    this.proxyProvider = proxyProvider;
    this.stopStatusCodes = stopStatusCodes;
  }

  private CloseableHttpAsyncClient getHttpClient(Site site) {
    if (site == null) {
      return httpClientGenerator.getClient(null);
    }
    String domain = site.getDomain();
    CloseableHttpAsyncClient httpClient = httpClients.get(domain);
    if (httpClient == null) {
      synchronized (this) {
        httpClient = httpClients.computeIfAbsent(domain, k -> httpClientGenerator.getClient(site));
        httpClient.start();
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

    Future<HttpResponse> futureResponse = null;
    HttpResponse httpResponse = null;
    int statusCode = 0;
    try {
      final HttpUriRequest httpUriRequest = getHttpUriRequest(request, site, headers);
      final CloseableHttpAsyncClient httpClient = getHttpClient(site);
      logger.debug("downloading page {} via proxy {}", request.getUrl(), request.getExtra(Request.PROXY));

      futureResponse = httpClient.execute(httpUriRequest, LoggingFutureCallback.create());

      httpResponse = futureResponse.get(TIMEOUT, TimeUnit.SECONDS);
      statusCode = httpResponse.getStatusLine().getStatusCode();

      if (statusCode == 200) {
        Page page = handleResponse(request, charset, httpResponse, task);
        if (!page.isNeedCycleRetry() && isPageBlocked(page)) {
          logger.debug("page is blocked, rescheduling: {}", request.getUrl());
          return addToCycleRetry(request, site);
        }
        onSuccess(request);
        return page;
      } else if (stopStatusCodes.contains(statusCode)) {
        logger.debug("status code {} is in stop list, terminate downloading: {}", statusCode, request.getUrl());
        return null;
      } else {
        logger.debug("status code {}, rescheduling: {}", statusCode, request.getUrl());
        return addToCycleRetry(request, site);
      }

    } catch (ExecutionException e) {
      logger.warn("download page error, rescheduling: " + request.getUrl(), e);
      return addToCycleRetry(request, site);
    } catch (InterruptedException e) {
      logger.error("downloader thread interrupted, terminating", e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      logger.error("io error when handing response, rescheduling: " + request.getUrl(), e);
      return addToCycleRetry(request, site);
    } catch (TimeoutException e) {
      futureResponse.cancel(true);
      logger.error("downloading timeout, rescheduling: " + request.getUrl(), e);
      return addToCycleRetry(request, site);
    } finally {
      request.putExtra(Request.STATUS_CODE, statusCode);
      if (httpResponse != null) {
        try {
          EntityUtils.consume(httpResponse.getEntity());
        } catch (IOException e) {
          logger.warn("fail to consume entity", e);
        }
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


  @Override
  public void close() throws IOException {
    for (final CloseableHttpAsyncClient client : httpClients.values()) {
      if (client != null) {
        client.close();
      }
    }
    httpClientGenerator.shutdown();
  }

}
