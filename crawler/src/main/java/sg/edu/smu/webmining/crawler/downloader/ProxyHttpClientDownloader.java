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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by weih1214 on 10/11/2016.
 */
public class ProxyHttpClientDownloader extends HttpClientDownloader {

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
        .setSocketTimeout(60000)
        .setConnectTimeout(site.getTimeOut())
        .setCookieSpec(CookieSpecs.BEST_MATCH)
        .setProxy(proxy);

    request.putExtra(Request.PROXY, proxy);

    final HttpUriRequest uriRequest = requestBuilder.setConfig(requestConfigBuilder.build()).build();
    uriRequest.setHeader("User-Agent", RandomStringUtils.random(32, true, true));
    return uriRequest;
  }
  
  public boolean PageChecker(Page page) {
	   try {
		String PageCheck = page.getHtml().toString();
		   if(PageCheck.isEmpty()){
		  		logger.error("Page"+page.getUrl().toString()+"is empty");
		  		return true;
		   }
		   String s1 = "Robot Check";
		   String s2 = "Sorry, we just need to make sure you're not a robot";
		   Pattern p1 = Pattern.compile(s1);
		   Pattern p2 = Pattern.compile(s2);
		   Matcher m1 = p1.matcher(PageCheck);
		   Matcher m2 = p2.matcher(PageCheck);
		   if (m1.find() && m2.find()) {
		  		logger.error("Page"+page.getUrl().toString()+"is blocked");
		  		return true;
		  	}
		  	return false;
	} catch (NullPointerException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		return true;
	}
 }
  
  
  @Override
  public Page download(Request request, Task task) {
	  
		ExecutorService executor = Executors.newFixedThreadPool(1);
		Future<Page> future = executor.submit(() -> {
			return super.download(request, task);
		});

		try {
			logger.debug("Downloading begins at: " + (new Date(System.currentTimeMillis())).getMinutes());
			Page page = future.get(300, TimeUnit.SECONDS);
			logger.debug("Downloading ends at: " + (new Date(System.currentTimeMillis())).getMinutes());
			if (page == null) {
				final Object statusCode = request.getExtra(Request.STATUS_CODE);
				if (statusCode != null && (Integer) statusCode != 200) {
					logger.debug("status code is not OK, request rescheduled");
					return addToCycleRetry(request, task.getSite());
				}
			}
			if (PageChecker(future.get())) {
				return addToCycleRetry(request, task.getSite());
			}
			return page;
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			logger.error("Timeout in downloading");
			return addToCycleRetry(request, task.getSite());
		}

//    	return addToCycleRetry(request, task.getSite());
    	
//    Page page = super.download(request, task);
//    if (page == null) {
//      final Object statusCode = request.getExtra(Request.STATUS_CODE);
//      if (statusCode != null && (Integer) statusCode != 200) {
//        logger.debug("status code is not OK, request rescheduled");
//        return addToCycleRetry(request, task.getSite());
//      }
//    }
//    if (PageChecker(page)) {
//    	return addToCycleRetry(request, task.getSite());
//    }
//    return page;
  }

  @Override
  protected Page addToCycleRetry(Request request, Site site) {
    proxyProvider.remove((HttpHost) request.getExtra(Request.PROXY));
    return super.addToCycleRetry(request, site);
  }

  @Override
  protected void onError(Request request) {
    logger.warn("Error happened, but not processed");
  }
  
  

}
