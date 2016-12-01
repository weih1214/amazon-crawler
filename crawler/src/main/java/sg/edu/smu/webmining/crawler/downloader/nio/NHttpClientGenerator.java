package sg.edu.smu.webmining.crawler.downloader.nio;

import org.apache.http.client.CookieStore;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import us.codecraft.webmagic.Site;

import java.util.Map;

/**
 * @author mtkachenko
 */
public class NHttpClientGenerator {

  private PoolingNHttpClientConnectionManager cm;

  public NHttpClientGenerator() {
    try {
      final ConnectingIOReactor reactor = new DefaultConnectingIOReactor(IOReactorConfig.DEFAULT, r -> new Thread(r, "io-reactor-thread"));

      final Registry<SchemeIOSessionStrategy> reg = RegistryBuilder.<SchemeIOSessionStrategy>create()
          .register("http", NoopIOSessionStrategy.INSTANCE)
          .register("https", SSLIOSessionStrategy.getDefaultStrategy())
          .build();

      cm = new PoolingNHttpClientConnectionManager(reactor, reg);
      cm.setDefaultMaxPerRoute(100);
    } catch (IOReactorException e) {
      throw new RuntimeException(e);
    }
  }

  public NHttpClientGenerator setPoolSize(int poolSize) {
    cm.setMaxTotal(poolSize);
    return this;
  }

  public PoolingNHttpClientConnectionManager getConnectionManager() {
    return cm;
  }

  public CloseableHttpAsyncClient getClient(Site site) {
    return generateClient(site);
  }

  private CloseableHttpAsyncClient generateClient(Site site) {
    final HttpAsyncClientBuilder builder = HttpAsyncClientBuilder.create()
        .setConnectionManager(cm);

    /*if (site == null || site.isUseGzip()) {
      builder.addInterceptorFirst((HttpRequestInterceptor) (request, context) -> {
        if (!request.containsHeader("Accept-Encoding")) {
          request.addHeader("Accept-Encoding", "gzip");
        }
      });
    }*/

    final IOReactorConfig socketConfig = IOReactorConfig.custom()
        .setSoKeepAlive(true)
        .setTcpNoDelay(true)
        .setSoTimeout(1000)
        .build();

    builder.setDefaultIOReactorConfig(socketConfig);
    generateCookie(builder, site);
    return builder.build();
  }

  private void generateCookie(HttpAsyncClientBuilder httpClientBuilder, Site site) {
    CookieStore cookieStore = new BasicCookieStore();
    for (Map.Entry<String, String> cookieEntry : site.getCookies().entrySet()) {
      BasicClientCookie cookie = new BasicClientCookie(cookieEntry.getKey(), cookieEntry.getValue());
      cookie.setDomain(site.getDomain());
      cookieStore.addCookie(cookie);
    }
    for (Map.Entry<String, Map<String, String>> domainEntry : site.getAllCookies().entrySet()) {
      for (Map.Entry<String, String> cookieEntry : domainEntry.getValue().entrySet()) {
        BasicClientCookie cookie = new BasicClientCookie(cookieEntry.getKey(), cookieEntry.getValue());
        cookie.setDomain(domainEntry.getKey());
        cookieStore.addCookie(cookie);
      }
    }
    httpClientBuilder.setDefaultCookieStore(cookieStore);
  }

}
