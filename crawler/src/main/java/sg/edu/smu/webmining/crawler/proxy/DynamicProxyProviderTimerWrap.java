package sg.edu.smu.webmining.crawler.proxy;

import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by mtkachenko.2015 on 3/11/2016.
 */
public class DynamicProxyProviderTimerWrap implements ProxyProvider {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private static final int DEFAULT_PERIOD = 300000; // 5mins

  private final DynamicProxyProvider provider;
  private final long period;

  private final Timer timer;

  public DynamicProxyProviderTimerWrap(DynamicProxyProvider provider) {
    this(provider, DEFAULT_PERIOD);
  }

  public DynamicProxyProviderTimerWrap(DynamicProxyProvider provider, long period) {
    this.provider = provider;
    this.period = period;
    this.timer = new Timer(true);
  }

  public void startAutoRefresh() {
    provider.refresh();
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        provider.refresh();
        logger.info("proxy list refreshed (# of proxies = {})", provider.getProxyList().size());
      }
    }, period, period);
  }

  public void stopAutoRefresh() {
    timer.cancel();
  }

  @Override
  public void remove(HttpHost proxy) {
    provider.remove(proxy);
  }

  @Override
  public HttpHost next() {
    return provider.next();
  }
}
