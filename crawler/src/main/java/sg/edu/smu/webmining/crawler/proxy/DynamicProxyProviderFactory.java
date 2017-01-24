package sg.edu.smu.webmining.crawler.proxy;

import sg.edu.smu.webmining.crawler.proxy.source.FPLNetSource;
import sg.edu.smu.webmining.crawler.proxy.source.InCloakSource;
import sg.edu.smu.webmining.crawler.proxy.source.SSLProxiesOrgSource;

import java.util.Random;

/**
 * Created by mtkachenko.2015 on 24/1/2017.
 */
public class DynamicProxyProviderFactory {

  public static ProxyProvider getDefault() {
    final DynamicProxyProvider proxyProvider = new DynamicProxyProvider(new Random(System.currentTimeMillis()))
        .addSource(new FPLNetSource())
        .addSource(new SSLProxiesOrgSource())
        .addSource(new InCloakSource());

    final DynamicProxyProviderTimerWrap wrap = new DynamicProxyProviderTimerWrap(proxyProvider);
    wrap.startAutoRefresh();
    return wrap;
  }

  private DynamicProxyProviderFactory() {
    throw new UnsupportedOperationException();
  }

}
