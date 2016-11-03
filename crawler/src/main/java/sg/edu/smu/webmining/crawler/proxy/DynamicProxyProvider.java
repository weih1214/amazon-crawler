package sg.edu.smu.webmining.crawler.proxy;

import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sg.edu.smu.webmining.crawler.proxy.source.ProxyListSource;

import java.io.IOException;
import java.util.*;

/**
 * Created by mtkachenko.2015 on 3/11/2016.
 */
public class DynamicProxyProvider implements ProxyProvider {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final List<ProxyListSource> sources = new LinkedList<>();

  private final ArrayList<HttpHost> proxies = new ArrayList<>();

  private final Random random;

  public DynamicProxyProvider() {
    this(new Random(System.currentTimeMillis()));
  }

  public DynamicProxyProvider(Random random) {
    this.random = random;
  }

  public DynamicProxyProvider addSource(ProxyListSource source) {
    sources.add(source);
    return this;
  }

  public void removeSource(ProxyListSource source) {
    sources.remove(source);
  }

  public synchronized void refresh() {
    final Set<HttpHost> dummy = new HashSet<>();
    for (final ProxyListSource source : sources) {
      try {
        dummy.addAll(source.fetch());
      } catch (IOException e) {
        logger.warn("Could not fetch proxy list for one of the sources: ", e);
      }
    }

    proxies.clear();
    proxies.addAll(dummy);
  }

  @Override
  public synchronized void remove(HttpHost proxy) {
    proxies.remove(proxy);
  }

  @Override
  public synchronized HttpHost next() {
    if (proxies.isEmpty()) {
      logger.error("proxy pool is empty, throw exception, terminate thread");
      throw new RuntimeException("Proxy pool is empty");
    }

    return proxies.get(random.nextInt(proxies.size()));
  }

  public List<HttpHost> getProxyList() {
    return Collections.unmodifiableList(proxies);
  }

}
