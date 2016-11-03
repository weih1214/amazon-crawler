package sg.edu.smu.webmining.crawler.proxy.source;

import org.apache.http.HttpHost;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Created by mtkachenko.2015 on 3/11/2016.
 */
public class DummyProxySource implements ProxyListSource {

  @Override
  public Collection<HttpHost> fetch() throws IOException {
    return new ArrayList<>(Collections.singletonList(new HttpHost("8.8.8.8", 80)));
  }
}
