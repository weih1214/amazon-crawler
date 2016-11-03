package sg.edu.smu.webmining.crawler.proxy.source;

import org.apache.http.HttpHost;

import java.io.IOException;
import java.util.Collection;

/**
 * Created by mtkachenko.2015 on 3/11/2016.
 */
public interface ProxyListSource {

  public Collection<HttpHost> fetch() throws IOException;

}
