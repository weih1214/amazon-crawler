package sg.edu.smu.webmining.crawler.proxy;

import org.apache.http.HttpHost;

/**
 * Created by mtkachenko.2015 on 3/11/2016.
 */
public interface ProxyProvider {

  void remove(HttpHost proxy);

  HttpHost next();

}
