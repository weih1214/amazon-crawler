package sg.edu.smu.webmining.crawler.proxy.source;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpHost;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mtkachenko.2015 on 3/11/2016.
 */
public class FPLNetSource implements ProxyListSource {

  @Override
  public List<HttpHost> fetch() throws IOException {
    Document html = Jsoup.connect("https://free-proxy-list.net/")
        .userAgent(RandomStringUtils.random(32, true, true))
        .get();

    Elements addrs = html.select("table td:eq(0)");
    Elements ports = html.select("table td:eq(1)");
    Elements protocols = html.select("table td:eq(6)");

    final List<HttpHost> proxies = new ArrayList<>(addrs.size());

    for (int i = 0; i < addrs.size(); i++) {
      if ("yes".equals(protocols.get(i).text())) {
        proxies.add(new HttpHost(addrs.get(i).text(), Integer.parseInt(ports.get(i).text())));
      }
    }

    return proxies;
  }

}
