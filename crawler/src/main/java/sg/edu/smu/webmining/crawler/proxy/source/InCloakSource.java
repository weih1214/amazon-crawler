package sg.edu.smu.webmining.crawler.proxy.source;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpHost;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by mtkachenko.2015 on 1/12/2016.
 */
public class InCloakSource implements ProxyListSource {

  @Override
  public Collection<HttpHost> fetch() throws IOException {
    final Document doc = Jsoup.connect("https://incloak.com/proxy-list/?type=s&anon=4#list")
        .userAgent(RandomStringUtils.random(32, true, true))
        .get();

    final List<HttpHost> proxies = new ArrayList<>();

    final Element table = doc.select("table.proxy__t").first();
    if (table != null) {
      for (Element e : table.select("tbody tr")) {
        Elements td = e.select("td");
        if (td.size() == 7) {
          final String hostname = td.get(0).text();
          final String port = td.get(1).text();
          proxies.add(new HttpHost(hostname, Integer.parseInt(port)));

          /*String[] schemes = td.get(4).text().toLowerCase().split("\\s*,\\s*");
          for (String scheme : schemes) {
            proxies.add(new HttpHost(hostname, Integer.parseInt(port), scheme));
          }*/
        }
      }
    }

    return proxies;
  }

}
