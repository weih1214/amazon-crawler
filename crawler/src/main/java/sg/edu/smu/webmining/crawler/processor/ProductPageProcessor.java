package sg.edu.smu.webmining.crawler.processor;

import org.apache.commons.io.FileUtils;
import sg.edu.smu.webmining.crawler.downloader.ProxyHttpClientDownloader;
import sg.edu.smu.webmining.crawler.pipeline.HtmlPipeline;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProvider;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProviderTimerWrap;
import sg.edu.smu.webmining.crawler.proxy.source.FPLNetSource;
import sg.edu.smu.webmining.crawler.proxy.source.SSLProxiesOrgSource;
import sg.edu.smu.webmining.crawler.robotstxt.HostDirectives;
import sg.edu.smu.webmining.crawler.robotstxt.RobotstxtParser;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


public class ProductPageProcessor implements PageProcessor {

  private Site site = Site.me()
      .setRetryTimes(5)
      .setSleepTime(5000)
      .setCharset("UTF-8")
      .setCycleRetryTimes(50);

  public String seedpage;
  public boolean seedPageIsVisited = false;
  public boolean robotsIsUpdated = false;
  public static HostDirectives AmazonRobots = null;

  public List<String> import_db() {
    String driver = "com.mysql.jdbc.Driver";
    String url = "jdbc:mysql://127.0.0.1:3306/amazon";
    String user = "root";
    String password = "nrff201607";
    PreparedStatement Import;
    try {
      Class.forName(driver);
      Connection conn = DriverManager.getConnection(url, user, password);
      String import_str = "select product_id from masterlist";
      Import = conn.prepareStatement(import_str);
      // List<Integer> im_id = new ArrayList<Integer>();
      List<String> im_url = new ArrayList<String>();
      ResultSet rs = Import.executeQuery();

      while (rs.next()) {
        im_url.add(rs.getString("product_id"));
        // System.out.println(rs.getString("na"));
      }

      if (!conn.isClosed())
        System.out.println("Succeeded connecting to the Database!");
      conn.close();
      return im_url;
    } catch (ClassNotFoundException e) {
      System.out.println("Sorry,can`t find the Driver!");
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  @SuppressWarnings("resource")
  @Override
  public void process(Page page) {

    if (!robotsIsUpdated) {
      URL url = null;
      try {
        url = new URL("https://amazon.com/robots.txt");
      } catch (MalformedURLException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      try {
        FileUtils.copyURLToFile(url, new File("D://robots.txt"));
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      String content = null;
      try {
        content = new Scanner(new File("D://robots.txt")).useDelimiter("\\Z").next();
      } catch (FileNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      AmazonRobots = RobotstxtParser.parse(content);
      robotsIsUpdated = true;
    }
    if (!seedPageIsVisited) {
      seedpage = page.getUrl().toString();
      seedPageIsVisited = true;
      page.setSkip(true);
      for (String str : import_db()) {
        if (AmazonRobots.Isallowed("/*/" + str)) {
          System.out.println(seedpage + "/dp/" + str);
          page.addTargetRequest(seedpage + "/dp/" + str);

        }
      }
    } else {
      page.putField("html", page.getHtml());
      page.putField("Filename", page.getUrl().regex("/dp/([a-zA-Z0-9]+)", 1).toString());
    }

  }

  @Override
  public Site getSite() {
    return site;
  }

  public static void main(String[] args) {
    DynamicProxyProviderTimerWrap provider = new DynamicProxyProviderTimerWrap(new DynamicProxyProvider()
        .addSource(new FPLNetSource())
        .addSource(new SSLProxiesOrgSource())
    );
    provider.startAutoRefresh();

    final Spider spider = new Spider(new ProductPageProcessor())
        .setDownloader(new ProxyHttpClientDownloader(provider))
        .addPipeline(new HtmlPipeline())
        .thread(1)
        .addUrl("https://www.amazon.com");

    spider.run();

    provider.stopAutoRefresh();

  }
}
