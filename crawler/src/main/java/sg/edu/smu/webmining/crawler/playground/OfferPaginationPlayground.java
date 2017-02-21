package sg.edu.smu.webmining.crawler.playground;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;

/**
 * Created by hwei on 19/1/2017.
 */
public class OfferPaginationPlayground {

  public static String getNextLink(Element paginationElement) {
    final String s1 = paginationElement.className();
    if (s1.contains("a-disabled")) {
      return null;
    } else {
      return paginationElement.select("a").attr("href");
    }
  }


  public static void main(String[] args) throws IOException {
    File file1 = new File("D:\\page1.html"); // Complete pagination
    File file2 = new File("D:\\page2.html"); // No pagination
    Document doc1 = Jsoup.parse(file1, "UTF-8", "https://www.amazon.com");
    Document doc2 = Jsoup.parse(file2, "UTF-8", "https://www.amazon.com");
    final Elements offerElements1 = doc1.select("ul.a-pagination li");
    if (!offerElements1.isEmpty()) {
      final String url1 = getNextLink(offerElements1.last());
      System.out.println("url1 is" + url1);
    }
    final Elements offerElements2 = doc2.select("ul.a-pagination li");
    if (!offerElements2.isEmpty()) {
      final String url2 = getNextLink(offerElements2.last());
      System.out.println("url2 is" + url2);
    }
  }

}
