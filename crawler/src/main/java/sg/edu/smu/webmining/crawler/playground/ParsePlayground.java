package sg.edu.smu.webmining.crawler.playground;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.File;
import java.io.IOException;

/**
 * Created by hwei on 18/1/2017.
 */
public class ParsePlayground {
  public static void main(String[] args) throws IOException {
//    StringBuilder sb = new StringBuilder("ABC");
//    sb.append("hahahah");
//    System.out.println(sb.toString());
    File file = new File("D://parse.html");
    Document doc = Jsoup.parse(file, "UTF-8", "https://www.amazon.com");
//    System.out.println(doc.baseUri());
    final String offerLink = doc.select("div#olp_feature_div a").attr("href");
//    if (offerLink.isEmpty()) {
//      System.out.println("This is null.");
//    }
    System.out.println(offerLink);
    final String questionLink = doc.select("div#ask_lazy_load_div").attr("href");
    System.out.println(questionLink);
    final String reviewLink = doc.select("a#dp-summary-see-all-reviews").attr("href");
    System.out.println(reviewLink);
  }
}
