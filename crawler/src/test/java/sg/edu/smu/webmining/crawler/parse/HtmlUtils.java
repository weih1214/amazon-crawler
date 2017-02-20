package sg.edu.smu.webmining.crawler.parse;

import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;
import org.junit.Assert;

/**
 * Created by mtkachenko.2015 on 20/2/2017.
 */
public class HtmlUtils {

  public static String relaxHTML(String html) {
    return Jsoup.clean(html, Whitelist.relaxed());
  }

  public static String extractText(String html) {
    return Jsoup.clean(html, Whitelist.none());
  }

  public static void assertSameRelaxedHTML(String expected, String actual) {
    Assert.assertEquals(relaxHTML(expected), relaxHTML(actual));
  }

  public static void assertSameText(String expected, String actual) {
    Assert.assertEquals(extractText(expected), extractText(actual));
  }

  private HtmlUtils() {
    throw new UnsupportedOperationException();
  }

}
