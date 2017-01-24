package sg.edu.smu.webmining.crawler.parse;

import org.jsoup.nodes.Element;

/**
 * Created by mtkachenko.2015 on 24/11/2016.
 */
public class JsoupParseUtils {

  public static Element selectFirst(Element e, String cssQuery) {
    return e.select(cssQuery).first();
  }

  public static String selectText(Element e, String cssQuery) {
    final Element selected = selectFirst(e, cssQuery);
    if (selected != null) {
      final String text = selected.text();
      if (!text.isEmpty()) {
        return text;
      }
    }
    return null;
  }


  private JsoupParseUtils() {
    throw new UnsupportedOperationException();
  }

}
