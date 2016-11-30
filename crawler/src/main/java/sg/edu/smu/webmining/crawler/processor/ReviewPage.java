package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.nodes.Element;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by mtkachenko.2015 on 30/11/2016.
 */
public class ReviewPage extends Review {

  private static final Pattern HELPFUL_PATTERN = Pattern.compile("(.*) of (.*) people");

  private final String reviewId;
  private final String productId;
  private final Element doc;

  public ReviewPage(String reviewId, String productId, Element reviewPage) {
    this.reviewId = reviewId;
    this.productId = productId;
    doc = reviewPage;
  }

  @Override
  public String getTitle() {
    return doc.select("table div[style = margin-bottom:0.5em;] b").first().text();
  }

  @Override
  public String getId() {
    return reviewId;
  }

  @Override
  public String getAuthorName() {
    return doc.select("table div[style = margin-bottom:0.5em;] span[style = font-weight: bold;]").text();
  }

  @Override
  public String getAuthorId() {
    return parseAuthorIdFromUrl(doc.select("table div[style = margin-bottom:0.5em;] div[style = float:left;] a").attr("href"));
  }

  @Override
  public String getDateString() {
    return doc.select("table div[style = margin-bottom:0.5em;] nobr").text().replace(",", "");
  }

  @Override
  public String getProductInformation() {
    return doc.select("table div.tiny b").text();
  }

  @Override
  public boolean isVerifiedPurchase() {
    final String s = doc.select("table div.tiny span.crVerifiedStripe b").text();
    return !s.isEmpty();
  }

  @Override
  public Double getRating() {
    final String ratingString = doc.select("table div[style = margin-bottom:0.5em;] img").attr("alt").trim();
    if (!ratingString.isEmpty()) {
      return Double.parseDouble(ratingString.substring(0, 3));
    }
    return null;
  }

  // when index == 1, returns upvotes, when index == 2, returns all votes
  private Integer getVotes(int index) {
    final String s = doc.select("table div[style = margin-bottom:0.5em;]").text().trim();
    final Matcher m = HELPFUL_PATTERN.matcher(s);
    if (m.find()) {
      return Integer.parseInt(m.group(index).replace(",", ""));
    }
    return null;
  }

  @Override
  public Integer getUpvotes() {
    return getVotes(1);
  }

  @Override
  public Integer getTotalVotes() {
    return getVotes(2);
  }

  @Override
  public String getContent() {
    return doc.select("table div.reviewText").text();
  }

  @Override
  public String getProductId() {
    return productId;
  }

}
