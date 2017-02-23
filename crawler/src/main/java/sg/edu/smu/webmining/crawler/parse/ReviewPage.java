package sg.edu.smu.webmining.crawler.parse;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hwei on 23/2/2017.
 */
public class ReviewPage extends Review {

  private static final Pattern HELPFUL_PATTERN = Pattern.compile("(.*) of (.*) people");
  private static final Pattern URL_REVIEW_ID_PATTERN = Pattern.compile("/customer-reviews/(.*?)/");
  private static final Pattern URL_PRODUCT_ID_PATTERN = Pattern.compile("ASIN=(.{10})");
  private static final Pattern COMMENT_NUMBER_PATTERN = Pattern.compile("(\\d+) posts");

  private final Document doc;
  private final String url;


  public ReviewPage(Document doc, String url) {
    this.doc = doc;
    this.url = url;
  }

  private static String parseWithRegexp(String url, Pattern pattern) {
    final Matcher m = pattern.matcher(url);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  @Override
  public String getTitle() {
    return doc.select("table div[style = margin-bottom:0.5em;] b").first().text();
  }

  @Override
  public String getId() {
    return parseWithRegexp(url, URL_REVIEW_ID_PATTERN);
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
    String productInfo = doc.select("table div.tiny b").text();
    productInfo = productInfo.replaceAll(".*is from:" , "").trim();
    return productInfo;
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
  public String getCommentLink() {
    return url;
  }

  @Override
  public String getContent() {
    return doc.select("table div.reviewText").text();
  }

  @Override
  public String getProductId() {
    return parseWithRegexp(url, URL_PRODUCT_ID_PATTERN);
  }

  @Override
  public List<String> getImageLinks() {
    List <String> imgList = new ArrayList<>();
    final Elements imgElements = doc.select("div img.review-image-thumbnail");
    for (Element sub : imgElements) {
      String imgUrl = sub.attr("src");
      imgUrl = imgUrl.replaceAll("\\._.*_\\.jpg", "\\.jpg");
      imgList.add(imgUrl);
    }
    return imgList;
  }

  public Integer getTotalComments() {
    final String totalComments = doc.select("div.fosmall div.cdPageInfo").text();
    if (totalComments == null || totalComments.isEmpty()) {
      // Log failure to parse
      return 0;
    }
    final Matcher m = COMMENT_NUMBER_PATTERN.matcher(totalComments);
    if (m.find()) {
      return Integer.parseInt(m.group(1));
    }
    // Log failure to regex
    return null;
  }
}

