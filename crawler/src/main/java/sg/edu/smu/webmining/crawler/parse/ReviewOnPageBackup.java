package sg.edu.smu.webmining.crawler.parse;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hwei on 23/2/2017.
 */
public class ReviewOnPageBackup extends Review{
  private final String productId;
  private final Element e;

  public ReviewOnPageBackup(String productId, Element reviewElement) {
    this.productId = productId;
    e = reviewElement;
  }

  @Override
  public String getTitle() {
    return e.select("a[data-hook=review-title]").text();
  }

  @Override
  public String getId() {
    return e.attr("id");
  }

  @Override
  public String getAuthorName() {
    return e.getElementsByAttributeValue("data-hook", "review-author").text();
  }

  @Override
  public String getAuthorId() {
    return parseAuthorIdFromUrl(e.select("a[data-hook = review-author]").attr("href"));
  }

  @Override
  public String getDateString() {
    final String data = e.select("span[data-hook= review-date]").text();
    if (data == null || data.isEmpty()) return null;
    return data.substring(3).replace(",", "");
  }

  @Override
  public String getProductInformation() {
    return e.select("a[data-hook= format-strip]").text();
  }

  @Override
  public boolean isVerifiedPurchase() {
    final String s = e.select("span[data-hook = avp-badge]").text();
    return !s.isEmpty();
  }

  @Override
  public Double getRating() {
    final String ratingString = e.select("i[data-hook = review-star-rating]").text();
    if (!ratingString.isEmpty()) {
      return Double.parseDouble(ratingString.substring(0, 3));
    }
    return null;
  }

  @Override
  public String getContent() {
    return e.select("span[data-hook = review-body]").text();
  }

  @Override
  public String getProductId() {
    return productId;
  }

  @Override
  public List <String> getImageLinks() {
    List <String> imgList = new ArrayList<>();
    final Elements imgElements = e.select("div.review-image-tile-section img");
    for (Element sub : imgElements) {
      String imgUrl = sub.attr("src");
      imgUrl = imgUrl.replaceAll("\\._.*_\\.jpg", "\\.jpg");
      imgList.add(imgUrl);
    }
    return imgList;
  }

  @Override
  public Integer getUpvotes() {
    return 0;
  }

  @Override
  public Integer getTotalVotes() {
    return 0;
  }

  @Override
  public String getCommentLink() {
    return null;
  }

  @Override
  public Integer getTotalComments() {
    return null;
  }


}
