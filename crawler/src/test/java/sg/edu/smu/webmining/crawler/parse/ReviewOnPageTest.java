package sg.edu.smu.webmining.crawler.parse;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by hwei on 23/2/2017.
 */
public class ReviewOnPageTest {
  @Test
  public void testReviewPage() throws IOException {
    final String html = IOUtils.toString(getClass().getResourceAsStream("/parse/reviewonpage.html"));
    final Document doc = Jsoup.parse(html,"https://www.amazon.com/");
    final String url = "https://www.amazon.com/Panasonic-RP-HJE120-PPK-In-Ear-Headphone-Black/product-reviews/B003EM8008/ref=cm_cr_arp_d_paging_btm_3837?ie=UTF8&reviewerType=avp_only_reviews&pageNumber=3837";
    final Element e = doc.select("div#R2DZ0N9ODRGE81").first();
    final ReviewOnPage page = new ReviewOnPage(e, url);
    final JSONObject answer = new JSONObject(IOUtils.toString(getClass().getResourceAsStream("/parse/reviewonpage.json")));

    Assert.assertEquals(answer.get("Product ID"), page.getProductId());
    Assert.assertEquals(answer.get("Review Title"), page.getTitle());
    Assert.assertEquals(answer.get("Review ID"), page.getId());
    Assert.assertEquals(answer.get("Author"), page.getAuthorName());
    Assert.assertEquals(answer.get("Author ID"), page.getAuthorId());
    Assert.assertEquals(answer.get("Positive Voters"), page.getUpvotes());
    Assert.assertEquals(answer.get("Total Voters"), page.getTotalVotes());
    Assert.assertEquals(answer.get("Product Information"), page.getProductInformation());
    Assert.assertEquals(answer.get("Purchase Verification"), page.isVerifiedPurchase());
    Assert.assertEquals(answer.get("Review Rating"), page.getRating());
    Assert.assertEquals(answer.get("Review Content"), page.getContent());
    Assert.assertEquals(answer.getJSONArray("Image List").toList(), page.getImageLinks());
    Assert.assertEquals(answer.get("Comment Link"), page.getCommentLink());
    Assert.assertEquals(answer.get("Total Comments"), page.getTotalComments());

  }
}
