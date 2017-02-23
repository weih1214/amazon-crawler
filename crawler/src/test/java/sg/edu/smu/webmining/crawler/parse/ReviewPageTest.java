package sg.edu.smu.webmining.crawler.parse;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by hwei on 22/2/2017.
 */
public class ReviewPageTest {


  @Test
  public void testReviewPage() throws IOException {
    final String html = IOUtils.toString(getClass().getResourceAsStream("/parse/reviewpage.html"));
    final Document doc = Jsoup.parse(html,"https://www.amazon.com/");
    final String commentLink = "https://www.amazon.com/gp/customer-reviews/R12J0UJC4DRYLC/ref=cm_cr_arp_d_rvw_ttl?ie=UTF8&ASIN=B003EM8008";
    final ReviewPage page = new ReviewPage(doc, commentLink);
    final JSONObject answer = new JSONObject(IOUtils.toString(getClass().getResourceAsStream("/parse/reviewpage.json")));

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
