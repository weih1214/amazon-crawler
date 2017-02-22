package sg.edu.smu.webmining.crawler.parse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * Created by mtkachenko.2015 on 20/2/2017.
 */
public class ProductPageTest {

  private void assertSameTables(Map<String, Object> expected, Map<String, Object> actual) {
    Assert.assertEquals(expected.keySet(), actual.keySet());
    for (String key : expected.keySet()) {
      HtmlUtils.assertSameRelaxedHTML(expected.get(key).toString(), StringEscapeUtils.escapeJava(actual.get(key).toString()));
    }
  }

  @Test
  public void testEarphone1() throws IOException {
    final String html = IOUtils.toString(getClass().getResourceAsStream("/parse/earphone1.html"));
    final ProductPage page = new ProductPage(html, "https://www.amazon.com");
    final JSONObject answer = new JSONObject(IOUtils.toString(getClass().getResourceAsStream("/parse/earphone1.json")));

    Assert.assertEquals(answer.get("Product ID"), page.getProductId());
    Assert.assertEquals(answer.get("Product Title"), page.getProductTitle());
    Assert.assertEquals(answer.get("Price"), page.getPrice());
    Assert.assertEquals(answer.getJSONArray("Image List").toList(), page.getImageLinks());
    Assert.assertEquals(answer.getJSONArray("Color List").toList(), page.getColorList());
    Assert.assertEquals(answer.getJSONArray("Color ASINs").toList(), page.getColorASINs());
    Assert.assertEquals(answer.getJSONArray("New Model List").toList(), page.getNewModelList());
    //Assert.assertEquals(answer.getJSONArray("Feature Bullets").toList(), page.getFeatureBullets());
    Assert.assertEquals(answer.getString("Warning"), page.getWarning());
    Assert.assertEquals(answer.getString("Technical Details"), page.getTechnicalDetails());
    //HtmlUtils.assertSameRelaxedHTML(answer.getString("Manufacturer Message"), page.getManufacturerMessage());
    //HtmlUtils.assertSameText(answer.getString("Manufacturer Message"), page.getManufacturerMessage());
    //assertSameTables(answer.getJSONObject("Product Information Table").toMap(), page.getProductInformationTable());
    //HtmlUtils.assertSameRelaxedHTML(answer.getString("Product Description Text"), page.getProductDescriptionText());
    Assert.assertEquals(answer.get("Important Information"), page.getImportantInformation());
    Assert.assertEquals(answer.getJSONObject("Best Sellers Rank").toMap(), page.getBestSellersRank());
    Assert.assertEquals(answer.getJSONArray("Customers Also Bought").toList(), page.getCustomersAlsoBought());
    Assert.assertEquals(answer.getJSONArray("Customers Also Shopped For").toList(), page.getCustomersAlsoShoppedFor());
    Assert.assertEquals(answer.getJSONArray("Customers Also Viewed").toList(), page.getCustomersAlsoViewed());
    Assert.assertEquals(answer.getJSONArray("Sponsored Product List1").toList(), page.getSponsoredProductList1());
    Assert.assertEquals(answer.getJSONArray("Sponsored Product List2").toList(), page.getSponsoredProductList2());
    Assert.assertEquals(answer.get("Offer Link"), page.getOfferLink());
    //Assert.assertEquals(answer.get("Question Link"), page.getQuestionLink());
    //Assert.assertEquals(answer.get("Review Link"), page.getReviewLink());
    //Assert.assertEquals(answer.get("Total Reviews"), page.getTotalReviews());
    Assert.assertEquals(answer.get("Total Offers"), page.getTotalOffers());
    Assert.assertEquals(answer.get("Total Questions"), page.getTotalQuestions());
  }

}
