package sg.edu.smu.webmining.crawler.parse;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hwei on 15/12/2016.
 */
public class Offer {

  private static final Pattern SELLER_ID_PATTERN = Pattern.compile("seller=([A-Z0-9]{13,14})");
  private static final Pattern SELLER_RATING_PATTERN = Pattern.compile("(\\d(\\.\\d)?) out of \\d");
  private static final Pattern POSITIVE_PERCENT_PATTERN = Pattern.compile("(\\d\\d{0,2}?)%");
  private static final Pattern TOTAL_RATINGS_PATTERN = Pattern.compile("\\(([\\d,]+?) total ratings\\)");
  private static final Pattern RECORD_PERIOD_PATTERN = Pattern.compile("over the past (\\d\\d?) months");

  private final Element offerElement;
  private final String productId;

  public Offer(Element offerElement, String productId) {
    this.offerElement = offerElement;
    this.productId = productId;
  }

  public String getSellerName() {
    final Element nameElement = offerElement.select("h3.olpSellerName").first();
    final String nameString = nameElement.text();
    if (!nameString.isEmpty()) {
      return nameString;
    } else {
      final String altString = nameElement.select("img").attr("alt");
      if (!altString.isEmpty()) {
        return altString;
      }
    }
    return null;
  }

  public String getSellerId() {
    final String sellerIdString = offerElement.select("div.olpSellerColumn h3.olpSellerName a").attr("href");
    final Matcher m = SELLER_ID_PATTERN.matcher(sellerIdString);
    if (m.find()) {
      return m.group(1);
    } else {
      return getSellerName();
    }
  }

  // Return 0.0 means no record; return null means parsing fails
  public Double getRating() {
    final Elements sellerElements = offerElement.select("div.olpSellerColumn p.a-spacing-small");
    if (!sellerElements.isEmpty()) {
      final String sellerString = sellerElements.first().text();
      final Matcher m = SELLER_RATING_PATTERN.matcher(sellerString);
      if (m.find()) {
        return Double.parseDouble(m.group(1));
      } else {
        return null;
      }
    } else {
      return 0.0;
    }
  }

  private Integer extractPositivePercentage(String percentString) {
    final Matcher m = POSITIVE_PERCENT_PATTERN.matcher(percentString);
    if (m.find()) {
      return Integer.parseInt(m.group(1));
    }
    return null;
  }

  public Integer getSellerPositivePercentage() {
    final String percentString = offerElement.select("div.olpSellerColumn p").text();
    if (!percentString.isEmpty()) {
      return extractPositivePercentage(percentString);
    } else {
      return null;
    }
  }

  public Integer extractTotalRatings(String totalRatingsString) {
    final Matcher m = TOTAL_RATINGS_PATTERN.matcher(totalRatingsString);
    if (m.find()) {
      return Integer.parseInt(m.group(1).replace(",", ""));
    }
    return null;
  }

  public Integer getSellerTotalRatings() {
    final String totalRatingsString = offerElement.select("div.olpSellerColumn p").text();
    if (!totalRatingsString.isEmpty()) {
      return extractTotalRatings(totalRatingsString);
    } else {
      return null;
    }
  }

  public Integer extractRecordPeriod(String periodString) {
    final Matcher m = RECORD_PERIOD_PATTERN.matcher(periodString);
    if (m.find()) {
      return Integer.parseInt(m.group(1));
    }
    return null;
  }

  public Integer getRecordPeriod() {
    final String periodString = offerElement.select("div.olpSellerColumn p").text();
    if (!periodString.isEmpty()) {
      return extractRecordPeriod(periodString);
    } else {
      return null;
    }
  }

  public String getCondition() {
    return offerElement.select("div.olpConditionColumn span").text();
  }

  public String getConditionDescription() {
    final String conditionDescription = offerElement.select("div.olpConditionColumn div.comments").text();
    if (!conditionDescription.isEmpty()) {
      return conditionDescription;
    }
    return null;
  }

  private String getPrice() {
    return offerElement.select("div.olpPriceColumn span.olpOfferPrice").text();
  }

  private boolean getFulfillmentInfo() {
    final String deliveryInfo = offerElement.select("div.olpDeliveryColumn").text();
    return deliveryInfo.contains("Fulfillment by Amazon");
  }

  private String getShippingInformation() {
    final String shippingInfo = offerElement.select("div.olpPriceColumn p.olpShippingInfo").text();
    return shippingInfo.replace("Details", "").replaceAll("[&+]", "").trim();
  }

  private String getProductId() {
    return productId;
  }

  public Map<String, Object> asMap() {
    final Map<String, Object> offerDoc = new LinkedHashMap<>();
    offerDoc.put("Seller Name", getSellerName());
    offerDoc.put("Seller ID", getSellerId());
    offerDoc.put("Product ID", getProductId());
    offerDoc.put("Seller Rating", getRating());
    offerDoc.put("Seller Positive Percentage (%)", getSellerPositivePercentage());
    offerDoc.put("Seller Total Ratings", getSellerTotalRatings());
    offerDoc.put("Seller Record Period (Months)", getRecordPeriod());
    offerDoc.put("Condition", getCondition());
    offerDoc.put("Condition Description", getConditionDescription());
    offerDoc.put("Price", getPrice());
    offerDoc.put("Fulfillment by Amazon", getFulfillmentInfo());
    offerDoc.put("Shipping", getShippingInformation());
    return offerDoc;
  }
}
