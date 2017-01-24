package sg.edu.smu.webmining.crawler.offlinework;

import org.apache.commons.lang3.StringEscapeUtils;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by mtkachenko.2015 on 24/11/2016.
 */
public class ProductPage {

  private final Document doc;
  private final String id;
  private final static Pattern REVIEWS_NUMBER_PATTERN = Pattern.compile("[\\d\\,]*");
  private final static Pattern QUESTION_NUMBER_PATTERN = Pattern.compile("\\d+");
  private final static Pattern OFFER_NUMBER_PATTERN = Pattern.compile("\\((\\d+)\\)");
  public ProductPage(Document doc, String id) {

    this.doc = doc;
    this.id = id;
  }
  public String getProductTitle() {
    return JsoupParseUtils.selectText(doc, "#productTitle");
  }

  public String getProductId() {
    return id;
  }

  public String getBrand() {
    return JsoupParseUtils.selectText(doc, "#brand");
  }

  public Double getRating() {
    final String ratingText = JsoupParseUtils.selectText(doc, "#reviewStarsLinkedCustomerReviews");
    if (ratingText != null) {
      return Double.parseDouble(ratingText.trim().substring(0, 3));
    }
    return null;
  }

  public List<String> getColorList() {
    return doc.select("#variation_color_name img").stream()
        .map(e -> e.attr("alt"))
        .collect(Collectors.toCollection(ArrayList::new));
  }

  public String getFeatureBullets() {
    return JsoupParseUtils.selectText(doc, "#feature-bullets");
  }

  public String getWarning() {
    return JsoupParseUtils.selectText(doc, "#cpsia-product-safety-warning_feature_div");
  }

  public String getTechnicalDetails() {
    return JsoupParseUtils.selectText(doc, "#technical-data div.content");
  }

  public String getManufacturerMessage() {
    return JsoupParseUtils.selectText(doc, "#aplus_feature_div");
  }

  public String getPrice() {
    return JsoupParseUtils.selectText(doc, "#priceblock_ourprice");
  }

  public List<String> getCustomersAlsoBought() {
    return parseCarouselIds(JsoupParseUtils.selectFirst(doc, "#purchase-sims-feature"));
  }

  public List<String> getCustomersAlsoViewed() {
    return parseCarouselIds(JsoupParseUtils.selectFirst(doc, "#session-sims-feature"));
  }

  private List<String> parseCarouselIds(Element e) {
    if (e != null && e.childNodeSize() > 0) {
      final String attr = e.child(0).attr("data-a-carousel-options").replace("\"", "");
      final Matcher m = Pattern.compile("id_list:\\[(.*)]").matcher(attr);
      if (m.find()) {
        return Arrays.asList(m.group(1).split(","));
      }
    }
    return Collections.emptyList();
  }

  public List<String> getColorASINs() {
    return doc.select("#variation_color_name li").stream()
        .map(e -> e.attr("data-defaultasin"))
        .collect(Collectors.toCollection(ArrayList::new));
  }

  private List<String> parseCarouselInitialIds(Element e) {
    if (e != null) {
      final String attr = StringEscapeUtils.unescapeJava(e.attr("data-a-carousel-options"));
      final Matcher m = Pattern.compile("initialSeenAsins\":\"(.*)\"\\s,\\s\"cir").matcher(attr);
      if (m.find()) {
        return Arrays.asList(m.group(1).replace("\"", "").split(","));
      }
    }
    return Collections.emptyList();
  }

  public List<String> getSponsoredProductList1() {
    return parseCarouselInitialIds(JsoupParseUtils.selectFirst(doc, "#sp_detail"));
  }

  public List<String> getSponsoredProductList2() {
    return parseCarouselInitialIds(JsoupParseUtils.selectFirst(doc, "#sp_detail2"));
  }

  public List<String> getNewModelList() {
    final List<String> result = new ArrayList<>();
    final Pattern p = Pattern.compile("/dp/(.{10})/");
    for (Element e : doc.select("#newer-version a.a-size-base.a-link-normal")) {
      final Matcher m = p.matcher(e.attr("href"));
      if (m.find()) {
        result.add(m.group(1));
      }
    }
    return result;
  }

  public Map<String, Object> getProductInformationTable() {
    final Map<String, Object> result = new LinkedHashMap<>();
    final Elements table = doc.select("#prodDetails table");

    // Under #prodDetails, there are two tables; But only the first one has "tr" elements. So we need size() >= 1.
    if (table.size() >= 1) {
      for (Element row : table.select("tr")) {
        result.put(row.child(0).text().replace(".", ""), row.child(1).text());
      }
    }
    return result;
  }

  public String getProductDescriptionText() {
    final Element e1 = JsoupParseUtils.selectFirst(doc, "#productDescription");
    if (e1 != null) {
      final String text = e1.select("p").text();
      if (!text.isEmpty()) {
        return text;
      }
    } else {
      final Element e2 = JsoupParseUtils.selectFirst(doc, "#pd-available");
      if (e2 != null) {
        final String data = e2.nextElementSibling().data();
        final Matcher m = Pattern.compile("productDescriptionWrapper%22%3E(.*?)%3Cdiv").matcher(data);
        if (m.find()) {
          try {
            return URLDecoder.decode(m.group(1).replaceAll("%0A", "%20"), "UTF-8").trim();
          } catch (UnsupportedEncodingException ex) {
            ex.printStackTrace(); // TODO: process
          }
        }
      }
    }
    return null;
  }

  public Map<String, Object> getProductDescriptionTable() {
    final Map<String, Object> result = new LinkedHashMap<>();
    final Elements table = doc.select("#productDescription table");
    if (table.size() == 1) {
      for (Element row : table.select("tr")) {
        Elements ths = row.select("th");
        if (!ths.isEmpty()) {
          final String value = ths.get(1).text();
          if (!value.isEmpty()) {
            final String name = ths.get(0).text();
            result.put(name.replace(".", ""), value);
          } else {
            continue;
          }
        }

        Elements tds = row.select("td");
        if (!tds.isEmpty()) {
          final String value = tds.get(1).text();
          if (!value.isEmpty()) {
            final String name = tds.get(0).text();
            result.put(name.replace(".", ""), value);
          }
        }
      }
    }

    return result;
  }

  public List<String> getOtherStyleProducts() {
    return doc.select("#variation_style_name li").stream()
        .map(e -> e.attr("data-defaultasin"))
        .filter(attr -> !attr.isEmpty())
        .collect(Collectors.toCollection(ArrayList::new));
  }

  public List<String> getCustomersAlsoShoppedFor() {
    return parseCarouselIds(JsoupParseUtils.selectFirst(doc, "#day0-sims-feature"));
  }

  public String getImportantInformation() {
    return JsoupParseUtils.selectText(doc, "#importantInformation .content");
  }

  public List<String> getItemsCustomersBuyAfterViewingThis() {
    final List<String> result = new ArrayList<>();
    final Pattern p = Pattern.compile("asin:(.{10})");
    for (Element e : doc.select("#view_to_purchase-sims-feature div.a-fixed-left-grid.p13n-asin")) {
      final Matcher m = p.matcher(e.attr("data-p13n-asin-metadata").replace("\"", ""));
      if (m.find()) {
        result.add(m.group(1));
      }
    }
    return result;
  }

  public Map<String, Integer> getBestSellersRank() {
    final Map<String, Object> table = getProductInformationTable();
    final String rank = (String) table.getOrDefault("Best Sellers Rank", null);
    if (rank != null) {
      final Map<String, Integer> result = new LinkedHashMap<>();
      final String[] rankList = rank.split("#");
      final Pattern p = Pattern.compile("(.*?) in (.*)");
      for (int i = 1; i < rankList.length; i++) {
        final Matcher m = p.matcher(rankList[i].trim());
        if (m.find()) {
          result.put(m.group(2), Integer.parseInt(m.group(1).replace(",", "")));
        }
      }
      return result;
    }
    return Collections.emptyMap();
  }

  public String getOffterLink() {
    StringBuilder sb = new StringBuilder(doc.baseUri());
    final String offerLink = doc.select("div#olp_feature_div a").attr("href");
    if (offerLink.isEmpty()) {
      sb.append("/gp/offer-listing/").append(id);
      return sb.toString();
    }
    sb.append(offerLink);
    return sb.toString();
  }

  public String getQuestionLink() {
    StringBuilder sb = new StringBuilder();
    sb.append("https://www.amazon.com/ask/questions/asin/").append(id).append("/ref=ask_dp_dpmw_ql_hza?isAnswered=true");
    return sb.toString();
  }

  public String getReviewLink() {
    StringBuilder sb = new StringBuilder(doc.baseUri());
    String reviewLink = doc.select("a#dp-summary-see-all-reviews").attr("href").trim();
    if (reviewLink.isEmpty()) {
      reviewLink = doc.select("div#revSum a.a-link-emphasis").attr("href").trim();
      if (reviewLink.isEmpty()) {
        sb.append("/product-reviews/").append(id);
        return sb.toString();
      }
      return reviewLink;
    }
    sb.append(reviewLink);
    return sb.toString();
  }

  public Integer getTotalReviews() {
    final String totalReviews = doc.select("span#acrCustomerReviewText").text();
    if (totalReviews == null || totalReviews == "") {
      return null;
    }
    final Matcher m = QUESTION_NUMBER_PATTERN.matcher(totalReviews);
    if (m.find()) {
      return Integer.parseInt(m.group(0).replace(",", ""));
    } else {
      // Log failure to regex
      return null;
    }

  }

  public Integer getTotalQuestions() {
    final String totalQuestions = doc.select("a#askATFLink span.a-size-base").text();
    if (totalQuestions == null || totalQuestions == "") {
      // Log non-existence
      return null;
    }
    final Matcher m = QUESTION_NUMBER_PATTERN.matcher(totalQuestions);
    if (m.find()) {
      return Integer.parseInt(m.group(0));
    }
    // Log failure to regex
    return null;

  }

  public Integer getTotalOffers() {
    final String totalOffers = doc.select("div#olp_feature_div").text();
    if (totalOffers == null || totalOffers == "") {
      // Log non-existence
      return null;
    }
    final Matcher m = OFFER_NUMBER_PATTERN.matcher(totalOffers);
    if (m.find()) {
      return Integer.parseInt(m.group(1));
    }
    // Log failure to regex
    return null;
  }

  public Map<String, Object> asMap(){
    Map<String, Object> productMap = new LinkedHashMap<>();
    productMap.put("Product Title", getProductTitle());
    productMap.put("Product Id", getProductId());
    productMap.put("Brand", getBrand());
    productMap.put("Price", getPrice());
    productMap.put("Color List", getColorList());
    productMap.put("Color ASINs", getColorASINs());
    productMap.put("New Model List", getNewModelList());
    productMap.put("Feature Bullets", getFeatureBullets());
    productMap.put("Warning", getWarning());
    productMap.put("Technical Details", getTechnicalDetails());
    productMap.put("Manufacturer Message", getManufacturerMessage());
    productMap.put("Product Information Table", getProductInformationTable());
    productMap.put("Product Description Text", getProductDescriptionText());
    productMap.put("Product Description Table", getProductDescriptionTable());
    productMap.put("Important Information", getImportantInformation());
    productMap.put("Best Sellers Rank", getBestSellersRank());
    productMap.put("Customers Also Bought", getCustomersAlsoBought());
    productMap.put("Customers Also Viewed", getCustomersAlsoViewed());
    productMap.put("Sponsored Product List1", getSponsoredProductList1());
    productMap.put("Sponsored Product List2", getSponsoredProductList2());
    productMap.put("Offer Link", getOffterLink());
    productMap.put("Question Link", getQuestionLink());
    productMap.put("Review Link", getReviewLink());
    productMap.put("Total Reviews", getTotalReviews());
    productMap.put("Total Questions", getTotalQuestions());
    productMap.put("Total Offers", getTotalOffers());
    return productMap;
  }


}
