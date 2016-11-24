package sg.edu.smu.webmining.crawler.offlinework;


import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by mtkachenko.2015 on 24/11/2016.
 */
public class ProductPageUpdater {


  public Map<String, Object> parse(InputStream is) throws IOException {
    final Document doc = Jsoup.parse(is, "UTF-8", "https://amazon.com/");

    Map<String, Object> result = new LinkedHashMap<>();
    ProductPage page = new ProductPage(doc);
    result.put("ProductTitle", page.getProductTitle());
    result.put("Brand", page.getBrand());
    result.put("AverageRating", page.getRating());
    result.put("Price", page.getPrice());
    result.put("Colors", page.getColorList());
    result.put("Products of Other Colors", page.getColorASINs());
    result.put("Feature-bullets", page.getFeatureBullets());
    result.put("Newer Model", page.getNewModelList());
    result.put("Sponsored Products Related", page.getSponsoredProductList1());
    result.put("Sponsored Products Related-2", page.getSponsoredProductList2());
    result.put("Customers Also Bought", page.getCustomersAlsoBought());
    result.put("Customers Also viewed", page.getCustomersAlsoViewed());
    result.put("Warning", page.getWarning());
    result.put("Technical Details", page.getTechnicalDetails());
    result.put("Product Description", page.getProductDescriptionText());
    result.put("Product Description Table", page.getProductDescriptionTable());
    result.put("Product Information Table", page.getProductInformationTable());
    result.put("From the Manufacturer", page.getManufacturerMessage());

    // TODO: add this:
    //fields.put("Products of Other Styles", getStyleProListOrNull(page, "#variation_style_name li"));
    //fields.put("Customers Also Shopped For", getBuyOrViewOrShopArrayOrNull(page, "#day0-sims-feature"));
    //fields.put("Best Sellers Ranking", getRankMapOrNull(fields));
    //fields.put("Important Information", getTextOrNull(page, "#importantInformation .content"));
    //fields.put("Other Items Customers Buy After Viewing This", getOthersItemsCusBuyArrayOrNull(page, "#view_to_purchase-sims-feature div.a-fixed-left-grid.p13n-asin"));

    return result;
  }

}
