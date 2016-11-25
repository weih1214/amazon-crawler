package sg.edu.smu.webmining.crawler.offlinework;


import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by mtkachenko.2015 on 24/11/2016.
 */
public class ProductPageUpdater {

  public Map<String, Object> parse(InputStream is) throws IOException {
    final ProductPage page = new ProductPage(is, "UTF-8", "https://amazon.com/");

    final Map<String, Object> result = new LinkedHashMap<>();
    result.put("Product Title", page.getProductTitle());
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
    result.put("Products of Other Styles", page.getOtherStyleProducts());
    result.put("Customers Also Shopped For", page.getCustomersAlsoShoppedFor());
    result.put("Important Information", page.getImportantInformation());
    result.put("Other Items Customers Buy After Viewing This", page.getItemsCustomersBuyAfterViewingThis());
    result.put("Best Sellers Rank", page.getBestSellersRank());
    return result;
  }


}
