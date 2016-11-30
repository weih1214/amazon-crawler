package sg.edu.smu.webmining.crawler.processor;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by mtkachenko.2015 on 30/11/2016.
 */
public abstract class Review {

  private static final Pattern AUTHOR_ID_PATTERN = Pattern.compile("profile/(.*?)/");

  private static final DateFormat DATE_FORMAT;

  static {
    DATE_FORMAT = new SimpleDateFormat("MMMM d yyyy");
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("PST"));
  }

  public abstract String getTitle();

  public abstract String getId();

  public abstract String getAuthorName();

  public abstract String getAuthorId();

  public abstract String getDateString();

  public abstract String getProductInformation();

  public abstract boolean isVerifiedPurchase();

  public abstract Double getRating();

  public abstract String getContent();

  public abstract String getProductId();

  public abstract Integer getUpvotes();

  public abstract Integer getTotalVotes();

  protected final String parseAuthorIdFromUrl(String url) {
    final Matcher m = AUTHOR_ID_PATTERN.matcher(url);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  public Date getDate() {
    try {
      return DATE_FORMAT.parse(getDateString());
    } catch (ParseException e1) {
      return null;
    }
  }

  public Map<String, Object> asMap() {
    final Map<String, Object> reviewDoc = new LinkedHashMap<>();
    reviewDoc.put("Product ID", getProductId());
    reviewDoc.put("Review Title", getTitle());
    reviewDoc.put("Review ID", getId());
    reviewDoc.put("Author", getAuthorName());
    reviewDoc.put("Author ID", getAuthorId());
    reviewDoc.put("Positive Voters", getUpvotes());
    reviewDoc.put("Total Voters", getTotalVotes());
    reviewDoc.put("Date", getDate());
    reviewDoc.put("Product Information", getProductInformation());
    reviewDoc.put("Purchase Verification", isVerifiedPurchase());
    reviewDoc.put("Review Rating", getRating());
    reviewDoc.put("Review Content", getContent());
    return reviewDoc;
  }

}
