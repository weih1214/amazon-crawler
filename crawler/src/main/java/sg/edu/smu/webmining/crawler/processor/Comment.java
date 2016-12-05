package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.nodes.Element;

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
 * Created by hwei on 2/12/2016.
 */
public class Comment {

  private static final Pattern HELPFUL_PATTERN = Pattern.compile("abuse (.*) of (.*) people");
  private static final Pattern REFERENCE_POST_PATTERN = Pattern.compile("ID=(.*)");
  private static final Pattern AUTHOR_ID_PATTERN = Pattern.compile("/profile/([a-zA-Z0-9]+)/");
  private static final Pattern COMMENT_ID_PATTERN = Pattern.compile("_(.*)");
  private static final Pattern DATE_PATTERN = Pattern.compile(" (.{23,26}PDT)");

  private static final DateFormat DATE_FORMAT;

  static {
    DATE_FORMAT = new SimpleDateFormat("MMM d yyyy h:m:s a z");
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("PST"));
  }

  private static Date parseDateQuietly(String dateString) {
    try {
      return DATE_FORMAT.parse(dateString);
    } catch (ParseException e) {
      return null;
    }
  }

  private final Element commentElement;
  private final String reviewId;

  public Comment(String reviewId, Element commentElement) {
    this.reviewId = reviewId;
    this.commentElement = commentElement;
  }


  public String getCommentId() {
    final String id = commentElement.select("div.postContent").attr("id");
    final Matcher m = COMMENT_ID_PATTERN.matcher(id);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  public String getReviewId() {
    return reviewId;
  }

  public String getAuthor() {
    return commentElement.select("div.postFrom a").text();
  }

  public String getAuthorId() {
    final String href = commentElement.select("div.postFrom a").attr("href");
    final Matcher m = AUTHOR_ID_PATTERN.matcher(href);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  // when index == 1, returns upvotes, when index == 2, returns all votes
  private Integer getVotes(int index) {
    final String s = commentElement.select("div.postFooter div.postFooterRight").text();
    final Matcher m = HELPFUL_PATTERN.matcher(s);
    if (m.find()) {
      return Integer.parseInt(m.group(index).replace(",", ""));
    }
    return null;
  }

  public Integer getUpvotes() {
    return getVotes(1);
  }

  public Integer getTotalVotes() {
    return getVotes(2);
  }

  public String getCommentText() {
    return commentElement.select("div.postContent").text();
  }

  public String getReferencePost() {
    final String name = commentElement.select("div.postHeader a").attr("name");
    final Matcher m = REFERENCE_POST_PATTERN.matcher(name);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  private Element getDateElement() {
    return commentElement.select("div.postHeader").first();
  }

  public Date getFirstPostDate() {
    final Element element = getDateElement();
    final String html = element.html();
    if (html.contains("<br>")) {
      final String[] lines = html.split("<br>");
      final Matcher m = DATE_PATTERN.matcher(lines[0].trim());
      if (m.find()) {
        final String dateString = m.group(1).replace(",", "");
        return parseDateQuietly(dateString);
      }
    } else {
      final String text = element.text();
      final Matcher m = DATE_PATTERN.matcher(text);
      if (m.find()) {
        final String dateString = m.group(1).replace(",", "");
        return parseDateQuietly(dateString);
      }
    }
    return null;
  }

  public Date getLastEditDate() {
    final Element element = getDateElement();
    final String s1 = element.html();
    if (s1.contains("<br>")) {
      String[] s2 = s1.split("<br>");
      final Matcher m = DATE_PATTERN.matcher(s2[1].trim());
      if (m.find()) {
        final String lastDateString = m.group(1).replace(",", "");
        return parseDateQuietly(lastDateString);
      }
    }
    return null;
  }

  public Map<String, Object> asMap() {
    final Map<String, Object> commentDoc = new LinkedHashMap<>();
    commentDoc.put("Comment ID", getCommentId());
    commentDoc.put("Review ID", getReviewId());
    commentDoc.put("Author", getAuthor());
    commentDoc.put("Author ID", getAuthorId());
    commentDoc.put("First Post Date", getFirstPostDate());
    commentDoc.put("Last Edit Date", getLastEditDate());
    commentDoc.put("Positive Voters", getUpvotes());
    commentDoc.put("Total Voters", getTotalVotes());
    commentDoc.put("Comment Text", getCommentText());
    commentDoc.put("Reference Post", getReferencePost());
    return commentDoc;
  }

}
