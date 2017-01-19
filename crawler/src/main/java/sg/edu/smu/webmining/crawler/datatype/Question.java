package sg.edu.smu.webmining.crawler.datatype;

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
 * Created by hwei on 7/12/2016.
 */
public class Question {

  private static final Pattern ASKER_ID_PATTERN = Pattern.compile("/profile/([a-zA-Z0-9]+)/");
  private static final Pattern DATE_PATTERN = Pattern.compile(" on (.*)");
  private static final Pattern PRODUCT_ID_PATTERN = Pattern.compile("/dp/([a-zA-Z0-9]+)/");

  private static final DateFormat DATE_FORMAT;

  static {
    DATE_FORMAT = new SimpleDateFormat("MMMM d yyyy");
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("PDT"));
  }

  private final Element questionElement;
  private final String questionId;
  private final String answerLink;

  public Question(String questionId, Element questionElement, String answerLink) {
    this.questionElement = questionElement;
    this.questionId = questionId;
    this.answerLink = answerLink;
  }
//    private final String questionId

  private String getQuestionId() {
    return questionId;
  }

  private String getAskerName() {
    return questionElement.select("div.cdAuthorInfoBlock a").text();
  }

  private String getAskerId() {
    final String askerLink = questionElement.select("div.cdAuthorInfoBlock a").attr("href");
    final Matcher m = ASKER_ID_PATTERN.matcher(askerLink);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  private Date getQuestionPostDate() {
    final String dateString = questionElement.select("div.cdAuthorInfoBlock").text().trim().replace(",", "");
    final Matcher m = DATE_PATTERN.matcher(dateString);
    if (m.find()) {
      final String cleanDateString = m.group(1);
      try {
        return DATE_FORMAT.parse(cleanDateString);
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }
    return null;
  }

  private String getProductId() {
    final String productId = questionElement.select("div.cdDescription a").attr("href");
    final Matcher m = PRODUCT_ID_PATTERN.matcher(productId);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  private String getQuestionText() {
    return questionElement.select("div.cdQuestionText").text().trim();
  }

  private String getAnswerLink() { return answerLink;}

  public Map<String, Object> asMap() {
    final Map<String, Object> questionDoc = new LinkedHashMap<>();
    questionDoc.put("Question ID", getQuestionId());
    questionDoc.put("Asker Name", getAskerName());
    questionDoc.put("Asker ID", getAskerId());
    questionDoc.put("Question Post Date", getQuestionPostDate());
    questionDoc.put("Product ID", getProductId());
    questionDoc.put("Question Text", getQuestionText());
    questionDoc.put("Answer Link", getAnswerLink());
    return questionDoc;
  }
}
