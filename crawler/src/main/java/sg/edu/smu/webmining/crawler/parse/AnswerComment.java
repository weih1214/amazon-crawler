package sg.edu.smu.webmining.crawler.parse;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

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
 * Created by hwei on 14/12/2016.
 */
public class AnswerComment {

  private static final Pattern ANSWERCOMMENT_ID_PATTERN = Pattern.compile("_(.*)");
  private static final Pattern AUTHOR_ID_PATTERN = Pattern.compile("/profile/([a-zA-Z0-9]+)/");
  private static final Pattern DATE_PATTERN = Pattern.compile("((January|February|March|April|May|June|July|August|September|October|November|December) \\d\\d? \\d{4})");

  private static final DateFormat DATE_FORMAT;

  static {
    DATE_FORMAT = new SimpleDateFormat("MMMM d yyyy");
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("PDT"));
  }

  private final Element answerCommentElement;
  private final String questionId;
  private final String answerId;
  private final String answerText;

  public AnswerComment(Element answerCommentElement, String questionId, String answerId, String answerText) {
    this.answerCommentElement = answerCommentElement;
    this.questionId = questionId;
    this.answerId = answerId;
    this.answerText = answerText;
  }

  public String getAnswerCommentId() {
    final Elements elements = answerCommentElement.select("div.cdCommentText span");
    if (!elements.isEmpty()) {
      final String AnswerCommentIdString = elements.first().attr("id");
      final Matcher m = ANSWERCOMMENT_ID_PATTERN.matcher(AnswerCommentIdString);
      if (m.find()) {
        return m.group(1);
      }
    }
    return null;
  }

  public String getAnswerId() {
    return answerId;
  }

  public String getQuestionId() { return questionId; }

  public String getAuthorName() {
    final Elements elements = answerCommentElement.select("div.cdByLine a");
    if (!elements.isEmpty()) {
      return answerCommentElement.select("div.cdByLine a").first().text();
    }
    return null;
  }

  public String getAuthorId() {
    final String authorIdString = answerCommentElement.select("div.cdByLine a").attr("href");
    final Matcher m = AUTHOR_ID_PATTERN.matcher(authorIdString);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  public Date getPostDate() {
    final Date date;
    final String dateString = answerCommentElement.select("div.cdByLine").text().replace(",", "").trim();
    final Matcher m = DATE_PATTERN.matcher(dateString);
    if (m.find()) {
      try {
        date = DATE_FORMAT.parse(m.group(1));
        return date;
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }
    return null;
  }

  public String getAnswerCommentText() {
    return answerCommentElement.select("div.cdCommentText").text();
  }

  public Map<String, Object> asMap() {
    final Map<String, Object> answerCommentDoc = new LinkedHashMap<>();
    answerCommentDoc.put("Answer Comment ID", getAnswerCommentId());
    answerCommentDoc.put("Answer ID", getAnswerId());
    answerCommentDoc.put("Question ID", getQuestionId());
    answerCommentDoc.put("Author Name", getAuthorName());
    answerCommentDoc.put("Author ID", getAuthorId());
    answerCommentDoc.put("Post Date", getPostDate());
    answerCommentDoc.put("Answer Comment Text", getAnswerCommentText());
    return answerCommentDoc;
  }

}
