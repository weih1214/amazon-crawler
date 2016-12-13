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
 * Created by hwei on 12/12/2016.
 */
public class Answer {

    private static final Pattern ANSWER_ID_PATTERN = Pattern.compile("_(.*)");
    private static final Pattern ANSWERER_ID_PATTERN = Pattern.compile("/profile/([a-zA-Z0-9]+)/");
    private static final Pattern DATE_PATTERN = Pattern.compile(" on (.*)");
    private static final Pattern HELPFUL_PATTERN = Pattern.compile("(.*) of (.*) found");

    private static final DateFormat DATE_FORMAT;

    static{
        DATE_FORMAT = new SimpleDateFormat("MMMM d yyyy");
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("PDT"));
    }

    private final String questionId;
    private final String productId;
    private final Element answerElement;

    public Answer(String questionId, String productId, Element answerElement) {
        this.questionId = questionId;
        this.productId = productId;
        this.answerElement = answerElement;
    }

    public String getAnswerId() {
        final String answerId = answerElement.select("div.cmQAListItemAnswer").attr("id");
        final Matcher m = ANSWER_ID_PATTERN.matcher(answerId);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    public String getAnswererName() {
        return answerElement.select("div.answerAuthor a").text();
    }

    public String getAnswererId() {
        final String answererId = answerElement.select("div.answerAuthor a").attr("href");
        final Matcher m = ANSWERER_ID_PATTERN.matcher(answererId);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    public String getQuestionId() {
        return questionId;
    }

    public String getProductId() {
        return productId;
    }

    public Date getAnswerDate() {
        final String dateString = answerElement.select("div.answerAuthor").text().replace(",", "");
        final Matcher m = DATE_PATTERN.matcher(dateString);
        Date date = null;
        if (m.find()) {
            final String cleanDateString = m.group(1).trim();
            try {
                date = DATE_FORMAT.parse(cleanDateString);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return date;
    }

    public String getAnswerText() {
        return answerElement.select("div.cdMessageInfo span").first().text();
    }

    private Integer getVotes(int index) {
        final String votingString = answerElement.select("span.votingInfo").text();
        final Matcher m = HELPFUL_PATTERN.matcher(votingString);
        if (m.find()) {
            return Integer.parseInt(m.group(index));
        }
        return 0;
    }

    public Integer getUpVotes() {
        return getVotes(1);
    }

    public Integer getTotalVotes() {
        return getVotes(2);
    }

    public Map<String, Object> asMap() {
        final Map<String, Object> answerDoc = new LinkedHashMap<>();
        answerDoc.put("Answer ID", getAnswerId());
        answerDoc.put("Answerer Name", getAnswererName());
        answerDoc.put("Answerer Id", getAnswererId());
        answerDoc.put("Question ID", getQuestionId());
        answerDoc.put("Product ID", getProductId());
        answerDoc.put("Answer Date", getAnswerDate());
        answerDoc.put("Answer Text", getAnswerText());
        answerDoc.put("Positive Voters", getUpVotes());
        answerDoc.put("Total Voters", getTotalVotes());
        return answerDoc;
    }
}
