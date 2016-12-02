package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.nodes.Element;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hwei on 2/12/2016.
 */
public abstract class Comment {

    private static final Pattern REFERENCE_POST_PATTERN = Pattern.compile("ID=(.*)");

    private static final Pattern AUTHOR_ID_PATTERN = Pattern.compile("/profile/([a-zA-Z0-9]+)/");

    private static final Pattern COMMENT_ID_PATTERN = Pattern.compile("_(.*)");

//    private static final Pattern REVIEW_ID_PATTERN = Pattern.compile("/review/([a-zA-Z0-9]+)/");
    
    private static DateFormat DATE_FORMAT = new SimpleDateFormat("MMM d yyyy h:m:s a z");
    
    private static final Pattern DATE_PATTERN = Pattern.compile(" (.{23,26}PDT)");

    public abstract String getCommentId();

    public abstract String getReviewId();

    public abstract String getAuthor();

    public abstract String getAuthorId();

    public abstract Integer getUpvotes();

    public abstract Integer getTotalVotes();

    public abstract String getCommentText();

    public abstract String getReferencePost();

    public abstract Element getDateElement();

    protected final String parseCommentIdFromIdAttr(String idAttr) {
        final Matcher m = COMMENT_ID_PATTERN.matcher(idAttr);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

//    protected final String parseReviewIdFromURL(String url) {
//        final Matcher m = REVIEW_ID_PATTERN.matcher(url);
//        if (m.find()) {
//            return m.group(1);
//        }
//        return null;
//    }

    protected final String parseAuthorIdFromUserURL(String userURL) {
        final Matcher m = AUTHOR_ID_PATTERN.matcher(userURL);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    protected final String parseRefPostFromRefAttr(String refAttr) {
        final Matcher m = REFERENCE_POST_PATTERN.matcher(refAttr);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    public Date getFirstPostDate() {
        String firstDateString = null;
        final Element element = getDateElement();
        String s1 =  element.html();
        if (s1.contains("<br>")) {
            String[] s2 = s1.split("<br>");
            final Matcher m = DATE_PATTERN.matcher(s2[0].trim());
            if (m.find()) {
                firstDateString = m.group(1).replace(",", "");
            }
            try {
                return DATE_FORMAT.parse(firstDateString);
            } catch (ParseException e) {
                    return null;
            }

        } else {
            s1 = element.text();
            final Matcher m = DATE_PATTERN.matcher(s1);
            if (m.find()) {
                firstDateString = m.group(1).replace(",","");
            }
            try {
                return DATE_FORMAT.parse(firstDateString);
            } catch (ParseException e) {
                return null;
            }
        }
    }

    public Date getLastEditDate() {
        String lastDateString = null;
        final Element element = getDateElement();
        final String s1 =  element.html();
        if (s1.contains("<br>")) {
            String[] s2 = s1.split("<br>");
            final Matcher m = DATE_PATTERN.matcher(s2[1].trim());
            if (m.find()) {
                lastDateString = m.group(1).replace(",", "");
            }
            try {
                return DATE_FORMAT.parse(lastDateString);
            } catch (ParseException e) {
                return null;
            }
        } else {
            return null;
        }
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
