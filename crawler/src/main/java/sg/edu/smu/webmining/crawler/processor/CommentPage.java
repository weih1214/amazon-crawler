package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.nodes.Element;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hwei on 2/12/2016.
 */
public class CommentPage extends Comment{
    private static final Pattern HELPFUL_PATTERN = Pattern.compile("abuse (.*) of (.*) people");

    private final String reviewId;
    private final Element commentElement;

    public CommentPage(String reviewId, Element commentPage) {
        this.reviewId = reviewId;
        this.commentElement = commentPage;
    }


    @Override
    public String getCommentId() {
        return parseCommentIdFromIdAttr(commentElement.select("div.postContent").attr("id"));
    }

    @Override
    public String getReviewId() {
        return reviewId;
    }

    @Override
    public String getAuthor() {
        return commentElement.select("div.postFrom a").text();
    }

    @Override
    public String getAuthorId() {
        return parseAuthorIdFromUserURL(commentElement.select("div.postFrom a").attr("href"));
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

    @Override
    public Integer getUpvotes() {
        return getVotes(1);
    }

    @Override
    public Integer getTotalVotes() {
        return getVotes(2);
    }

    @Override
    public String getCommentText() {
        return commentElement.select("div.postContent").text();
    }

    @Override
    public String getReferencePost() {
        return parseRefPostFromRefAttr(commentElement.select("div.postHeader a").attr("name"));
    }

    @Override
    public Element getDateElement() {
        return commentElement.select("div.postHeader").first();
    }
}
