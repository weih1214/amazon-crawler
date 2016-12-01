package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import sg.edu.smu.webmining.crawler.downloader.ProxyHttpClientDownloader;
import sg.edu.smu.webmining.crawler.pipeline.CommentPipeline;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProvider;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProviderTimerWrap;
import sg.edu.smu.webmining.crawler.proxy.source.FPLNetSource;
import sg.edu.smu.webmining.crawler.proxy.source.SSLProxiesOrgSource;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hwei on 30/11/2016.
 */
public class CommentPageProcessor implements PageProcessor {

    private final Site site = Site.me()
            .setCycleRetryTimes(Integer.MAX_VALUE)
            .setSleepTime(1000)
            .setRetryTimes(3)
            .setCharset("UTF-8");

    public String[] getReviewLinks(String folderPath) {
        List<Document> docList = ManagerTesting.load(ManagerTesting.listFilesForFolder(folderPath));
        List<String> reviewLinks = new ArrayList<String>();
        for (Document doc : docList) {

            String s1 = doc.select("#revSum .a-link-emphasis.a-nowrap").attr("href");
            if (!s1.isEmpty()) {
                String s2 = s1.replaceAll("&reviewerType=.*?&", "&reviewerType=all_reviews&").replaceAll("&sortBy=.*", "&sortBy=recent");
                reviewLinks.add(s2);
            }
        }
        System.out.println(reviewLinks.size());
        System.out.println(reviewLinks);
        return reviewLinks.toArray(new String[0]);
    }


    public String getCommentId(String s1) {
        String s2 = "_(.*)";
        Pattern p1 = Pattern.compile(s2);
        Matcher m1 = p1.matcher(s1);
        if (m1.find()) {
            return m1.group(1);
        } else {
            return null;
        }
    }

    public String getauthorId(String s1) {
        String s2 = "/profile/([a-zA-Z0-9]+)/";
        Pattern p1 = Pattern.compile(s2);
        Matcher m1 = p1.matcher(s1);
        if (m1.find()) {
            return m1.group(1);
        } else {
            return null;
        }
    }

    public String getReplyEarlierPost(String s1) {
        String s2 = "ID=(.*)";
        Pattern p1 = Pattern.compile(s2);
        Matcher m1 = p1.matcher(s1);
        if (m1.find()) {
            return m1.group(1);
        } else {
            return null;
        }
    }

    public String getreviewId(String s1) {
        String s2 = "/review/([a-zA-Z0-9]+)/";
        Pattern p1 = Pattern.compile(s2);
        Matcher m1 = p1.matcher(s1);
        if (m1.find()) {
            return m1.group(1);
        } else {
            return null;
        }
    }

    public void putDatesIntoCommentMap(Map<String, Object> commentMap, Element element) {

        Date firstPostDate, lastEditedDate;
        firstPostDate = lastEditedDate = null;
        String firstDateString, lastDateString;
        firstDateString = lastDateString = null;
        String s1 = element.html();
        if (s1.contains("<br>")) {
            String[] s2 = s1.split("<br>");
            firstDateString = s2[0].substring(10).replace(",","").trim();
            lastDateString = s2[1].substring(29).replace(",","").trim();
            SimpleDateFormat parser = new SimpleDateFormat("MMM d yyyy h:m:s a z");
            try {
                firstPostDate = parser.parse(firstDateString);
                commentMap.put("First Post Date", firstPostDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            try {
                lastEditedDate = parser.parse(lastDateString);
                commentMap.put("Last Edit Date", lastEditedDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        } else {
            s1 = element.text();
            String s2 = " (.{23,26}PDT)";
            Pattern p1 = Pattern.compile(s2);
            Matcher m1 = p1.matcher(s1);
            if (m1.find()) {
                firstDateString = m1.group(1).replace(",","");
            }
            SimpleDateFormat parser = new SimpleDateFormat("MMM d yyyy h:m:s a z");
            try {
                firstPostDate = parser.parse(firstDateString);
                commentMap.put("First Post Date", firstPostDate);
                commentMap.put("Last Edit Date", lastEditedDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public void putVotersIntoCommentMap(Map<String, Object> commentMap, String s1) {
        Integer positiveVoters, totalVoters;
        positiveVoters = totalVoters = 0;
        String s2 = "abuse (.*) of (.*) people";
        Pattern p1 = Pattern.compile(s2);
        Matcher m1 = p1.matcher(s1);
        if (m1.find()) {
            positiveVoters = Integer.parseInt(m1.group(1).replace(",", "").trim());
            totalVoters = Integer.parseInt(m1.group(2).replace(",", "").trim());
        }
        commentMap.put("Positive Voters", positiveVoters);
        commentMap.put("Total Voters", totalVoters);
    }

    public void parseCommentPage(Page page, Document doc) {
        String commentId, reviewId, author, authorId, commentText, referencePost;
        commentId = reviewId = author = authorId = commentText = referencePost = null;
        Elements result = doc.select("div.postBody");
        for(Element temp: result) {
            Map<String, Object> commentMap = new LinkedHashMap<>();
            commentId = getCommentId(temp.select("div.postContent").attr("id"));
            reviewId = getreviewId(page.getUrl().toString());
            author = temp.select("div.postFrom a").text();
            authorId = getauthorId(temp.select("div.postFrom a").attr("href"));
            commentText = temp.select("div.postContent").text();
            referencePost = getReplyEarlierPost(temp.select("div.postHeader a").attr("name"));

            commentMap.put("Comment ID", commentId);
            commentMap.put("Review ID", reviewId);
            commentMap.put("Author", author);
            commentMap.put("Author ID", authorId);
            putDatesIntoCommentMap(commentMap, temp.select("div.postHeader").first());
            putVotersIntoCommentMap(commentMap, temp.select("div.postFooter div.postFooterRight").text());
            commentMap.put("Comment Text", commentText);
            commentMap.put("Reference Post", referencePost);
            page.putField(commentId, commentMap);
        }
    }

    @Override
    public void process(Page page) {
        Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com");
        String URL = doc.select("div.cdPageSelectorPagination a").last().attr("href");
        if (!URL.isEmpty()) {
            page.addTargetRequest(URL);
        }
        parseCommentPage(page, doc);
    }

    @Override
    public Site getSite() {
        return site;
    }

    public static void main(String[] args) {
        DynamicProxyProviderTimerWrap provider = new DynamicProxyProviderTimerWrap(
                new DynamicProxyProvider()
                        .addSource(new FPLNetSource())
                        .addSource(new SSLProxiesOrgSource())
        );
        provider.startAutoRefresh();
        Spider spider = Spider.create(new CommentPageProcessor())
                .setDownloader(new ProxyHttpClientDownloader(provider))
                .addPipeline(new CommentPipeline())
                .addUrl("https://www.amazon.com/review/RTRDKUJDZCO4B/ref=cm_cd_pg_pg1?ie=UTF8&asin=B00L9EOQCO&cdForum=FxI41PZD7HV6XF&cdPage=1&cdSort=oldest&cdThread=Tx2G64ASDNBAH46&store=amazon-home#wasThisHelpful")
                .thread(5);
        spider.run();
        provider.stopAutoRefresh();
    }

}
