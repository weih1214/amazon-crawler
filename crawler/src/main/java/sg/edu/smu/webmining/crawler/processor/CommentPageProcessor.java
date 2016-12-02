package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by hwei on 30/11/2016.
 */
public class CommentPageProcessor implements PageProcessor {

    private static final Pattern REVIEW_ID_PATTERN = Pattern.compile("/review/([a-zA-Z0-9]+)/");

    private final Site site = Site.me()
            .setCycleRetryTimes(Integer.MAX_VALUE)
            .setSleepTime(1000)
            .setRetryTimes(3)
            .setCharset("UTF-8");

//    public String[] getReviewLinks(String folderPath) {
//        List<Document> docList = ManagerTesting.load(ManagerTesting.listFilesForFolder(folderPath));
//        List<String> reviewLinks = new ArrayList<String>();
//        for (Document doc : docList) {
//
//            String s1 = doc.select("#revSum .a-link-emphasis.a-nowrap").attr("href");
//            if (!s1.isEmpty()) {
//                String s2 = s1.replaceAll("&reviewerType=.*?&", "&reviewerType=all_reviews&").replaceAll("&sortBy=.*", "&sortBy=recent");
//                reviewLinks.add(s2);
//            }
//        }
//        System.out.println(reviewLinks.size());
//        System.out.println(reviewLinks);
//        return reviewLinks.toArray(new String[0]);
//    }

    protected final String parseReviewIdFromURL(String url) {
        final Matcher m = REVIEW_ID_PATTERN.matcher(url);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }



    @Override
    public void process(Page page) {
        Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com");
        final String url = page.getUrl().toString();
        final String nextURL = doc.select("div.cdPageSelectorPagination a").last().attr("href");
        if (!nextURL.isEmpty()) {
            page.addTargetRequest(nextURL);
        }
        for (Element e: doc.select("div.postBody")) {
            final String reviewId = parseReviewIdFromURL(url);
            final Comment comment = new CommentPage(reviewId, e);
            page.putField(comment.getCommentId(), comment.asMap());
        }
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
