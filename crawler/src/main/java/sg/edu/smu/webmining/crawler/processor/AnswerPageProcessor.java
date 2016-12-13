package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import sg.edu.smu.webmining.crawler.downloader.ProxyBHttpClientDownloader;
import sg.edu.smu.webmining.crawler.pipeline.AnswerPipeline;
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
 * Created by hwei on 13/12/2016.
 */
public class AnswerPageProcessor implements PageProcessor {

    private static final Pattern QUESTION_ID_PATTERN_1 = Pattern.compile("/-/([a-zA-Z0-9]+)/");
    private static final Pattern QUESTION_ID_PATTERN_2 = Pattern.compile("/forum/([a-zA-Z0-9]+)/");
    private static final Pattern PRODUCT_ID_PATTERN = Pattern.compile("asin=([a-zA-Z0-9]{10})");

    private final Site site = Site.me()
            .setCycleRetryTimes(Integer.MAX_VALUE)
            .setSleepTime(1000)
            .setRetryTimes(3)
            .setCharset("UTF-8");

    private String getQuestionId(String pageUrl) {
        final Matcher m1 = QUESTION_ID_PATTERN_1.matcher(pageUrl);
        if (m1.find()) {
            return m1.group(1);
        }
        else {
            final Matcher m2 = QUESTION_ID_PATTERN_2.matcher(pageUrl);
            if (m2.find()) {
                return m2.group(1);
            }
        }
        return null;
    }

    private String getProductId(String pageUrl) {
        final Matcher m = PRODUCT_ID_PATTERN.matcher(pageUrl);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    private String getNextLink(Element e) {
        return e.children().last().attr("href");
    }

    @Override
    public void process(Page page) {
        // Answer Page Processing
        final Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com");
        final String pageUrl = page.getUrl().toString();
        final String questionId = getQuestionId(pageUrl);
        final String productId = getProductId(pageUrl);
        final Elements answerElements = doc.select("div.cmQAList div.cmQAListItemAnswer");
        for (Element e: answerElements) {
            final Answer answerContent = new Answer(questionId, productId, e);
            page.putField(answerContent.getAnswerId(), answerContent.asMap());
        }
        // Pagination
        final String nextLink = doc.select("div.cdPageSelectorPagination").first().children().last().attr("href");
        if (!nextLink.isEmpty()) {
            page.addTargetRequest(nextLink);
        }

    }

    @Override
    public Site getSite() {
        return site;
    }

    public static void main(String[] args) {
        final String testUrl = "https://www.amazon.com/forum/-/TxHMVM0JY262YZ/ref=ask_ql_ql_al_hza?asin=B003EM8008";

        DynamicProxyProviderTimerWrap provider = new DynamicProxyProviderTimerWrap(
                new DynamicProxyProvider()
                        .addSource(new FPLNetSource())
                        .addSource(new SSLProxiesOrgSource())
        );
        provider.startAutoRefresh();

        Spider spider = Spider.create(new AnswerPageProcessor())
                .setDownloader(new ProxyBHttpClientDownloader(provider))
                .addPipeline(new AnswerPipeline())
                .addUrl(testUrl)
                .thread(5);
        spider.run();

        provider.stopAutoRefresh();
    }

}
