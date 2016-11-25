package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import sg.edu.smu.webmining.crawler.downloader.ProxyHttpClientDownloader;
import sg.edu.smu.webmining.crawler.offlinework.MongoDBTest;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProvider;
import sg.edu.smu.webmining.crawler.proxy.DynamicProxyProviderTimerWrap;
import sg.edu.smu.webmining.crawler.proxy.source.FPLNetSource;
import sg.edu.smu.webmining.crawler.proxy.source.SSLProxiesOrgSource;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;

import java.util.ArrayList;
import java.util.List;

public class ReviewPageProcessor implements PageProcessor {
	
	private final Site site = Site.me()
		      .setCycleRetryTimes(Integer.MAX_VALUE)
		      .setSleepTime(1000)
		      .setRetryTimes(3)
		      .setCharset("UTF-8");
	
	public static String[] getReviewLinks(String folderPath) {
		List<Document> docList = MongoDBTest.load(MongoDBTest.listFilesForFolder(folderPath));
		List<String> reviewLinks = new ArrayList<String> ();
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

	public void parseSimpleReview(Page page, Element e) {
		String reviewTitle, reviewID, reviewAuthor, reviewDate, productInfor, verifiedPurchased, ratingString, reviewContent;
		reviewTitle = reviewID = reviewAuthor = reviewDate = productInfor = verifiedPurchased = ratingString = reviewContent = null;
		Double rating = null;
		reviewTitle = e.select("a.a-size-base.a-link-normal.review-title.a-color-base.a-text-bold").text();
		reviewID = e.attr("id");
		reviewAuthor = e.getElementsByAttributeValue("data-hook", "review-author").text();
		reviewDate = e.select("span[data-hook='review-date']").text();
		productInfor = e.select("a[data-hook='format-strip']").text();
		verifiedPurchased = e.select("span.a-declarative").text();
		ratingString = e.select(".a-icon.a-icon-star.a-star-5.review-rating").text().trim();
		reviewContent = e.select("div.a-row.review-data").text();
		if (!ratingString.isEmpty()) {
			rating = Double.parseDouble(ratingString.substring(0,3));
		}
		page.putField("Review Title", reviewTitle);
		page.putField("Review ID", reviewID);
		page.putField("Author", reviewAuthor);
		page.putField("Date", reviewDate);
		page.putField("Product Information", productInfor);
		page.putField("Purchase Information", verifiedPurchased);
		page.putField("Review Rating", rating);
		page.putField("Review Content", reviewContent);
		//System.out.println(page.getResultItems());
	}
	
	public void parseProductReviews(Page page) {
		Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com/");
		Elements result = doc.select("#cm_cr-review_list div.a-section.review");
		for (Element temp: result) {
			System.out.println(temp.attr("id"));
			String commentNo = temp.select("span.review-comment-total.aok-hidden").text().trim();
			String helpfulVotes = temp.select("span.review-votes").text().trim();
			//System.out.println(Integer.parseInt(commentNo));
			//System.out.println(helpfulVotes.contains("found this helpful"));
			if ((Integer.parseInt(commentNo)!=0) || helpfulVotes.contains("found this helpful")) {
				String reviewLink = temp.select("a.a-size-base.a-link-normal.review-title.a-color-base.a-text-bold").attr("href");
				if (!reviewLink.isEmpty()) {
				//	System.out.println(reviewLink);
				//	page.addTargetRequest(reviewLink);
				}
			} else {
					parseSimpleReview(page, temp);
			}
		}
	}

	public void parseCustomerReviews(Page page) {

	}


	public void process(Page page) {


		if (page.getUrl().toString().contains("product-reviews")) {
			parseProductReviews(page);
			String nextLink = page.getHtml().css("#cm_cr-pagination_bar .a-pagination li.a-last a").links().toString();
			if (!nextLink.isEmpty()) {
				System.out.println(nextLink);
//				page.addTargetRequest(nextLink);
			}
		} else if(page.getUrl().toString().contains("customer-reviews")) {
			parseCustomerReviews(page);

		}



	}

	@Override
	public Site getSite() {
		// TODO Auto-generated method stub
		return site;

	}
	
	public static void main(String[] args) {
		 DynamicProxyProviderTimerWrap provider = new DynamicProxyProviderTimerWrap(
			        new DynamicProxyProvider()
			            .addSource(new FPLNetSource())
			            .addSource(new SSLProxiesOrgSource())
			    );
		 provider.startAutoRefresh();
		 Spider spider = Spider.create(new ReviewPageProcessor())
		            .setDownloader(new ProxyHttpClientDownloader(provider))
		            .addUrl("https://www.amazon.com/Panasonic-Headphones-RP-HJE120-K-Ergonomic-Comfort-Fit/product-reviews/B003EM8008/ref=cm_cr_arp_d_paging_btm_5?ie=UTF8&reviewerType=avp_only_reviews&showViewpoints=1&sortBy=recent&pageNumber=5" +
							"")
		            .thread(5);
		 // ReviewPageProcessor.getReviewLinks("E:\\melaka")
		 spider.run();
		 provider.stopAutoRefresh();


	}


}
