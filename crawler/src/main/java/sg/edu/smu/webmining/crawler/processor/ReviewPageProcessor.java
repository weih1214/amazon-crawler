package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import sg.edu.smu.webmining.crawler.downloader.ProxyHttpClientDownloader;
import sg.edu.smu.webmining.crawler.pipeline.ReviewPipeline;
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

public class ReviewPageProcessor implements PageProcessor {
	
	private final Site site = Site.me()
		      .setCycleRetryTimes(Integer.MAX_VALUE)
		      .setSleepTime(1000)
		      .setRetryTimes(3)
		      .setCharset("UTF-8");
	
	public static String[] getReviewLinks(String folderPath) {
		List<Document> docList = ManagerTesting.load(ManagerTesting.listFilesForFolder(folderPath));
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

	public String getAuthorID(String authorURL) {

		String regexr = "profile/(.*)?/";
		Pattern p1 = Pattern.compile(regexr);
		Matcher m1 = p1.matcher(authorURL);
		if (m1.find()) {
			return m1.group(1);
		} else {
			return null;
		}
	}

	public void getVotingNumber(Page page, Document doc) {
		Integer totalVoters = 0;
		Integer positiveVoters = 0;
		String s1 = doc.select("table div[style = margin-bottom:0.5em;]").text().trim();
		String s2 = "(.*) of (.*) people";
		Pattern p1 = Pattern.compile(s2);
		Matcher m1 = p1.matcher(s1);
		if (m1.find()) {
			positiveVoters = Integer.parseInt(m1.group(1).replace(",", ""));
			totalVoters = Integer.parseInt(m1.group(2).replace(",", ""));
		}
		page.putField("Positive Voters", positiveVoters);
		page.putField("Total Voters", totalVoters);

	}

	public String getreviewID(String s1) {
		String s2 = "/customer-reviews/(.*)?/";
		Pattern p1 = Pattern.compile(s2);
		Matcher m1 = p1.matcher(s1);
		if (m1.find()) {
			return m1.group(1);
		} else {
			return null;
		}
	}

	public String getProductIDForPro(String s1) {
		String s2 = "/product-reviews/(.*)?/";
		Pattern p1 = Pattern.compile(s2);
		Matcher m1 = p1.matcher(s1);
		if (m1.find()) {
			return m1.group(1);
		} else {
			return null;
		}
	}

	public String getProductIDForCus(String s1) {
		String s2 = "ASIN=(.{10})";
		Pattern p1 = Pattern.compile(s2);
		Matcher m1 = p1.matcher(s1);
		if (m1.find()) {
			return m1.group(1);
		} else {
			return null;
		}
	}

	public Date getReviewDate(String s1) {
		SimpleDateFormat parser = new SimpleDateFormat("MMMMMMMM d yyyy");
		parser.setTimeZone(TimeZone.getTimeZone("UTC"));

		Date date = null;
		try {
			date = parser.parse(s1);
			return date;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}

	}

	public void parseSimpleReview(Page page, Element e) {
		String reviewTitle, reviewID, reviewAuthor, authorID, productID, reviewDate, productInfor, verifiedPurchased, ratingString, reviewContent;
		reviewTitle = reviewID = reviewAuthor = authorID = productID = reviewDate = productInfor = verifiedPurchased = ratingString = reviewContent = null;
		Double rating = null;
		reviewTitle = e.select("a[data-hook=review-title]").text();
		reviewID = e.attr("id");
		reviewAuthor = e.getElementsByAttributeValue("data-hook", "review-author").text();
		authorID = getAuthorID(e.select("a[data-hook = review-author]").attr("href"));
		productID = getProductIDForPro(page.getUrl().toString());
		reviewDate = e.select("span[data-hook= review-date]").text().substring(3).replace(",","");
		productInfor = e.select("a[data-hook= format-strip]").text();
		verifiedPurchased = e.select("span[data-hook = avp-badge]").text();
		ratingString = e.select("i[data-hook = review-star-rating]").text().trim();
		reviewContent = e.select("span[data-hook = review-body]").text();
		if (!ratingString.isEmpty()) {
			rating = Double.parseDouble(ratingString.substring(0,3));
		}
		Map<String, Object> reviewDoc = new LinkedHashMap<>();
		reviewDoc.put("Review Title", reviewTitle);
		reviewDoc.put("Review ID", reviewID);
		reviewDoc.put("Product ID", productID);
		reviewDoc.put("Author", reviewAuthor);
		reviewDoc.put("Author ID", authorID);
		reviewDoc.put("Positive Voters", 0);
		reviewDoc.put("Total Voters", 0);
		reviewDoc.put("Date", getReviewDate(reviewDate));
		reviewDoc.put("Product Information", productInfor);
		if (verifiedPurchased.isEmpty()) {
			reviewDoc.put("Purchase Verification", false);
		} else {
			reviewDoc.put("Purchase Verification", true);
		}
		reviewDoc.put("Review Rating", rating);
		reviewDoc.put("Review Content", reviewContent);
		page.putField(reviewTitle, reviewDoc);
	}

	public void parseCustomerReviews(Page page) {
		Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com/");
		String reviewTitle, reviewID, reviewAuthor, authorID, productID, reviewDate, productInfor, verifiedPurchased, ratingString, reviewContent;
		reviewTitle = reviewID = reviewAuthor = authorID = productID = reviewDate = productInfor = verifiedPurchased = ratingString = reviewContent = null;
		Double rating = 0.0;
		reviewTitle = doc.select("table div[style = margin-bottom:0.5em;] b").first().text();
		reviewID = getreviewID(page.getUrl().toString());
		reviewAuthor = doc.select("table div[style = margin-bottom:0.5em;] span[style = font-weight: bold;]").text();
		authorID = getAuthorID(doc.select("table div[style = margin-bottom:0.5em;] div[style = float:left;] a").attr("href"));
		productID = getProductIDForCus(page.getUrl().toString());
		reviewDate = doc.select("table div[style = margin-bottom:0.5em;] nobr").text().replace(",","");
		productInfor = doc.select("table div.tiny b").text();
		verifiedPurchased = doc.select("table div.tiny span.crVerifiedStripe b").text();
		ratingString = doc.select("table div[style = margin-bottom:0.5em;] img").attr("alt").trim();
		reviewContent = doc.select("table div.reviewText").text();
		if (!ratingString.isEmpty()) {
			rating = Double.parseDouble(ratingString.substring(0,3));
		}

		page.putField("Review Title", reviewTitle);
		page.putField("Review ID", reviewID);
		page.putField("Product ID", productID);
		page.putField("Author", reviewAuthor);
		page.putField("Author ID", authorID);
		getVotingNumber(page, doc);
		page.putField("Date", getReviewDate(reviewDate));
		page.putField("Product Information", productInfor);
		if (verifiedPurchased.isEmpty()) {
			page.putField("Purchase Verification", false);
		} else {
			page.putField("Purchase Verification", true);
		}
		page.putField("Review Rating", rating);
		page.putField("Review Content", reviewContent);
		System.out.println(page.getResultItems().getAll().toString());
	}

	public void parseProductReviews(Page page) {
		Document doc = Jsoup.parse(page.getRawText(), "https://www.amazon.com/");
		Elements result = doc.select("#cm_cr-review_list div.a-section.review");
		for (Element temp: result) {
			String commentNo = temp.select("span.review-comment-total.aok-hidden").text().trim();
			String helpfulVotes = temp.select("span.review-votes").text().trim();
			if ((Integer.parseInt(commentNo)!=0) || helpfulVotes.contains("found this helpful")) {
				String reviewLink = temp.select("a.a-size-base.a-link-normal.review-title.a-color-base.a-text-bold").attr("href");
				if (!reviewLink.isEmpty()) {
					System.out.println(reviewLink);
					page.addTargetRequest(reviewLink);
				}
			} else {
					parseSimpleReview(page, temp);
			}
		}
	}

	public void process(Page page) {

		String url = page.getUrl().toString();
		if (url.contains("product-reviews")) {
			parseProductReviews(page);
			String nextLink = page.getHtml().css("#cm_cr-pagination_bar .a-pagination li.a-last a").links().toString();
			if (!nextLink.isEmpty()) {
				page.addTargetRequest(nextLink);
			}
		} else if(url.contains("customer-reviews")) {
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
				 	.addPipeline(new ReviewPipeline())
		            .addUrl(ReviewPageProcessor.getReviewLinks("E:\\melaka"))
		            .thread(5);
		 spider.run();
		 provider.stopAutoRefresh();
	}


}
