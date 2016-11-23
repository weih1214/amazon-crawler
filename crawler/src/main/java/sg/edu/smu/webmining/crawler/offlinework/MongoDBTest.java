package sg.edu.smu.webmining.crawler.offlinework;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;

import org.apache.commons.lang3.StringEscapeUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.net.URLDecoder;

public class MongoDBTest {

	public static List<File> listFilesForFolder(final String folderPath) {
		File folder = new File(folderPath);
		List<File> files = new ArrayList<File>();
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				listFilesForFolder(fileEntry.getAbsolutePath());
			} else {
				files.add(fileEntry);
			}
		}
		return files;
	}

	public static List<org.jsoup.nodes.Document> load(List<File> files) {
		List<org.jsoup.nodes.Document> docList = new ArrayList<org.jsoup.nodes.Document>();
		for (File file : files) {
			try {
				org.jsoup.nodes.Document doc = Jsoup.parse(file, "UTF-8", "https://www.amazon.com/");
				docList.add(doc);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return docList;
	}

	public static void imgStore(List<org.jsoup.nodes.Document> docList) {
		for (org.jsoup.nodes.Document doc : docList) {
			String fileSuffix = doc.head().getElementsByAttributeValue("rel", "canonical").attr("href");
			File dir = new File("E:\\Img\\" + fileSuffix.substring(fileSuffix.length() - 10));
			dir.mkdirs();
			Elements imgList = doc.select("#altImages img");
			BufferedImage image = null;
			String rule = "/images/./(.*?)$";
			for (Element img : imgList) {
				try {
					URL url = new URL(img.attr("src").replace("._SS40_", ""));
					image = ImageIO.read(url);
					Pattern p1 = Pattern.compile(rule);
					Matcher r1 = p1.matcher(img.attr("src").replace("._SS40_.jpg", ""));
					if (r1.find()) {
						System.out.println(r1.group(1));
						ImageIO.write(image, "jpg", new File(dir.getAbsolutePath() + "\\" + r1.group(1) + ".jpg"));
					}
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	public static String getTextOrNull(Element e, String cssQuery) {
		Elements result = e.select(cssQuery);
		if (result.size() > 0) {
			// System.out.println(result.get(0));
			if (result.get(0).text().isEmpty()) {
				return null;
			} else {
				return result.get(0).text();
			}
		}
		return null;
	}

	public static String getValueOrNull(Element e, String cssQuery) {
		Elements result = e.select(cssQuery);
		if (result.size() > 0) {
			if (result.get(0).text().isEmpty()) {
				return null;
			} else {
				return result.get(0).text();
			}
		}
		return null;
	}

	public static Double getRatingOrNull(Element e, String cssQuery) {
		Elements result = e.select(cssQuery);
		if (result.size() > 0) {
			if (result.get(0).text().isEmpty()) {
				return null;
			} else {
				return Double.parseDouble(result.get(0).text().substring(0, 3));
			}
		}
		return null;
	}

	public static Map<String, Object> getProDesTableOrNull(Element e, String cssQuery) {
		Map<String, Object> tableContent = new LinkedHashMap<>();
		Elements table = e.select(cssQuery);
		if (table.size() == 1) {
			for (Element row : table.select("tr")) {
				Elements ths = row.select("th");
				if (!ths.isEmpty()) {
					if (!ths.get(1).text().isEmpty()) {
						tableContent.put(ths.get(0).text().replace(".", ""), ths.get(1).text());
					} else {
						continue;
					}
				}
				Elements tds = row.select("td");
				if (!tds.isEmpty()) {
					if (!tds.get(1).text().isEmpty()) {
						tableContent.put(tds.get(0).text().replace(".", ""), tds.get(1).text());
					} else {
						continue;
					}
				}
			}
		}
		return tableContent;
	}

	public static List<String> getColorOrNull(Element e, String cssQuery) {
		// Map<String, Object> tableContent = new LinkedHashMap<> ();
		Elements result = e.select(cssQuery);
		List<String> colorList = new ArrayList<>();
		if (result.size() > 0) {
			for (Element temp : result) {
				colorList.add(temp.attr("alt"));
			}
		}
		return colorList;
	}
	
	public static List<String> getStyleProListOrNull(Element e, String cssQuery) {
		
		Elements result = e.select(cssQuery);
		List<String> styleProList = new ArrayList<> ();
		if (result.size() > 0) {
			for (Element temp: result) {
				if (!temp.attr("data-defaultasin").isEmpty()) {
					styleProList.add(temp.attr("data-defaultasin"));
				}
			}
		}
		return styleProList;
	}

	public static String getProDesTextOrNull(Element e, String cssQuery) {
		Elements result = e.select(cssQuery);
		if (result.size() > 0) {
			// System.out.println(result.get(0));
			if (result.get(0).select("p").text().isEmpty()) {
				return null;
			} else {
				return result.get(0).select("p").text();
			}
		} else {
			Elements elements = e.select("#pd-available");
			if (elements.size() > 0) { 
			Element e1 = elements.first().nextElementSibling();
			String s1 = e1.data();
			String s2 = "productDescriptionWrapper%22%3E(.*?)%3Cdiv";
			Pattern p1 = Pattern.compile(s2);
			Matcher m1 = p1.matcher(s1);
			if (m1.find()) {
				String de = null;
				try {
					de = URLDecoder.decode(m1.group(1).replaceAll("%0A", "%20"), "UTF-8").trim();
				} catch (UnsupportedEncodingException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
				return de;
			} else {
				return null;
			}
			}
			return null;
		}
	}

	public static Map<String, Object> getProInforTableOrNull(Element e, String cssQuery) {
		Map<String, Object> tableContent = new LinkedHashMap<>();
		Elements table = e.select(cssQuery);
		// Under #prodDetails, there are two tables; But only the first one has
		// "tr" elements. So we need size() >= 1.
		if (table.size() >= 1) {
			for (Element row : table.select("tr")) {
				tableContent.put(row.child(0).text().replace(".", ""), row.child(1).text());
			}
		}
		return tableContent;
	}

	public static List<String> getSPRArrayOrNull(Element e, String cssQuery) {
		Elements result = e.select(cssQuery);
		List<String> l1 = new ArrayList<String>();
		if (result.size() > 0) {
			String s1 = result.first().attr("data-a-carousel-options");
			s1 = StringEscapeUtils.unescapeJava(s1);
			String s2 = "initialSeenAsins\":\"(.*)\"\\s,\\s\"cir";
			Pattern p1 = Pattern.compile(s2);
			Matcher m1 = p1.matcher(s1);
			if (m1.find()) {
				s1 = m1.group(1).replace("\"", "");
				l1 = Arrays.asList(s1.split(","));
			}
		}
		return l1;
	}

	public static List<String> getColorProListOrNull(Element e, String cssQuery) {
		Elements result = e.select(cssQuery);
		List<String> colorProList = new ArrayList<>();
		if (result.size() > 0) {
			for (Element temp : result) {
				colorProList.add(temp.attr("data-defaultasin"));
			}
		}
		return colorProList;
	}

	public static List<String> getNewModelListOrNull(Element e, String cssQuery) {
		Elements result = e.select(cssQuery);
		List<String> newModelList = new ArrayList<>();
		if (result.size() > 0) {
			for (Element temp : result) {
				String s1 = temp.attr("href");
				String s2 = "/dp/(.{10})/";
				Pattern p1 = Pattern.compile(s2);
				Matcher m1 = p1.matcher(s1);
				if (m1.find()) {
					newModelList.add(m1.group(1));
				}
			}
		}
		return newModelList;
	}

	public static List<String> getBuyOrViewOrShopArrayOrNull(Element e, String cssQuery) {

		Elements result = e.select(cssQuery);
		List<String> proList = new ArrayList<>();
		if (result.size() > 0) {
			String s1 = result.first().child(0).attr("data-a-carousel-options").replace("\"", "");
			String s2 = "id_list:\\[(.*)]";
			Pattern p1 = Pattern.compile(s2);
			Matcher m1 = p1.matcher(s1);
			if (m1.find()) {
				s1 = m1.group(1);
				String[] id_list = s1.split(",");
				proList = Arrays.asList(id_list);
			}
		}
		return proList;
	}
	
	public static List<String> getOthersItemsCusBuyArrayOrNull(Element e, String cssQuery) {
		
		Elements result = e.select(cssQuery);
		List<String> proList = new ArrayList<>();
		if(result.size() > 0) {
			for (Element temp: result) {
				String s1 = temp.attr("data-p13n-asin-metadata").replace("\"", "");
				String s2 = "asin:(.{10})";
				Pattern p1 = Pattern.compile(s2);
				Matcher m1 = p1.matcher(s1);
				if (m1.find()) {
					proList.add(m1.group(1));
				}
			}
		}
		return proList;
	}
	
	public static Map<String, Integer> getRankMapOrNull(Map<String, Object> fields) {
		
		Map<String, Integer> rankMap = new LinkedHashMap<>();
		String s1 = null;
		try {
			s1 = (String) fields.get("Best Sellers Rank");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (s1 != null) {
		String[] rankList = s1.split("#");
		for (int i = 1; i <= (rankList.length-1); i += 1) {
			String s2 = "(.*?) in (.*)";
			Pattern p1 = Pattern.compile(s2);
			Matcher m1 = p1.matcher(rankList[i].trim());
			if (m1.find()) {
				rankMap.put(m1.group(2), Integer.parseInt(m1.group(1).replace(",", "")));
			}
		}
		}
		return rankMap;
	}
	
	public static Map<String, Object> parse(org.jsoup.nodes.Document page) {
		
		Map<String, Object> fields = new LinkedHashMap<>();		
		fields.put("Product Title", getTextOrNull(page, "#title"));
		fields.put("Brand", getTextOrNull(page, "#brand"));
		fields.put("AverageRating", getRatingOrNull(page, "#reviewStarsLinkedCustomerReviews"));
		fields.put("Price", getValueOrNull(page, "#priceblock_ourprice"));
		fields.put("Colors", getColorOrNull(page, "#variation_color_name img"));
		fields.put("Products of Other Colors", getColorProListOrNull(page, "#variation_color_name li"));
		fields.put("Products of Other Styles", getStyleProListOrNull(page, "#variation_style_name li"));
		fields.put("Feature-bullets", getTextOrNull(page, "#feature-bullets"));
		fields.put("Newer Model", getNewModelListOrNull(page, "#newer-version a.a-size-base.a-link-normal"));
		fields.put("Sponsored Products Related", getSPRArrayOrNull(page, "#sp_detail"));
		fields.put("Customers Also Bought", getBuyOrViewOrShopArrayOrNull(page, "#purchase-sims-feature"));
		fields.put("Customers Also viewed", getBuyOrViewOrShopArrayOrNull(page, "#session-sims-feature"));
		fields.put("Customers Also Shopped For", getBuyOrViewOrShopArrayOrNull(page, "#day0-sims-feature"));
		fields.put("Warning", getTextOrNull(page, "#cpsia-product-safety-warning_feature_div"));
		fields.put("Technical Details", getTextOrNull(page, "#technical-data div.content"));
		fields.put("Product Description", getProDesTextOrNull(page, "#productDescription"));
		fields.putAll(getProDesTableOrNull(page, "#productDescription table"));
		fields.putAll(getProInforTableOrNull(page, "#prodDetails table"));
		fields.put("Best Sellers Ranking", getRankMapOrNull(fields));
		fields.put("Important Information", getTextOrNull(page, "#importantInformation .content"));
		fields.put("From the Manufacturer", getTextOrNull(page, "#aplus_feature_div"));
		fields.put("Sponsored Products Related-2", getSPRArrayOrNull(page, "#sp_detail2"));
		fields.put("Other Items Customers Buy After Viewing This", getOthersItemsCusBuyArrayOrNull(page, "#view_to_purchase-sims-feature div.a-fixed-left-grid.p13n-asin"));
		/*
		 * To be continued
		 */
		return fields;

	}

	public static void offlineProcess(String folderPath) {
		
		// To connect to MongoDB server
		@SuppressWarnings("resource")
		MongoClient mongoClient = new MongoClient("localhost", 27017);

		// Now connect to your databases and switch to the Collection
		@SuppressWarnings({})
		MongoDatabase db = mongoClient.getDatabase("HtmlPage");
		MongoCollection<org.bson.Document> collection = db.getCollection("content");
		
		List<File> fileList = listFilesForFolder(folderPath);
		List<org.jsoup.nodes.Document> docList = load(fileList);
		for(org.jsoup.nodes.Document doc:docList) {			
			org.bson.Document dbDoc1 = new org.bson.Document(parse(doc));
			collection.insertOne(dbDoc1);
		}
		
//		imgStore(docList);
		
	}

	public static void main(String args[]) {
		offlineProcess("E:\\Test");
//		File f1 = new File("E:\\Test\\B00HWFRJQO.html");
//		try {
//			org.jsoup.nodes.Document doc = Jsoup.parse(f1, "UTF-8", "https://www.amazon.com");
//			System.out.println(parse(doc).get("Important Information"));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
}
