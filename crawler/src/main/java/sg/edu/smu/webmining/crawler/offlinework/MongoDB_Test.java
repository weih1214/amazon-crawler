package sg.edu.smu.webmining.crawler.offlinework;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;

import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

public class MongoDB_Test {

	public static ArrayList<File> listFilesForFolder(final String FolderPath) {
		File folder = new File(FolderPath);
		ArrayList<File> files = new ArrayList<File>();
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				listFilesForFolder(fileEntry.getAbsolutePath());
			} else {
				System.out.println(fileEntry.getName());
				files.add(fileEntry);
			}
		}
		return files;
	}

	public static ArrayList<Document> Load(ArrayList<File> files) {
		ArrayList<Document> DocList = new ArrayList<Document>();
		for (File file : files) {
			try {
				Document doc = Jsoup.parse(file, "UTF-8", "https://www.amazon.com/");
				DocList.add(doc);
				System.out.println(doc.head().getElementsByAttributeValue("rel", "canonical").attr("href"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return DocList;
	}

	public static void ImgStore(ArrayList<Document> DocList) {
		for (Document doc : DocList) {
			String FileSuffix = doc.head().getElementsByAttributeValue("rel", "canonical").attr("href");
			File Dir = new File("E:\\Img\\" + FileSuffix.substring(FileSuffix.length() - 10));
			Dir.mkdirs();
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
						ImageIO.write(image, "jpg", new File(Dir.getAbsolutePath() + "\\" + r1.group(1) + ".jpg"));
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

	public Map<String, String> Parse(Document page) {
		Map<String, String> fields = new HashMap<>();
		fields.put("ProductTitle", page.getElementById("productTitle").text());
		fields.put("Price", page.getElementById("priceblock_ourprice").text());
		fields.put("Description", page.getElementById("feature-bullets").text());
		/*
		 * To be continued
		 */
		return fields;

	}

	public static void main(String args[]) {
		try {

			// To connect to MongoDB server
			@SuppressWarnings("resource")
			MongoClient mongoClient = new MongoClient("localhost:27017");

			// Now connect to your databases and switch to the Collection
			@SuppressWarnings({})
			DB db = mongoClient.getDB("HtmlPage");
			DBCollection collection = db.getCollection("content");

			BasicDBObject temp = new BasicDBObject();

			// Initialize the BasicDBObject for MongoDB operations
			// BasicDBObject temp = new BasicDBObject("title","greatness");
			// collection.save(temp);
			//
			// for (DBObject doc : collection.find()) {
			// System.out.println(doc);
			// }

			// Save images into the filesystem
			ImgStore(Load(listFilesForFolder("E:\\Test")));

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			e.printStackTrace();
		}

	}
}
