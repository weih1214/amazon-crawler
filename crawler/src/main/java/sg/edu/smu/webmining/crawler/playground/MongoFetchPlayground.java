package sg.edu.smu.webmining.crawler.playground;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Iterator;

/**
 * Created by hwei on 9/1/2017.
 */
public class MongoFetchPlayground {
  public static void main(String[] args) {
    MongoClient client = new MongoClient("localhost", 27017);
    MongoDatabase db = client.getDatabase("OfferPage");
    MongoCollection <Document> collection = db.getCollection("content");
    Iterator i = collection.find().iterator();
    while (i.hasNext()) {
      Document doc = (Document) i.next();
      System.out.println(doc.get("Seller Name"));
    }
  }
}
