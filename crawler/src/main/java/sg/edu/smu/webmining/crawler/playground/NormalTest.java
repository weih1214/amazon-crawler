package sg.edu.smu.webmining.crawler.playground;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by hwei on 6/1/2017.
 */
public class NormalTest {
  public static void main(String[] args) throws IOException {
//    String s1 = "sjdsdashuidaicxojosd\nsjidjaodhasodoqudw\nsdjaoojwd\t\t\tsjdioadjasod\n";
//    InputStream in = new ByteArrayInputStream(s1.getBytes("UTF-8"));
//    new File("D:\\Record\\9c").mkdirs();
//    Path destination = Paths.get("D:\\Record\\9c\\5");
//    Files.copy(in, destination);
//    GeneralMongoDBManager client = new GeneralMongoDBManager("localhost", 27017, "OfferPage", "content");
    MongoClient client = new MongoClient("localhost", 27017);
    MongoDatabase db = client.getDatabase("OfferPage");
    MongoCollection<Document> collection = db.getCollection("content");
    Iterator i = collection.find().iterator();
    while (i.hasNext()) {
      Document doc = (Document) i.next();
      System.out.println(doc.get("Seller Name"));
    }
  }
}
