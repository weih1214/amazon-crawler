package sg.edu.smu.webmining.crawler.databasemanager;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Map;

/**
 * Created by hwei on 18/1/2017.
 */
public class MasterlistMongoDBManager implements AutoCloseable, MongoDBManager {

  private final MongoClient mongoClient;
  private final MongoDatabase mongoDatabase;
  private final MongoCollection<Document> mongoCollection;

  public MasterlistMongoDBManager(String location, Integer portNumber, String databaseName, String collectionName) {
    mongoClient = new MongoClient(location, portNumber);
    mongoDatabase = mongoClient.getDatabase(databaseName);
    mongoCollection = mongoDatabase.getCollection(collectionName);
  }

  @Override
  public void close() throws Exception {
    mongoClient.close();
  }

  @Override
  public void update(Map<String, Object> resultMap, String source) throws Exception {

  }

  public void update(String productId, String url, String source) {
    final Document doc = new Document();
    doc.append("Product Id", productId).append("Url", url).append("Source", source);
    mongoCollection.insertOne(doc);
  }

  public void update(String seedpage, Integer totalProducts) {
    final Document doc = new Document();
    doc.append("Seedpage", seedpage).append("Total Products", totalProducts);
    mongoCollection.insertOne(doc);
  }
}
