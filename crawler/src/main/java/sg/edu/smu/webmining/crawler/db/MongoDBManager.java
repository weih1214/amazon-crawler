package sg.edu.smu.webmining.crawler.db;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Map;

/**
 * Created by hwei on 13/12/2016.
 */
public class MongoDBManager implements AutoCloseable {

  private static final String SOURCE_FIELD = "Source";
  private static final String SEED_PAGE_FIELD = "Seed Page";
  private static final String TOTAL_PRODUCTS_FIELD = "Total Products";
  // On linux, it is URL; On my desktop, it is Url
  public static final String URL_FIELD = "URL";
  public static final String PRODUCT_ID_FIELD = "Product ID";

  private final MongoClient client;
  private final MongoDatabase db;
  private final MongoCollection<Document> collection;

  public MongoDBManager(String location, int port, String databaseName, String collectionName) {
    client = new MongoClient(location, port);
    db = client.getDatabase(databaseName);
    collection = db.getCollection(collectionName);
  }

  public void addRecord(Map<String, Object> resultMap, String source) throws MongoException {
    collection.insertOne(new Document(resultMap)
        .append(SOURCE_FIELD, source)
    );
  }

  public void addUrlRecord(String productId, String url, String source) throws MongoException {
    collection.insertOne(new Document()
        .append(PRODUCT_ID_FIELD, productId)
        .append(URL_FIELD, url)
        .append(SOURCE_FIELD, source)
    );
  }

  public void addSeedRecord(String seedpage, Integer totalProducts) throws MongoException {
    collection.insertOne(new Document()
        .append(SEED_PAGE_FIELD, seedpage)
        .append(TOTAL_PRODUCTS_FIELD, totalProducts)
    );
  }

  @Override
  public void close() {
    client.close();
  }

}
