package sg.edu.smu.webmining.crawler.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Map;

/**
 * Created by hwei on 13/12/2016.
 */
public class GeneralMongoDBManager implements AutoCloseable, MongoDBManager {

  private final MongoClient mongoClient;
  private final MongoDatabase mongoDatabase;
  private final MongoCollection<Document> mongoCollection;

  public GeneralMongoDBManager(String location, Integer portNumber, String databaseName, String collectionName) {
    mongoClient = new MongoClient(location, portNumber);
    mongoDatabase = mongoClient.getDatabase(databaseName);
    mongoCollection = mongoDatabase.getCollection(collectionName);
  }

  @Override
  public void close() throws Exception {
    mongoClient.close();
  }

  @Override
  public void update(Map<String, Object> resultMap) throws Exception {
    final Document doc = new Document(resultMap);
    mongoCollection.insertOne(doc);
  }

}
