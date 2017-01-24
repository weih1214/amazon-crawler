package sg.edu.smu.webmining.crawler.seedpagefetcher;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import org.bson.Document;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hwei on 16/1/2017.
 */
public class DBSeedpageManager implements SeedpageManager {

  private final MongoClient mongoClient;
  private final MongoDatabase mongoDatabase;
  private final MongoCollection<Document> mongoCollection;
  private final FindIterable<Document> queryResult;
  private final String fieldName;


  public DBSeedpageManager(String location, Integer port, String databaseName, String collectionName, String fieldName) {
    mongoClient = new MongoClient(location, port);
    mongoDatabase = mongoClient.getDatabase(databaseName);
    mongoCollection = mongoDatabase.getCollection(collectionName);
    queryResult = mongoCollection.find().projection(Projections.include(fieldName));
    this.fieldName = fieldName;
  }

  @Override
  public String[] get() throws SQLException {
    final List<String> seedpageList = new ArrayList<>();
    for (Document doc: queryResult) {
      if (doc.get(fieldName) == null) {
        continue;
      }
      final String seedpage = doc.get(fieldName).toString();
      System.out.println(seedpage);
      seedpageList.add(seedpage);
    }
    String[] seedpage = new String[seedpageList.size()];
    seedpage = seedpageList.toArray(seedpage);
    mongoClient.close();
    return seedpage;
  }
}
